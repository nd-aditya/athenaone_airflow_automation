import ast
import select
import sys, os
import time
from typing import Optional
import logging
from django import db
from django.db import transaction
from django.db.utils import OperationalError, InterfaceError
from django.utils import timezone
import datetime
import os


from worker.models import Task, Chain
from worker.utils import get_expiry, get_machine_id
from worker.worker_settings import (
    MACHINE_EXPIRY_OFFSET_TIMEOUT,
    MAX_TASK_PER_WORKER,
    WORKER_POLL_TIME,
    SOFT_DELETE_AGE,
    WORKER_ID,
)
from datetime import timedelta
from deIdentification.nd_logger import nd_logger


_SPLIT_TASK_PICK_AND_EXECUTION_LOCKS = ast.literal_eval(
    os.environ.get("WORKER_SEPARATE_PICK_EXECUTION_LOCKS", "False")
)


class TaskWorker:
    def __init__(
        self,
        max_tasks_to_process=MAX_TASK_PER_WORKER,
        worker_poll_time=WORKER_POLL_TIME,
        back_off_algo=None,
    ):
        self.worker_id = WORKER_ID
        self.num_tasks_processed = 0
        self.max_tasks_to_process = max_tasks_to_process
        self.worker_poll_time = worker_poll_time
        self.notify_channel = "taskchannel"
        self.back_off_algo = back_off_algo
        self.machine_id = get_machine_id()
        nd_logger.info(f"Initialized worker id:{self.worker_id} on {self.machine_id}!")

    def work(self):
        """close all connections inherited from parent process and  django will recreate new connection objects per worker process"""
        # close all connections inherited from parent process
        # django will recreate new connection objects per worker process
        try:
            db.connections.close_all()
        except Exception:
            nd_logger.exception("error while closing connections")

        self._listen()
        while True:
            try:
                self._wait(timeout=self.worker_poll_time)
                self.execute_ready_tasks()
            except (OperationalError, InterfaceError) as e:
                nd_logger.warning(f"Database connection error in work loop: {e}, closing connections and retrying")
                try:
                    db.connections.close_all()
                except Exception:
                    nd_logger.exception("error while closing connections after error")
                time.sleep(1)  # Brief pause before retrying
            except Exception as e:
                nd_logger.exception(f"Unexpected error in work loop: {e}")
                try:
                    db.connections.close_all()
                except Exception:
                    pass
                time.sleep(1)

    def _listen(self):
        try:
            with db.connection.cursor() as cur:
                cur.execute('LISTEN "{}";'.format(self.notify_channel))
        except OperationalError:
            # not psql likely
            pass

    def _wait(self, timeout):
        try:
            db.connection.connection.poll()
            notifies = db.connection.connection.notifies
            if notifies:
                while notifies:
                    notifies.pop(0)
                return notifies

            select.select([db.connection.connection], [], [], timeout)
            db.connection.connection.poll()
            notifies = db.connection.connection.notifies
            while notifies:
                notifies.pop(0)
            return notifies
        except AttributeError:
            # prob not psql
            # use simple timeout
            time.sleep(timeout)
            return

    def clean_soft_deleted(self):
        """Clean up soft-deleted tasks and chains in batches to avoid memory issues"""
        creation_time = datetime.datetime.now() - timedelta(seconds=SOFT_DELETE_AGE)
        batch_size = 1000  # Reduced batch size to prevent memory issues
        
        # Process tasks in batches without counting all records first
        last_id = 0
        while True:
            try:
                # Get a batch of task IDs to delete
                task_ids = list(
                    Task.all_objects.filter(
                        soft_delete=True,
                        created_at__lte=creation_time,
                        id__gt=last_id
                    )
                    .order_by("id")
                    .values_list("id", flat=True)[:batch_size]
                )
                
                if not task_ids:
                    break
                
                # Delete this batch
                num_deleted, details = Task.all_objects.filter(id__in=task_ids).delete()
                if num_deleted > 0:
                    nd_logger.info(
                        f"Soft deleted tasks actually deleted. "
                        f"Num deleted = {num_deleted}, details = {details}"
                    )
                
                last_id = max(task_ids)
                
                # Close connection to free memory
                db.connections.close_all()
                
            except (OperationalError, InterfaceError) as e:
                nd_logger.warning(f"Database error during task cleanup: {e}, closing connections")
                db.connections.close_all()
                break
            except Exception as e:
                nd_logger.exception(f"Error during task cleanup: {e}")
                break

        # Process chains in batches
        last_id = 0
        while True:
            try:
                # Get a batch of chain IDs to delete
                chain_ids = list(
                    Chain.all_objects.filter(
                        soft_delete=True,
                        created_at__lte=creation_time,
                        id__gt=last_id
                    )
                    .order_by("id")
                    .values_list("id", flat=True)[:batch_size]
                )
                
                if not chain_ids:
                    break
                
                # Delete this batch
                num_deleted, details = Chain.all_objects.filter(id__in=chain_ids).delete()
                if num_deleted > 0:
                    nd_logger.info(
                        f"Soft deleted graphs actually deleted. "
                        f"Num deleted = {num_deleted}, details = {details}"
                    )
                
                last_id = max(chain_ids)
                
                # Close connection to free memory
                db.connections.close_all()
                
            except (OperationalError, InterfaceError) as e:
                nd_logger.warning(f"Database error during chain cleanup: {e}, closing connections")
                db.connections.close_all()
                break
            except Exception as e:
                nd_logger.exception(f"Error during chain cleanup: {e}")
                break

    def _run_task(self, task: Task) -> None:
        # if task is querying the db, it means it is alive, and if expired or about to expire soon update the expiry
        # machine = Machine.objects.get(machine_id=self.machine_id)
        # if machine.expires_at - timezone.now() <= datetime.timedelta(seconds=MACHINE_EXPIRY_OFFSET_TIMEOUT):
        #     machine.expires_at = get_expiry()
        #     machine.save(update_fields=["expires_at"])

        self.num_tasks_processed += 1
        task.run(nd_logger, self.worker_id)

    def execute_ready_tasks(self):
        """Picks ready task from Task and runs the task and if no task found just returns back to caller"""
        nd_logger.debug("Polling for ready tasks")
        while True:
            try:
                if self.max_tasks_to_process > 0:
                    if self.num_tasks_processed >= self.max_tasks_to_process:
                        nd_logger.info(
                            f"shutting down after processing "
                            f"{self.num_tasks_processed} tasks"
                        )
                        if os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER") == "WORKER":
                            self.clean_soft_deleted()
                        sys.exit(1)
                if _SPLIT_TASK_PICK_AND_EXECUTION_LOCKS:
                    # This is thread safe as it runs in a top level transaction
                    task: Optional[Task] = Task.get_ready_task(
                        self.back_off_algo, self.machine_id
                    )
                    if task is None:
                        if os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER") == "POLL":
                            self.clean_soft_deleted()
                        return

                    with transaction.atomic():
                        task: Optional[Task] = (
                            Task.objects.select_for_update(skip_locked=True)
                            .filter(id=task.id)
                            .first()
                        )
                        if task:
                            self._run_task(task)
                        else:
                            if (
                                os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER")
                                == "POLL"
                            ):
                                self.clean_soft_deleted()
                            return
                else:
                    with transaction.atomic():
                        task = Task.get_ready_task(self.back_off_algo, self.machine_id)
                        if task:
                            self._run_task(task)
                        else:
                            if (
                                os.getenv("SOFT_DELETE_CLEANUP_SCHEDULE", "WORKER")
                                == "POLL"
                            ):
                                self.clean_soft_deleted()
                            return
            except (OperationalError, InterfaceError) as e:
                nd_logger.warning(f"Database connection error in execute_ready_tasks: {e}, closing connections")
                try:
                    db.connections.close_all()
                except Exception:
                    nd_logger.exception("error while closing connections after error")
                return  # Return to main loop which will retry
            except Exception as e:
                nd_logger.exception(f"Unexpected error in execute_ready_tasks: {e}")
                try:
                    db.connections.close_all()
                except Exception:
                    pass
                return  # Return to main loop which will retry
