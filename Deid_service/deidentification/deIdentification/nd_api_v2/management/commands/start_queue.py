from django.core.management.base import BaseCommand
from nd_api_v2.models.incremental_queue import IncrementalQueue, QueueStatus
import time
from nd_api_v2.services.process_queue import process_incremental_queue


class Command(BaseCommand):
    help = 'Start queue management operation'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Queue started successfully.'))
        while True:
            incremental_queue = IncrementalQueue.objects.filter().exclude(queue_status__in=[QueueStatus.COMPLETED, QueueStatus.MANUALY_SKIPPED]).order_by('-created_at').last()
            # incremental_queue = IncrementalQueue.objects.filter().last()
            # process_incremental_queue(incremental_queue)
            # break
            if incremental_queue is None:
                self.stdout.write(self.style.ERROR('No incremental queue found to start.'))
                time.sleep(5)
                continue
            elif incremental_queue.queue_status == QueueStatus.NOT_STARTED:
                process_incremental_queue(incremental_queue)
                incremental_queue.queue_status = QueueStatus.IN_PROGRESS
                incremental_queue.save()
            elif incremental_queue.queue_status in [QueueStatus.IN_PROGRESS]:
                self.stdout.write(self.style.ERROR('Incremental queue is already in progress.'))
                time.sleep(5)
                continue
            elif incremental_queue.queue_status in [QueueStatus.INTERUPTED]:
                self.stdout.write(self.style.ERROR('Incremental queue is interrupted. please fix this or marked it as manually skipped.'))
                time.sleep(5)
                continue
            elif incremental_queue.queue_status == QueueStatus.FAILED:
                self.stdout.write(self.style.ERROR('Incremental queue is failed. please fix this or marked it as manually skipped.'))
                time.sleep(5)
                continue
            else:
                self.stdout.write(self.style.ERROR('Incremental queue is in an unknown state. please fix this or marked it as manually skipped.'))
                time.sleep(5)
                continue
