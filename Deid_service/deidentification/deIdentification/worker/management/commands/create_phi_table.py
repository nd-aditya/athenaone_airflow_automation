import os

import logging
from django.core.management.base import BaseCommand
from worker.worker import TaskWorker

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """provides options to add args and start the taskworker"""

    help = "Create PHI Table"
    task_worker = TaskWorker()

    def handle(self, *args, **options):
        """calls teh work function of the Taskworker class"""
        self.task_worker.work()
