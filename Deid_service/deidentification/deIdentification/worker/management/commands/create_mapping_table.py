import os

import logging
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """provides options to add args and start the taskworker"""

    help = "Create Mapping Table"

    def handle(self, *args, **options):
        """calls teh work function of the Taskworker class"""
        pass
