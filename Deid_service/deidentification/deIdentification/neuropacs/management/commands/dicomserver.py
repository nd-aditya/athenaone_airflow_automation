import logging

from django.core.management.base import BaseCommand
from neuropacs.dicomserver.server import DicomServer


logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            '-aet',
            '--ae-title',
            type=str,
            help="AE Title for Dicom Server",
            default="NEUROAI"
        )

        parser.add_argument(
            '-p',
            '--port',
            type=int,
            help='TCP/IP port, where  dicom server will  listen.....',
            default=5252
        )

        parser.add_argument(
            '-bf',
            '--buffer',
            type=int,
            help='Size of the buffer for association requests',
            default=1000
        )

        parser.add_argument(
            '-t',
            '--threads',
            type=int,
            help='Number of threads that process the association requests',
            default=4
        )

        parser.add_argument(
            '-ma',
            '--max-associations',
            type=int,
            help='Maximum number of associations that can happen at any given time',
            default=10
        )

        parser.add_argument(
            '-esp',
            '--dicom-error-store-path',
            type=str,
            help='Path to the directory where dicom files that could not be processed by dicom server will be stored.',
            default="/errors/dicoms"
        )

    def handle(self, *args, **options):

        DicomServer(options).run()
