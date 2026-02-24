from django.conf import settings
from core.dbPkg.dbhandler import NDDBHandler
import redis
from tqdm import tqdm
from core.dbPkg.pii_loader import PII_TABLE_NAME
from decimal import Decimal
from datetime import datetime, date
from core.dbPkg.utils import parse_patientid_column_string
import logging
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    TABLE_NAME = PII_TABLE_NAME
    help = f"Load Table {TABLE_NAME} values to Redis"

    def handle(self, *args, **options):
        connection_string = options['connection_str']
        nd_connection = NDDBHandler(connection_string)
        pid_columns = parse_patientid_column_string(settings.PATIENTID_COLUMNS)

        all_rows = nd_connection.fetch_all(self.TABLE_NAME)
        redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
        for row in tqdm(all_rows, desc=f"Loading value to redis for table: {self.TABLE_NAME}"):
            sanitized_row = {
                k: str(v) if isinstance(v, (Decimal, datetime, date)) else (v if v is not None else '')
                for k, v in row.items()
            }
            # sanitized_row = {k: (v if v is not None else '') for k, v in row.items()}
            for pid_column, col_type in pid_columns:
                pid = row[pid_column]
                redis_key = f'pii_data_{pid_column}:{pid}'
                redis_client.hset(redis_key, mapping=sanitized_row)
        

    def add_arguments(self, parser):
        parser.add_argument('connection_str', type=str, help='connection_str')
