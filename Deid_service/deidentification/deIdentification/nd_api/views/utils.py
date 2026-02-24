import hashlib
import tempfile
import subprocess
import traceback
from urllib.parse import urlparse
from django.conf import settings
from google.cloud import storage

from nd_api.models import Table, Status, TableGCPStatus, ClientDataDump
from nd_api.schemas.table_config import TableDetailsForUI, ColumnDetailsForUI
from core.process_df.utils import TABLE_UNIQUE_COLUMN_NAME
from deIdentification.nd_logger import nd_logger
from qc_package.scanner import DbScanner


def calculate_checksum_for_file(file_path, algorithm="md5"):
    hash_func = hashlib.new(algorithm)
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        return str(e)


def parse_mysql_connection_string(conn_str):
    url = urlparse(conn_str)
    return {
        "host": url.hostname,
        "user": url.username,
        "password": url.password,
        "database": url.path.lstrip("/"),
        "port": url.port or 3306,
    }


def take_dump_and_upload_to_cloud(table_id: int, reupload=False):
    try:
        table_obj = Table.objects.get(id=table_id)
        if table_obj.gcp.cloud_uploaded and (not reupload):
            nd_logger.info(
                f"Table {table_obj.table_name} is already uploaded on the cloud, not uploading again"
            )
        conn_str = table_obj.dump.get_destination_db_connection_str()
        creds = parse_mysql_connection_string(conn_str)
        blob_name = f"{settings.CLIENT_NAME}/{table_obj.dump.dump_name}/{table_obj.table_name}.sql"
        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as temp_file:
            dump_path = temp_file.name

            cmd = [
                "mysqldump",
                "-h",
                creds["host"],
                "-u",
                creds["user"],
                f"--password={creds['password']}",
                creds["database"],
                table_obj.table_name,
                "--single-transaction",
                "--quick",
                "--no-create-db",
            ]

            with open(dump_path, "w") as f:
                subprocess.run(cmd, stdout=f, check=True)
            md5sum = calculate_checksum_for_file(dump_path)
            upload_to_gcs(dump_path, blob_name)
            gcp_obj: TableGCPStatus = table_obj.gcp
            gcp_obj.md5sum = md5sum
            gcp_obj.cloud_uploaded = Status.COMPLETED
            gcp_obj.save()
            
            # Send GCP upload completion status
            from ndwebsocket.utils import broadcast_table_status_update
            broadcast_table_status_update(
                table_id=table_id,
                table_name=table_obj.table_name,
                process_type='gcp',
                status='completed',
                message=f"GCP upload completed for table {table_obj.table_name}",
                save_to_db=False
            )
    except Exception as e:
        gcp_obj = table_obj.gcp
        gcp_obj.cloud_uploaded = TableGCPStatus.FAILED
        gcp_obj.failure_remarks = {"error": f"{e}", "traceback": traceback.format_exc()}
        gcp_obj.save()
        
        # Send GCP upload failure status
        from ndwebsocket.utils import broadcast_table_status_update
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.table_name,
            process_type='gcp',
            status='failed',
            message=f"GCP upload failed for table {table_obj.table_name}",
            error_details={"error": str(e), "traceback": traceback.format_exc()},
            save_to_db=False
        )


def upload_to_gcs(file_path, blob_name):
    client = storage.Client()
    bucket = client.bucket(settings.CLOUD_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    nd_logger.info(
        f"Uploaded {file_path} to gs://{settings.CLOUD_BUCKET_NAME}/{blob_name}"
    )


def run_auto_qc(table_id: int, read_limit: int = 10000):
    try:
        db_scanner = DbScanner()
        result = db_scanner.scan_table(table_id, read_limit)
    except Table.DoesNotExist:
        nd_logger.error(f"QC Task failed: Table ID {table_id} not found.")
        raise Exception(f"QC Task failed: Table ID {table_id} not found.")
    except Exception as e:
        nd_logger.error(f"QC Task error: {e}, table_id: {table_id}")
        nd_logger.error(traceback.format_exc())
        raise Exception(f"QC Task error: {e}, table_id: {table_id}")


def register_table_and_generate_analytics(
    table, dump_obj: ClientDataDump, rerun: bool = False
):
    source_db_connection = dump_obj.get_source_db_connection()
    table_obj, created = Table.register_table(table, dump_obj)
    if not created:
        table_stats = {"rows_count": table_obj.rows_count}
        if not rerun:
            return table, table_stats, table_obj.rows_count
    table_metadata = table_obj.metadata
    table_columns = list(
        set(source_db_connection.get_column_names(table) + [TABLE_UNIQUE_COLUMN_NAME])
    )
    table_metadata.columns = {"columms": table_columns}
    table_metadata.primary_key = {"primary_key": []}
    table_metadata.save()
    table_obj.table_details_for_ui = _get_default_table_details_for_ui(table_columns)
    table_obj.rows_count = source_db_connection.get_rows_count(table)
    table_obj.save()

    table_stats = {"rows_count": table_obj.rows_count}
    source_db_connection.close()
    return table, table_stats, table_obj.rows_count


def _get_default_table_details_for_ui(columns_names: list[str]) -> TableDetailsForUI:
    """
    "ignore_rows": {
        "operator": "or",
        "columns": [{"name": "UserType", "value": 3, "condition": "neq"}]
    }
    """
    columns_details = []
    for column_name in columns_names:
        columns_details.append(
            ColumnDetailsForUI(
                column_name=column_name,
                is_phi=False,
                mask_value=None,
                de_identification_rule=None,
                add_to_phi_table=False,
                column_name_for_phi_table=None,
                ignore_rows={},
                reference_mapping={},
            )
        )
    table_details_for_ui = TableDetailsForUI(
        columns_details=columns_details,
        ignore_rows={},
        batch_size=1000,
        patient_identifier_column=None,
        patient_identifier_type=None,
        reference_mapping={},
    )
    return table_details_for_ui
