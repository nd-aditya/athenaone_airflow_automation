import traceback
from typing import TypedDict
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Clients, ClientDataDump, Status, Table
from worker.models import Task, Chain
from django.db import transaction
from deIdentification.settings import CREATE_SAVEPOINT_IN_TRANSACTION
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from nd_api.decorator import conditional_authentication
from django.conf import settings
from core.dbPkg.mapping_table.create_table import MappingTable
from core.dbPkg.mapping_table.appointment import AppointmentMappingConfig, AppointmentMappingTable
from core.dbPkg.phi_table.create_table import PIITable
from sqlalchemy import create_engine, text
import pandas as pd
import hashlib
from deIdentification.nd_logger import nd_logger
from nd_api.views.utils import register_table_and_generate_analytics
from portal.alerts import alert_sender
from ndwebsocket.utils import (
    broadcast_task_status, 
    broadcast_task_progress, 
    broadcast_task_error,
    save_notification_to_db
)
from ndwebsocket.models import NotificationType, NotificationPriority


class RequestCtx(TypedDict):
    dump_name: str
    source_db_config: dict
    run_config: dict
    pii_config: dict
    qc_config: dict


@conditional_authentication
class ClientDumpView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, client_id: int):
        try:
            data: RequestCtx = request.data
            client_obj = Clients.objects.get(id=client_id)
            dump_name = data['dump_name']
            dump_date = data['dump_date']
            dump_obj, created = ClientDataDump.objects.get_or_create(dump_name=dump_name, client=client_obj)
            if not created:
                return Response(
                    {
                        "message": f"dump name: {dump_name}, already exists in the database, please choose different name"
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            source_db_config = data['source_db_config']
            
            dump_obj.dump_date = dump_date
            dump_obj.source_db_config = source_db_config
            dump_obj.save()
            chain, created = Chain.all_objects.get_or_create(
                reference_uuid=f"stats_generation_{dump_obj.id}"
            )
            task = Task.create_task(
                chain=chain,
                fn=run_stats_generation_task,
                arguments={"dump_id": dump_obj.id, "rerun": True},
                dependencies=[],
            )
            nd_logger.info(f"dump registered successfully by user {request.user}")
            return Response(
                {"message": "dump registered successfully", "dump_id": dump_obj.id, "dump_name": dump_obj.dump_name}, status=status.HTTP_200_OK
            )
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request, client_id: int):
        try:
            client = Clients.objects.get(id=client_id)
            client_dumps = ClientDataDump.objects.filter(client=client)
            client_dumps_list = []
            for client_dump in client_dumps:
                client_dumps_list.append({
                    'dump_id': client_dump.id,
                    "dump_name": client_dump.dump_name,
                    'dump_date': client_dump.dump_date,
                    "is_dump_processing_done": client_dump.is_dump_processing_done,
                    "is_primary_key_uploaded": client_dump.is_primary_key_uploaded
                })
            nd_logger.info(f"get all db call for client: {client_id}, completed successfully")
            return Response(client_dumps_list, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

@conditional_authentication
class DumpDetailsView(APIView):
    authentication_classes = [IsAuthenticated]
    
    def get(self, request, client_id: int, dump_id: int):
        try:
            client_dump = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
        except ClientDataDump.DoesNotExist:
            return Response(
                {
                    "message": f"dump with dump-id: {dump_id}, client-id: {client_id} not exists",
                    "success": False,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            response_json = {
                "dump_id": dump_id,
                "dump_name": client_dump.dump_name,
                "source_db_config": client_dump.source_db_config,
                "is_dump_processing_done": client_dump.is_dump_processing_done,
                "is_primary_key_uploaded": client_dump.is_primary_key_uploaded,
                "stats_generated_status": client_dump.stats_generated_status,
                "tables_deid_status": {
                    "not_started": {
                        "count": client_dump.tables.filter(
                            deid__deid_status=Status.NOT_STARTED
                        ).count(),
                    },
                    "in_progress": {
                        "count": client_dump.tables.filter(
                            deid__deid_status=Status.IN_PROGRESS
                        ).count(),
                    },
                    "completed": {
                        "count": client_dump.tables.filter(
                            deid__deid_status=Status.COMPLETED
                        ).count(),
                    },
                    "failed": {
                        "count": client_dump.tables.filter(
                            deid__deid_status=Status.FAILED
                        ).count(),
                    },
                }
            }
            return Response(response_json, status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error, {e} for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class StartProcessingForDumpView(APIView):
    authentication_classes = [IsAuthenticated]

    
    def get(self, request, client_id: int, dump_id: int):
        try:
            client_dump = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
        except ClientDataDump.DoesNotExist:
            return Response(
                {
                    "message": f"dump with dump-id: {dump_id} not exists",
                    "success": False,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            create_task_for_processing_dump(client_dump)
            return Response(f'created processing taks for dump: { client_dump}', status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error, {e} for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        

def create_task_for_processing_dump(dump_obj: ClientDataDump):
    # Broadcast dump processing start
    broadcast_task_status(
        status="started",
        task_name=f"Dump Processing: {dump_obj.dump_name}",
        message=f"Starting dump processing for {dump_obj.dump_name}",
        priority=NotificationPriority.HIGH,
        notification_type=NotificationType.TASK_STATUS,
        data={"dump_id": dump_obj.id, "client_id": dump_obj.client.id}
    )
    
    with transaction.atomic(savepoint=CREATE_SAVEPOINT_IN_TRANSACTION):
        chain, created = Chain.all_objects.get_or_create(
            reference_uuid=f"dump_processing_{dump_obj.id}"
        )
        if not created:
            chain.revive_and_save()
        tasks = []
        auto_incr_task = Task.create_task(
            chain=chain,
            fn=add_nd_auto_increment_id_column,
            arguments={"dump_id": dump_obj.id},
            dependencies=[],
        )
        tasks.append(auto_incr_task)
        patient_mapping_task = Task.create_task(
            chain=chain,
            fn=run_patient_mapping_generation_task,
            arguments={"dump_id": dump_obj.id},
            dependencies=[],
        )
        tasks.append(patient_mapping_task)
        enc_mapping_task = Task.create_task(
            chain=chain,
            fn=run_encounter_mapping_generation_task,
            arguments={"dump_id": dump_obj.id},
            dependencies=[patient_mapping_task],
        )
        tasks.append(enc_mapping_task)
        appt_mapping_task = Task.create_task(
            chain=chain,
            fn=run_appointment_mapping_generation_task,
            arguments={"dump_id": dump_obj.id},
            dependencies=[patient_mapping_task],
        )
        tasks.append(appt_mapping_task)
        master_table_task = Task.create_task(
            chain=chain,
            fn=run_master_table_generation_task,
            arguments={"dump_id": dump_obj.id},
            dependencies=[patient_mapping_task],
        )
        tasks.append(master_table_task)

        master_table_task = Task.create_task(
            chain=chain,
            fn=mark_dump_processing_completed,
            arguments={"dump_id": dump_obj.id},
            dependencies=tasks,
        )
        return {"message": f"task created succeffully"}

def mark_dump_processing_completed(dump_id: int, dependencies: list[Task] = []):
    dump_obj = ClientDataDump.objects.get(id=dump_id)
    dump_obj.is_dump_processing_done = True
    dump_obj.save()
    
    # Broadcast successful completion
    broadcast_task_status(
        status="completed",
        task_name=f"Dump Processing: {dump_obj.dump_name}",
        message=f"Dump processing completed successfully for {dump_obj.dump_name}",
        priority=NotificationPriority.HIGH,
        notification_type=NotificationType.SUCCESS,
        data={"dump_id": dump_id, "client_id": dump_obj.client.id}
    )
    
    return {"message": "successfully processing completed"}


def run_stats_generation_task(
    dump_id: int, rerun: bool = False, dependencies: list[Task] = []
):
    try:
        nd_logger.info(f"inside the run stats generation task for dump-id:  {dump_id}")
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        
        # Broadcast stats generation start
        broadcast_task_status(
            status="started",
            task_name=f"Stats Generation: {dump_obj.dump_name}",
            message=f"Starting stats generation for dump {dump_obj.dump_name}",
            priority=NotificationPriority.HIGH,
            notification_type=NotificationType.TASK_STATUS,
            data={"dump_id": dump_id, "client_id": dump_obj.client.id}
        )
        
        source_db_connection = dump_obj.get_source_db_connection()
        all_tables = source_db_connection.get_all_tables()
        dump_stats = {"tables_stats": {}}

        # Adjust max_workers based on your system capabilities
        with ThreadPoolExecutor(
            max_workers=settings.STATS_GENERATION_MAX_WORKER_COUNT
        ) as executor:
            future_to_table = {
                executor.submit(register_table_and_generate_analytics, table, dump_obj, rerun): table
                for table in all_tables
            }

            for future in tqdm(
                as_completed(future_to_table),
                total=len(all_tables),
                desc="Generating table stats",
            ):
                table, table_stats, rows_count = future.result()
                dump_stats["tables_stats"][table] = table_stats

        dump_obj.dump_stats = dump_stats
        dump_obj.save()
        dump_obj.marked_stats_generation_as_completed()
        
        # Broadcast successful completion
        broadcast_task_status(
            status="completed",
            task_name=f"Stats Generation: {dump_obj.dump_name}",
            message=f"Stats generation completed successfully for {len(all_tables)} tables",
            priority=NotificationPriority.HIGH,
            notification_type=NotificationType.SUCCESS,
            data={
                "dump_id": dump_id,
                "client_id": dump_obj.client.id,
                "tables_processed": len(all_tables),
                "rerun": rerun
            }
        )
        
        return {}
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Stats Generation: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            error=f"Stats generation failed for dump ID {dump_id}: {str(e)}",
            error_code="STATS_GENERATION_ERROR",
            details={
                "dump_id": dump_id,
                "client_id": dump_obj.client.id if 'dump_obj' in locals() else None,
                "rerun": rerun
            }
        )
        
        prepare_message = {
            "alert_type": "Stats Generation task Failed",
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            "client_identifier": f"{dump_obj.client.id if 'dump_obj' in locals() else 'Unknown'} - {dump_obj.client.client_name if 'dump_obj' in locals() else 'Unknown'}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise e


def run_patient_mapping_generation_task(dump_id: int, dependencies: list[Task] = []):
    try:
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        all_client_dumps = dump_obj.client.dumps.all().count()
        mapping_config = dump_obj.get_mapping_db_config()
        if mapping_config.get("auto_generate_table", True):
            source_connection_str = dump_obj.source_db_config["connection_str"]
            mptable = MappingTable(source_connection_str, mapping_config, dump_obj.client.id, dump_obj.id, dump_obj.client.emr_type)

            if all_client_dumps < 2:
                mptable.generate_patient_mapping_table()
            else:
                mptable.update_patient_mapping_table()
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Patient Mapping: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            error=f"Patient mapping generation failed for dump ID {dump_id}: {str(e)}",
            error_code="PATIENT_MAPPING_ERROR",
            details={"dump_id": dump_id, "client_id": dump_obj.client.id if 'dump_obj' in locals() else None}
        )
        
        prepare_message = {
            "alert_type": "Patient Mapping Generation task Failed",
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            "client_identifier": f"{dump_obj.client.id if 'dump_obj' in locals() else 'Unknown'} - {dump_obj.client.client_name if 'dump_obj' in locals() else 'Unknown'}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise Exception(f"Raised issue because of {e}: trackeback:")

def run_encounter_mapping_generation_task(dump_id: int, dependencies: list[Task] = []):
    try:
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        mapping_config = dump_obj.get_mapping_db_config()
        if mapping_config.get("auto_generate_table", True):
            source_connection_str = dump_obj.source_db_config["connection_str"]
            mptable = MappingTable(source_connection_str, mapping_config, dump_obj.client.id, dump_obj.id, dump_obj.client.emr_type)
            mptable.generate_encounter_mapping_table()
    except Exception as e:
        # Broadcast error notification
        nd_logger.info(traceback.format_exc())
        broadcast_task_error(
            task_name=f"Encounter Mapping: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            error=f"Encounter mapping generation failed for dump ID {dump_id}: {str(e)}",
            error_code="ENCOUNTER_MAPPING_ERROR",
            details={"dump_id": dump_id, "client_id": dump_obj.client.id if 'dump_obj' in locals() else None}
        )
        
        prepare_message = {
            "alert_type": "Ecnounter Mapping Generation task Failed",
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            "client_identifier": f"{dump_obj.client.id if 'dump_obj' in locals() else 'Unknown'} - {dump_obj.client.client_name if 'dump_obj' in locals() else 'Unknown'}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)

def run_appointment_mapping_generation_task(dump_id: int, dependencies: list[Task] = []):
    try:
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        appt_mapping_config = dump_obj.get_appointment_mapping_config()
        if appt_mapping_config.get("auto_generate_table", True) and appt_mapping_config.get("appointment_mapping_present", False):
            source_connection_str = dump_obj.source_db_config["connection_str"]
            mptable = AppointmentMappingTable(source_connection_str, appt_mapping_config, dump_obj.client.id, dump_obj.id)
            mptable.generate_appointment_mapping_table()
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Appointment Mapping: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            error=f"Appointment mapping generation failed for dump ID {dump_id}: {str(e)}",
            error_code="APPOINTMENT_MAPPING_ERROR",
            details={"dump_id": dump_id, "client_id": dump_obj.client.id if 'dump_obj' in locals() else None}
        )
        
        prepare_message = {
            "alert_type": "Appointment Mapping Generation task Failed",
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            "client_identifier": f"{dump_obj.client.id if 'dump_obj' in locals() else 'Unknown'} - {dump_obj.client.client_name if 'dump_obj' in locals() else 'Unknown'}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)


def run_master_table_generation_task(dump_id: int, dependencies: list[Task] = []):
    try:
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        
        src_db_url = dump_obj.source_db_config["connection_str"]
        master_db_url = dump_obj.get_pii_schema_connection_str()
        mapping_db_url = dump_obj.get_mapping_db_connection_str()
        pii_table_config = dump_obj.client.master_db_config.get("pii_tables", {})
        pii = PIITable(src_db_url, master_db_url, mapping_db_url, pii_table_config, dump_obj.client.id, dump_obj.id)
        pii.generate_or_update_pii_table()

    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Master Table Generation: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            error=f"Master table generation failed for dump ID {dump_id}: {str(e)}",
            error_code="MASTER_TABLE_ERROR",
            details={"dump_id": dump_id, "client_id": dump_obj.client.id if 'dump_obj' in locals() else None}
        )
        
        prepare_message = {
            "alert_type": "Master Table Generation task Failed",
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            "client_identifier": f"{dump_obj.client.id if 'dump_obj' in locals() else 'Unknown'} - {dump_obj.client.client_name if 'dump_obj' in locals() else 'Unknown'}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise e


def run_dbsetup_post_client_dump_creation(dump_id: int, dependencies: list[Task] = []):
    dump_obj = ClientDataDump.objects.get(id=dump_id)
    admin_conn = dump_obj.get_admin_db_connection()

    dest_dbname = dump_obj.get_destination_dbname()
    admin_conn.create_database(dest_dbname)


def hash_to_bigint(*cols) -> int:
    """
    Generate a deterministic positive BIGINT from a combination of columns.
    """
    concat = "|".join(str(c) for c in cols)
    h = hashlib.sha256(concat.encode()).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=False)  # unsigned 64-bit



def add_nd_hash_id_column_to_a_table(
    connection_string: str, table_name: str, unique_key_column_raw
):
    try:
        engine = create_engine(connection_string, pool_size=10, max_overflow=20, pool_timeout=60)

        # Prepare unique key columns
        if isinstance(unique_key_column_raw, list):
            unique_key_columns = unique_key_column_raw
        else:
            unique_key_columns = [col.strip() for col in unique_key_column_raw.split("|") if col.strip()]

        if not unique_key_columns:
            nd_logger.error(f"[ERROR] {table_name} - No unique_key_columns provided.")
            return

        db_type = engine.dialect.name.lower()
        uq_name = f"uq_{table_name}_nd_id"

        with engine.begin() as conn:
            # ---- MYSQL ----
            if db_type == "mysql":
                # Check column exists
                col_check = conn.execute(
                    text("""
                        SELECT COUNT(*) 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = :table 
                        AND COLUMN_NAME = 'nd_auto_increment_id'
                    """),
                    {"table": table_name}
                ).scalar()

                if col_check == 0:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN nd_auto_increment_id BIGINT"))

                concat_expr = "CONCAT_WS('|', " + ", ".join([f"COALESCE({col}, '')" for col in unique_key_columns]) + ")"
                update_sql = f"""
                    UPDATE {table_name}
                    SET nd_auto_increment_id = CONV(SUBSTRING(SHA2({concat_expr}, 256), 1, 16), 16, 10)  % 9223372036854775807
                """
                conn.execute(text(update_sql))

            # ---- MSSQL ----
            elif db_type == "mssql":
                # Check column exists
                col_check = conn.execute(
                    text("""
                        SELECT COUNT(*) 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_CATALOG = DB_NAME() 
                        AND TABLE_NAME = :table 
                        AND COLUMN_NAME = 'nd_auto_increment_id'
                    """),
                    {"table": table_name}
                ).scalar()

                if col_check == 0:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD nd_auto_increment_id BIGINT"))

                # Fetch all composite key columns
                select_cols = ", ".join(unique_key_columns)
                df = pd.read_sql(text(f"SELECT {select_cols} FROM {table_name}"), conn)
                casted_cols = [f"CAST({col} AS NVARCHAR(255))" for col in unique_key_columns]
                concat_expr = " + '|' + ".join(casted_cols)

                # Compute hash BIGINT for each row
                update_sql = f"""
                    UPDATE {table_name}
                    SET nd_auto_increment_id =
                        ABS(CAST(CAST(HASHBYTES('SHA1', {concat_expr}) AS BINARY(8)) AS BIGINT))
                """
                conn.execute(text(update_sql))

                # exists = conn.execute(text(f"""
                #     SELECT COUNT(*) FROM sys.objects 
                #     WHERE type = 'UQ' AND name = :uq_name
                # """), {"uq_name": uq_name}).scalar()
                # if exists == 0:
                #     conn.execute(text(
                #         f"ALTER TABLE {table_name} ADD CONSTRAINT {uq_name} UNIQUE (nd_auto_increment_id)"
                #     ))
            else:
                nd_logger.error(f"[ERROR] {table_name} - Unsupported DB type: {db_type}")
                return

        nd_logger.info(f"[DONE] {table_name} - nd_auto_increment_id assigned using hashing.")

    except Exception as e:
        nd_logger.error(f"[ERROR] {table_name} - {str(e)}")

def add_nd_auto_increment_id_column_to_a_table(connection_string: str, table_name: str, unique_key_column_raw: list[str] = []):
    engine = create_engine(connection_string, pool_pre_ping=True)
    with engine.begin() as conn:
        try:
            print(f"\n[{table_name}] Starting auto increment addition...")

            # 1. Drop column if exists
            col_exists = conn.execute(text(f"""
                SELECT COUNT(*) 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                  AND TABLE_NAME = '{table_name}' 
                  AND COLUMN_NAME = 'nd_auto_increment_id';
            """)).scalar()
            if col_exists:
                print(f"[{table_name}] Dropping existing nd_auto_increment_id column...")
                conn.execute(text(f"ALTER TABLE {table_name} DROP COLUMN nd_auto_increment_id;"))

            # 2. Add fresh column
            print(f"[{table_name}] Adding nd_auto_increment_id column...")
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN nd_auto_increment_id BIGINT;"))

            # 3. Check for primary key
            pk_col = conn.execute(text(f"""
                SELECT COLUMN_NAME 
                FROM information_schema.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = DATABASE() 
                  AND TABLE_NAME = '{table_name}' 
                  AND CONSTRAINT_NAME = 'PRIMARY' 
                LIMIT 1;
            """)).scalar()

            # 4. If no PK, create temporary auto-increment PK
            tmp_pk_created = False
            if not pk_col:
                print(f"[{table_name}] No PK found. Creating temporary auto-increment PK 'tmp_id'...")
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN tmp_id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST;"))
                pk_col = "tmp_id"
                tmp_pk_created = True

            # 5. Assign sequential IDs using the (real or temporary) PK
            print(f"[{table_name}] Assigning sequential IDs to nd_auto_increment_id...")
            conn.execute(text(f"SET @rownum := 0;"))
            conn.execute(text(f"""
                UPDATE {table_name} t
                JOIN (
                    SELECT {pk_col}, (@rownum := @rownum + 1) AS rn
                    FROM {table_name}
                    ORDER BY {pk_col}
                ) ranked
                ON t.{pk_col} = ranked.{pk_col}
                SET t.nd_auto_increment_id = ranked.rn;
            """))
            print(f"[{table_name}] IDs assigned successfully.")

            # 6. Drop temporary PK if created
            if tmp_pk_created:
                print(f"[{table_name}] Dropping temporary PK 'tmp_id'...")
                conn.execute(text(f"ALTER TABLE {table_name} DROP PRIMARY KEY, DROP COLUMN tmp_id;"))

            # 7. Add UNIQUE index on nd_auto_increment_id
            print(f"[{table_name}] Adding UNIQUE index on nd_auto_increment_id...")
            conn.execute(text(f"ALTER TABLE {table_name} ADD UNIQUE INDEX (nd_auto_increment_id);"))

            print(f"[{table_name}] ✅ Completed.")
            return
        except Exception as e:
            print(f"[{table_name}] ❌ Failed: {e}")
            return f"❌ {table_name} failed: {e}"

def add_nd_auto_increment_id_column(dump_id: int, table_names : list[str] = [], dependencies: list[Task] = []):
    try:
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        results = []
        THREAD_COUNT = 10
        if len(table_names)> 0:
            tables = list(dump_obj.tables.filter(table_name__in=table_names))
        else:
            tables = list(dump_obj.tables.all())

        # tables = Table.objects.filter(table_name="progressnotes")

        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            futures = [
                executor.submit(
                    add_nd_auto_increment_id_column_to_a_table,
                    dump_obj.source_db_config['connection_str'],
                    table_obj.table_name,
                    table_obj.metadata.primary_key['primary_key'],
                )
                for table_obj in tables
            ]
            
            completed_tables = 0
            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Hashing with SQL"
            ):
                result = future.result()
                nd_logger.info(result)
                results.append(result)
        return results
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Auto Increment ID: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            error=f"Auto increment ID generation failed for dump ID {dump_id}: {str(e)}",
            error_code="AUTO_INCREMENT_ERROR",
            details={"dump_id": dump_id, "client_id": dump_obj.client.id if 'dump_obj' in locals() else None}
        )
        
        prepare_message = {
            "alert_type": "Auto Increment ID Generation task Failed",
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            "client_identifier": f"{dump_obj.client.id if 'dump_obj' in locals() else 'Unknown'} - {dump_obj.client.client_name if 'dump_obj' in locals() else 'Unknown'}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise e
