# import subprocess
# import pymysql
# import glob
# import os
# from django.conf import settings
# # from nd_api.models import DataDump, RestoreDump
# from deIdentification.nd_logger import nd_logger
# from worker.models import Task, Chain

# def generate_mysqldump_command(db_name, table_name, config, destination_path):
#     os.makedirs(destination_path, exist_ok=True)
#     dump_file = os.path.join(destination_path, f"{db_name}_{table_name}.sql")
#     cmd = (
#         f"mysqldump --default-character-set=utf8 -h {config['host']} -P {config['port']} "
#         f"-u {config['user']} -p{config['password']} {db_name} {table_name} > {dump_file}"
#     )
#     return cmd

# def create_dump(db, table, destination_path, config):
#     dump_command = generate_mysqldump_command(db, table, config, destination_path)
#     nd_logger.info(f"Executing: {dump_command}")
#     subprocess.run(dump_command, shell=True, check=True)

# def generate_mysqlrestore_command(db_name, config, source_path):
#     cmd = (
#         f"mysql -h {config['host']} -P {config['port']} "
#         f"-u {config['user']} -p'{config['password']}' {db_name} < {source_path}"
#     )
#     return cmd

# def restore_dump(db, source_path, config):
#     restore_command = generate_mysqlrestore_command(db, config, source_path)
#     nd_logger.info(f"Executing: {restore_command}")
#     subprocess.run(restore_command, shell=True, check=True)

# def dump_database(datadump_obj: DataDump):
#     """ Generate and execute mysqldump for each table """
#     destination_path = os.path.join(settings.DATA_DUMP_DESTINATION_PATH, datadump_obj.dump_name)
#     config = datadump_obj.config
    
#     all_tasks = []
#     for db in config["db_names"]:
#         conn = pymysql.connect(host=config["host"], user=config["user"], password=config["password"], database=db)
#         cursor = conn.cursor()
#         cursor.execute("SHOW TABLES")
#         all_tables = [row[0] for row in cursor.fetchall()]
#         cursor.close()
#         conn.close()
        
#         chain, created = Chain.all_objects.get_or_create(
#             reference_uuid=f"dump_creation_{datadump_obj.id}"
#         )
#         if not created:
#             chain.revive_and_save()
        
#         for table in all_tables:
#             task = Task.create_task(
#                 fn=create_dump,
#                 chain=chain,
#                 arguments={
#                     "db": db,
#                     "table": table,
#                     "destination_path": destination_path,
#                     "config": config,
#                 },
#                 hooks={"failure": dump_creation_failure_hook},
#             )
#             all_tasks.append(task)
#     return all_tasks

# def restore_database(restore_dump_obj: RestoreDump):
#     """ Restore the generated dump to a destination database """
#     datadump_obj: DataDump = restore_dump_obj.dump
#     config = restore_dump_obj.config

#     source_path = os.path.join(settings.DATA_DUMP_DESTINATION_PATH, datadump_obj.dump_name)
#     dump_file_paths = glob.glob(os.path.join(source_path, "*.sql"))

#     all_dump_task = []
#     chain, created = Chain.all_objects.get_or_create(
#         reference_uuid=f"restore_creation_{datadump_obj.id}"
#     )
#     if not created:
#         chain.revive_and_save()
    
#     for dump_file in dump_file_paths:
#         task = Task.create_task(
#             fn=restore_dump,
#             chain=chain,
#             arguments={
#                 "db": config['database'],
#                 "source_path": dump_file,
#                 "config": config,
#             },
#             hooks={"failure": dump_creation_failure_hook},
#         )
#         all_dump_task.append(task)
#     return all_dump_task

# def dump_creation_failure_hook(chain_obj: Chain):
#     pass
