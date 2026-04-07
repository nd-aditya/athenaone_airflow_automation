"""
GCP dump: dump MySQL tables (DEIDENTIFIED_SCHEMA) to local SQL files and upload to GCS.
Each run writes under gcp_dump/<MMDDYYYY>/ (e.g. 03182026): sql_dump_stats.csv + <schema>/*.sql.
Uploads to gs://bucket/<prefix>/<MMDDYYYY>/...
"""
import hashlib
import os
import shutil
import subprocess
from datetime import datetime
from urllib.parse import unquote

from sqlalchemy import create_engine, inspect

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    DEIDENTIFIED_SCHEMA,
    GCP_DUMP_OUTPUT_DIR,
    GCP_TRANSFER_CSV_PATH,
    GCP_BUCKET,
    GCP_DESTINATION_PREFIX,
    GCP_FULL_REFRESH_FLAG,
)


def _resolve_dump_schema(dump_schema: str | None = None) -> str:
    """
    Return the schema to dump from.
    GCP_FULL_REFRESH_FLAG=True  → DEIDENTIFIED_SCHEMA (full merged table)
    GCP_FULL_REFRESH_FLAG=False → dump_schema passed by caller (e.g. diff_*_deid from DAG2/DAG4)
    GCP_FULL_REFRESH_FLAG=None  → caller must skip before reaching here
    """
    if GCP_FULL_REFRESH_FLAG:
        return DEIDENTIFIED_SCHEMA
    if dump_schema:
        return dump_schema
    return DEIDENTIFIED_SCHEMA  # safe fallback when False but no schema passed


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _gcp_date_folder() -> str:
    """Folder name per run: MMDDYYYY e.g. 03182026."""
    return datetime.now().strftime("%m%d%Y")


def gcp_dump_date_root(date_folder: str, output_dir: str | None = None) -> str:
    """
    Absolute path to gcp_dump/<date_folder>/ only (not the whole gcp_dump root).
    Use as upload source_folder so only this run's date directory is uploaded.
    """
    root_base = output_dir or os.path.join(_project_root(), GCP_DUMP_OUTPUT_DIR)
    return os.path.abspath(os.path.join(root_base, date_folder))


def clear_dump_directory(
    schema: str = DEIDENTIFIED_SCHEMA,
    output_dir: str | None = None,
    clear_entire_root: bool = False,
) -> dict:
    """
    Remove and recreate today's date folder under gcp_dump (e.g. gcp_dump/03182026/).
    Does not delete other date folders. Returns date_folder and date_root.
    """
    root = output_dir or os.path.join(_project_root(), GCP_DUMP_OUTPUT_DIR)
    date_folder = _gcp_date_folder()
    date_root = os.path.join(root, date_folder)
    if os.path.isdir(date_root):
        shutil.rmtree(date_root)
    os.makedirs(date_root, exist_ok=True)
    return {
        "output_dir": root,
        "date_folder": date_folder,
        "date_root": date_root,
        "schema": schema,
    }


def get_tables_to_dump(schema: str = DEIDENTIFIED_SCHEMA, csv_path: str | None = None) -> list[str]:
    """
    Return list of table names to dump.
    If csv_path is provided and the file exists, read TABLE_NAME column; else all tables in schema.
    """
    path = csv_path or os.path.join(_project_root(), GCP_TRANSFER_CSV_PATH)
    if os.path.isfile(path):
        import pandas as pd
        df = pd.read_csv(path)
        if "TABLE_NAME" not in df.columns:
            raise ValueError(f"CSV {path} must have a TABLE_NAME column")
        return df["TABLE_NAME"].astype(str).str.strip().dropna().tolist()
    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/",
        pool_pre_ping=True,
    )
    try:
        tables = inspect(engine).get_table_names(schema=schema)
        return sorted(tables)
    finally:
        engine.dispose()


def run_mysqldump_dump(
    schema: str = DEIDENTIFIED_SCHEMA,
    tables: list[str] | None = None,
    output_dir: str | None = None,
) -> dict:
    """
    Dump into gcp_dump/<MMDDYYYY>/<schema>/table.sql and write sql_dump_stats.csv in the date folder.
    file_path in CSV is relative like 03182026/schema/TABLE.sql (from gcp_dump base).
    """
    root_base = output_dir or os.path.join(_project_root(), GCP_DUMP_OUTPUT_DIR)
    date_folder = _gcp_date_folder()
    date_root = os.path.join(root_base, date_folder)
    schema_dir = os.path.join(date_root, schema)

    if os.path.isdir(date_root):
        shutil.rmtree(date_root)
    os.makedirs(schema_dir, exist_ok=True)

    if not tables:
        return {
            "output_dir": date_root,
            "root_base": root_base,
            "date_folder": date_folder,
            "gcs_prefix": f"{GCP_DESTINATION_PREFIX.strip('/')}/{date_folder}",
            "schema": schema,
            "dumped": 0,
            "failed": [],
            "stats_path": None,
        }

    env = os.environ.copy()
    env["MYSQL_PWD"] = unquote(MYSQL_PASSWORD or "") if MYSQL_PASSWORD else ""
    dumped = []
    failed = []
    for table in tables:
        dump_file = os.path.join(schema_dir, f"{table}.sql")
        try:
            with open(dump_file, "w") as out:
                subprocess.run(
                    [
                        "mysqldump",
                        "--default-character-set=utf8",
                        "-h", MYSQL_HOST or "localhost",
                        "-P", "3306",
                        "-u", MYSQL_USER or "",
                        schema,
                        table,
                    ],
                    env=env,
                    stdout=out,
                    stderr=subprocess.PIPE,
                    check=True,
                    text=True,
                )
            dumped.append(table)
        except subprocess.CalledProcessError as e:
            err = e.stderr
            if err is None:
                err = ""
            elif isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            failed.append({"table": table, "error": (err or str(e)).strip()})

    file_data = []
    for root_walk, _dirs, files in os.walk(date_root):
        for f in files:
            if not f.endswith(".sql"):
                continue
            path = os.path.join(root_walk, f)
            rel_from_base = os.path.relpath(path, root_base).replace("\\", "/")
            with open(path, "rb") as fp:
                h = hashlib.md5()
                for chunk in iter(lambda: fp.read(4096), b""):
                    h.update(chunk)
            file_data.append({
                "file_path": rel_from_base,
                "checksum": h.hexdigest(),
                "file_size_bytes": os.path.getsize(path),
            })

    stats_path = os.path.join(date_root, "sql_dump_stats.csv")
    if file_data:
        import pandas as pd
        pd.DataFrame(file_data).to_csv(stats_path, index=False)

    gcs_prefix = f"{GCP_DESTINATION_PREFIX.strip('/')}/{date_folder}"
    return {
        "output_dir": date_root,
        "root_base": root_base,
        "date_folder": date_folder,
        "gcs_prefix": gcs_prefix,
        "schema": schema,
        "dumped": len(dumped),
        "failed": failed,
        "stats_path": stats_path if file_data else None,
    }


def upload_dump_to_gcs(
    source_folder: str,
    bucket: str = GCP_BUCKET,
    destination_prefix: str | None = None,
) -> dict:
    """
    Upload .sql and sql_dump_stats.csv under source_folder only (must be the date
    folder path, e.g. gcp_dump/03182026 — not gcp_dump root).
    """
    source_folder = os.path.abspath(source_folder)
    if not os.path.isdir(source_folder):
        return {
            "bucket": bucket,
            "destination_prefix": (destination_prefix or GCP_DESTINATION_PREFIX).strip("/"),
            "uploaded": 0,
            "errors": [{"path": source_folder, "gcs": "", "error": "source_folder does not exist"}],
        }
    prefix = (destination_prefix or GCP_DESTINATION_PREFIX).strip("/")
    uploaded = 0
    errors = []
    for root, _dirs, files in os.walk(source_folder):
        for f in files:
            if not f.endswith(".sql") and f != "sql_dump_stats.csv":
                continue
            path = os.path.join(root, f)
            rel = os.path.relpath(path, source_folder).replace("\\", "/")
            gcs_uri = f"gs://{bucket}/{prefix}/{rel}"
            try:
                subprocess.run(
                    ["gsutil", "cp", path, gcs_uri],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                uploaded += 1
            except subprocess.CalledProcessError as e:
                errors.append({"path": path, "gcs": gcs_uri, "error": (e.stderr or e.stdout or str(e))[:200]})
    return {
        "bucket": bucket,
        "destination_prefix": prefix,
        "uploaded": uploaded,
        "errors": errors,
    }


def run_gcp_dump_pipeline(dump_schema: str | None = None) -> dict:
    """
    Full pipeline: get tables (CSV or all in schema), mysqldump, upload to GCS.
    dump_schema: explicit schema to dump from (e.g. diff_*_deid passed by DAG2/DAG4).
                 If None, _resolve_dump_schema chooses based on GCP_FULL_REFRESH_FLAG.
    Returns combined summary for XCom.
    """
    schema = _resolve_dump_schema(dump_schema)
    tables = get_tables_to_dump(schema=schema)
    dump_result = run_mysqldump_dump(schema=schema, tables=tables)
    if dump_result["failed"]:
        failed_list = ", ".join(f["table"] for f in dump_result["failed"])
        first_err = dump_result["failed"][0].get("error", "")
        raise RuntimeError(
            f"Dump failed for {len(dump_result['failed'])} table(s): {failed_list}. First error: {first_err}"
        )
    if dump_result["dumped"] == 0:
        return {
            "status": "SKIPPED",
            "tables_requested": 0,
            "dump": dump_result,
            "upload": None,
        }
    upload_result = upload_dump_to_gcs(
        source_folder=gcp_dump_date_root(dump_result["date_folder"]),
        destination_prefix=dump_result["gcs_prefix"],
    )
    return {
        "status": "SUCCESS" if not upload_result["errors"] else "PARTIAL",
        "tables_requested": len(tables),
        "dump": dump_result,
        "upload": upload_result,
    }
