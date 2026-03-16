"""
GCP dump: dump MySQL tables (DEIDENTIFIED_SCHEMA) to local SQL files and upload to GCS.
Tables to dump: from optional CSV (TABLE_NAME column) or all tables in schema if CSV missing.
"""
import hashlib
import os
import subprocess

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
)


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


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
    Run mysqldump for each table into output_dir/schema/table.sql.
    Returns summary with dumped count, failed list, and path to sql_dump_stats.csv.
    """
    root = output_dir or os.path.join(_project_root(), GCP_DUMP_OUTPUT_DIR)
    schema_dir = os.path.join(root, schema)
    os.makedirs(schema_dir, exist_ok=True)
    if not tables:
        return {
            "output_dir": root,
            "schema": schema,
            "dumped": 0,
            "failed": [],
            "stats_path": None,
        }
    env = os.environ.copy()
    env["MYSQL_PWD"] = MYSQL_PASSWORD or ""
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
    for root_walk, _dirs, files in os.walk(root):
        for f in files:
            if f.endswith(".sql"):
                path = os.path.join(root_walk, f)
                with open(path, "rb") as fp:
                    h = hashlib.md5()
                    for chunk in iter(lambda: fp.read(4096), b""):
                        h.update(chunk)
                file_data.append({
                    "file_path": path,
                    "checksum": h.hexdigest(),
                    "file_size_bytes": os.path.getsize(path),
                })
    stats_path = os.path.join(root, "sql_dump_stats.csv")
    if file_data:
        import pandas as pd
        pd.DataFrame(file_data).to_csv(stats_path, index=False)
    return {
        "output_dir": root,
        "schema": schema,
        "dumped": len(dumped),
        "failed": failed,
        "stats_path": stats_path if file_data else None,
    }


def upload_dump_to_gcs(
    source_folder: str,
    bucket: str = GCP_BUCKET,
    destination_prefix: str = GCP_DESTINATION_PREFIX,
) -> dict:
    """
    Upload all .sql under source_folder to gs://bucket/destination_prefix/..., and sql_dump_stats.csv.
    Uses gsutil (assumes gcloud auth already done). Returns summary.
    """
    uploaded = 0
    errors = []
    for root, _dirs, files in os.walk(source_folder):
        for f in files:
            if not f.endswith(".sql") and f != "sql_dump_stats.csv":
                continue
            path = os.path.join(root, f)
            rel = os.path.relpath(path, source_folder).replace("\\", "/")
            gcs_uri = f"gs://{bucket}/{destination_prefix}/{rel}"
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
        "destination_prefix": destination_prefix,
        "uploaded": uploaded,
        "errors": errors,
    }


def run_gcp_dump_pipeline() -> dict:
    """
    Full pipeline: get tables (CSV or all in schema), mysqldump, upload to GCS.
    Returns combined summary for XCom.
    """
    tables = get_tables_to_dump()
    dump_result = run_mysqldump_dump(tables=tables)
    if dump_result["failed"]:
        return {
            "status": "PARTIAL",
            "tables_requested": len(tables),
            "dump": dump_result,
            "upload": None,
            "error": f"Dump failed for {len(dump_result['failed'])} table(s)",
        }
    if dump_result["dumped"] == 0:
        return {
            "status": "SKIPPED",
            "tables_requested": 0,
            "dump": dump_result,
            "upload": None,
        }
    upload_result = upload_dump_to_gcs(source_folder=dump_result["output_dir"])
    return {
        "status": "SUCCESS" if not upload_result["errors"] else "PARTIAL",
        "tables_requested": len(tables),
        "dump": dump_result,
        "upload": upload_result,
    }
