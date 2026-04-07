import os
import sys
import subprocess
import psutil
import hashlib
import argparse
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List

import pandas as pd
from google.cloud import storage


# =========================
# Transfer QC functionality
# =========================

def log_memory_usage():
    process = psutil.Process(os.getpid())
    print(f"Memory usage: {process.memory_info().rss / (1024 ** 2):.2f} MB")


def calculate_md5_from_gcs(blob):
    """Stream blob content and calculate MD5 checksum efficiently."""
    md5_hash = hashlib.md5()
    with blob.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest(), blob.size


def run_transfer_qc(bucket_name: str, folder_name: str) -> None:
    """
    Run transfer QC for all .sql files under the given GCS bucket/prefix.
    Raises RuntimeError with details if any checksum or file size mismatches are found.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    reference_csv_path = f"{folder_name}sql_dump_stats.csv"
    csv_blob = bucket.blob(reference_csv_path)
    reference_df = pd.read_csv(BytesIO(csv_blob.download_as_bytes()))
    reference_df["checksum"] = reference_df["checksum"].astype(str)
    reference_df["file_size_bytes"] = reference_df["file_size_bytes"].astype(int)

    sql_files_folder = f"{folder_name}"
    sql_blobs = client.list_blobs(bucket_name, prefix=sql_files_folder)

    file_data = []
    for blob in sql_blobs:
        log_memory_usage()
        if blob.name.endswith(".sql"):
            file_path = blob.name
            checksum, file_size = calculate_md5_from_gcs(blob)
            file_data.append({"file_path": file_path, "checksum": checksum, "file_size_bytes": file_size})
        log_memory_usage()

    log_memory_usage()
    file_df = pd.DataFrame(file_data)

    reference_df["file_path"] = reference_df["file_path"].str.split("\\").apply(
        lambda x: "/".join(x[-2:])
    )
    file_df["file_path"] = file_df["file_path"].str.split("/").apply(
        lambda x: "/".join(x[-3:])
    )

    comparison_df = file_df.merge(reference_df, on=["file_path"], how="left", suffixes=("", "_ref"))
    comparison_df["checksum_match"] = comparison_df["checksum"] == comparison_df["checksum_ref"]
    comparison_df["file_size_match"] = comparison_df["file_size_bytes"] == comparison_df["file_size_bytes_ref"]

    mismatched_files = comparison_df[
        (~comparison_df["checksum_match"]) | (~comparison_df["file_size_match"])
    ]

    output_csv = BytesIO()
    comparison_df.to_csv(output_csv, index=False)
    output_csv.seek(0)
    output_blob_path = f"{folder_name}qc_results.csv"
    output_blob = bucket.blob(output_blob_path)
    output_blob.upload_from_file(output_csv, content_type="text/csv")
    print(f"QC results uploaded to GCS: {output_blob_path}")

    if not mismatched_files.empty:
        mismatch_details = mismatched_files[
            ["file_path", "checksum", "checksum_ref", "file_size_bytes", "file_size_bytes_ref"]
        ].to_string()
        print("Discrepancies found! Review the following files:")
        print(mismatch_details)
        raise RuntimeError(
            f"Transfer QC failed: {len(mismatched_files)} file(s) have checksum or file size mismatches. "
            f"Details:\n{mismatch_details}"
        )

    print("All .sql files passed the QC check.")


# =========================
# Restore functionality
# =========================

DB_HOST = os.getenv("MYSQL_HOST", "")
DB_USER = os.getenv("MYSQL_USER", "")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

BUCKET_NAME: Optional[str] = None
FOLDER_PREFIX: Optional[str] = None
LOCAL_TEMP_DIR = "/tmp/sql-files"


def list_sql_files_in_gcs(bucket_name, folder_prefix):
    """List all .sql files in a specific GCS folder, including nested subfolders."""
    print(f"Fetching .sql files from GCS bucket '{bucket_name}' with prefix '{folder_prefix}'...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_prefix)

    sql_files = []
    for blob in blobs:
        if blob.name.endswith(".sql"):
            sql_files.append(blob.name)

    if not sql_files:
        print("No .sql files found in the specified GCS folder.")
    return sql_files


def download_sql_file(blob_name, bucket_name, local_dir):
    """Download a single .sql file from GCS to a local directory."""
    os.makedirs(local_dir, exist_ok=True)
    local_file_path = os.path.join(local_dir, os.path.basename(blob_name))
    print(f"Downloading {blob_name} to {local_file_path}...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_file_path)
    return local_file_path


def restore_sql_file(sql_file_path: str, db_name: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    """Restore a single .sql file to the database. Returns (success, error_message)."""
    print(f"Restoring {sql_file_path} to the database '{db_name}'...")
    try:
        command = ["mysql", f"-h{DB_HOST}", f"-u{DB_USER}", f"-p{DB_PASSWORD}", db_name]
        with open(sql_file_path, "r") as sql_file:
            process = subprocess.run(
                command,
                stdin=sql_file,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        if process.returncode == 0:
            print(f"Successfully restored {sql_file_path}.")
            return (True, None)
        error_msg = process.stderr.strip() if process.stderr else f"mysql exited with code {process.returncode}"
        print(f"Error restoring {sql_file_path}: {error_msg}")
        return (False, error_msg)
    except Exception as e:
        error_msg = str(e)
        print(f"Exception occurred while restoring {sql_file_path}: {error_msg}")
        return (False, error_msg)


def process_sql_file(blob_name: str, db_name: Optional[str] = None) -> Tuple[bool, str, Optional[str]]:
    """Download, restore, and delete a single .sql file. Returns (success, blob_name, error_message)."""
    print(f"Processing {blob_name} to the database '{db_name}'...")
    try:
        local_file_path = download_sql_file(blob_name, BUCKET_NAME, LOCAL_TEMP_DIR)
        success, error_msg = restore_sql_file(local_file_path, db_name)
        if not success:
            return (False, blob_name, error_msg)
        os.remove(local_file_path)
        print(f"Deleted local file: {local_file_path}")
        return (True, blob_name, None)
    except Exception as e:
        error_msg = str(e)
        print(f"Error processing {blob_name}: {error_msg}")
        return (False, blob_name, error_msg)


def _restore_main(db_name: Optional[str] = None, max_workers: int = 10, tables_filter: Optional[str] = None):
    all_sql_files = list_sql_files_in_gcs(BUCKET_NAME, FOLDER_PREFIX)
    if tables_filter:
        tables_set = {t.strip().lower() for t in tables_filter.split(",") if t.strip()}
        sql_files = [f for f in all_sql_files if os.path.basename(f).replace(".sql", "").lower() in tables_set]
        print(f"Filtering to {len(sql_files)} table(s): {', '.join(sorted(tables_set))}")
    else:
        sql_files = all_sql_files
    print(len(sql_files), sql_files)

    table_names = [os.path.basename(f)[:-4] for f in sql_files if f.endswith(".sql")]
    if table_names:
        print(f"RESTORE_TABLES_LIST: {','.join(sorted(set(table_names)))}")

    failed_restores: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_sql_file, blob, db_name): blob for blob in sql_files}
        for future in as_completed(futures):
            blob = futures[future]
            try:
                success, blob_name, error_msg = future.result()
                if not success:
                    failed_restores.append((blob_name, error_msg or "Unknown error"))
            except Exception as e:
                failed_restores.append((blob, str(e)))
                print(f"Unexpected error while processing {blob}: {str(e)}")

    if failed_restores:
        failed_blobs = [blob for blob, _ in failed_restores]
        table_names_failed = [os.path.basename(b).replace(".sql", "") for b in failed_blobs]
        details = "\n".join(f"  - {blob}: {err}" for blob, err in failed_restores[:20])
        if len(failed_restores) > 20:
            details += f"\n  ... and {len(failed_restores) - 20} more"
        print(f"RESTORE_FAILED_TABLES: {','.join(table_names_failed)}")
        raise RuntimeError(
            f"Restore failed: {len(failed_restores)} table(s) could not be restored. "
            f"Failed tables: {', '.join(table_names_failed[:10])}{'...' if len(table_names_failed) > 10 else ''}. "
            f"Details:\n{details}"
        )

    print("All files have been processed.")


def run_restore(
    bucket_name: str,
    folder_prefix: str,
    db_name: Optional[str] = None,
    max_workers: int = 10,
    tables_filter: Optional[str] = None,
) -> None:
    global BUCKET_NAME, FOLDER_PREFIX
    BUCKET_NAME = bucket_name
    FOLDER_PREFIX = folder_prefix
    _restore_main(db_name=db_name, max_workers=max_workers, tables_filter=tables_filter)


# =========================
# Helper functions for CLI
# =========================

def build_bucket_name(client_name: str) -> str:
    return f"nd-platform-{client_name}"


def build_folder_prefix(date_folder: str, client_name: str) -> str:
    name = client_name.lower()
    if name == "dent":
        return f"tables/{date_folder}/"
    if name == "tng":
        return f"EHR/Athena One/{date_folder}/"
    return f"EHR/{date_folder}/"


# =========================
# CLI Entry Point
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Run transfer QC or restore SQL dump files from GCS to MySQL database."
    )
    parser.add_argument("--task", type=str, required=True, choices=["qc", "restore"])
    parser.add_argument("--client_name", type=str, required=True)
    parser.add_argument("--db_name", type=str, required=False)
    parser.add_argument("--date_folder", type=str, required=True)
    parser.add_argument("--max_workers", type=int, default=10)
    parser.add_argument("--tables", type=str, required=False)

    args = parser.parse_args()
    bucket_name   = build_bucket_name(args.client_name)
    folder_prefix = build_folder_prefix(args.date_folder, args.client_name)

    try:
        if args.task == "qc":
            print(f"Running transfer QC for bucket: {bucket_name}, folder: {folder_prefix}")
            run_transfer_qc(bucket_name=bucket_name, folder_name=folder_prefix)
        elif args.task == "restore":
            if not args.db_name:
                parser.error("--db_name is required when --task is 'restore'")
            run_restore(
                bucket_name=bucket_name,
                folder_prefix=folder_prefix,
                db_name=args.db_name,
                max_workers=args.max_workers,
                tables_filter=args.tables,
            )
    except RuntimeError as e:
        print(f"FAILED: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
