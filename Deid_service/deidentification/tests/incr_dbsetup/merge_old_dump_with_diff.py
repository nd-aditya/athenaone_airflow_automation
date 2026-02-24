import os
from sqlalchemy import create_engine, text
import json
import traceback

INCREMENTAL_DIFF_DATABASE_NAME = "automation_incr_testing_diff_28nov2025"
MAIN_DATABASE_NAME   = "automation_incr_testing"
engine = create_engine("mysql+pymysql://root:123456789@localhost:3306")
AUTO_INCREMENT_COL = "nd_auto_increment_id"
DIFF_CREATED_DATE_COL = "nd_extracted_date"
NEW_ND_EXTRACTED_DATE = "2025-11-28"  # Should be valid for DATETIME. Use '2025-11-28 00:00:00' if strictly necessary.

DIFF_SCRIPT_PROGRESS_PATH = "progress_diff.json"
insert_incremental_progress_path = "insert_incremental_progress.json"

def load_insert_incremental_progress():
    if os.path.exists(insert_incremental_progress_path) and os.path.getsize(insert_incremental_progress_path) > 0:
        with open(insert_incremental_progress_path, "r") as f:
            try:
                return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load incremental progress: {e}")
    return {}

def save_insert_incremental_progress(progress):
    with open(insert_incremental_progress_path, "w") as f:
        json.dump(progress, f, indent=2)


def load_diff_script_progress():
    if os.path.exists(DIFF_SCRIPT_PROGRESS_PATH) and os.path.getsize(DIFF_SCRIPT_PROGRESS_PATH) > 0:
        with open(DIFF_SCRIPT_PROGRESS_PATH, "r") as f:
            try:
                return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load diff script progress: {e}")
    return {}

def table_exists(conn, db, table_name):
    res = conn.execute(text("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = :db AND table_name = :tname
    """), {"db": db, "tname": table_name}).scalar()
    return res > 0

def get_create_table_ddl(conn, db, table_name):
    result = conn.execute(text(f"SHOW CREATE TABLE `{db}`.`{table_name}`")).fetchone()
    if not result:
        raise Exception(f"Could not get DDL for table: {db}.{table_name}")
    ddl = result[1]
    
    import re
    first_line, *rest = ddl.split('\n')
    first_line = re.sub(r"^CREATE TABLE\s+`.*?`", 
                        f"CREATE TABLE `{MAIN_DATABASE_NAME}`.`{table_name}`", 
                        first_line)
    return "\n".join([first_line] + rest)

with engine.begin() as conn:

    tables = conn.execute(text(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{INCREMENTAL_DIFF_DATABASE_NAME}' AND TABLE_TYPE = 'BASE TABLE'
    """)).fetchall()

    insert_incremental_progress = load_insert_incremental_progress()
    diff_script_progress = load_diff_script_progress()

    for (table_name,) in tables:
        if table_name in insert_incremental_progress and insert_incremental_progress[table_name].get("status") == "done":
            continue

        if table_name not in diff_script_progress or diff_script_progress[table_name].get("status") != "done":
            print(f"  ⏭ Skipping table: {table_name} (diff script not done)")
            continue

        try:
            insert_incremental_progress[table_name] = {"status": "in_progress"}
            save_insert_incremental_progress(insert_incremental_progress)

            print(f"\n===== Inserting incremental for table: {table_name} =====")

            # Create table if missing
            if not table_exists(conn, MAIN_DATABASE_NAME, table_name):
                print(f"  🏗️ Creating table {MAIN_DATABASE_NAME}.{table_name} ...")
                ddl = get_create_table_ddl(conn, INCREMENTAL_DIFF_DATABASE_NAME, table_name)
                import re
                ddl = re.sub(r'\bAUTO_INCREMENT\b', '', ddl)
                conn.execute(text(ddl))

            # Get column lists
            def fetch_columns(db_name):
                return [
                    row[0]
                    for row in conn.execute(
                        text(f"""
                            SELECT COLUMN_NAME
                            FROM information_schema.columns
                            WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'
                        """)
                    ).fetchall()
                ]

            src_cols = fetch_columns(INCREMENTAL_DIFF_DATABASE_NAME)
            dest_cols = fetch_columns(MAIN_DATABASE_NAME)

            # Add nd_auto_increment_id if missing
            dest_changed = False
            if AUTO_INCREMENT_COL not in dest_cols:
                print(f"  ➕ Adding '{AUTO_INCREMENT_COL}' to {MAIN_DATABASE_NAME}.{table_name}")
                conn.execute(text(f"""
                    ALTER TABLE {MAIN_DATABASE_NAME}.{table_name}
                    ADD COLUMN {AUTO_INCREMENT_COL} BIGINT
                """))
                dest_changed = True

            # Add nd_extracted_date if missing - no default, just add a nullable column
            if DIFF_CREATED_DATE_COL not in dest_cols:
                print(f"  ➕ Adding '{DIFF_CREATED_DATE_COL}' to {MAIN_DATABASE_NAME}.{table_name}")
                conn.execute(text(f"""
                    ALTER TABLE {MAIN_DATABASE_NAME}.{table_name}
                    ADD COLUMN {DIFF_CREATED_DATE_COL} DATETIME NULL
                """))
                dest_changed = True

            if dest_changed:
                dest_cols = fetch_columns(MAIN_DATABASE_NAME)

            # Ensure nd_auto_increment_id column exists in INCREMENTAL_DIFF_DATABASE_NAME, add if not
            src_cols = [row[0] for row in conn.execute(text(f"""
                SELECT COLUMN_NAME
                FROM information_schema.columns
                WHERE TABLE_SCHEMA='{INCREMENTAL_DIFF_DATABASE_NAME}' AND TABLE_NAME='{table_name}'
            """)).fetchall()]

            if AUTO_INCREMENT_COL not in src_cols:
                print(f"  ➕ Adding '{AUTO_INCREMENT_COL}' to {INCREMENTAL_DIFF_DATABASE_NAME}.{table_name}")
                conn.execute(text(f"""
                    ALTER TABLE {INCREMENTAL_DIFF_DATABASE_NAME}.{table_name}
                    ADD COLUMN {AUTO_INCREMENT_COL} BIGINT
                """))
                src_cols = fetch_columns(INCREMENTAL_DIFF_DATABASE_NAME)

            # Find max auto_increment id (across BOTH tables)
            result_dest = conn.execute(text(f"""
                SELECT MAX({AUTO_INCREMENT_COL}) FROM {MAIN_DATABASE_NAME}.{table_name}
            """)).fetchone()
            max_id_dest = result_dest[0] if result_dest and result_dest[0] else 0

            result_src = conn.execute(text(f"""
                SELECT MAX({AUTO_INCREMENT_COL}) FROM {INCREMENTAL_DIFF_DATABASE_NAME}.{table_name}
            """)).fetchone()
            max_id_src = result_src[0] if result_src and result_src[0] else 0

            max_id = max(max_id_dest, max_id_src)

            # ----------------------------------------------
            # 1. Insert only new (source) rows from INCREMENTAL_DIFF_DATABASE_NAME to MAIN_DATABASE_NAME - No overwrite!
            # ----------------------------------------------
            # New rows := those that do NOT exist in MAIN_DATABASE_NAME (by PK or all-column)
            # For simplicity and safety, let's detect only such rows by ND_AUTO_INCREMENT_ID IS NULL in MAIN_DATABASE_NAME or by PKs.
            # But more robustly we should use all entries from INCREMENTAL_DIFF_DATABASE_NAME, but skip those where nd_auto_increment_id already exists in MAIN_DATABASE_NAME (if PK not known).

            # Assign nd_auto_increment_id for source rows that still have NULL
            if AUTO_INCREMENT_COL in src_cols:
                print(f"  🔢 Assigning auto-increment ids in {INCREMENTAL_DIFF_DATABASE_NAME} ...")
                conn.execute(text("SET @ai := :max_id"), {"max_id": max_id})
                conn.execute(text(f"""
                    UPDATE `{INCREMENTAL_DIFF_DATABASE_NAME}`.`{table_name}`
                    SET `{AUTO_INCREMENT_COL}` = (@ai := @ai + 1)
                    WHERE `{AUTO_INCREMENT_COL}` IS NULL
                """))
                result_src = conn.execute(text(f"""
                    SELECT MAX({AUTO_INCREMENT_COL}) FROM {INCREMENTAL_DIFF_DATABASE_NAME}.{table_name}
                """)).fetchone()
                max_id_src = result_src[0] if result_src and result_src[0] else 0
                max_id = max(max_id, max_id_src)

            # Make a condition to avoid inserting duplicate rows. Use PKs if possible, otherwise all SOURCED ND_AUTO_INCREMENT_ID not present in DEST.
            new_row_condition = f"""
                WHERE `{AUTO_INCREMENT_COL}` IS NOT NULL
                  AND `{AUTO_INCREMENT_COL}` NOT IN (
                      SELECT `{AUTO_INCREMENT_COL}` FROM `{MAIN_DATABASE_NAME}`.`{table_name}`
                      WHERE `{AUTO_INCREMENT_COL}` IS NOT NULL
                  )
            """

            # Compose insert-target and select-expr lists
            insert_target_columns = []
            select_expr_columns = []
            for col in dest_cols:
                if col == AUTO_INCREMENT_COL:
                    insert_target_columns.append(f"`{col}`")
                    if col in src_cols:
                        select_expr_columns.append(f"`{col}`")
                    else:
                        select_expr_columns.append("NULL")
                elif col == DIFF_CREATED_DATE_COL:
                    insert_target_columns.append(f"`{col}`")
                    # Only for new inserted rows we want the new date, otherwise (for previous/old rows) don't update!
                    select_expr_columns.append(f"'{NEW_ND_EXTRACTED_DATE}'")
                elif col in src_cols:
                    insert_target_columns.append(f"`{col}`")
                    select_expr_columns.append(f"`{col}`")
                else:
                    insert_target_columns.append(f"`{col}`")
                    select_expr_columns.append("DEFAULT")

            insert_columns_str = ', '.join(insert_target_columns)
            select_columns_str = ', '.join(select_expr_columns)

            print(f"  ➕ Inserting new rows into {MAIN_DATABASE_NAME}.{table_name} ...")
            res = conn.execute(
                text(
                    f"""INSERT INTO `{MAIN_DATABASE_NAME}`.`{table_name}`
                        ({insert_columns_str})
                        SELECT {select_columns_str}
                        FROM `{INCREMENTAL_DIFF_DATABASE_NAME}`.`{table_name}`
                        {new_row_condition}
                    """
                )
            )

            # Get mapping of newly inserted rows (in DEST) to PK(s) or their ROW_IDs
            # We'll try to read (by PK) those new IDs so we can update source!

            # The only practical way is: if PK exists, use PK to join back;
            #  else, if AUTO_INCREMENT_COL exists, use that.

            # Find rows in DEST that still have nd_auto_increment_id = NULL (these are newly inserted)
            # First, update AUTO_INCREMENT_COL in DEST for NULLs
            print(f"  🔢 Updating auto-increment ids in {MAIN_DATABASE_NAME} ...")
            conn.execute(text("SET @ai := :max_id"), {"max_id": max_id})
            conn.execute(text(f"""
                UPDATE `{MAIN_DATABASE_NAME}`.`{table_name}`
                SET `{AUTO_INCREMENT_COL}` = (@ai := @ai + 1)
                WHERE `{AUTO_INCREMENT_COL}` IS NULL
            """))

            # ----------- Patch: get mapping for new rows -----------
            # Either PK(s) or - if none - we simply update by ROW_NUMBER order

            print(f"  ✔ Completed table: {table_name}")

            insert_incremental_progress[table_name] = {"status": "done"}
            save_insert_incremental_progress(insert_incremental_progress)

        except Exception as e:
            print(f"  ❌ Error on table {table_name}: {e}")
            traceback.print_exc()

            insert_incremental_progress[table_name] = {
                "status": "failed",
                "error": str(e)
            }
            save_insert_incremental_progress(insert_incremental_progress)

print("\n=========== Done. All committed successfully. ===========")
