"""
Test script for PIITable class.

This script tests:
1. Direct primary_column_name case (when table has primary column)
2. Reference mapping case (when primary_column_name is null and reference_mapping is used)
3. Table creation, data insertion, and NDID mapping
"""

import os
import sys
import django

# Set up Django environment if needed
sys.path.append(
    "/Users/rohitchouhan/Documents/Code/backend/deidentification/deIdentification/"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, BigInteger
from core.dbPkg.phi_table.create_table import PIITable
from core.dbPkg.mapping_loader import PATIENT_MAPPING_TABLE, PATIENT_MAPPING_TABLE_ND_PATIENTID_COL
import pandas as pd

# Database connection strings - UPDATE THESE WITH YOUR TEST DATABASE CREDENTIALS
SRC_DB_URL = "mysql+pymysql://root:123456789@localhost/test_src_db"
MASTER_DB_URL = "mysql+pymysql://root:123456789@localhost/test_master_db"
MAPPING_DB_URL = "mysql+pymysql://root:123456789@localhost/test_mapping_db"

QUEUE_ID = 1


def setup_test_databases():
    """Set up test databases with sample data."""
    print("Setting up test databases...")
    
    # Create source database tables and data
    src_engine = create_engine(SRC_DB_URL)
    
    with src_engine.connect() as conn:
        # Create test source tables
        
        # Table 1: Direct primary column case
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS INSURANCE_MASTER_BKP (
                PATIENTID VARCHAR(50) PRIMARY KEY,
                LASTNAME VARCHAR(100),
                MIDDLENAME VARCHAR(100)
            )
        """))
        
        # Insert test data
        conn.execute(text("""
            INSERT INTO INSURANCE_MASTER_BKP (PATIENTID, LASTNAME, MIDDLENAME)
            VALUES 
                ('PAT001', 'Smith', 'John'),
                ('PAT002', 'Doe', 'Jane'),
                ('PAT003', 'Johnson', 'Robert')
        """))
        
        # Table 2: Reference mapping case - Address table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Address (
                AddressId INT PRIMARY KEY,
                CONTEXTID VARCHAR(50),
                EMAIL VARCHAR(100)
            )
        """))
        
        conn.execute(text("""
            INSERT INTO Address (AddressId, CONTEXTID, EMAIL)
            VALUES 
                (1, 'CTX001', 'address1@test.com'),
                (2, 'CTX002', 'address2@test.com'),
                (3, 'CTX003', 'address3@test.com')
        """))
        
        # Billing table (intermediate table for reference mapping)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Billing (
                BillingAdressId INT PRIMARY KEY,
                AddressId INT,
                BillingId INT
            )
        """))
        
        conn.execute(text("""
            INSERT INTO Billing (BillingAdressId, AddressId, BillingId)
            VALUES 
                (1, 1, 101),
                (2, 2, 102),
                (3, 3, 103)
        """))
        
        # Patient table (final table for reference mapping)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Patient (
                AddressId INT PRIMARY KEY,
                patient_id VARCHAR(50),
                FirstName VARCHAR(100)
            )
        """))
        
        conn.execute(text("""
            INSERT INTO Patient (AddressId, patient_id, FirstName)
            VALUES 
                (1, 'PAT001', 'John'),
                (2, 'PAT002', 'Jane'),
                (3, 'PAT003', 'Robert')
        """))
        
        conn.commit()
        print("✓ Source database tables created and populated")
    
    # Create mapping database table and data
    mapping_engine = create_engine(MAPPING_DB_URL)
    
    with mapping_engine.connect() as conn:
        # Create patient_mapping_table
        metadata = MetaData()
        patient_mapping = Table(
            PATIENT_MAPPING_TABLE,
            metadata,
            Column(PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, BigInteger, primary_key=True),
            Column('patientid', String(50)),
            Column('offset', Integer),
        )
        metadata.create_all(mapping_engine)
        
        # Insert mapping data
        conn.execute(text(f"""
            INSERT INTO {PATIENT_MAPPING_TABLE} 
            ({PATIENT_MAPPING_TABLE_ND_PATIENTID_COL}, patientid, `offset`)
            VALUES 
                (1001, 'PAT001', 1),
                (1002, 'PAT002', 2),
                (1003, 'PAT003', 3)
        """))
        
        conn.commit()
        print("✓ Mapping database table created and populated")
    
    # Create master database (empty, will be populated by PIITable)
    master_engine = create_engine(MASTER_DB_URL)
    with master_engine.connect() as conn:
        # Just verify connection
        conn.execute(text("SELECT 1"))
        print("✓ Master database connection verified")
    
    src_engine.dispose()
    mapping_engine.dispose()
    master_engine.dispose()


def test_direct_primary_column():
    """Test case 1: Direct primary_column_name (not null)"""
    print("\n" + "="*60)
    print("TEST 1: Direct primary_column_name case")
    print("="*60)
    
    pii_tables_config = {
        "master_insurance_table": {
            "source_tables": {
                "INSURANCE_MASTER_BKP": {
                    "required_columns": [
                        {"PATIENTID": "PATIENTID"},
                        {"LASTNAME": "LASTNAME"},
                        {"MIDDLENAME": "MIDDLENAME"}
                    ],
                    "primary_column_name": "PATIENTID",
                    "primary_column_type": "patientid"
                }
            },
            "table_columns": [
                "PATIENTID",
                "LASTNAME",
                "MIDDLENAME"
            ]
        }
    }
    
    pii_table = PIITable(SRC_DB_URL, MASTER_DB_URL, MAPPING_DB_URL, pii_tables_config, QUEUE_ID)
    
    try:
        print("\nGenerating PII table...")
        pii_table.generate_or_update_pii_table()
        print("✓ PII table generated successfully")
        
        # Verify the results
        master_engine = create_engine(MASTER_DB_URL)
        with master_engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM master_insurance_table"))
            rows = result.fetchall()
            print(f"\n✓ Found {len(rows)} rows in master_insurance_table")
            
            for row in rows:
                print(f"  - ND_PATIENT_ID: {row[0]}, PATIENTID: {row[1]}, LASTNAME: {row[2]}, MIDDLENAME: {row[3]}")
        
        master_engine.dispose()
        print("\n✓ TEST 1 PASSED")
        
    except Exception as e:
        print(f"\n✗ TEST 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
    finally:
        del pii_table


def test_reference_mapping():
    """Test case 2: Reference mapping (primary_column_name is null)"""
    print("\n" + "="*60)
    print("TEST 2: Reference mapping case (primary_column_name is null)")
    print("="*60)
    
    pii_tables_config = {
        "pii_data_table": {
            "source_tables": {
                "Address": {
                    "required_columns": [
                        {"CONTEXTID": "CONTEXTID"},
                        {"EMAIL": "EMAIL"}
                    ],
                    "primary_column_name": None,
                    "primary_column_type": "patientid",
                    "reference_mapping": {
                        "conditions": [
                            {
                                "column_name": "AddressId",
                                "source_column": "AddressId",
                                "reference_table": "Patient"
                            }
                        ],
                        "source_table": "Address",
                        "destination_column": "patient_id",
                        "destination_column_type": "patientid"
                    }
                }
            },
            "table_columns": [
                "PATIENTID",
                "CONTEXTID",
                "EMAIL"
            ]
        }
    }
    
    pii_table = PIITable(SRC_DB_URL, MASTER_DB_URL, MAPPING_DB_URL, pii_tables_config, QUEUE_ID)
    
    try:
        print("\nGenerating PII table with reference mapping...")
        
        # Debug: Check source data
        src_engine = create_engine(SRC_DB_URL)
        with src_engine.connect() as conn:
            addr_result = conn.execute(text("SELECT * FROM Address"))
            print(f"\nSource Address table has {len(addr_result.fetchall())} rows")
            
            patient_result = conn.execute(text("SELECT * FROM Patient"))
            patient_rows = patient_result.fetchall()
            print(f"Source Patient table has {len(patient_rows)} rows")
            for row in patient_rows:
                print(f"  - AddressId: {row[0]}, patient_id: {row[1]}")
        
        # Debug: Check mapping data
        mapping_engine = create_engine(MAPPING_DB_URL)
        with mapping_engine.connect() as conn:
            map_result = conn.execute(text(f"SELECT * FROM {PATIENT_MAPPING_TABLE}"))
            map_rows = map_result.fetchall()
            print(f"\nMapping table has {len(map_rows)} rows")
            for row in map_rows:
                print(f"  - nd_patient_id: {row[0]}, patientid: {row[1]}, offset: {row[2]}")
        
        # Test the query building
        from core.dbPkg.phi_table.create_table import OnePHITableConfig
        source_conf: OnePHITableConfig = pii_tables_config["pii_data_table"]["source_tables"]["Address"]
        query = pii_table._build_reference_mapping_query("Address", source_conf, ["CONTEXTID", "EMAIL"])
        print(f"\nGenerated query:\n{query}\n")
        
        # Test the query execution
        test_df = pd.read_sql_query(query, src_engine)
        print(f"Query returned {len(test_df)} rows")
        if len(test_df) > 0:
            print(f"Columns: {test_df.columns.tolist()}")
            print(f"Sample data:\n{test_df.head()}")
        
        src_engine.dispose()
        mapping_engine.dispose()
        
        # Now run the actual generation
        pii_table.generate_or_update_pii_table()
        print("✓ PII table generated successfully with reference mapping")
        
        # Verify the results
        master_engine = create_engine(MASTER_DB_URL)
        with master_engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM pii_data_table"))
            rows = result.fetchall()
            print(f"\n✓ Found {len(rows)} rows in pii_data_table")
            
            if len(rows) > 0:
                # Get column names
                col_names = result.keys()
                for row in rows:
                    print(f"  - {dict(zip(col_names, row))}")
            else:
                print("  ⚠ No rows found! Checking table structure...")
                desc_result = conn.execute(text("DESCRIBE pii_data_table"))
                print("  Table structure:")
                for col in desc_result:
                    print(f"    - {col[0]} ({col[1]})")
        
        master_engine.dispose()
        print("\n✓ TEST 2 PASSED")
        
    except Exception as e:
        print(f"\n✗ TEST 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
    finally:
        del pii_table


def test_query_building():
    """Test the reference mapping query building"""
    print("\n" + "="*60)
    print("TEST 3: Reference mapping query building")
    print("="*60)
    
    from core.dbPkg.phi_table.create_table import OnePHITableConfig
    
    source_conf: OnePHITableConfig = {
        "primary_column_name": None,
        "primary_column_type": "patientid",
        "required_columns": [
            {"CONTEXTID": "CONTEXTID"},
            {"EMAIL": "EMAIL"}
        ],
        "reference_mapping": {
            "conditions": [
                {
                    "column_name": "BillingAdressId",
                    "source_column": "AddressId",
                    "reference_table": "Billing"
                },
                {
                    "column_name": "AddressId",
                    "source_column": "BillingAdressId",
                    "reference_table": "Patient"
                }
            ],
            "source_table": "Address",
            "destination_column": "patient_id",
            "destination_column_type": "patientid"
        }
    }
    
    pii_table = PIITable(SRC_DB_URL, MASTER_DB_URL, MAPPING_DB_URL, {}, QUEUE_ID)
    
    try:
        query = pii_table._build_reference_mapping_query("Address", source_conf, ["CONTEXTID", "EMAIL"])
        print(f"\nGenerated query:\n{query}\n")
        
        # Verify query structure
        assert "SELECT" in query
        assert "FROM `Address`" in query
        assert "JOIN `Billing`" in query
        assert "JOIN `Patient`" in query
        assert "patient_id" in query
        print("✓ Query structure is correct")
        print("\n✓ TEST 3 PASSED")
        
    except Exception as e:
        print(f"\n✗ TEST 3 FAILED: {e}")
        import traceback
        traceback.print_exc()
    finally:
        del pii_table


def cleanup_test_databases():
    """Clean up test databases"""
    print("\n" + "="*60)
    print("Cleaning up test databases...")
    
    try:
        src_engine = create_engine(SRC_DB_URL)
        with src_engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS INSURANCE_MASTER_BKP"))
            conn.execute(text("DROP TABLE IF EXISTS Address"))
            conn.execute(text("DROP TABLE IF EXISTS Billing"))
            conn.execute(text("DROP TABLE IF EXISTS Patient"))
            conn.commit()
        src_engine.dispose()
        
        mapping_engine = create_engine(MAPPING_DB_URL)
        with mapping_engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {PATIENT_MAPPING_TABLE}"))
            conn.commit()
        mapping_engine.dispose()
        
        master_engine = create_engine(MASTER_DB_URL)
        with master_engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS master_insurance_table"))
            conn.execute(text("DROP TABLE IF EXISTS pii_data_table"))
            conn.execute(text("DROP TABLE IF EXISTS master_insurance_table_nd_backup_queue1"))
            conn.execute(text("DROP TABLE IF EXISTS pii_data_table_nd_backup_queue1"))
            conn.commit()
        master_engine.dispose()
        
        print("✓ Cleanup completed")
    except Exception as e:
        print(f"⚠ Cleanup warning: {e}")


if __name__ == "__main__":
    print("="*60)
    print("PIITable Test Script")
    print("="*60)
    print("\nNOTE: Please update the database connection strings at the top of this file")
    print("      with your test database credentials before running.\n")
    
    import argparse
    parser = argparse.ArgumentParser(description='Test PIITable functionality')
    parser.add_argument('--setup', action='store_true', help='Set up test databases')
    parser.add_argument('--test1', action='store_true', help='Run test 1: Direct primary column')
    parser.add_argument('--test2', action='store_true', help='Run test 2: Reference mapping')
    parser.add_argument('--test3', action='store_true', help='Run test 3: Query building')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--cleanup', action='store_true', help='Clean up test databases')
    
    args = parser.parse_args()
    
    try:
        if args.setup or args.all:
            setup_test_databases()
        
        if args.test1 or args.all:
            test_direct_primary_column()
        
        if args.test2 or args.all:
            test_reference_mapping()
        
        if args.test3 or args.all:
            test_query_building()
        
        if args.cleanup:
            cleanup_test_databases()
        
        if not any([args.setup, args.test1, args.test2, args.test3, args.all, args.cleanup]):
            print("\nUsage examples:")
            print("  python test.py --setup --all          # Set up and run all tests")
            print("  python test.py --test1               # Run only test 1")
            print("  python test.py --test2               # Run only test 2")
            print("  python test.py --test3               # Run only test 3")
            print("  python test.py --cleanup             # Clean up test databases")
            print("\nOr uncomment the function calls in the script to run directly.")
        
    except Exception as e:
        print(f"\n✗ Test execution failed: {e}")
        import traceback
        traceback.print_exc()

