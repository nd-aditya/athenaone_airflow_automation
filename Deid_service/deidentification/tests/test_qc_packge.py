import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/rohitchouhan/Documents/Code/backend/deidentification/deIdentification/"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from nd_api.models import Table, ClientDataDump
from worker.models import Task, Chain
from django.contrib.auth.models import User
from keycloakauth.rolemodel import RoleModel, get_default_permissions
from nd_scripts.create_account import create_account


def clean_db():
    RoleModel.objects.all().delete()
    User.objects.all().delete()
    Chain.objects.all().delete()
    ClientDataDump.objects.all().delete()


table_config = {
    "batch_size": 1,
    "ignore_config": {"operator": "and", "ignore_rows": []},
    "columns_details": [
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "uid",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "PATIENT_ID",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<PATIENT_NAME>>",
            "column_name": "patient_name",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<PATIENT_NAME>>",
            "column_name": "uname",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<UPWD>>",
            "column_name": "upwd",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<PATIENT_NAME>>",
            "column_name": "first_name",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": False,
            "mask_value": None,
            "column_name": "last_name",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": None,
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<CITY>>",
            "column_name": "city",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": False,
            "mask_value": None,
            "column_name": "state",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": None,
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<PO_BOX_STREET>>",
            "column_name": "address",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "zipcode",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "ZIP_CODE",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "dob",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "PATIENT_DOB",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<EMAIL_ID>>",
            "column_name": "email",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<SSN>>",
            "column_name": "ssn",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": "<<PHONE_NUMBER>>",
            "column_name": "phone",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "MASK",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": False,
            "mask_value": None,
            "column_name": "sex",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": None,
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "register_date",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "DATE_OFFSET",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "notes",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": "NOTES",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": False,
            "mask_value": None,
            "column_name": "UserType",
            "ignore_column": {},
            "add_to_phi_table": False,
            "de_identification_rule": None,
            "column_name_for_phi_table": None,
        },
    ],
    "reference_enc_id_column": None,
    "reference_patient_id_column": "uid",
}

table_name = "users"

from qc_package.scanner import DbScanner
Chain.objects.all().delete()
src_connstr = "mysql+pymysql://root:123456789@localhost:3306/nddenttest"
dest_connstr = "mysql+pymysql://root:123456789@localhost:3306/deidentify_client_34_dump_34"
mapping_config = {
    "connection_str": "mysql+pymysql://root:123456789@localhost:3306/nddenttest_helper",
    "inhouse_mapping_table": False,
}

qc_config = {
    "PATIENT_ID": {"prefix_value": "100100", "length_of_value": 18},
    "ENCOUNTER_ID": {"prefix_value": "110100", "length_of_value": None},
}
db_scanner = DbScanner(src_connstr, dest_connstr, mapping_config, qc_config)

table_obj = Table.objects.get(table_name="users3")
output_result = db_scanner.scan_table(table_config, table_obj.id, read_limit=1000)
print(output_result)

# clean_db()

# prefi
# length_of_value


# Table1
# PatientID, registerdate, Notes ....
# 10 , 10-10-2024, "random notes"
# 10 , 10-12-2024, "random notes"
# 10 , 15-12-2024, "random notes"

# deidentified table: Table1
# PatientID, registerdate, Notes ....
# 11001111101 , 20-10-2024, "random notes"
# 11001111101 , 20-12-2024, "random notes"
# 11001111101 , 25-12-2024, "random notes"
