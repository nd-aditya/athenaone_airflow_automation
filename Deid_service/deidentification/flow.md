# We will register the client

# User will register the client with following details
- ClientName
- PatientID Start Value: 1001101011
- Default Offset Value: 30/34 etc
- emr_type : AnthenaOne/eCW
- patient_identifier_columns: ["PATIENTID", "CHARTID", "PROFILEID", "PID"]

# For register client user will register the first dump (history dump)
### In background we will generate the stats for register dump, load all the tables into the 
### postgres database for portal operations

# Mapping/Master Table creation + ND Auto column add to all the table
### Then to start the futher process first we have to create (if first dump)/update (if not first dump) the
### mapping table, master table and we have to add nd_auto_increment_id column to each of the source table
### for this we will ask user to provide the below config (only for the first dump or historical dump, if its not
### the first dump then we will use mapping and master table config from the previous dump)

### Master table config
```python
pii_tables_config = {
    "pii_data_table": {
        "source_tables": {
            "table1": {
                "primary_column_name": "patient_id",
                "primary_column_type": "PATIENTID",
                "required_columns": [{'notes': 'Notes1'}],
            },
            "table2": {
                "primary_column_name": "chartid",
                "primary_column_type": "CHARTID",
                "required_columns": [{'dept': 'Dept1'}, {"notes2": "Notes1"}],
            }
        }
    }
}
```

### Mapping Table Config
```python
example_config = {
    "schema_name": "dummy_mapping_schema_output",
    "patient_mapping_config": {
        "primary_id_column": "PATIENTID",
        "tables": [
            {
                "table_name": "table1",
                "columns": {
                    "PATIENTID": "patient_id",
                    "CHARTID": "chartid",
                },
            },
            {
                "table_name": "table2",
                "columns": {
                    "CHARTID": "chartid",
                    "PROFILEID": "profileid",
                },
            },
            {
                "table_name": "table4",
                "columns": {
                    "PROFILEID": "profileid",
                    "PID": "pid",
                },
            },
        ],
    },
    "encounter_mapping_config": {
        "table_name": "encounters",
        "encounter_id_column": "enc_id",
        "patient_identifier_column": "patient_id",
        "patient_identifier_type": "PATIENTID",
        "encounter_date_column": "registration_date",
    },
}
```
### for the nd_auto_increment_id column, either user can use the previously uploaded csv or can upload the new one
### if there is any table difference then we can ask client to upload the csv for newly tables only (a csv contains newly added tables or existing tables which is updated the previous csv)

# Once the config is set we will create independent task to generate/update the master table, mapping table and nd_auto_increment_id column add task

# once this task is completed, we are ready for the de-identification
## we will ask user to pass the pii masking config, qc config and other process related config include

### PII Masking
```python
pii_config = {}
secondary_config = {}
```

### QC Config
```python
qc_config = {
    "PATIENT_PATIENTID": {"prefix_value": "100100", "length_of_value": 18},
    "PATIENT_CHARTID": {"prefix_value": "100100", "length_of_value": 18},
    "ENCOUNTER_ID": {"prefix_value": "110100", "length_of_value": None},
}
```

### run config
```python
run_config = {
    'auto_qc_enabled': True,
    'auto_gcp_upload_enabled': True,
    "auto_embedding_generation_enabled": True
}
```

# Other Information
- If any table failed during the de-identification, failed due to code bug, or failed in QC, then there will be option to select failed tables and restart the process for that task, also if will start from where it failed and not whole pipeline 
- If auto QC/GCP upload is not enabled, users can manually start the process by selecting the tables

