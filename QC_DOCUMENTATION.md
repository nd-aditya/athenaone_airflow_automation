# Athenaone Deidentification QC Documentation

## Overview

The QC process validates that the deidentification pipeline has correctly processed all eligible records. After deidentification runs, it compares two schemas:

- **Diff schema** (`diff_YYYYMMDD`) — snapshot of the historical schema for this run (source of truth)
- **Deid schema** (`diff_YYYYMMDD_deid`) — deidentified output produced by the deid tool

For each table, it calculates:

| Column | Description |
|---|---|
| **Orig Count** | Total rows in the diff schema |
| **Deid Count** | Total rows in the deid schema |
| **Diff** | `Orig Count - Deid Count` — rows not deidentified |
| **Ignore Rows** | Rows legitimately excluded because their patient identifier is not present in the mapping tables |
| **Status** | PASS / NEED TO CHECK / FAILED (see below) |

---

## Status Logic

| Status | Condition | Meaning |
|---|---|---|
| **PASS** | `abs(Diff) == Ignore Rows` | Every non-mappable row is accounted for |
| **NEED TO CHECK** | `abs(Diff) != Ignore Rows` | More or fewer rows dropped than expected |
| **FAILED** | Table missing from deid schema entirely | Table was never deidentified |

> Tables with **Orig Count = 0** are excluded from the report entirely.
> When **Diff = 0** the comment column is left blank regardless of ignore rows.

---

## How Ignore Rows Are Calculated

Ignore rows = rows in the diff schema whose patient identifier does **not** exist in the mapping tables. These records are expected to be absent from the deid schema because the patient was never mapped.

There are four lookup strategies, selected per table via `TABLE_IDENTIFIER_MAP`:

### 1. Direct Column
The table has a patient identifier column directly.
```sql
SELECT COUNT(*) FROM diff_schema.TABLE t
WHERE NOT EXISTS (
    SELECT 1 FROM mapping_schema.mapping_table m
    WHERE m.col = t.col
)
```

### 2. Custom Mapping Table Join
The table has a non-patient identifier (e.g. `clinicalencounterid`) that is checked against a specific mapping table.
```sql
SELECT COUNT(*) FROM diff_schema.TABLE t
WHERE NOT EXISTS (
    SELECT 1 FROM mapping_schema.mapping_table m
    WHERE m.mapping_col = t.join_col
)
```

### 3. Single Reference Hop
The table links to another historical table that holds the patient identifier.
```sql
SELECT COUNT(DISTINCT t.nd_auto_increment_id)
FROM diff_schema.TABLE t
LEFT JOIN historical_schema.REF_TABLE r ON t.join_col = r.join_col
WHERE r.ref_col IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM mapping_schema.patient_mapping_table m
    WHERE m.ref_col = r.ref_col
)
```

### 4. Reference Chain (Multi-hop)
The table requires multiple joins to reach the patient identifier.
```sql
SELECT COUNT(DISTINCT t.nd_auto_increment_id)
FROM diff_schema.TABLE t
LEFT JOIN historical_schema.TABLE_A r0 ON t.col_A = r0.col_A
LEFT JOIN historical_schema.TABLE_B r1 ON r0.col_B = r1.col_B
WHERE r1.patient_col IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM mapping_schema.patient_mapping_table m
    WHERE m.patient_col = r1.patient_col
)
```

### Auto-detection Fallback
Tables **not listed** in `TABLE_IDENTIFIER_MAP` are auto-detected in this order:
1. Check for `patientID` column → direct check against `patient_mapping_table`
2. Check for `chartID` column → direct check against `patient_mapping_table`
3. Check for `documentID` column → single hop via `DOCUMENT` table → check `patientID`
4. None found → Ignore Rows = N/A, Comment = "No patient identifier column in this table"

---

## Table Identifier Map Reference

### Mapping Schemas Used

| Schema variable | Schema name | Purpose |
|---|---|---|
| `MAPPING_SCHEMA` | `mapping_prod` | Default — patient and encounter mappings |
| `BRIDGE_TABLE_SCHEMA` | `BRIDGE_TABLES_TNG` | Bridge tables for document/appointment/encounter-linked tables |

---

### Full Table Mapping

| Table | Strategy | Join Column | Mapping Schema | Mapping Table | Mapping Column |
|---|---|---|---|---|---|
| ALLERGY | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| APPOINTMENT | Custom mapping | PATIENT_ID | mapping_prod | patient_mapping_table | patientid |
| APPOINTMENTNOTE | Custom mapping | APPOINTMENTID | BRIDGE_TABLES_TNG | bridge_table_APPOINTMENTNOTE | APPOINTMENTID |
| APPOINTMENTVIEW | Direct | — | mapping_prod | patient_mapping_table | PATIENTID |
| CHART | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| CHARTQUESTIONNAIRE | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| CLINICALENCOUNTER | Custom mapping | clinicalencounterid | mapping_prod | encounter_mapping_table | encounter_id |
| CLINICALENCOUNTERDATA | Custom mapping | clinicalencounterid | mapping_prod | encounter_mapping_table | encounter_id |
| CLINICALENCOUNTERDIAGNOSIS | Custom mapping | clinicalencounterid | mapping_prod | encounter_mapping_table | encounter_id |
| CLINICALENCOUNTERPREPNOTE | Custom mapping | clinicalencounterid | mapping_prod | encounter_mapping_table | encounter_id |
| CLINICALPRESCRIPTION | Custom mapping | DOCUMENTID | BRIDGE_TABLES_TNG | bridge_table_clinicalprescription | DOCUMENTID |
| CLINICALRESULT | Custom mapping | DOCUMENTID | BRIDGE_TABLES_TNG | bridge_table_clinicalresult | DOCUMENTID |
| CLINICALRESULTOBSERVATION | Custom mapping | CLINICALRESULTID | BRIDGE_TABLES_TNG | bridge_table_clinicalresultobservation | CLINICALRESULTID |
| CLINICALSERVICE | Custom mapping | clinicalencounterid | mapping_prod | encounter_mapping_table | encounter_id |
| CLINICALTEMPLATE | Custom mapping | clinicalencounterid | mapping_prod | encounter_mapping_table | encounter_id |
| DOCUMENT | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| PATIENTMEDICATION | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| PATIENTPASTMEDICALHISTORY | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| PATIENTSOCIALHISTORY | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| PATIENTSURGERY | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| PATIENTSURGICALHISTORY | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| SOCIALHXFORMRESPONSE | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| SOCIALHXFORMRESPONSEANSWER | Custom mapping | SOCIALHXFORMRESPONSEID | BRIDGE_TABLES_TNG | bridge_table_socialhxformresponseanswer | SOCIALHXFORMRESPONSEID |
| VISIT | Direct | — | mapping_prod | patient_mapping_table | PATIENTID |
| VITALATTRIBUTEREADING | Direct | — | mapping_prod | patient_mapping_table | CHARTID |
| VITALSIGN | Custom mapping | clinicalencounterid | mapping_prod | encounter_mapping_table | encounter_id |
| All other tables | Auto-detect | patientID → chartID → documentID | mapping_prod | patient_mapping_table | — |

---

## Running the QC

### Full QC with Email (all tables in diff schema)
```bash
python run_qc_report.py
```
Edit schema names at the top of the file:
```python
DIFF_SCHEMA = "diff_20260325"
DEID_SCHEMA  = "diff_20260325_deid"
```

### Priority Tables Only — Terminal Output
```bash
python run_qc_priority.py
```
Edit schema names and email flag at the top:
```python
DIFF_SCHEMA  = "Tng-athenaone"
DEID_SCHEMA  = "deidentified_merged"
SEND_EMAIL   = False   # Set True to also send email
```

### Automated via DAG 2
The QC runs automatically after the deidentification workers stop in **DAG 2 (Athenaone_Deid_Priority_Tables)**. It runs in parallel with the merge step and emails results to `EMAIL_RECIPIENTS` in `config.py`.

---

## Email Report

The email report contains:
- **Client name heading** — from `CLIENT_NAME` in `config.py`
- **Summary badges** — counts of PASS / NEED TO CHECK / FAILED / Errors
- **Per-table rows** — colour-coded: green (PASS), yellow (NEED TO CHECK), red (FAILED)
- **Errors section** — tables that threw exceptions during QC

Email is sent via Gmail SMTP using:
```python
EMAIL_SENDER       = 'aditya.goyal@neurodiscovery.ai'
EMAIL_APP_PASSWORD = '...'          # Gmail App Password
EMAIL_RECIPIENTS   = ['...', '...']
```

---

## Adding or Updating a Table Mapping

Edit `TABLE_IDENTIFIER_MAP` in `services/qc_service.py`. Choose the appropriate form:

```python
# Direct column
"NEWTABLE": {"col": "CHARTID"},

# Custom mapping table join
"NEWTABLE": {"join_col": "clinicalencounterid",
             "mapping_table": "encounter_mapping_table",
             "mapping_col": "encounter_id"},

# Custom mapping table with non-default schema
"NEWTABLE": {"join_col": "DOCUMENTID",
             "mapping_schema": BRIDGE_TABLE_SCHEMA,
             "mapping_table": "bridge_table_newtable",
             "mapping_col": "DOCUMENTID"},

# Single reference hop via a historical table
"NEWTABLE": {"ref_table": "DOCUMENT",
             "join_col": "DOCUMENTID",
             "ref_col": "PATIENTID"},

# Multi-hop chain
"NEWTABLE": {"chain": [("CLINICALRESULT", "CLINICALRESULTID"),
                        ("DOCUMENT", "DOCUMENTID")],
             "col": "patientID"},
```

The same `TABLE_IDENTIFIER_MAP` in `qc_service.py` is used by both `run_qc_report.py` (full QC + email) and `run_qc_priority.py` (priority tables + terminal). Changes only need to be made in one place.
