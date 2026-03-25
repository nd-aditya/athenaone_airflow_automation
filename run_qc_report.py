#!/usr/bin/env python3
"""
Standalone QC report runner.
Edit the schema names below and run:
    python run_qc_report.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# ─── SET YOUR SCHEMA NAMES HERE ───────────────────────────────────────────────
DIFF_SCHEMA = "diff_20260324"
DEID_SCHEMA  = "diff_20260324_deid"
# ──────────────────────────────────────────────────────────────────────────────

from services.qc_service import run_qc
from services.email_service import send_qc_report_email

if __name__ == "__main__":
    print(f"Running QC: {DIFF_SCHEMA}  vs  {DEID_SCHEMA}")
    result = run_qc(DIFF_SCHEMA, DEID_SCHEMA)
    print(f"  PASS: {result['pass_count']}  |  NEED_TO_CHECK: {result['fail_count']}  |  Errors: {len(result['errors'])}")

    sent = send_qc_report_email(result)
    if sent:
        print("Email sent successfully.")
    else:
        print("Email failed — check EMAIL_SENDER / EMAIL_APP_PASSWORD in services/config.py")
