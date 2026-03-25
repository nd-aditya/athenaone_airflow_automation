#!/usr/bin/env python3
"""
One-time cleanup: find TableMetadata records that are case-insensitive duplicates,
keep the one that has PHI config (or the oldest if neither/both have config),
and delete the rest.

Run from the project root:
    python cleanup_duplicate_table_metadata.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

_DEID_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "Deid_service", "deidentification", "deIdentification")
)
sys.path.insert(0, _DEID_PROJECT_ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

import django
django.setup()

from nd_api_v2.models.table_details import TableMetadata

# ── find all case-insensitive duplicate groups ────────────────────────────────
all_metadata = list(TableMetadata.objects.all())
seen = {}   # normalised_name -> list of TableMetadata objects

for obj in all_metadata:
    key = obj.table_name.upper()
    seen.setdefault(key, []).append(obj)

duplicates = {k: v for k, v in seen.items() if len(v) > 1}

if not duplicates:
    print("No duplicates found.")
    sys.exit(0)

print(f"Found {len(duplicates)} duplicate group(s):\n")

to_delete = []

for norm_name, records in duplicates.items():
    # Sort: prefer records that have table_details_for_ui configured (PHI config present)
    # then by pk ascending (oldest first) as tiebreaker.
    def has_config(r):
        details = r.table_details_for_ui
        if not details:
            return False
        # Check if any column has a non-default rule configured
        if isinstance(details, list):
            return any(
                col.get("rule") not in (None, "", "KEEP")
                for col in details
                if isinstance(col, dict)
            )
        return False

    records_sorted = sorted(records, key=lambda r: (not has_config(r), r.pk))
    keeper = records_sorted[0]
    losers = records_sorted[1:]

    print(f"  Group: {norm_name}")
    print(f"    KEEP  → id={keeper.pk}  name='{keeper.table_name}'  has_config={has_config(keeper)}")
    for r in losers:
        print(f"    DELETE→ id={r.pk}  name='{r.table_name}'  has_config={has_config(r)}")
    print()

    to_delete.extend(losers)

# ── confirm before deleting ───────────────────────────────────────────────────
answer = input(f"Delete {len(to_delete)} duplicate record(s)? [y/N]: ").strip().lower()
if answer != "y":
    print("Aborted. Nothing was deleted.")
    sys.exit(0)

delete_ids = [r.pk for r in to_delete]
deleted_count, _ = TableMetadata.objects.filter(pk__in=delete_ids).delete()
print(f"\nDeleted {deleted_count} record(s). Done.")
