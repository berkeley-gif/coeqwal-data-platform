"""
Fix channel_entity.csv — repair corrupted source_ids column.

The CSV was generated with source_ids values like {1,3,4} written as unquoted
multi-line quoted fields.  The effect is that csv.DictReader maps:

  entity_version_id → multi-line garbage string   (should be integer 1)
  source_ids        → '3'                         (should be '{1,3,4}')

This script:
  1. Reads the CSV with DictReader (which correctly handles multi-line fields)
  2. Resets entity_version_id = '1' and source_ids = '{1,3,4}' for every row
  3. Writes a clean, properly-quoted CSV back to the same path

Run from repo root:
    python3 database/scripts/fix_channel_entity_csv.py
"""

import csv
import os

INFILE  = "database/seed_tables/04_calsim_data/channel_entity.csv"
TMPFILE = INFILE + ".tmp"

FIELDNAMES = [
    "network_arc_id", "short_code", "name", "description", "subtype",
    "entity_type_id", "schematic_type_id", "hydrologic_region_id",
    "boundary_condition", "from_node", "to_node", "length_m",
    "has_tiers", "is_main", "has_gis_data", "entity_version_id", "source_ids",
    "watershed_short_code", "unimp_sv_variable", "has_mif", "has_eflows",
    "channel_class",
]

fixed = 0
total = 0

with open(INFILE, newline="", encoding="utf-8") as infile, \
     open(TMPFILE, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    writer = csv.DictWriter(
        outfile,
        fieldnames=FIELDNAMES,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\r\n",
    )
    writer.writeheader()

    for row in reader:
        total += 1

        # Drop ghost rows that have no network_arc_id — these are artefacts of
        # the original multi-line source_ids CSV corruption.
        if not row.get("network_arc_id", "").strip():
            fixed += 1
            continue

        # Detect and fix the corruption: entity_version_id should always be
        # a small integer (1).  If it contains a newline or non-numeric
        # characters it is the multi-line garbage field.
        ev = row.get("entity_version_id", "")
        if not ev.strip().lstrip("-").isdigit():
            row["entity_version_id"] = "1"
            row["source_ids"]        = "{1,3,4}"
            fixed += 1

        # has_mif and has_eflows are NOT NULL BOOLEAN columns.
        # Non-DV rows have empty strings which become NULL on COPY → violation.
        # Default to 'false'; migration 23 will update the 60 DV channels.
        if not row.get("has_mif", "").strip():
            row["has_mif"] = "false"
        if not row.get("has_eflows", "").strip():
            row["has_eflows"] = "false"

        # Ensure the new env-flow columns are present (may be missing in old rows)
        for col in ("watershed_short_code", "unimp_sv_variable",
                    "has_mif", "has_eflows", "channel_class"):
            if col not in row:
                row[col] = ""

        writer.writerow({f: row.get(f, "") for f in FIELDNAMES})

# Atomic replace
os.replace(TMPFILE, INFILE)

print(f"Done.  {total} rows processed, {fixed} rows repaired.")
print(f"Output written to {INFILE}")
