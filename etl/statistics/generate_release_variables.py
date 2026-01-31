#!/usr/bin/env python3
"""
Generate release variable rows for reservoir_variable.csv

This script generates 276 rows (92 reservoirs × 3 release types):
- release_total: C_{code} - Total release from reservoir
- release_normal: C_{code}_NCF - Normal controlled release (≤ capacity)
- release_flood: C_{code}_FLOOD - Flood spill (> release capacity)

From constraints-FloodSpill.wresl:
    C_{xxxxx}_NCF + C_{xxxxx}_Flood = C_{xxxxx}

Usage:
    python generate_release_variables.py > release_variables.csv
    cat release_variables.csv >> reservoir_variable.csv
"""

import csv
import uuid
import sys
from pathlib import Path


def generate_release_variables():
    """Generate release variable rows from reservoir_entity.csv"""

    # Path to reservoir_entity.csv
    base_path = Path(__file__).parent.parent.parent / "database" / "seed_tables" / "04_calsim_data"
    entity_path = base_path / "reservoir_entity.csv"

    # Read reservoir entities
    entities = []
    with open(entity_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            entities.append(row)

    # Variable types to generate
    release_types = [
        {
            'suffix': '',
            'type': 'release_total',
            'name_suffix': 'Total Release',
            'desc_template': 'Total release from {name}'
        },
        {
            'suffix': '_NCF',
            'type': 'release_normal',
            'name_suffix': 'Normal Release',
            'desc_template': 'Normal controlled release from {name} (≤ release capacity)'
        },
        {
            'suffix': '_FLOOD',
            'type': 'release_flood',
            'name_suffix': 'Flood Spill',
            'desc_template': 'Flood spill from {name} (above release capacity)'
        }
    ]

    # Starting ID (after existing ~195 rows)
    next_id = 196

    # Generate rows
    variables = []
    for entity in entities:
        entity_id = entity['id']
        short_code = entity['short_code']
        name = entity['name']

        for rel_type in release_types:
            calsim_id = f"C_{short_code}{rel_type['suffix']}"
            var_name = f"{name} {rel_type['name_suffix']}"
            description = rel_type['desc_template'].format(name=name)

            variables.append({
                'id': next_id,
                'calsim_id': calsim_id,
                'name': var_name,
                'description': description,
                'reservoir_entity_id': entity_id,
                'variable_type': rel_type['type'],
                'is_aggregate': 'false',
                'aggregated_variable_ids': '',
                'trigger_threshold': '',
                'unit_id': 2,  # CFS
                'temporal_scale_id': 3,  # monthly
                'variable_version_id': 1,
                'variable_id': str(uuid.uuid4()),
                'source_ids': '{1,3,4}',
                'created_by': 1,
                'updated_by': ''
            })
            next_id += 1

    return variables


def main():
    """Main entry point"""
    variables = generate_release_variables()

    # CSV header (must match existing reservoir_variable.csv)
    fieldnames = [
        'id', 'calsim_id', 'name', 'description', 'reservoir_entity_id',
        'variable_type', 'is_aggregate', 'aggregated_variable_ids',
        'trigger_threshold', 'unit_id', 'temporal_scale_id',
        'variable_version_id', 'variable_id', 'source_ids',
        'created_by', 'updated_by'
    ]

    # Write to stdout (can be redirected to file)
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)

    # Don't write header since we're appending to existing file
    # writer.writeheader()

    for var in variables:
        writer.writerow(var)

    # Print summary to stderr
    print(f"\nGenerated {len(variables)} release variable rows", file=sys.stderr)
    print(f"  - release_total: 92 rows", file=sys.stderr)
    print(f"  - release_normal: 92 rows", file=sys.stderr)
    print(f"  - release_flood: 92 rows", file=sys.stderr)
    print(f"  - ID range: 196-{196 + len(variables) - 1}", file=sys.stderr)


if __name__ == '__main__':
    main()
