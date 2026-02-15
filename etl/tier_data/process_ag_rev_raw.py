#!/usr/bin/env python3
"""
Process AG_REV tier data from raw CSV file.
Reads directly from the source file to avoid manual transcription errors.

Input: ag_usecase_tier_results.csv
Output: 
  - tier_location_result_ag_rev.csv (location-level tier assignments)
  - tier_result_ag_rev.csv (aggregated tier counts per scenario)
"""

import csv
from collections import defaultdict

# Input/output paths
INPUT_FILE = '/Users/jfantauzza/Downloads/ag_usecase_tier_results.csv'
OUTPUT_LOCATIONS = '/Users/jfantauzza/coeqwal-backend/database/seed_tables/10_tier/tier_location_result_ag_rev.csv'
OUTPUT_RESULTS = '/Users/jfantauzza/coeqwal-backend/database/seed_tables/10_tier/tier_result_ag_rev.csv'

def main():
    # Data structures
    locations = []  # List of (scenario, location_id, tier_level)
    tier_counts = defaultdict(lambda: defaultdict(int))  # {scenario: {tier_level: count}}
    
    print(f"Reading from: {INPUT_FILE}")
    
    # Read the raw CSV
    with open(INPUT_FILE, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            scenario = row['scenario']
            location_id = row['region']
            tier_level = int(row['tier'])
            
            locations.append({
                'scenario_short_code': scenario,
                'tier_short_code': 'AG_REV',
                'location_type': 'demand_unit',
                'location_id': location_id,
                'location_name': '',
                'tier_level': tier_level,
                'tier_value': '',
                'display_order': ''
            })
            
            tier_counts[scenario][tier_level] += 1
    
    print(f"Parsed {len(locations)} location records")
    
    # Count unique locations per scenario
    scenarios = set(loc['scenario_short_code'] for loc in locations)
    for scenario in sorted(scenarios):
        count = sum(1 for loc in locations if loc['scenario_short_code'] == scenario)
        print(f"  {scenario}: {count} locations")
    
    # Write tier_location_result CSV
    print(f"\nWriting locations to: {OUTPUT_LOCATIONS}")
    with open(OUTPUT_LOCATIONS, 'w', newline='') as f:
        fieldnames = ['scenario_short_code', 'tier_short_code', 'location_type', 
                      'location_id', 'location_name', 'tier_level', 'tier_value', 'display_order']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Add display_order per scenario
        display_orders = defaultdict(int)
        for loc in locations:
            scenario = loc['scenario_short_code']
            display_orders[scenario] += 1
            loc['display_order'] = display_orders[scenario]
            writer.writerow(loc)
    
    print(f"  Written {len(locations)} rows")
    
    # Calculate tier_result aggregates
    print(f"\nWriting aggregates to: {OUTPUT_RESULTS}")
    with open(OUTPUT_RESULTS, 'w', newline='') as f:
        fieldnames = ['scenario_short_code', 'tier_short_code', 'tier_1_value', 'tier_2_value',
                      'tier_3_value', 'tier_4_value', 'norm_tier_1', 'norm_tier_2', 
                      'norm_tier_3', 'norm_tier_4', 'total_value', 'single_tier_level']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for scenario in sorted(tier_counts.keys()):
            counts = tier_counts[scenario]
            total = sum(counts.values())
            
            row = {
                'scenario_short_code': scenario,
                'tier_short_code': 'AG_REV',
                'tier_1_value': counts.get(1, 0),
                'tier_2_value': counts.get(2, 0),
                'tier_3_value': counts.get(3, 0),
                'tier_4_value': counts.get(4, 0),
                'norm_tier_1': round(counts.get(1, 0) / total, 3) if total > 0 else 0,
                'norm_tier_2': round(counts.get(2, 0) / total, 3) if total > 0 else 0,
                'norm_tier_3': round(counts.get(3, 0) / total, 3) if total > 0 else 0,
                'norm_tier_4': round(counts.get(4, 0) / total, 3) if total > 0 else 0,
                'total_value': total,
                'single_tier_level': ''
            }
            writer.writerow(row)
            print(f"  {scenario}: T1={counts.get(1,0)}, T2={counts.get(2,0)}, T3={counts.get(3,0)}, T4={counts.get(4,0)}, Total={total}")
    
    print("\n✅ Done!")
    print(f"\nNext steps:")
    print(f"1. Upload these files to S3: s3://coeqwal-seeds-dev/10_tier/")
    print(f"2. Run the upsert SQL in the database")

if __name__ == '__main__':
    main()

