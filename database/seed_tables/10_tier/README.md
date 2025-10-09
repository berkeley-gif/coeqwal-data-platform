# TIER SEED TABLES

## Overview

Tier tables store interpretive framework indicator values for scenario reporting. Tiers represent performance levels (1-4) for various water system indicators, designed for D3.js visualization.

## Table Structure

### tier_definition.csv
Defines the tier indicators and their characteristics.

**Columns:**
- `short_code`: Unique identifier (e.g., ENV_FLOWS, DELTA_ECOLOGY)
- `name`: Display name for reporting
- `description`: Detailed description of the indicator
- `tier_type`: 'multi_value' (4 tier values) or 'single_value' (1 tier level)
- `tier_count`: Number of tier values (1 or 4)
- `is_active`: Whether indicator is currently used

### tier_result.csv
Stores actual tier values for each scenario.

**Columns:**
- `scenario_short_code`: Scenario identifier (s0011, s0020, s0021)
- `tier_short_code`: Links to tier_definition.short_code
- `tier_1_value` through `tier_4_value`: Individual tier counts (NULL for single-value tiers)
- `norm_tier_1` through `norm_tier_4`: Normalized values (0-1 scale) for D3 charts
- `total_value`: Sum of all tier values (for multi-value normalization)
- `single_tier_level`: Single tier level 1-4 (for single-value tiers)

## Tier Types

### Multi-Value Tiers
Have 4 separate values representing counts/amounts in each tier level:
- Environmental flows: Count of locations in each tier
- Reservoir storage: Count of reservoirs in each tier
- Groundwater storage: Count of areas in each tier
- Community water system deliveries: (future)
- Agricultural revenue: (future)

### Single-Value Tiers
Have one value representing the overall tier level (1-4):
- Delta ecology: Overall tier level
- Freshwater for in-Delta uses: Overall tier level
- Freshwater for Delta exports: Overall tier level
- Salmon abundance: Overall tier level

## D3 Bar Chart Comparability

### Multi-Value Tiers
- **Raw counts**: tier_1_value through tier_4_value (e.g., [0, 5, 12, 0])
- **Normalized values**: norm_tier_1 through norm_tier_4 (e.g., [0, 0.294, 0.706, 0])
- **Pre-calculated**: No client-side normalization needed
- **Example**: ENV_FLOWS raw [0,5,12,0] → normalized [0, 0.294, 0.706, 0]
- **Visualization**: Direct use in D3 stacked bars or lollipops

### Single-Value Tiers
- **Single level**: single_tier_level (1-4)
- **Example**: DELTA_ECOLOGY = 4 (Tier 4 performance)
- **Visualization**: Single bar at tier level or color indicator

### Pre-Calculated Normalization
Multi-value tiers include pre-calculated normalized values for D3 efficiency:
```
norm_tier_1 = tier_1_value / total_value
norm_tier_2 = tier_2_value / total_value
norm_tier_3 = tier_3_value / total_value
norm_tier_4 = tier_4_value / total_value
```

**Benefits:**
- No client-side calculation needed
- Direct use in D3 bar width/height
- All multi-value tiers on same 0-1 scale
- Faster chart rendering

## Current Data

**Scenarios:** s0011, s0020, s0021
**Indicators:** 9 total (7 with data, 2 future)
**Results:** 21 scenario-indicator combinations

## Usage Examples

### Query all tier results for a scenario:
```sql
SELECT td.name, tr.weighted_score, tr.total_value
FROM tier_result tr
JOIN tier_definition td ON tr.tier_short_code = td.short_code
WHERE tr.scenario_short_code = 's0011'
ORDER BY tr.weighted_score DESC;
```

### Compare scenarios on weighted scores:
```sql
SELECT tr.tier_short_code, td.name,
       AVG(tr.weighted_score) as avg_score,
       MIN(tr.weighted_score) as min_score,
       MAX(tr.weighted_score) as max_score
FROM tier_result tr
JOIN tier_definition td ON tr.tier_short_code = td.short_code
GROUP BY tr.tier_short_code, td.name
ORDER BY avg_score DESC;
```
