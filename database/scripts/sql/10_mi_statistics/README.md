# M&I (Municipal & Industrial) Statistics Tables

This directory contains SQL scripts for creating and loading tables related to Municipal & Industrial water delivery statistics.

## Table Overview

### Entity Tables (reference data)

| Table | Description | Records |
|-------|-------------|---------|
| `du_urban_entity` | Urban demand unit metadata (region, type, acres) | 126 |
| `du_urban_variable` | CalSim variable mappings for 71 canonical CWS units | 71 |
| `du_urban_delivery_arc` | Multi-arc delivery mappings for units requiring summation | 10 |
| `du_urban_group` | Groupings of demand units (tier, nod, sod) | 6 |
| `du_urban_group_member` | Group membership mappings | 71 |
| `mi_contractor` | SWP/CVP contractor entities | 30 |
| `mi_contractor_group` | Contractor groupings | 6 |
| `mi_contractor_delivery_arc` | Contractor delivery arc mappings | 39 |
| `cws_aggregate_entity` | System-level aggregate definitions | 6 |

### Statistics Tables (scenario results)

| Table | Description |
|-------|-------------|
| `du_delivery_monthly` | Monthly delivery statistics per demand unit |
| `du_shortage_monthly` | Monthly shortage statistics per demand unit |
| `du_period_summary` | Period-of-record summary per demand unit |
| `mi_delivery_monthly` | Monthly delivery statistics per contractor |
| `mi_shortage_monthly` | Monthly shortage statistics per contractor |
| `mi_contractor_period_summary` | Period-of-record summary per contractor |
| `cws_aggregate_monthly` | Monthly statistics for system aggregates |
| `cws_aggregate_period_summary` | Period summary for system aggregates |

## Script Execution Order

```bash
# 1. Create and load demand unit entities
psql -f 01_create_du_urban_entity.sql
psql -f 01b_load_du_urban_entity_from_s3.sql
psql -f 01c_create_du_urban_variable.sql
psql -f 01d_load_du_urban_variable.sql

# 2. Create demand unit statistics tables and groups
psql -f 02_create_du_statistics_tables.sql
psql -f 02b_create_du_urban_group_tables.sql
psql -f 02c_load_du_urban_group_from_s3.sql

# 3. Create and load contractor entities
psql -f 03_create_mi_contractor_entity_tables.sql
psql -f 04_load_mi_contractor_entity_from_s3.sql

# 4. Create contractor statistics tables
psql -f 05_create_mi_statistics_tables.sql

# 5. Create CWS aggregate tables (with seed data)
psql -f 06_create_cws_aggregate_tables.sql
```

## CalSim Variable Naming Conventions

CalSim3 uses consistent prefixes to identify variable types:

| Prefix | Meaning | Example | Used For |
|--------|---------|---------|----------|
| `DL_*` | Total Delivery | `DL_02_PU` | WBA demand unit total delivery |
| `DN_*` | Demand Node | `DN_02_PU` | Water demand/request |
| `D_*` | Arc Delivery | `D_WTPNBR_FRFLD` | Delivery from specific source |
| `D_*_PMI` | M&I Arc Delivery | `D_SBA029_ACWD_PMI` | SWP contractor M&I delivery |
| `SHRTG_*` | Shortage | `SHRTG_02_PU` | Surface water shortage (demand - delivery) |
| `GW_SHORT_*` | GW Restriction Shortage | `GW_SHORT_71_NU` | Groundwater pumping restriction shortage |
| `GP_*` | Groundwater Pumping | `GP_71_NU` | Groundwater extraction |
| `SHORT_D_*` | Arc Shortage | `SHORT_D_SBA029_ACWD_PMI` | SWP contractor shortage |

### Variable Type Categories in `du_urban_variable`

The `variable_type` column indicates how to extract delivery data:

| Type | Description | Delivery Variable | Shortage Variable |
|------|-------------|-------------------|-------------------|
| `DL` | WBA total delivery | `DL_{du_id}` | `SHRTG_{du_id}` |
| `D` | Arc delivery (may need summing) | `D_*_{du_id}` | `SHORT_D_*` or none |
| `GP` | Groundwater only | `GP_{du_id}` | `GW_SHORT_{du_id}` |
| `MISSING` | No CalSim variable found | N/A | N/A |

## Canonical CWS Demand Units (71 units)

The 71 canonical Community Water System demand units come from the tier matrix (`etl/pipelines/CWS/all_scenarios_tier_matrix.csv`). These are the units with tier scores across scenarios.

### Variable Mapping Categories

**Category 1: WBA-Style Units with DL_* delivery (40 units)**
- Examples: `02_PU`, `26N_NU1`, `90_PU`
- Delivery: `DL_{du_id}`
- Shortage: `SHRTG_{du_id}` or `GW_SHORT_{du_id}`

**Category 2: Groundwater-Only Units (3 units)**
- Units: `71_NU`, `72_NU`, `72_PU`
- These have NO surface water delivery (no `DL_*` variable)
- Only groundwater pumping: `GP_{du_id}`
- Shortage: `GW_SHORT_{du_id}` (groundwater restriction shortage)

**Category 3: SWP Contractor Deliveries (10 units)**
- Examples: `CSB038`, `ESB324`, `SBA029`
- Delivery: `D_{arc}_{contractor}_PMI`
- Shortage: `SHORT_D_{arc}_{contractor}_PMI`

**Category 4: Named Localities (15 units)**
- Examples: `FRFLD`, `NAPA`, `MWD`
- Delivery: Various `D_*` patterns (water treatment plants, etc.)
- Some require summing multiple arcs (see `du_urban_delivery_arc`)

**Category 5: Not Found (2 units)**
- Units: `JLIND`, `UPANG`
- No matching CalSim variables in scenario output
- Marked as `variable_type = 'MISSING'`

## Known Discrepancies

### 1. Groundwater-Only Communities

The following demand units have **no surface water delivery** in CalSim output:

| du_id | Variables Found | Notes |
|-------|-----------------|-------|
| `71_NU` | `GP_71_NU`, `GW_SHORT_71_NU` | Zone 71 - Patterson area, groundwater dependent |
| `72_NU` | `GP_72_NU`, `GW_SHORT_72_NU` | Zone 72 - Los Banos/Newman area, groundwater dependent |
| `72_PU` | `GP_72_PU`, `GW_SHORT_72_PU` | Zone 72 Project Urban, groundwater dependent |

These communities rely entirely on groundwater pumping. The `GW_SHORT_*` variable represents shortage due to groundwater pumping restrictions, not delivery shortage.

### 2. Entity Table ID Mismatch

The `du_urban_entity` seed file contains `72_PU2` but the tier matrix and CalSim variables use `72_PU`. This needs investigation:
- CalSim output has: `GP_72_PU`, `GW_SHORT_72_PU`
- Entity table has: `72_PU2`

### 3. Missing CalSim Variables

Two canonical demand units have no matching CalSim variables:
- `JLIND` (Jenny Lind/Valley Springs) - Calaveras County WD
- `UPANG` (City of Angels) - Union PUD

These may be supplied via different mechanisms or aggregated elsewhere.

### 4. Multi-Arc Delivery Units

Some demand units receive water from multiple sources that must be summed:

| du_id | Delivery Arcs |
|-------|---------------|
| `AMADR` | `D_TBAUD_AMADR_NU` + `D_TGC003_AMADR_NU` |
| `AMCYN` | `D_WTPAMC_AMCYN` + `D_WTPJAC_AMCYN` |
| `ANTOC` | `D_CCC007_ANTOC` + `D_SJR006_ANTOC` |
| `FRFLD` | `D_WTPNBR_FRFLD` + `D_WTPWMN_FRFLD` |
| `GRSVL` | `D_CSD014_GRSVL` + `D_DES006_GRSVL` |

These are tracked in `du_urban_delivery_arc` with `requires_sum = TRUE` in `du_urban_variable`.

## Data Sources

- **CalSim3 Main Report**: Variable naming conventions
- **WRESL model files**: Variable definitions
- **Geopackage**: Demand unit geometries and metadata
- **Tier matrix CSV**: Canonical list of 71 CWS units
- **Scenario output CSV**: Verification of available variables (s0020_coeqwal_calsim_output.csv)
