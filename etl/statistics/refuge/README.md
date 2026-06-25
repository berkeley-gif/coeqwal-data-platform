# Wildlife refuge delivery: CalSim reference

Reference companion for the `refuge/` module. The metrics, reliability definition, unit conversion, DSS date convention, shortage handling, demand-unit summary (managed-by, provider, GW/SW), output tables, and open questions live in the consolidated [statistics README](../README.md#wildlife-refuge-statistics). This file holds the CalSim-3 detail that does not belong in that methodology narrative: the delivery variable inventory and the demand-unit conveyance and composition reference.

## Where the entity data lives

The full refuge demand-unit catalog is a database table, not just documentation. `du_refuge_entity` (DDL and 18-row seed: `database/sql_archive/log/20_create_refuge_entity_table.sql`, `database/seed_tables/04_calsim_data/du_refuge_entity.csv`) carries `refuge_or_wildlife_area`, `managed_by`, `provider`, `gw`, `sw`, `point_of_diversion_conveyance`, `cs3_type`, `total_acres`, `polygon_count`, and `hydrologic_region`. The `refuge_du_full` view denormalizes these for API and frontend use and decodes `cs3_type` into a label (`PR` = Project Refuge, CVP contract deliveries; `NR` = Non-project Refuge, water rights only).

The tables below are the human-readable reference drawn from CalSim 3 Main Report Tables 3-9 (Sacramento) and 3-10 (San Joaquin and Tulare Lake). For live values always query `du_refuge_entity` or `refuge_du_full`.

## Delivery variables (`DN_{DU_ID}`)

All 18 refuge deliveries are read from the DV output (`*_coeqwal_calsim_output.csv`) by Part B variable name. Part C is `SW-DELIVERY-NET` for Sacramento and `SW_DELIVERY-NET` (underscore) for San Joaquin and Tulare, but the loader selects by Part B so the difference does not affect selection.

| Region | `DN_{DU_ID}` variables |
|--------|------------------------|
| Sacramento | `DN_08N_PR1`, `DN_08N_PR2`, `DN_08S_PR`, `DN_09_PR`, `DN_11_PR`, `DN_17N_NR`, `DN_17N_PR`, `DN_17S_PR` |
| San Joaquin / Tulare | `DN_63_PR1`, `DN_63_PR2`, `DN_63_PR3`, `DN_72_PR1`, `DN_72_PR2`, `DN_72_PR3`, `DN_72_PR4`, `DN_72_PR5`, `DN_72_PR6`, `DN_91_PR` |

Shortage is not derived from delivery alone. The WRESL model defines a `meetAW` constraint for refuge DUs, so model shortage variables exist for all 18 (`SHRTG_*` for Sacramento, `GW_SHORT_*` for San Joaquin and Tulare). The ETL prefers those and falls back to `max(AW - DN, 0)` only when a shortage column is missing. See the [statistics README shortage section](../README.md#shortage-model-variables-preferred).

## Point of diversion and conveyance

Sacramento River hydrologic region (Table 3-9):

| DU_ID | Refuge / wildlife area | Point of diversion / conveyance |
|-------|------------------------|---------------------------------|
| `08N_PR1` | Sacramento NWR | Glenn-Colusa Canal |
| `08N_PR2` | Delevan NWR | Glenn-Colusa Canal |
| `08S_PR` | Colusa NWR | Glenn-Colusa Canal, Colusa Basin Drain |
| `09_PR` | Llano Seco, Upper Butte Basin WA, Sacramento River NWR | Sacramento River, Butte Creek |
| `11_PR` | Upper Butte Basin WA (Little Dry Creek and Howard Slough) | Thermalito Afterbay via Western Canal and Sutter-Butte Canal |
| `17N_NR` | Butte Sink Duck Clubs | Thermalito Afterbay via Western Canal, Butte Creek |
| `17N_PR` | Gray Lodge WA | Thermalito Afterbay via Biggs-West Gridley WD canals |
| `17S_PR` | Sutter NWR | Sutter Bypass, Sutter Extension Canal |

San Joaquin River and Tulare Lake hydrologic regions (Table 3-10):

| DU_ID | Refuge / wildlife area | Point of diversion / conveyance |
|-------|------------------------|---------------------------------|
| `63_PR1` | Arena Plains and Snow Bird units, Merced NWR | Drainage water from Turlock ID and Stevenson WC |
| `63_PR2` | Merced and Lone Tree Units, Merced NWR | Merced ID, via Deadman Creek and Duck Slough |
| `63_PR3` | East Bear Creek Unit, San Luis NWR | Eastside Bypass, Bear Creek, Livingston Drain |
| `72_PR1` | Volta WA | Delta-Mendota Canal via the Volta Wasteway, Central California ID Main Canal |
| `72_PR2` | Kesterson NWR, Freitas and Blue Goose Units (San Luis NWR) | Grassland WD via San Luis Canal, Santa Fe Canal, and Fremont Canal |
| `72_PR3` | San Luis Unit and West Bear Creek Unit, San Luis NWR | San Luis Canal Company via island C Canal, Salt Slough |
| `72_PR4` | Los Banos WA, Gadwall/Salt Slough/China Island Units (North Grassland WA) | San Luis Canal Company via San Pedro Canal, West Delta Canal, Grassland WD Boundary Drain, and Salt Slough |
| `72_PR5` | Grassland WD north | Delta-Mendota Canal via the Volta Wasteway, Central California ID Main Canal |
| `72_PR6` | Grassland WD south | Central California ID via Main Canal, Arroyo Canal, and San Pedro Canal |
| `91_PR` | Mendota WA | Mendota Pool via Fresno Slough |

## Notes from CalSim 3 Main Report

- `09_PR` encompasses Llano Seco Unit (CDFW) and Sacramento River NWR (USFWS), two managing agencies.
- `17N_NR` is the only non-project (NR) unit. It represents private Butte Sink duck clubs, not a federally managed refuge.
- `72_PR2` aggregates Kesterson NWR plus the Freitas and Blue Goose Units of San Luis NWR.
- `72_PR3` aggregates the San Luis Unit and West Bear Creek Unit of San Luis NWR.
- `72_PR4` aggregates Los Banos WA plus three North Grassland WA units (8 polygons total).
- `72_PR5` and `72_PR6` are privately managed Grassland Water District lands.
- `91_PR` is in the Tulare Lake hydrologic region but diverts from Fresno Slough (San Joaquin watershed), and has no GIS geometry in the database, so its acreage is not recorded.
