# Environmental river flows: CalSim reference

Reference companion for the `env_flows/` module. The methodology, metric definitions, calculations, reach inventory tables, per-scenario variable availability, and open questions live in the consolidated [statistics README](../README.md#environmental-river-flow-statistics). This file holds the CalSim-3 detail that does not belong in that methodology: the CEFF season catalog and the Sacramento mainstem unimpaired-flow split.

The module computes three metrics, referred to by name throughout: **% unimpaired** (simulated flow as a percent of natural unimpaired flow), **% functional flows** (seasonal flow as a percent of the prescribed functional-flow target), and the **flow alteration index** (Pearson correlation between simulated and unimpaired monthly flow).

## CEFF season catalog

The % functional flows metric is computed by water-year season using the five-season California Environmental Flows Framework (CEFF) calendar. Water month 1 = October, water month 12 = September. These rows are seeded in `env_flow_season` (`database/seed_tables/01_lookup/env_flow_season.csv`), which is the source of truth.

| `short_code` | Name | Calendar months | WY months | Ecological role |
|--------------|------|-----------------|-----------|-----------------|
| `wet_peak` | Wet Season Peak | December, January, February | 3, 4, 5 | High-flow pulse. Creates spawning habitat and flushes fine sediment from gravel beds |
| `wet_base` | Wet Season Base | March, April | 6, 7 | Sustained wet-season baseflow. Maintains inundated floodplain habitat |
| `spring_recession` | Spring Recession | May, June | 8, 9 | Gradual snowmelt recession. Supports juvenile salmon and steelhead outmigration |
| `dry` | Dry Season | July, August, September, October | 10, 11, 12, 1 | Summer and early-fall low flows. Critical thermal-stress period for cold-water fish |
| `fall_pulse` | Fall Pulse | November | 2 | First-flush storm event. Triggers adult salmon migration and opens river-mouth sand bars |

The dry season spans the water-year boundary (WY months 10-12 plus month 1). The ETL groups by `(water_year, season_id)` treating October as the preceding water year's dry season (October 1922 belongs to water year 1922, not 1923).

## Sacramento mainstem unimpaired-flow split

The Sacramento River mainstem uses two unimpaired references, split at Bend Bridge (river mile 257). The assignment lives in `channel_entity.unimp_sv_variable`. `UNIMP_SRBB` ("Sacramento River below Bend Bridge") captures additional inflow (Cottonwood Creek, Stony Creek) between Shasta and Bend Bridge.

| Sub-reach | Position | `UNIMP_*` | Channels |
|-----------|----------|-----------|----------|
| SAC_UPPER | Above Bend Bridge | `UNIMP_SHAS` | SHSTA, KSWCK, SAC289 |
| SAC_LOWER | At and below Bend Bridge | `UNIMP_SRBB` | SAC257, SAC240, SAC201, SAC148, SAC122, SAC120, SAC085, SAC083, SAC049, SAC048, SAC041, SAC029B, SAC007, SAC000, SSL001, YBP020 |

The Mokelumne has no `UNIMP_MOK` variable in the CalSim SV (it is fully regulated by East Bay MUD below Camanche Reservoir), so `unimp_sv_variable` is NULL for MOK019 and MOK028. The % unimpaired and flow-alteration-index metrics cannot be computed there. The % functional flows metric against `EFLOWS_MOK028` still can.

## Resolved reference questions

Decisions settled during the initial build, kept here as a decision log.

| Question | Resolution |
|----------|------------|
| MOK028 unimpaired mapping | No `UNIMP_MOK` in the SV. The % unimpaired and flow-alteration-index metrics are skipped for Mokelumne reaches. The % functional flows metric (against EFLOWS) is still computed for MOK028 |
| SAC mainstem unimpaired sub-reach mapping | Split at Bend Bridge (rm 257): `UNIMP_SHAS` above, `UNIMP_SRBB` at and below (see table above) |
| Season calendar | CEFF five-season calendar, seeded in `env_flow_season` |
| Trend-report CSV staging | Not required. All variables are in the standard `_coeqwal_calsim_output.csv` (DV) and `_coeqwal_sv_input.csv` (SV) already staged in S3 |
| `UNIMP_*_UHH` variants | Excluded. The `_UHH` suffix is an upper-half-hydrology alternative baseline. Always use the base `UNIMP_*` names |
| `C_SAC000_MIF` absence | SAC000 has no MIF companion in the DV (`has_mif = false`). The % unimpaired metric uses `UNIMP_SRBB`, and the % functional flows metric uses `EFLOWS_SAC000` from the SV |
