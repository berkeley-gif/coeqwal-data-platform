# COEQWAL Monthly Database Audit

**Generated:** 2026-05-24 14:39:51  

**Database:** coeqwal_scenario  
**Connected as:** jfantauzza  
**PostgreSQL:** PostgreSQL 17.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC  
**Total DB size:** 495 MB  



## 1. DATABASE CONTENT AUDIT


### 1a. Table inventory

| table | layer | columns | rows | audit_trigger |
| --- | --- | --- | --- | --- |
| ag_aggregate_entity | 03_entity | 13 | 9 | yes |
| ag_aggregate_monthly | 10+_results | 29 | 8,208 | yes |
| ag_aggregate_period_summary | 10+_results | 31 | 684 | yes |
| ag_du_demand_monthly | 10+_results | 26 | 119,472 | yes |
| ag_du_gw_pumping_monthly | 10+_results | 27 | 119,472 | yes |
| ag_du_period_summary | 10+_results | 52 | 9,956 | yes |
| ag_du_shortage_monthly | 10+_results | 28 | 67,668 | yes |
| ag_du_sw_delivery_monthly | 10+_results | 26 | 118,560 | yes |
| assumption_category | 05_assumptions_operations | 9 | 2 | yes |
| assumption_definition | 05_assumptions_operations | 13 | 6 | yes |
| audit_log | 00_versioning | 13 | 0 | no |
| calsim_model_variable_type | 04_variable | 9 | 8 | yes |
| channel_entity | 03_entity | 28 | 669 | yes |
| channel_variable | 04_variable | 19 | 1,352 | yes |
| compliance_station | 03_entity | 16 | 2 | yes |
| cws_aggregate_entity | 03_entity | 14 | 6 | yes |
| cws_aggregate_monthly | 10+_results | 45 | 5,472 | yes |
| cws_aggregate_period_summary | 10+_results | 36 | 456 | yes |
| delta_monthly | 10+_results | 27 | 7,296 | yes |
| delta_period_summary | 10+_results | 14 | 608 | yes |
| derived_variable_type | 04_variable | 9 | 4 | yes |
| developer | 00_versioning | 16 | 6 | yes |
| domain_family_map | 00_versioning | 10 | 93 | yes |
| du_agriculture_entity | 03_entity | 36 | 144 | yes |
| du_delivery_monthly | 10+_results | 28 | 73,872 | yes |
| du_period_summary | 10+_results | 33 | 6,156 | yes |
| du_refuge_entity | 03_entity | 26 | 18 | yes |
| du_shortage_monthly | 10+_results | 27 | 38,688 | yes |
| du_urban_delivery_arc | 03_entity | 9 | 57 | yes |
| du_urban_entity | 03_entity | 27 | 145 | yes |
| du_urban_group | 03_entity | 10 | 11 | yes |
| du_urban_group_member | 03_entity | 9 | 142 | yes |
| du_urban_variable | 04_variable | 16 | 90 | yes |
| env_flow_channel_monthly | 10+_results | 58 | 53,808 | yes |
| env_flow_channel_period_summary | 10+_results | 32 | 4,484 | yes |
| env_flow_channel_seasonal | 10+_results | 61 | 22,420 | yes |
| env_flow_season | 10+_results | 12 | 5 | yes |
| geometry_type | 01_lookup | 9 | 4 | yes |
| hydroclimate | 07_hydroclimate | 17 | 6 | yes |
| hydrologic_region | 01_lookup | 8 | 7 | yes |
| mi_contractor | 03_entity | 14 | 30 | yes |
| mi_contractor_delivery_arc | 03_entity | 9 | 39 | yes |
| mi_contractor_group | 03_entity | 10 | 6 | yes |
| mi_contractor_group_member | 03_entity | 9 | 60 | yes |
| mi_contractor_period_summary | 10+_results | 34 | 1,745 | yes |
| mi_delivery_monthly | 10+_results | 28 | 20,940 | yes |
| mi_shortage_monthly | 10+_results | 27 | 20,940 | yes |
| model_source | 01_lookup | 10 | 1 | yes |
| network | 02_network | 19 | 6,908 | yes |
| network_arc | 02_network | 15 | 2,610 | yes |
| network_entity_type | 01_lookup | 9 | 4 | yes |
| network_gis | 02_network | 14 | 4,154 | yes |
| network_node | 02_network | 17 | 1,544 | yes |
| network_subtype | 01_lookup | 12 | 28 | yes |
| network_type | 01_lookup | 12 | 21 | yes |
| operation_category | 05_assumptions_operations | 9 | 9 | yes |
| operation_definition | 05_assumptions_operations | 13 | 28 | yes |
| refuge_du_delivery_monthly | 10+_results | 26 | 16,416 | yes |
| refuge_du_period_summary | 10+_results | 32 | 1,368 | yes |
| refuge_du_shortage_monthly | 10+_results | 29 | 16,416 | yes |
| reservoir | 03_entity | 16 | 7 | yes |
| reservoir_entity | 03_entity | 23 | 92 | yes |
| reservoir_group | 03_entity | 10 | 4 | yes |
| reservoir_group_member | 03_entity | 9 | 24 | yes |
| reservoir_monthly_percentile | 10+_results | 26 | 82,080 | yes |
| reservoir_period_summary | 10+_results | 43 | 6,840 | yes |
| reservoir_spill_monthly | 10+_results | 18 | 16,416 | yes |
| reservoir_storage_monthly | 10+_results | 42 | 82,080 | yes |
| scenario | 06_scenario | 13 | 77 | yes |
| scenario_author | 06_scenario | 11 | 3 | yes |
| scenario_backup | other | 13 | 75 | no |
| scenario_hydroclimate_sibling | 06_scenario | 9 | 27 | yes |
| scenario_key_assumption_link | 05_assumptions_operations | 6 | 73 | yes |
| scenario_key_operation_link | 05_assumptions_operations | 6 | 514 | yes |
| scenario_tag | 06_scenario | 9 | 10 | yes |
| scenario_tag_link | 06_scenario | 6 | 109 | yes |
| sensitivity_climate | other | 15 | 306,272 | no |
| sensitivity_operational | other | 15 | 39,702 | no |
| slr | 07_hydroclimate | 11 | 4 | yes |
| source | 01_lookup | 8 | 12 | yes |
| spatial_ref_sys | other | 5 | 8,500 | no |
| spatial_scale | 01_lookup | 9 | 11 | yes |
| statistic_category | 01_lookup | 8 | 3 | yes |
| statistic_type | 01_lookup | 9 | 20 | yes |
| temporal_scale | 01_lookup | 9 | 8 | yes |
| theme | 08_theme | 18 | 6 | yes |
| theme_scenario_link | 08_theme | 6 | 79 | yes |
| tier_definition | 10+_results | 12 | 9 | yes |
| tier_location | other | 10 | 280 | no |
| tier_location_result | 10+_results | 14 | 21,076 | yes |
| tier_result | 10+_results | 19 | 655 | yes |
| unit | 01_lookup | 9 | 5 | yes |
| variable_type | 04_variable | 9 | 6 | yes |
| version | 00_versioning | 9 | 14 | yes |
| version_family | 00_versioning | 9 | 14 | yes |
| watershed | 01_lookup | 11 | 13 | yes |
| wba | 03_entity | 15 | 42 | yes |

_Schema snapshot saved to `schema_snapshot.json`. Tables summary saved to `tables_summary.csv`._


### 1b. Schema vs. ERD comparison

**Tables in ERD but NOT in DB:** cws_du_link, cws_entity, cws_list, cws_list_du_member


**Tables with ERD stub entries (no column tree, skipped from column check):** 97

  `ag_aggregate_entity`, `ag_aggregate_monthly`, `ag_aggregate_period_summary`, `ag_du_demand_monthly`, `ag_du_gw_pumping_monthly`, `ag_du_period_summary`, `ag_du_shortage_monthly`, `ag_du_sw_delivery_monthly`, `assumption_category`, `assumption_definition`, `audit_log`, `calsim_model_variable_type`, `channel_entity`, `channel_variable`, `compliance_station`, `cws_aggregate_entity`, `cws_aggregate_monthly`, `cws_aggregate_period_summary`, `delta_monthly`, `delta_period_summary`, `derived_variable_type`, `developer`, `domain_family_map`, `du_agriculture_entity`, `du_delivery_monthly`, `du_period_summary`, `du_refuge_entity`, `du_shortage_monthly`, `du_urban_delivery_arc`, `du_urban_entity`, `du_urban_group`, `du_urban_group_member`, `du_urban_variable`, `env_flow_channel_monthly`, `env_flow_channel_period_summary`, `env_flow_channel_seasonal`, `env_flow_season`, `geometry_type`, `hydroclimate`, `hydrologic_region`, `mi_contractor`, `mi_contractor_delivery_arc`, `mi_contractor_group`, `mi_contractor_group_member`, `mi_contractor_period_summary`, `mi_delivery_monthly`, `mi_shortage_monthly`, `model_source`, `network`, `network_arc`, `network_entity_type`, `network_gis`, `network_node`, `network_subtype`, `network_type`, `operation_category`, `operation_definition`, `refuge_du_delivery_monthly`, `refuge_du_period_summary`, `refuge_du_shortage_monthly`, `reservoir`, `reservoir_entity`, `reservoir_group`, `reservoir_group_member`, `reservoir_monthly_percentile`, `reservoir_period_summary`, `reservoir_spill_monthly`, `reservoir_storage_monthly`, `scenario`, `scenario_author`, `scenario_backup`, `scenario_hydroclimate_sibling`, `scenario_key_assumption_link`, `scenario_key_operation_link`, `scenario_tag`, `scenario_tag_link`, `sensitivity_climate`, `sensitivity_operational`, `slr`, `source`, `spatial_ref_sys`, `spatial_scale`, `statistic_category`, `statistic_type`, `temporal_scale`, `theme`, `theme_scenario_link`, `tier_definition`, `tier_location`, `tier_location_result`, `tier_result`, `unit`, `variable_type`, `version`, `version_family`, `watershed`, `wba`

  _Add column trees to the ERD to enable column-level verification for these tables._


_Correct tables: 0_


### 1c. Row counts vs. expected (layers 00-08)

| table | actual | expected | status |
| --- | --- | --- | --- |
| audit_log | 0 | — | OK (no target) |
| compliance_station | 2 | 2 | PASS |
| developer | 6 | 2 | PASS |
| domain_family_map | 93 | — | OK (no target) |
| du_agriculture_entity | 144 | 144 | PASS |
| du_refuge_entity | 18 | 18 | PASS |
| du_urban_entity | 145 | 145 | PASS |
| geometry_type | 4 | 4 | PASS |
| hydrologic_region | 7 | 7 | PASS |
| mi_contractor | 30 | 30 | PASS |
| model_source | 1 | 1 | PASS |
| network | 6,908 | 6,908 | PASS |
| network_arc | 2,610 | 2,610 | PASS |
| network_entity_type | 4 | 4 | PASS |
| network_gis | 4,154 | 4,154 | PASS |
| network_node | 1,544 | 1,544 | PASS |
| network_subtype | 28 | 28 | PASS |
| network_type | 21 | 21 | PASS |
| reservoir | 7 | 7 | PASS |
| reservoir_entity | 92 | 92 | PASS |
| scenario | 77 | — | OK (no target) |
| scenario_tag | 10 | 10 | PASS |
| scenario_tag_link | 109 | — | OK (no target) |
| source | 12 | 12 | PASS |
| spatial_scale | 11 | — | OK (no target) |
| statistic_category | 3 | 3 | PASS |
| statistic_type | 20 | 20 | PASS |
| temporal_scale | 8 | — | OK (no target) |
| theme | 6 | — | OK (no target) |
| theme_scenario_link | 79 | — | OK (no target) |
| unit | 5 | — | OK (no target) |
| version | 14 | 14 | PASS |
| version_family | 14 | 14 | PASS |
| watershed | 13 | — | OK (no target) |
| wba | 42 | 42 | PASS |


### 1d. Reference data downloads (layers 00-08)

| layer | table | rows |
| --- | --- | --- |
| 00_versioning | developer | 6 |
| 00_versioning | version_family | 14 |
| 00_versioning | version | 14 |
| 00_versioning | domain_family_map | 93 |
| 00_versioning | audit_log | 0 |
| 01_lookup | hydrologic_region | 7 |
| 01_lookup | source | 12 |
| 01_lookup | model_source | 1 |
| 01_lookup | unit | 5 |
| 01_lookup | spatial_scale | 11 |
| 01_lookup | temporal_scale | 8 |
| 01_lookup | statistic_category | 3 |
| 01_lookup | statistic_type | 20 |
| 01_lookup | geometry_type | 4 |
| 01_lookup | network_entity_type | 4 |
| 01_lookup | network_type | 21 |
| 01_lookup | network_subtype | 28 |
| 01_lookup | watershed | 13 |
| 02_network | network | 6908 |
| 02_network | network_arc | 2610 |
| 02_network | network_node | 1544 |
| 02_network | network_gis | 4154 |
| 03_entity | reservoir | 7 |
| 03_entity | compliance_station | 2 |
| 03_entity | du_agriculture_entity | 144 |
| 03_entity | du_urban_entity | 145 |
| 03_entity | du_refuge_entity | 18 |
| 03_entity | reservoir_entity | 92 |
| 03_entity | mi_contractor | 30 |
| 03_entity | wba | 42 |
| 03_entity | channel_entity | 669 |
| 03_entity | ag_aggregate_entity | 9 |
| 03_entity | cws_aggregate_entity | 6 |
| 03_entity | du_urban_group | 11 |
| 03_entity | du_urban_group_member | 142 |
| 03_entity | du_urban_delivery_arc | 57 |
| 03_entity | mi_contractor_delivery_arc | 39 |
| 03_entity | mi_contractor_group | 6 |
| 03_entity | mi_contractor_group_member | 60 |
| 03_entity | reservoir_group | 4 |
| 03_entity | reservoir_group_member | 24 |
| 04_variable | calsim_model_variable_type | 8 |
| 04_variable | derived_variable_type | 4 |
| 04_variable | variable_type | 6 |
| 04_variable | channel_variable | 1352 |
| 04_variable | du_urban_variable | 90 |
| 05_assumptions_operations | assumption_category | 2 |
| 05_assumptions_operations | assumption_definition | 6 |
| 05_assumptions_operations | operation_category | 9 |
| 05_assumptions_operations | operation_definition | 28 |
| 05_assumptions_operations | scenario_key_assumption_link | 73 |
| 05_assumptions_operations | scenario_key_operation_link | 514 |
| 06_scenario | scenario_hydroclimate_sibling | 27 |
| 06_scenario | scenario | 77 |
| 06_scenario | scenario_author | 3 |
| 06_scenario | scenario_tag | 10 |
| 06_scenario | scenario_tag_link | 109 |
| 07_hydroclimate | hydroclimate | 6 |
| 07_hydroclimate | slr | 4 |
| 08_theme | theme | 6 |
| 08_theme | theme_scenario_link | 79 |

_CSVs written to `layer_exports/`._


### 1e. Results data samples (layers 10+)

_First 10 and last 10 rows per table._

| table | head | tail | status |
| --- | --- | --- | --- |
| tier_definition | 9 | 9 | OK |
| tier_result | 10 | 10 | OK |
| tier_location_result | 10 | 10 | OK |
| reservoir_storage_monthly | 10 | 10 | OK |
| reservoir_spill_monthly | 10 | 10 | OK |
| reservoir_period_summary | 10 | 10 | OK |
| reservoir_monthly_percentile | 10 | 10 | OK |
| du_delivery_monthly | 10 | 10 | OK |
| du_shortage_monthly | 10 | 10 | OK |
| du_period_summary | 10 | 10 | OK |
| mi_delivery_monthly | 10 | 10 | OK |
| mi_shortage_monthly | 10 | 10 | OK |
| mi_contractor_period_summary | 10 | 10 | OK |
| cws_aggregate_monthly | 10 | 10 | OK |
| cws_aggregate_period_summary | 10 | 10 | OK |
| ag_du_demand_monthly | 10 | 10 | OK |
| ag_du_gw_pumping_monthly | 10 | 10 | OK |
| ag_du_sw_delivery_monthly | 10 | 10 | OK |
| ag_du_shortage_monthly | 10 | 10 | OK |
| ag_du_period_summary | 10 | 10 | OK |
| ag_aggregate_monthly | 10 | 10 | OK |
| ag_aggregate_period_summary | 10 | 10 | OK |
| refuge_du_delivery_monthly | 10 | 10 | OK |
| refuge_du_shortage_monthly | 10 | 10 | OK |
| refuge_du_period_summary | 10 | 10 | OK |
| env_flow_season | 5 | 5 | OK |
| env_flow_channel_monthly | 10 | 10 | OK |
| env_flow_channel_seasonal | 10 | 10 | OK |
| env_flow_channel_period_summary | 10 | 10 | OK |
| delta_monthly | 10 | 10 | OK |
| delta_period_summary | 10 | 10 | OK |

_CSVs written to `results_samples/`._


## 2. DATABASE CONTENT VERIFICATION


### 2a. NULL audit fields

_Rows with created_by = NULL — trigger was not active during insert. Should return no rows._

_No rows returned._


### 2a. Orphaned statistics rows

_Results rows referencing non-existent scenarios. Should all be 0._

| table_name | orphan_rows |
| --- | --- |
| reservoir_period_summary | 0 |
| mi_contractor_period_summary | 0 |
| ag_aggregate_period_summary | 0 |


### 2a. Invalid water_month values

_water_month must be 1-12. Non-zero = data integrity error._

| table_name | invalid_count |
| --- | --- |
| mi_delivery_monthly | 0 |
| du_delivery_monthly | 0 |
| ag_du_demand_monthly | 0 |


### 2a. Tables missing audit trigger

These tables have audit columns but no `set_audit_fields()` trigger: scenario_backup, tier_location


### 2a. Tables attributed to system account only

Every row has `created_by = 1` (system). Likely mis-attributed bulk loads: ag_aggregate_monthly, ag_aggregate_period_summary, ag_du_demand_monthly, ag_du_gw_pumping_monthly, ag_du_period_summary, ag_du_shortage_monthly, ag_du_sw_delivery_monthly, cws_aggregate_monthly, cws_aggregate_period_summary, delta_monthly, delta_period_summary, du_delivery_monthly, du_period_summary, du_shortage_monthly, mi_contractor_period_summary, mi_delivery_monthly, mi_shortage_monthly


### 2b. Per-scenario ETL coverage

_Every active scenario should have non-zero rows in each results table. Zeros indicate a missed ETL run._

| short_code | is_active | reservoir | du_delivery | ag_delivery | mi_summary | tiers |
| --- | --- | --- | --- | --- | --- | --- |
| s0011 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0020 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0021 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0022 | False | 1080 | 972 | 1572 | 23 | 0 |
| s0023 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0024 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0025 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0026 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0027 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0028 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0029 | False | 1080 | 972 | 1572 | 23 | 8 |
| s0030 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0031 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0032 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0033 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0035 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0036 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0037 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0038 | False | 0 | 0 | 0 | 0 | 0 |
| s0039 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0040 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0041 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0042 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0044 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0045 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0046 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0047 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0048 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0049 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0050 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0051 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0056 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0057 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0058 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0059 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0060 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0062 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0063 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0065 | True | 1080 | 972 | 1572 | 22 | 8 |
| s0067 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0068 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0069 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0070 | False | 1080 | 972 | 1572 | 23 | 0 |
| s0071 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0072 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0073 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0074 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0075 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0076 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0077 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0078 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0079 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0080 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0081 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0082 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0083 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0084 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0085 | True | 1080 | 972 | 1572 | 22 | 9 |
| s0087 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0088 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0089 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0090 | False | 1080 | 972 | 1572 | 23 | 0 |
| s0091 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0092 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0093 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0094 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0095 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0096 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0097 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0098 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0099 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0100 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0101 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0102 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0103 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0104 | True | 1080 | 972 | 1572 | 23 | 9 |
| s0105 | True | 1080 | 972 | 1572 | 22 | 9 |


### 2c. ETL accuracy status summary

_Reports directory not found at `/home/ec2-user/environment/coeqwal-backend/audits/verification_reports`. Run `verify_all_sections.py` to generate verification reports._


## 3. DATABASE HEALTH


### 3a. Cache hit ratio

_Should be > 99%. Below that = too many disk reads._

| cache_hits | disk_reads | cache_hit_pct |
| --- | --- | --- |
| 1287416858 | 6127 | 100.00 |


### 3b. Connection utilization

_Watch for pct_used > 80%. Many idle = connection leak._

| active_connections | max_connections | pct_used | idle | active | waiting |
| --- | --- | --- | --- | --- | --- |
| 5 | 1705 | 0.3 | 1 | 1 | 2 |


### 3c. Dead tuple accumulation

_Dead tuples are old row versions left by UPDATE/DELETE. High counts after ETL = autovacuum is behind._

| table_name | live_rows | dead_rows | dead_pct | last_autovacuum | last_autoanalyze |
| --- | --- | --- | --- | --- | --- |
| sensitivity_climate | 306272 | 8299 | 2.6 | 2026-04-09 12:16 | 2026-04-09 12:16 |
| sensitivity_operational | 39702 | 3429 | 8.0 | 2026-04-09 12:15 | 2026-04-09 12:16 |

> **What is a dead tuple?** PostgreSQL never overwrites a row in-place. UPDATE/DELETE marks the old version 'dead'; it stays on disk until VACUUM reclaims it. High dead_pct wastes storage and slows scans. Run `VACUUM ANALYZE <table>` after large ETL loads if autovacuum hasn't caught up.


### 3d. Table bloat estimate

_bloat_pct > 20% = significant wasted space. VACUUM ANALYZE recommended._

| table_name | current_size | dead_rows | live_rows | bloat_pct |
| --- | --- | --- | --- | --- |
| sensitivity_climate | 48 MB | 8299 | 306272 | 2.6 |
| sensitivity_operational | 6328 kB | 3429 | 39702 | 8.0 |


## 4. DATABASE COST


### 4a. Table sizes (top 25)

_Total = data + indexes._

| table_name | total_size | data_size | index_size | bytes |
| --- | --- | --- | --- | --- |
| sensitivity_climate | 98 MB | 48 MB | 50 MB | 102359040 |
| ag_du_demand_monthly | 36 MB | 22 MB | 14 MB | 37371904 |
| ag_du_gw_pumping_monthly | 34 MB | 21 MB | 13 MB | 35209216 |
| ag_du_sw_delivery_monthly | 33 MB | 20 MB | 12 MB | 34152448 |
| reservoir_storage_monthly | 28 MB | 17 MB | 11 MB | 28925952 |
| env_flow_channel_monthly | 27 MB | 21 MB | 6744 kB | 28762112 |
| reservoir_monthly_percentile | 27 MB | 16 MB | 12 MB | 28524544 |
| du_delivery_monthly | 23 MB | 13 MB | 10096 kB | 24190976 |
| ag_du_shortage_monthly | 18 MB | 10 MB | 8416 kB | 19357696 |
| network_gis | 17 MB | 4512 kB | 13 MB | 18309120 |
| du_agriculture_entity | 17 MB | 472 kB | 17 MB | 18014208 |
| tier_location_result | 14 MB | 5096 kB | 8736 kB | 14163968 |
| sensitivity_operational | 12 MB | 6328 kB | 6360 kB | 12992512 |
| du_shortage_monthly | 11 MB | 5760 kB | 5448 kB | 11476992 |
| env_flow_channel_seasonal | 10 MB | 7544 kB | 3096 kB | 10895360 |
| spatial_ref_sys | 7144 kB | 6896 kB | 248 kB | 7315456 |
| mi_delivery_monthly | 6888 kB | 4080 kB | 2808 kB | 7053312 |
| mi_shortage_monthly | 5984 kB | 3176 kB | 2808 kB | 6127616 |
| reservoir_spill_monthly | 4912 kB | 1824 kB | 3088 kB | 5029888 |
| wba | 4840 kB | 24 kB | 4816 kB | 4956160 |
| refuge_du_delivery_monthly | 4656 kB | 2800 kB | 1856 kB | 4767744 |
| ag_du_period_summary | 4560 kB | 3128 kB | 1432 kB | 4669440 |
| refuge_du_shortage_monthly | 4496 kB | 2640 kB | 1856 kB | 4603904 |
| reservoir_period_summary | 3136 kB | 1464 kB | 1672 kB | 3211264 |
| du_urban_entity | 3008 kB | 512 kB | 2496 kB | 3080192 |


### 4b. Unused indexes

_idx_scan = 0 since last stats reset. PKs and UNIQUE constraints excluded. Each unused index adds write overhead with no read benefit._

| table_name | index_name | index_size | times_used |
| --- | --- | --- | --- |
| ag_du_gw_pumping_monthly | uq_ag_du_gw_pumping_monthly | 6256 kB | 0 |
| ag_du_sw_delivery_monthly | uq_ag_du_sw_delivery_monthly | 5976 kB | 0 |
| env_flow_channel_monthly | uq_env_flow_monthly | 3448 kB | 0 |
| mi_shortage_monthly | uq_mi_shortage_monthly | 1256 kB | 0 |
| refuge_du_shortage_monthly | uq_refuge_shortage_monthly | 872 kB | 0 |
| sensitivity_operational | idx_sensitivity_operational_module_month | 624 kB | 0 |
| reservoir_monthly_percentile | idx_reservoir_percentile_active | 600 kB | 0 |
| reservoir_storage_monthly | idx_storage_monthly_active | 560 kB | 0 |
| reservoir_spill_monthly | idx_spill_monthly_frequency | 512 kB | 0 |
| ag_aggregate_monthly | uq_ag_aggregate_monthly | 456 kB | 0 |
| env_flow_channel_seasonal | idx_env_flow_seasonal_arc_scenario | 456 kB | 0 |
| ag_du_period_summary | uq_ag_du_period_summary | 448 kB | 0 |
| cws_aggregate_monthly | uq_cws_aggregate_monthly | 248 kB | 0 |
| network | idx_network_source_list | 248 kB | 0 |
| reservoir_period_summary | idx_period_summary_cv | 232 kB | 0 |
| env_flow_channel_seasonal | idx_env_flow_seasonal_season | 224 kB | 0 |
| reservoir_period_summary | idx_period_summary_dead_prob | 216 kB | 0 |
| reservoir_spill_monthly | idx_spill_monthly_entity | 192 kB | 0 |
| network_gis | idx_network_gis_network_id | 192 kB | 0 |
| env_flow_channel_period_summary | uq_env_flow_period_summary | 176 kB | 0 |
| env_flow_channel_period_summary | idx_env_flow_period_summary_arc_scenario | 176 kB | 0 |
| network_gis | idx_network_gis_geom | 176 kB | 0 |
| reservoir_spill_monthly | idx_spill_monthly_active | 168 kB | 0 |
| network | idx_network_model_list | 160 kB | 0 |
| reservoir_period_summary | idx_period_summary_dead_pool_prob | 144 kB | 0 |
| network_arc | idx_network_arc_connectivity | 136 kB | 0 |
| network | idx_network_strm_code | 80 kB | 0 |
| reservoir_period_summary | idx_period_summary_active | 80 kB | 0 |
| network_gis | idx_network_gis_precision | 72 kB | 0 |
| refuge_du_period_summary | uq_refuge_period_summary | 72 kB | 0 |
| cws_aggregate_monthly | idx_cws_aggregate_monthly_entity | 64 kB | 0 |
| network_arc | idx_network_arc_to_node | 64 kB | 0 |
| network_arc | idx_network_arc_from_node | 64 kB | 0 |
| channel_variable | idx_channel_variable_entity | 64 kB | 0 |
| env_flow_channel_period_summary | idx_env_flow_period_summary_arc | 56 kB | 0 |
| reservoir_period_summary | idx_period_summary_flood_prob | 48 kB | 0 |
| delta_period_summary | uq_delta_period_summary | 40 kB | 0 |
| network_node | idx_network_node_strm_code | 40 kB | 0 |
| network_node | idx_network_node_version | 32 kB | 0 |
| refuge_du_period_summary | idx_refuge_period_summary_du_id | 32 kB | 0 |
| scenario_key_assumption_link | idx_scenario_assumption_reverse | 16 kB | 0 |
| hydroclimate | idx_hydroclimate_active | 16 kB | 0 |
| hydroclimate | idx_hydroclimate_source | 16 kB | 0 |
| cws_aggregate_period_summary | idx_cws_agg_period_aggregate | 16 kB | 0 |
| assumption_definition | idx_assumption_definition_active | 16 kB | 0 |
| assumption_definition | idx_assumption_definition_category_id | 16 kB | 0 |
| operation_definition | idx_operation_definition_active | 16 kB | 0 |
| calsim_model_variable_type | idx_calsim_model_variable_type_active | 16 kB | 0 |
| derived_variable_type | idx_derived_variable_type_active | 16 kB | 0 |
| operation_definition | idx_operation_definition_category_id | 16 kB | 0 |
| slr | idx_slr_active | 16 kB | 0 |
| du_refuge_entity | idx_du_refuge_entity_hydrologic_region | 16 kB | 0 |
| du_refuge_entity | idx_du_refuge_entity_cs3_type | 16 kB | 0 |
| channel_entity | idx_channel_entity_has_eflows | 16 kB | 0 |
| mi_contractor_delivery_arc | idx_mi_contractor_delivery_arc_arc | 16 kB | 0 |
| tier_definition | idx_tier_definition_version | 16 kB | 0 |
| tier_definition | idx_tier_definition_active | 16 kB | 0 |
| tier_result | idx_tier_result_version | 16 kB | 0 |
| scenario | idx_scenario_run_name_active | 16 kB | 0 |
| scenario | idx_scenario_active | 16 kB | 0 |
| scenario | idx_scenario_hydroclimate | 16 kB | 0 |
| scenario | idx_scenario_active_version | 16 kB | 0 |
| scenario_tag_link | idx_scenario_tag_link_reverse | 16 kB | 0 |
| network_type | idx_network_type_active | 16 kB | 0 |
| network_subtype | idx_network_subtype_type | 16 kB | 0 |
| network_subtype | idx_network_subtype_active | 16 kB | 0 |
| tier_location | idx_tier_location_active | 16 kB | 0 |
| network_entity_type | idx_network_entity_type_active | 16 kB | 0 |
| wba | idx_wba_id | 16 kB | 0 |
| compliance_station | idx_compliance_tier | 16 kB | 0 |
| reservoir_entity | idx_reservoir_entity_region | 16 kB | 0 |
| reservoir_group_member | uq_reservoir_group_member | 16 kB | 0 |
| reservoir_group_member | idx_reservoir_group_member_reservoir | 16 kB | 0 |
| du_urban_entity | idx_du_urban_entity_wba_id | 16 kB | 0 |
| du_urban_entity | idx_du_urban_entity_type | 16 kB | 0 |
| scenario_hydroclimate_sibling | idx_hydro_sibling_baseline | 16 kB | 0 |
| du_urban_group_member | uq_du_urban_group_member | 16 kB | 0 |
| mi_contractor_group_member | uq_mi_contractor_group_member | 16 kB | 0 |
| mi_contractor_delivery_arc | uq_delivery_arc | 16 kB | 0 |
| mi_contractor | idx_mi_contractor_project | 16 kB | 0 |
| mi_contractor | idx_mi_contractor_type | 16 kB | 0 |
| mi_contractor | idx_mi_contractor_region | 16 kB | 0 |
| mi_contractor_group_member | idx_mi_contractor_group_member_contractor | 16 kB | 0 |
| tier_definition | idx_tier_definition_tier_type | 16 kB | 0 |
| cws_aggregate_entity | idx_cws_aggregate_entity_project | 16 kB | 0 |
| cws_aggregate_period_summary | idx_cws_aggregate_period_entity | 16 kB | 0 |
| du_urban_variable | idx_du_urban_variable_type | 16 kB | 0 |
| du_agriculture_entity | idx_du_ag_region | 16 kB | 0 |
| du_agriculture_entity | idx_du_ag_type | 16 kB | 0 |
| du_agriculture_entity | idx_du_ag_provider | 16 kB | 0 |
| ag_aggregate_entity | idx_ag_agg_project | 16 kB | 0 |
| theme | idx_theme_short_code_active | 16 kB | 0 |
| theme | idx_theme_active | 16 kB | 0 |
| scenario_author | idx_scenario_author_active | 16 kB | 0 |
| delta_period_summary | idx_delta_summary_variable | 16 kB | 0 |
| domain_family_map | idx_domain_family_map_version_family | 16 kB | 0 |
| watershed | idx_watershed_hydrologic_region | 16 kB | 0 |
| reservoir_entity | idx_reservoir_entity_is_main | 8192 bytes | 0 |
| reservoir_entity | idx_reservoir_entity_has_tiers | 8192 bytes | 0 |
| reservoir | idx_reservoir_geom | 8192 bytes | 0 |
| compliance_station | idx_compliance_geom | 8192 bytes | 0 |
| wba | idx_wba_geom | 8192 bytes | 0 |
| audit_log | idx_audit_log_changed_by | 8192 bytes | 0 |
| audit_log | idx_audit_log_changed_at | 8192 bytes | 0 |
| audit_log | idx_audit_log_record | 8192 bytes | 0 |


### 4c. Total storage

**Total database size:** 495 MB


## 5. AUDIT SUMMARY

- **PASS** **Health: Cache hit ratio > 99%**

- **PASS** **Health: Connections below 80% of max**

- **PASS** **Health: No tables with dead_pct > 20%**

- **FAIL** **Cost: No large unused indexes**: 105 unused index(es) adding write overhead

- **PASS** **Content: All expected row counts pass**

- **PASS** **Content: No expected tables missing from DB**

- **FAIL** **Content: ERD synchronized with live schema**: see section 1b for details

- **PASS** **Verification: All active scenarios have ETL coverage**

- **PASS** **Verification: Zero NULL audit fields**

- **PASS** **Verification: Zero orphaned statistics rows**

- **PASS** **Verification: Zero invalid water_month values**


---

_Report generated in 4.2s by `database/audit/run_monthly_audit.py`_
