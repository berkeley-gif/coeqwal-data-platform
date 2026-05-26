# COEQWAL DATABASE ERD
**Generated from audit**: 2026-04-06T20:51:21.095625
**Database**: coeqwal_scenario
**PostgreSQL**: PostgreSQL 17.4 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 12.4.0, 64-bit

---

## DATABASE SUMMARY

- **Total Tables**: 96
- **Total Records**: 402,233
- **Audit Date**: 2026-04-06T20:51:21.095625

## TABLE OF CONTENTS

### **Versioning**
- `version_family` (14 records)
- `version` (14 records)
- `developer` (2 records)
- `domain_family_map` (93 records)

### **Lookup/Reference**
- `hydrologic_region` (7 records)
- `source` (12 records)
- `model_source` (1 records)
- `unit` (5 records)
- `spatial_scale` (11 records)
- `temporal_scale` (8 records)
- `statistic_type` (20 records)
- `geometry_type` (4 records)
- `variable_type` (6 records)

### **Network**
- `network` (6,908 records)
- `network_node` (1,544 records)
- `network_arc` (2,610 records)
- `network_gis` (4,154 records)
- `network_type` (21 records)
- `network_subtype` (28 records)
- `network_entity_type` (4 records)

### **Entities**
- `reservoir_entity` (92 records)
- `reservoir` (7 records, geometry table backing `reservoir_entity` polygons)
- `channel_entity` (669 records)
- `du_urban_entity` (145 records)
- `du_agriculture_entity` (144 records)
- `du_refuge_entity` (18 records)
- `wba` (42 records)
- `compliance_station` (2 records, point-geometry table for in-Delta compliance stations)
- `cws_entity` (planned — ~476 records, see "PLANNED TABLES — community water systems (CWS)" below)
- `cws_du_link` (planned — ~586 records)
- `cws_list` + `cws_list_du_member` (planned — list/registry pattern for project vs CalSim DU lists)

### **Tier System**
- `tier_definition` (9 records)
- `tier_location` (67 records, narrow catalog: per-tier location membership; names/geometry resolved via entity joins)
- `tier_result` (536 records)
- `tier_location_result` (17,600 records)

### **Statistics**
- `reservoir_group` (4 records)
- `reservoir_group_member` (24 records)
- `reservoir_monthly_percentile` (34,560 records)
- `reservoir_storage_monthly` (34,560 records)
- `reservoir_spill_monthly` (9,552 records)
- `reservoir_period_summary` (2,880 records)

### **System**
- `spatial_ref_sys` (8,500 records)

### **Uncategorized**
- `ag_aggregate_entity` (9 records)
- `ag_aggregate_monthly` (2,916 records)
- `ag_aggregate_period_summary` (243 records)
- `ag_du_demand_monthly` (49,572 records)
- `ag_du_gw_pumping_monthly` (49,572 records)
- `ag_du_period_summary` (4,131 records)
- `ag_du_shortage_monthly` (15,600 records)
- `ag_du_sw_delivery_monthly` (43,740 records)
- `assumption_category` (2 records)
- `assumption_definition` (6 records)
- `audit_log` (0 records)
- `calsim_model_variable_type` (8 records)
- `channel_variable` (1,352 records)
- `cws_aggregate_entity` (6 records)
- `cws_aggregate_monthly` (2,160 records)
- `cws_aggregate_period_summary` (180 records)
- `delta_monthly` (2,688 records)
- `delta_period_summary` (224 records)
- `derived_variable_type` (4 records)
- `du_delivery_monthly` (28,320 records)
- `du_period_summary` (2,360 records)
- `du_shortage_monthly` (14,496 records)
- `du_urban_delivery_arc` (57 records)
- `du_urban_group` (11 records)
- `du_urban_group_member` (142 records)
- `du_urban_variable` (90 records)
- `env_flow_channel_monthly` (19,824 records)
- `env_flow_channel_period_summary` (1,652 records)
- `env_flow_channel_seasonal` (8,260 records)
- `env_flow_season` (5 records)
- `hydroclimate` (6 records)
- `mi_contractor` (30 records)
- `mi_contractor_delivery_arc` (39 records)
- `mi_contractor_group` (6 records)
- `mi_contractor_group_member` (60 records)
- `mi_contractor_period_summary` (644 records)
- `mi_delivery_monthly` (7,728 records)
- `mi_shortage_monthly` (7,728 records)
- `operation_category` (9 records)
- `operation_definition` (28 records)
- `refuge_du_delivery_monthly` (6,048 records)
- `refuge_du_period_summary` (504 records)
- `refuge_du_shortage_monthly` (6,048 records)
- `scenario` (77 records)
- `scenario_author` (3 records)
- `scenario_backup` (-1 records)
- `scenario_hydroclimate_sibling` (27 records)
- `scenario_key_assumption_link` (73 records)
- `scenario_key_operation_link` (514 records)
- `scenario_tag` (10 records)
- `scenario_tag_link` (109 records)
- `sensitivity_climate` (-1 records)
- `sensitivity_operational` (-1 records)
- `slr` (4 records)
- `statistic_category` (3 records)
- `theme` (6 records)
- `theme_scenario_link` (79 records)
- `watershed` (13 records)

---

## VERSIONING TABLES

### **version_family**

```
Table: version_family
Records: 14
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  description                    text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **version**

```
Table: version
Records: 14
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  version_family_id              integer             
  version_number                 text                
  changelog                      text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **developer**

```
Table: developer
Records: 2
Columns: 16
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  email                          text                
  name                           text                
  display_name                   text                
  affiliation                    text                
  role                           text                
  aws_sso_user_id                text                
  aws_sso_username               text                
  is_bootstrap                   boolean             
  sync_source                    text                
  is_active                      boolean             
  last_login                     timestamp with time zone
  created_at                     timestamp with time zone
  updated_at                     timestamp with time zone
  created_by                     integer             
  updated_by                     integer             
```

**Indexes**: Present

### **domain_family_map**

```
Table: domain_family_map
Records: 93
Columns: 10
Audit: Full audit trail

Columns:
  schema_name                    text                
  table_name                     text                
  version_family_id              integer             
  note                           text                
  database_level                 character varying   
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

---

## LOOKUP/REFERENCE TABLES

### **hydrologic_region**

```
Table: hydrologic_region
Records: 7
Columns: 8
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **source**

```
Table: source
Records: 12
Columns: 8
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  source                         text                
  description                    text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **model_source**

```
Table: model_source
Records: 1
Columns: 10
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  name                           text                
  description                    text                
  contact                        text                
  notes                          text                
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **unit**

```
Table: unit
Records: 5
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  full_name                      text                
  canonical_group                text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **spatial_scale**

```
Table: spatial_scale
Records: 11
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  description                    text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **temporal_scale**

```
Table: temporal_scale
Records: 8
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  description                    text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **statistic_type**

```
Table: statistic_type
Records: 20
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  description                    text                
  statistic_category_id          integer             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **geometry_type**

```
Table: geometry_type
Records: 4
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  description                    text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **variable_type**

```
Table: variable_type
Records: 6
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  description                    text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

---

## NETWORK TABLES

### **network**

```
Table: network
Records: 6,908
Columns: 19
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  name                           character varying   
  description                    text                
  comment                        text                
  entity_type_id                 integer             
  type_id                        integer             
  subtype_ids                    ARRAY               
  model_list                     ARRAY               
  source_list                    ARRAY               
  has_gis                        boolean             
  hydrologic_region_id           integer             
  riv_sys                        character varying   
  strm_code                      character varying   
  network_version_id             integer             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **network_node**

```
Table: network_node
Records: 1,544
Columns: 17
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  network_id                     integer             
  riv_mi                         numeric             
  c2vsim_gw                      character varying   
  c2vsim_sw                      character varying   
  nrest_gage                     character varying   
  strm_code                      character varying   
  rm_ii                          character varying   
  model_source_id                integer             
  source_id                      integer             
  network_version_id             integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **network_arc**

```
Table: network_arc
Records: 2,610
Columns: 15
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  network_id                     integer             
  river                          character varying   
  from_node                      character varying   
  to_node                        character varying   
  shape_length_m                 numeric             
  model_source_id                integer             
  source_id                      integer             
  network_version_id             integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **network_gis**

```
Table: network_gis
Records: 4,154
Columns: 14
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  network_id                     integer             
  precision_level                character varying   
  geom_wkt                       text                
  srid                           integer             
  geom                           USER-DEFINED        
  estimated_accuracy_meters      numeric             
  source_id                      integer             
  network_version_id             integer             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **network_type**

```
Table: network_type
Records: 21
Columns: 12
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  label                          character varying   
  description                    text                
  network_entity_type_id         integer             
  model_source_id                integer             
  source_id                      integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **network_subtype**

```
Table: network_subtype
Records: 28
Columns: 12
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  label                          character varying   
  description                    text                
  type_id                        integer             
  model_source_id                integer             
  source_id                      integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **network_entity_type**

```
Table: network_entity_type
Records: 4
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     text                
  label                          text                
  description                    text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

---

## ENTITIES TABLES

### **reservoir_entity**

```
Table: reservoir_entity
Records: 92
Columns: 23
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  network_node_id                character varying   
  short_code                     character varying   
  name                           character varying   
  description                    text                
  associated_river               character varying   
  entity_type_id                 integer             
  schematic_type_id              integer             
  hydrologic_region_id           integer             
  capacity_taf                   numeric             
  dead_pool_taf                  numeric             
  surface_area_acres             numeric             
  operational_purpose            character varying   
  has_tiers                      boolean             
  is_main                        boolean             
  has_gis_data                   integer             
  entity_version_id              integer             
  source_ids                     text                
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **channel_entity**

```
Table: channel_entity
Records: 669
Columns: 28
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  network_arc_id                 character varying   
  short_code                     character varying   
  name                           character varying   
  description                    text                
  subtype                        character varying   
  entity_type_id                 integer             
  schematic_type_id              integer             
  hydrologic_region_id           character varying   
  boundary_condition             character varying   
  from_node                      character varying   
  to_node                        character varying   
  length_m                       numeric             
  has_tiers                      boolean             
  is_main                        boolean             
  has_gis_data                   integer             
  entity_version_id              integer             
  source_ids                     text                
  watershed_short_code           character varying   
  unimp_sv_variable              character varying   
  has_mif                        boolean             
  has_eflows                     boolean             
  channel_class                  character varying   
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **du_urban_entity**

```
Table: du_urban_entity
Records: 145
Columns: 27
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  du_id                          character varying   
  wba_id                         character varying   
  hydrologic_region              character varying   
  dups                           integer             
  du_class                       character varying   
  cs3_type                       character varying   
  total_acres                    numeric             
  polygon_count                  integer             
  community_agency               text                
  gw                             character varying   
  sw                             character varying   
  point_of_diversion             text                
  source                         character varying   
  model_source                   character varying   
  has_gis_data                   boolean             
  primary_contractor_short_code  character varying   
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
  hydrologic_region_id           integer             
  model_source_id                integer             
  geom_wkt                       text                 -- .archive/56_add_du_geometry_columns.sql
  srid                           integer              -- .archive/56_add_du_geometry_columns.sql
  geom                           geometry(MultiPolygon, 4326) -- .archive/56_add_du_geometry_columns.sql
```

**Indexes**: Present; `idx_du_urban_entity_geom (geom) USING GIST` added by `.archive/56_add_du_geometry_columns.sql`

### **du_agriculture_entity**

```
Table: du_agriculture_entity
Records: 144
Columns: 36
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  du_id                          character varying   
  wba_id                         character varying   
  hydrologic_region              character varying   
  dups                           integer             
  du_class                       character varying   
  cs3_type                       character varying   
  total_acres                    numeric             
  polygon_count                  integer             
  source                         character varying   
  model_source                   character varying   
  agency                         character varying   
  provider                       character varying   
  gw                             boolean             
  sw                             boolean             
  point_of_diversion             text                
  diversion_arc                  character varying   
  river_reach                    character varying   
  river_mile_start               numeric             
  river_mile_end                 numeric             
  bank                           character varying   
  area_acres                     numeric             
  annual_diversion_taf           numeric             
  demand_unit                    character varying   
  table_id                       character varying   
  has_gis_data                   boolean             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
  hydrologic_region_id           integer             
  model_source_id                integer             
  geom_wkt                       text                 -- .archive/56_add_du_geometry_columns.sql
  srid                           integer              -- .archive/56_add_du_geometry_columns.sql
  geom                           geometry(MultiPolygon, 4326) -- .archive/56_add_du_geometry_columns.sql
```

**Indexes**: Present; `idx_du_agriculture_entity_geom (geom) USING GIST` added by `.archive/56_add_du_geometry_columns.sql`

### **du_refuge_entity**

```
Table: du_refuge_entity
Records: 18
Columns: 26
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  du_id                          character varying   
  wba_id                         character varying   
  hydrologic_region              character varying   
  dups                           integer             
  du_class                       character varying   
  cs3_type                       character varying   
  total_acres                    numeric             
  polygon_count                  integer             
  refuge_or_wildlife_area        text                
  managed_by                     character varying   
  provider                       character varying   
  gw                             boolean             
  sw                             boolean             
  point_of_diversion_conveyance  text                
  source                         character varying   
  model_source                   character varying   
  has_gis_data                   boolean             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
  geom_wkt                       text                 -- .archive/56_add_du_geometry_columns.sql
  srid                           integer              -- .archive/56_add_du_geometry_columns.sql
  geom                           geometry(MultiPolygon, 4326) -- .archive/56_add_du_geometry_columns.sql
```

**Indexes**: Present; `idx_du_refuge_entity_geom (geom) USING GIST` added by `.archive/56_add_du_geometry_columns.sql`

### **wba**

```
Table: wba
Records: 42
Columns: 15
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  wba_id                         character varying   
  wba_name                       character varying   
  geom_wkt                       text                
  srid                           integer             
  geom                           USER-DEFINED        
  area_acres                     numeric             
  comments                       text                
  data_source                    character varying   
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
  hydrologic_region_id           integer             
  source_id                      integer             
```

**Indexes**: Present

### **reservoir**

```
Table: reservoir
Records: 7
Columns: 16
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  calsim_short_code              character varying    [UNIQUE]
  reservoir_name                 character varying   
  geom_wkt                       text                
  srid                           integer              DEFAULT 4326
  geom                           USER-DEFINED        
  area_sqkm                      numeric             
  elevation_m                    numeric             
  gnis_id                        character varying   
  nhd_permanent_id               character varying   
  data_source                    character varying    DEFAULT 'NHD'
  created_at                     timestamp with time zone DEFAULT now()
  created_by                     integer              FK to developer.id
  updated_at                     timestamp with time zone DEFAULT now()
  updated_by                     integer              FK to developer.id
  source_id                      integer              FK to source.id
```

**Indexes**: `reservoir_pkey (id)`, `reservoir_calsim_short_code_key (calsim_short_code)` UNIQUE, `idx_reservoir_calsim_code (calsim_short_code)` UNIQUE (duplicate of the key-backed unique index — candidate for cleanup), `idx_reservoir_geom (geom) USING GIST`.

**Notes**: Polygon-geometry table backing the `reservoir` `location_type` resolution path in `tier_location`. The entity-side names live in `reservoir_entity`; this table carries the geometry alone, keyed by `calsim_short_code` (e.g. `SHSTA`, `OROVL`, `SLUIS`). The shared `SLUIS` polygon is referenced by both `SLUIS_CVP` and `SLUIS_SWP` entity rows. Seeded from NHD; one row per CalSim reservoir for which we have a polygon. See [`etl/common/tier_location_entities.py`](../../etl/common/tier_location_entities.py).

### **compliance_station**

```
Table: compliance_station
Records: 2
Columns: 16
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  station_code                   character varying    [UNIQUE]
  station_name                   character varying   
  latitude                       numeric             
  longitude                      numeric             
  srid                           integer              DEFAULT 4326
  geom_wkt                       text                
  geom                           USER-DEFINED        
  tier_use                       character varying   
  data_source                    character varying   
  notes                          text                
  created_at                     timestamp with time zone DEFAULT now()
  created_by                     integer              FK to developer.id
  updated_at                     timestamp with time zone DEFAULT now()
  updated_by                     integer              FK to developer.id
  source_id                      integer              FK to source.id
```

**Indexes**: `compliance_station_pkey (id)`, `compliance_station_station_code_key (station_code)` UNIQUE, `idx_compliance_code (station_code)` UNIQUE (duplicate of the key-backed unique index — candidate for cleanup), `idx_compliance_geom (geom) USING GIST`, `idx_compliance_tier (tier_use)`.

**Notes**: Point-geometry table for in-Delta compliance stations, used by the `compliance_station` `location_type` resolution path in `tier_location` (currently `EM` = Emmaton, `JP` = Jersey Point for the `FW_DELTA_USES` tier). Seeded from [`database/seed_tables/03_GIS/compliance_stations.csv`](../seed_tables/03_GIS/compliance_stations.csv). See [`etl/common/tier_location_entities.py`](../../etl/common/tier_location_entities.py).

---

## TIER SYSTEM TABLES

### **tier_definition**

```
Table: tier_definition
Records: 9
Columns: 12
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  name                           character varying   
  description                    text                
  tier_type                      character varying   
  tier_count                     integer             
  tier_version_id                integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **tier_location**

```
Table: tier_location
Records: 67 (matches the active members across the 9 tier outcomes)
Columns: 10
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  tier_short_code                character varying    [FK -> tier_definition.short_code]
  location_type                  character varying    [enum: network_node, wba, reservoir, compliance_station, region, demand_unit]
  location_id                    character varying    [natural key; joins to entity tables per location_type]
  display_order                  integer
  is_active                      boolean              [soft-delete flag; preserves history]
  created_at                     timestamp with time zone
  created_by                     integer
  updated_at                     timestamp with time zone
  updated_by                     integer

Constraints:
  UNIQUE (tier_short_code, location_id)
  CHECK  (location_type IN ('network_node', 'wba', 'reservoir', 'compliance_station', 'region', 'demand_unit'))
```

**Indexes**: `tier_short_code`, `location_type`, `is_active`

**Source of truth**: The tier teams' staging CSVs in `etl/tier_data/staging/`. Reconciled with [`etl/tier_data/scripts/sync_tier_locations_from_staging.py`](../../etl/tier_data/scripts/sync_tier_locations_from_staging.py).

**Resolution map**: `location_id` joins to entity tables for display name and geometry. See [`etl/common/tier_location_entities.py`](../../etl/common/tier_location_entities.py) for the registry. Summary:

Both attribute and geometry lookups are tier-aware. `TIER_ATTRIBUTE_OVERRIDES` and `TIER_GEOMETRY_OVERRIDES` in [`etl/common/tier_location_entities.py`](../../etl/common/tier_location_entities.py) route AG_REV demand-unit ids to `du_agriculture_entity`, while CWS_DEL (and the default for any other `demand_unit` tier) routes to `du_urban_entity`. DU polygons live in those same entity tables (added by [`.archive/56_add_du_geometry_columns.sql`](../scripts/sql/.archive/56_add_du_geometry_columns.sql), loaded by [`load_du_geometries.py`](../scripts/data_processing/load_du_geometries.py)). `26N_NA` is the one `du_id` that exists in both urban and ag entity tables; both rows carry the same dissolved polygon, so either resolver returns the same geometry. 54 `du_id`s lack a polygon in the source gpkg today and are listed in [`docs/du_geometry_gap.md`](../../docs/du_geometry_gap.md).

| `location_type` (tier) | Attribute table (name) | Geometry table | Notes |
|---|---|---|---|
| `network_node` | `network.short_code` -> `network.name` | `network_gis.short_code` -> `geom` (POINT) | `DISTINCT ON (short_code) ORDER BY (precision_level = 'precise') DESC` |
| `demand_unit` (CWS_DEL) | `du_urban_entity.du_id` (name = id today) | `du_urban_entity.du_id` -> `geom` (MULTIPOLYGON, 4326) | Polygons from `database/seed_tables/03_GIS/du_4326.gpkg`; 41 urban `du_id`s have no polygon today |
| `demand_unit` (AG_REV) | `du_agriculture_entity.du_id` (name = id today) | `du_agriculture_entity.du_id` -> `geom` (MULTIPOLYGON, 4326) | 12 ag `du_id`s have no polygon today |
| `reservoir` | `reservoir_entity.short_code` -> `reservoir_entity.name` | `reservoir.calsim_short_code` -> `geom` (POLYGON) | `SLUIS_CVP` and `SLUIS_SWP` both render against the shared `SLUIS` polygon |
| `wba` | `wba.wba_id` -> `wba.wba_name` | `wba.wba_id` -> `geom` (POLYGON) | Includes the `DETAW` row for the Legal Delta |
| `compliance_station` | `compliance_station.station_code` -> `station_name` | `compliance_station.station_code` -> `geom` (POINT) | `EM`, `JP` |
| `region` | `hydrologic_region.short_code` -> `label` | (none) | Reserved; no tier outcome uses this today |

### **tier_result**

```
Table: tier_result
Records: 536
Columns: 19
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  scenario_short_code            character varying   
  tier_short_code                character varying   
  tier_1_value                   integer             
  tier_2_value                   integer             
  tier_3_value                   integer             
  tier_4_value                   integer             
  norm_tier_1                    numeric             
  norm_tier_2                    numeric             
  norm_tier_3                    numeric             
  norm_tier_4                    numeric             
  total_value                    integer             
  single_tier_level              integer             
  tier_version_id                integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **tier_location_result**

```
Table: tier_location_result
Records: 17,600
Columns: 14
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  scenario_short_code            character varying   
  tier_short_code                character varying   
  location_type                  character varying   
  location_id                    character varying   
  location_name                  character varying   
  tier_level                     integer             
  tier_value                     integer             
  display_order                  integer             
  tier_version_id                integer             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

---

## STATISTICS TABLES

### **reservoir_group**

```
Table: reservoir_group
Records: 4
Columns: 10
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  short_code                     character varying   
  label                          character varying   
  description                    text                
  display_order                  integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **reservoir_group_member**

```
Table: reservoir_group_member
Records: 24
Columns: 9
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  reservoir_group_id             integer             
  reservoir_entity_id            integer             
  display_order                  integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **reservoir_monthly_percentile**

```
Table: reservoir_monthly_percentile
Records: 34,560
Columns: 26
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  scenario_short_code            character varying   
  reservoir_entity_id            integer             
  water_month                    integer             
  q0                             numeric             
  q10                            numeric             
  q30                            numeric             
  q50                            numeric             
  q70                            numeric             
  q90                            numeric             
  q100                           numeric             
  mean_value                     numeric             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
  q0_taf                         numeric             
  q10_taf                        numeric             
  q30_taf                        numeric             
  q50_taf                        numeric             
  q70_taf                        numeric             
  q90_taf                        numeric             
  q100_taf                       numeric             
  mean_taf                       numeric             
  capacity_taf                   numeric             
```

**Indexes**: Present

### **reservoir_storage_monthly**

```
Table: reservoir_storage_monthly
Records: 34,560
Columns: 42
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  scenario_short_code            character varying   
  reservoir_entity_id            integer             
  water_month                    integer             
  storage_avg_taf                numeric             
  storage_cv                     numeric             
  storage_pct_capacity           numeric             
  q0                             numeric             
  q10                            numeric             
  q30                            numeric             
  q50                            numeric             
  q70                            numeric             
  q90                            numeric             
  q100                           numeric             
  capacity_taf                   numeric             
  sample_count                   integer             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
  q0_taf                         numeric             
  q10_taf                        numeric             
  q30_taf                        numeric             
  q50_taf                        numeric             
  q70_taf                        numeric             
  q90_taf                        numeric             
  q100_taf                       numeric             
  exc_p5                         numeric             
  exc_p10                        numeric             
  exc_p25                        numeric             
  exc_p50                        numeric             
  exc_p75                        numeric             
  exc_p90                        numeric             
  exc_p95                        numeric             
  exc_p5_taf                     numeric             
  exc_p10_taf                    numeric             
  exc_p25_taf                    numeric             
  exc_p50_taf                    numeric             
  exc_p75_taf                    numeric             
  exc_p90_taf                    numeric             
  exc_p95_taf                    numeric             
```

**Indexes**: Present

### **reservoir_spill_monthly**

```
Table: reservoir_spill_monthly
Records: 9,552
Columns: 18
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  scenario_short_code            character varying   
  reservoir_entity_id            integer             
  water_month                    integer             
  spill_months_count             integer             
  total_months                   integer             
  spill_frequency_pct            numeric             
  spill_avg_cfs                  numeric             
  spill_max_cfs                  numeric             
  spill_q50                      numeric             
  spill_q90                      numeric             
  spill_q100                     numeric             
  storage_at_spill_avg_pct       numeric             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
```

**Indexes**: Present

### **reservoir_period_summary**

```
Table: reservoir_period_summary
Records: 2,880
Columns: 43
Audit: Full audit trail

Columns:
  id                             integer              [PK]
  scenario_short_code            character varying   
  reservoir_entity_id            integer             
  simulation_start_year          integer             
  simulation_end_year            integer             
  total_years                    integer             
  storage_exc_p5                 numeric             
  storage_exc_p10                numeric             
  storage_exc_p25                numeric             
  storage_exc_p50                numeric             
  storage_exc_p75                numeric             
  storage_exc_p90                numeric             
  storage_exc_p95                numeric             
  dead_pool_taf                  numeric             
  dead_pool_pct                  numeric             
  spill_threshold_pct            numeric             
  spill_years_count              integer             
  spill_frequency_pct            numeric             
  spill_mean_cfs                 numeric             
  spill_peak_cfs                 numeric             
  annual_spill_avg_taf           numeric             
  annual_spill_cv                numeric             
  annual_spill_max_taf           numeric             
  annual_max_spill_q50           numeric             
  annual_max_spill_q90           numeric             
  annual_max_spill_q100          numeric             
  capacity_taf                   numeric             
  is_active                      boolean             
  created_at                     timestamp with time zone
  created_by                     integer             
  updated_at                     timestamp with time zone
  updated_by                     integer             
  flood_pool_prob_all            numeric             
  flood_pool_prob_september      numeric             
  flood_pool_prob_april          numeric             
  dead_pool_prob_all             numeric             
  dead_pool_prob_september       numeric             
  storage_cv_all                 numeric             
  storage_cv_april               numeric             
  storage_cv_september           numeric             
  annual_avg_taf                 numeric             
  april_avg_taf                  numeric             
  september_avg_taf              numeric             
```

**Indexes**: Present

---

## SYSTEM TABLES

### **spatial_ref_sys**

```
Table: spatial_ref_sys
Records: 8,500
Columns: 5

Columns:
  srid                           integer             
  auth_name                      character varying   
  auth_srid                      integer             
  srtext                         character varying   
  proj4text                      character varying   
```

**Indexes**: Present

---

## UNCATEGORIZED TABLES

### **ag_aggregate_entity**

```
Table: ag_aggregate_entity
Records: 9
Columns: 13
```

### **ag_aggregate_monthly**

```
Table: ag_aggregate_monthly
Records: 2,916
Columns: 29
```

### **ag_aggregate_period_summary**

```
Table: ag_aggregate_period_summary
Records: 243
Columns: 31
```

### **ag_du_demand_monthly**

```
Table: ag_du_demand_monthly
Records: 49,572
Columns: 26
```

### **ag_du_gw_pumping_monthly**

```
Table: ag_du_gw_pumping_monthly
Records: 49,572
Columns: 27
```

### **ag_du_period_summary**

```
Table: ag_du_period_summary
Records: 4,131
Columns: 52
```

### **ag_du_shortage_monthly**

```
Table: ag_du_shortage_monthly
Records: 15,600
Columns: 28
```

### **ag_du_sw_delivery_monthly**

```
Table: ag_du_sw_delivery_monthly
Records: 43,740
Columns: 26
```

### **assumption_category**

```
Table: assumption_category
Records: 2
Columns: 9
```

### **assumption_definition**

```
Table: assumption_definition
Records: 6
Columns: 13
```

### **audit_log**

```
Table: audit_log
Records: 0
Columns: 13
```

### **calsim_model_variable_type**

```
Table: calsim_model_variable_type
Records: 8
Columns: 9
```

### **channel_variable**

```
Table: channel_variable
Records: 1,352
Columns: 19
```

### **cws_aggregate_entity**

```
Table: cws_aggregate_entity
Records: 6
Columns: 14
```

### **cws_aggregate_monthly**

```
Table: cws_aggregate_monthly
Records: 2,160
Columns: 45
```

### **cws_aggregate_period_summary**

```
Table: cws_aggregate_period_summary
Records: 180
Columns: 36
```

### **delta_monthly**

```
Table: delta_monthly
Records: 2,688
Columns: 27
```

### **delta_period_summary**

```
Table: delta_period_summary
Records: 224
Columns: 14
```

### **derived_variable_type**

```
Table: derived_variable_type
Records: 4
Columns: 9
```

### **du_delivery_monthly**

```
Table: du_delivery_monthly
Records: 28,320
Columns: 28
```

### **du_period_summary**

```
Table: du_period_summary
Records: 2,360
Columns: 33
```

### **du_shortage_monthly**

```
Table: du_shortage_monthly
Records: 14,496
Columns: 27
```

### **du_urban_delivery_arc**

```
Table: du_urban_delivery_arc
Records: 57
Columns: 9
```

### **du_urban_group**

```
Table: du_urban_group
Records: 11
Columns: 10
```

### **du_urban_group_member**

```
Table: du_urban_group_member
Records: 142
Columns: 9
```

### **du_urban_variable**

```
Table: du_urban_variable
Records: 90
Columns: 16
```

### **env_flow_channel_monthly**

```
Table: env_flow_channel_monthly
Records: 19,824
Columns: 58
```

### **env_flow_channel_period_summary**

```
Table: env_flow_channel_period_summary
Records: 1,652
Columns: 32
```

### **env_flow_channel_seasonal**

```
Table: env_flow_channel_seasonal
Records: 8,260
Columns: 61
```

### **env_flow_season**

```
Table: env_flow_season
Records: 5
Columns: 12
```

### **hydroclimate**

```
Table: hydroclimate
Records: 6
Columns: 17
```

### **mi_contractor**

```
Table: mi_contractor
Records: 30
Columns: 14
```

### **mi_contractor_delivery_arc**

```
Table: mi_contractor_delivery_arc
Records: 39
Columns: 9
```

### **mi_contractor_group**

```
Table: mi_contractor_group
Records: 6
Columns: 10
```

### **mi_contractor_group_member**

```
Table: mi_contractor_group_member
Records: 60
Columns: 9
```

### **mi_contractor_period_summary**

```
Table: mi_contractor_period_summary
Records: 644
Columns: 34
```

### **mi_delivery_monthly**

```
Table: mi_delivery_monthly
Records: 7,728
Columns: 28
```

### **mi_shortage_monthly**

```
Table: mi_shortage_monthly
Records: 7,728
Columns: 27
```

### **operation_category**

```
Table: operation_category
Records: 9
Columns: 9
```

### **operation_definition**

```
Table: operation_definition
Records: 28
Columns: 13
```

### **refuge_du_delivery_monthly**

```
Table: refuge_du_delivery_monthly
Records: 6,048
Columns: 26
```

### **refuge_du_period_summary**

```
Table: refuge_du_period_summary
Records: 504
Columns: 32
```

### **refuge_du_shortage_monthly**

```
Table: refuge_du_shortage_monthly
Records: 6,048
Columns: 29
```

### **scenario**

```
Table: scenario
Records: 77
Columns: 13
```

### **scenario_author**

```
Table: scenario_author
Records: 3
Columns: 11
```

### **scenario_backup**

```
Table: scenario_backup
Records: -1
Columns: 0
```

### **scenario_hydroclimate_sibling**

```
Table: scenario_hydroclimate_sibling
Records: 27
Columns: 9
```

### **scenario_key_assumption_link**

```
Table: scenario_key_assumption_link
Records: 73
Columns: 6
```

### **scenario_key_operation_link**

```
Table: scenario_key_operation_link
Records: 514
Columns: 6
```

### **scenario_tag**

```
Table: scenario_tag
Records: 10
Columns: 9
```

### **scenario_tag_link**

```
Table: scenario_tag_link
Records: 109
Columns: 6
```

### **sensitivity_climate**

```
Table: sensitivity_climate
Records: -1
Columns: 0
```

### **sensitivity_operational**

```
Table: sensitivity_operational
Records: -1
Columns: 0
```

### **slr**

```
Table: slr
Records: 4
Columns: 11
```

### **statistic_category**

```
Table: statistic_category
Records: 3
Columns: 8
```

### **theme**

```
Table: theme
Records: 6
Columns: 18
```

### **theme_scenario_link**

```
Table: theme_scenario_link
Records: 79
Columns: 6
```

### **watershed**

```
Table: watershed
Records: 13
Columns: 11
```

---

## PLANNED TABLES — community water systems (CWS)

> Designed but **not yet implemented**. Source data lives in `reference/community_water_systems/`. See `database/README.md` → "03_ENTITY: entity tables and the entity-attribute pattern" → "Project list vs CalSim list (community water systems)" for the rationale and reconciliation work needed before these are created.

### **cws_entity** (planned)

One row per California Public Water System (PWSID), ~476 rows from `Master list of systems served for sw units updated april 13.xlsx`.

```
Table: cws_entity
Records: ~476 (planned)
Columns: 13
Audit: Full audit trail

Columns:
  id                       integer              [PK]
  short_code               text                 [UNIQUE NOT NULL]   -- normalised PWSID
  pwsid                    text                 NOT NULL            -- e.g. "CA0110001"
  system_name              text                 NOT NULL
  pop_served               integer
  system_lat               numeric
  system_lon               numeric
  geom                     geometry(Point,4326)
  hydrologic_region_id     integer              [FK → hydrologic_region.id]
  source_id                integer              [FK → source.id]
  is_active                boolean              NOT NULL DEFAULT TRUE
  created_at               timestamp with time zone
  created_by               integer              [FK → developer.id]
  updated_at               timestamp with time zone
  updated_by               integer              [FK → developer.id]

Constraints:
  ├── UNIQUE (pwsid)
  ├── FK: hydrologic_region_id → hydrologic_region.id
  └── FK: source_id → source.id

Indexes:
  ├── idx_cws_entity_pwsid (pwsid)
  ├── idx_cws_entity_hydrologic_region (hydrologic_region_id)
  └── idx_cws_entity_geom (geom) USING GIST
```

### **cws_du_link** (planned)

M:N junction `cws_entity` ↔ `du_urban_entity`. ~586 rows from the systems-served list (a system may serve multiple DUs and a DU may be served by multiple systems).

```
Table: cws_du_link
Records: ~586 (planned)
Columns: 8
Audit: Full audit trail

Columns:
  id              integer              [PK]
  cws_entity_id   integer              NOT NULL  [FK → cws_entity.id]
  du_id           character varying    NOT NULL  [FK → du_urban_entity.du_id]
  is_active       boolean              NOT NULL DEFAULT TRUE
  created_at      timestamp with time zone
  created_by      integer              [FK → developer.id]
  updated_at      timestamp with time zone
  updated_by      integer              [FK → developer.id]

Constraints:
  ├── UNIQUE (cws_entity_id, du_id)
  ├── FK: cws_entity_id → cws_entity.id
  └── FK: du_id → du_urban_entity.du_id

Indexes:
  ├── idx_cws_du_link_cws (cws_entity_id)
  └── idx_cws_du_link_du (du_id)
```

### **cws_list** (planned, layer 01_lookup)

Registry of named CWS-domain DU lists. Lets us record which list any given DU belongs to (project master, focal SW, focal GW, HHS allocation, M&I crosswalk, CalSim urban, tier matrix, etc.). One row per named list.

```
Table: cws_list
Records: ~7 (planned)
Columns: 10
Audit: Full audit trail

Columns:
  id              integer              [PK]
  short_code      text                 [UNIQUE NOT NULL]   -- e.g. "coeqwal_master_du"
  label           text                 NOT NULL
  description     text
  source_id       integer              [FK → source.id]
  is_active       boolean              NOT NULL DEFAULT TRUE
  created_at      timestamp with time zone
  created_by      integer              [FK → developer.id]
  updated_at      timestamp with time zone
  updated_by      integer              [FK → developer.id]

Initial rows (planned):
  - coeqwal_master_du      (124 DUs)
  - coeqwal_focal_sw_du    (75 DUs)
  - coeqwal_focal_gw_du    (83 DUs)
  - calsim_urban_du        (145 DUs)
  - tier_matrix            (71 DUs — already in du_urban_group.tier)
  - hhs_allocation         (76 DUs)
  - mi_delivery_crosswalk  (75 DUs)
```

### **cws_list_du_member** (planned)

M:N junction `cws_list` ↔ `du_urban_entity`. The membership table for the registry above.

```
Table: cws_list_du_member
Records: ~700 (planned, sum of list sizes)
Columns: 8
Audit: Full audit trail

Columns:
  id              integer              [PK]
  cws_list_id     integer              NOT NULL  [FK → cws_list.id]
  du_id           character varying    NOT NULL  [FK → du_urban_entity.du_id]
  is_active       boolean              NOT NULL DEFAULT TRUE
  created_at      timestamp with time zone
  created_by      integer              [FK → developer.id]
  updated_at      timestamp with time zone
  updated_by      integer              [FK → developer.id]

Constraints:
  ├── UNIQUE (cws_list_id, du_id)
  ├── FK: cws_list_id → cws_list.id
  └── FK: du_id → du_urban_entity.du_id

Indexes:
  ├── idx_cws_list_du_member_list (cws_list_id)
  └── idx_cws_list_du_member_du (du_id)
```

### Planned column additions to **du_urban_entity**

Promoted from per-DU attributes in the new master CSVs.

```
ALTER TABLE du_urban_entity
  ADD COLUMN is_sw_du                          boolean,
  ADD COLUMN is_gw_du                          boolean,
  ADD COLUMN largest_system_centroid_lat       numeric,
  ADD COLUMN largest_system_centroid_lon       numeric,
  ADD COLUMN calsim_centroid_lat               numeric,
  ADD COLUMN calsim_centroid_lon               numeric,
  ADD COLUMN hhs_allocation_taf                numeric;
```

### Planned `du_urban_variable` update

Re-load with `Updated Master crosswalk SW DUs M&I May7 2026.xlsx` so each of the 75 SW DUs carries the agreed `delivery_variable`. Triggers a full re-run of `etl/statistics/du_urban/run_all.py` for every active scenario.

---
