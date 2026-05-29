BEGIN;

UPDATE developer
SET    affiliation = 'Berkeley Geospatial Innovation Facility',
       updated_at  = NOW()
WHERE  id = 2;

UPDATE version_family
SET    short_code   = 'audit',
       label        = 'Audit',
       description  = 'Layer 00 system tables: versioning, developer registry, domain mapping, audit log',
       updated_at   = NOW()
WHERE  id = 14;

UPDATE domain_family_map
SET    created_by = 2,
       updated_by = 2,
       updated_at = NOW()
WHERE  created_by = 1 OR updated_by = 1;

UPDATE domain_family_map SET updated_at = NOW(), database_level = CASE table_name
    WHEN 'developer'                        THEN '00'
    WHEN 'version_family'                   THEN '00'
    WHEN 'version'                          THEN '00'
    WHEN 'domain_family_map'                THEN '00'
    WHEN 'audit_log'                        THEN '00'
    WHEN 'hydrologic_region'                THEN '01'
    WHEN 'source'                           THEN '01'
    WHEN 'model_source'                     THEN '01'
    WHEN 'unit'                             THEN '01'
    WHEN 'spatial_scale'                    THEN '01'
    WHEN 'temporal_scale'                   THEN '01'
    WHEN 'statistic_category'               THEN '01'
    WHEN 'statistic_type'                   THEN '01'
    WHEN 'geometry_type'                    THEN '01'
    WHEN 'network_type'                     THEN '01'
    WHEN 'network_subtype'                  THEN '01'
    WHEN 'network_entity_type'              THEN '01'
    WHEN 'watershed'                        THEN '01'
    WHEN 'network'                          THEN '02'
    WHEN 'network_arc'                      THEN '02'
    WHEN 'network_node'                     THEN '02'
    WHEN 'network_gis'                      THEN '02'
    WHEN 'reservoir'                        THEN '03'
    WHEN 'reservoir_entity'                 THEN '03'
    WHEN 'reservoir_group'                  THEN '03'
    WHEN 'reservoir_group_member'           THEN '03'
    WHEN 'compliance_station'               THEN '03'
    WHEN 'du_agriculture_entity'            THEN '03'
    WHEN 'du_urban_entity'                  THEN '03'
    WHEN 'du_urban_group'                   THEN '03'
    WHEN 'du_urban_group_member'            THEN '03'
    WHEN 'du_refuge_entity'                 THEN '03'
    WHEN 'mi_contractor'                    THEN '03'
    WHEN 'mi_contractor_group'              THEN '03'
    WHEN 'mi_contractor_group_member'       THEN '03'
    WHEN 'ag_aggregate_entity'              THEN '03'
    WHEN 'cws_aggregate_entity'             THEN '03'
    WHEN 'channel_entity'                   THEN '03'
    WHEN 'wba'                              THEN '03'
    WHEN 'calsim_model_variable_type'       THEN '04'
    WHEN 'derived_variable_type'            THEN '04'
    WHEN 'variable_type'                    THEN '04'
    WHEN 'channel_variable'                 THEN '04'
    WHEN 'du_urban_variable'                THEN '04'
    WHEN 'assumption_category'              THEN '05'
    WHEN 'assumption_definition'            THEN '05'
    WHEN 'operation_category'               THEN '05'
    WHEN 'operation_definition'             THEN '05'
    WHEN 'scenario_key_assumption_link'     THEN '05'
    WHEN 'scenario_key_operation_link'      THEN '05'
    WHEN 'scenario'                         THEN '06'
    WHEN 'scenario_author'                  THEN '06'
    WHEN 'hydroclimate'                     THEN '07'
    WHEN 'slr'                              THEN '07'
    WHEN 'theme'                            THEN '08'
    WHEN 'theme_scenario_link'              THEN '08'
    WHEN 'spatial_ref_sys'                  THEN '09'
    WHEN 'du_urban_delivery_arc'            THEN '10'
    WHEN 'mi_contractor_delivery_arc'       THEN '10'
    WHEN 'tier_definition'                  THEN '10'
    WHEN 'tier_result'                      THEN '10'
    WHEN 'tier_location_result'             THEN '10'
    WHEN 'reservoir_storage_monthly'        THEN '11'
    WHEN 'reservoir_spill_monthly'          THEN '11'
    WHEN 'reservoir_period_summary'         THEN '11'
    WHEN 'reservoir_monthly_percentile'     THEN '11'
    WHEN 'du_delivery_monthly'              THEN '12'
    WHEN 'du_shortage_monthly'              THEN '12'
    WHEN 'du_period_summary'                THEN '12'
    WHEN 'mi_delivery_monthly'              THEN '12'
    WHEN 'mi_shortage_monthly'              THEN '12'
    WHEN 'mi_contractor_period_summary'     THEN '12'
    WHEN 'cws_aggregate_monthly'            THEN '12'
    WHEN 'cws_aggregate_period_summary'     THEN '12'
    WHEN 'ag_du_delivery_monthly'           THEN '13'
    WHEN 'ag_du_shortage_monthly'           THEN '13'
    WHEN 'ag_du_period_summary'             THEN '13'
    WHEN 'ag_du_demand_monthly'             THEN '13'
    WHEN 'ag_du_gw_pumping_monthly'         THEN '13'
    WHEN 'ag_du_sw_delivery_monthly'        THEN '13'
    WHEN 'ag_aggregate_monthly'             THEN '13'
    WHEN 'ag_aggregate_period_summary'      THEN '13'
    WHEN 'refuge_du_delivery_monthly'       THEN '13'
    WHEN 'refuge_du_shortage_monthly'       THEN '13'
    WHEN 'refuge_du_period_summary'         THEN '13'
    WHEN 'env_flow_season'                  THEN '14'
    WHEN 'env_flow_channel_monthly'         THEN '14'
    WHEN 'env_flow_channel_seasonal'        THEN '14'
    WHEN 'env_flow_channel_period_summary'  THEN '14'
    WHEN 'delta_monthly'                    THEN '15'
    WHEN 'delta_period_summary'             THEN '15'
    ELSE NULL
END;


UPDATE wba
SET    hydrologic_region_id = 3,
       updated_at = NOW()
WHERE  id = 1;

INSERT INTO statistic_category (id, short_code, label, description, created_by, updated_by)
VALUES
    (1, 'summary',         'Summary',         'Aggregate summary statistics (mean, median, min, max, cv, stdev)', 2, 2),
    (2, 'percentile_band', 'Percentile Band', 'Standard quantile bands aligned with DWR water year types',       2, 2),
    (3, 'exceedance',      'Exceedance',      'Exceedance percentiles for flow duration and reliability analysis', 2, 2);

SELECT setval('statistic_category_id_seq', (SELECT MAX(id) FROM statistic_category));

INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, database_level, created_by, updated_by)
VALUES ('public', 'statistic_category', 11, 'Statistic category lookup', '01', 2, 2)
ON CONFLICT (schema_name, table_name) DO UPDATE
SET note = EXCLUDED.note, database_level = EXCLUDED.database_level;


UPDATE statistic_type SET id = -14 WHERE id = 14;
UPDATE statistic_type SET id = -15 WHERE id = 15;
UPDATE statistic_type SET id = -7  WHERE id = 7;
UPDATE statistic_type SET id = -8  WHERE id = 8;
UPDATE statistic_type SET id = -9  WHERE id = 9;
UPDATE statistic_type SET id = -10 WHERE id = 10;
UPDATE statistic_type SET id = -11 WHERE id = 11;
UPDATE statistic_type SET id = -12 WHERE id = 12;
UPDATE statistic_type SET id = -13 WHERE id = 13;

UPDATE statistic_type SET id = 5  WHERE id = -14;
UPDATE statistic_type SET id = 6  WHERE id = -15;
UPDATE statistic_type SET id = 7  WHERE id = -7;
UPDATE statistic_type SET id = 8  WHERE id = -8;
UPDATE statistic_type SET id = 9  WHERE id = -9;
UPDATE statistic_type SET id = 10 WHERE id = -10;
UPDATE statistic_type SET id = 11 WHERE id = -11;
UPDATE statistic_type SET id = 12 WHERE id = -12;
UPDATE statistic_type SET id = 13 WHERE id = -13;

INSERT INTO statistic_type (id, short_code, label, description, is_percentile, created_by, updated_by)
VALUES
    (14, 'EXC_P5',  '5th exceedance',  'Value exceeded 5% of time (very wet)',    true, 2, 2),
    (15, 'EXC_P10', '10th exceedance', 'Value exceeded 10% of time (wet)',        true, 2, 2),
    (16, 'EXC_P25', '25th exceedance', 'Value exceeded 25% of time (above avg)',  true, 2, 2),
    (17, 'EXC_P50', '50th exceedance', 'Value exceeded 50% of time (median)',     true, 2, 2),
    (18, 'EXC_P75', '75th exceedance', 'Value exceeded 75% of time (below avg)',  true, 2, 2),
    (19, 'EXC_P90', '90th exceedance', 'Value exceeded 90% of time (dry)',        true, 2, 2),
    (20, 'EXC_P95', '95th exceedance', 'Value exceeded 95% of time (very dry)',   true, 2, 2);

SELECT setval('statistic_type_id_seq', (SELECT MAX(id) FROM statistic_type));

UPDATE statistic_type SET statistic_category_id = 1 WHERE short_code IN ('MEAN','MEDIAN','MIN','MAX','CV','STDEV');
UPDATE statistic_type SET statistic_category_id = 2 WHERE short_code LIKE 'Q%';
UPDATE statistic_type SET statistic_category_id = 3 WHERE short_code LIKE 'EXC_%';

COMMIT;

\echo ''
\echo '30b DATA CHANGES COMPLETE'
\echo '========================='
\echo 'Now run 30c_finalize_schema.sql as postgres.'
