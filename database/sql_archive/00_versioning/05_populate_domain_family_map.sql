-- =============================================================================
-- 05_populate_domain_family_map.sql
-- Populates domain_family_map with all database tables mapped to version families
-- Updated: March 2026 (post-migration 44  - includes database_level column)

\echo '============================================================================'
\echo 'POPULATING DOMAIN_FAMILY_MAP'
\echo '============================================================================'

-- =============================================================================
-- 1. Ensure audit version family (id=14) exists
-- =============================================================================
\echo ''
\echo 'Adding audit version family...'

INSERT INTO version_family (id, short_code, label, description, is_active)
VALUES (14, 'audit', 'Audit', 'Layer 00 system tables: versioning, developer registry, domain mapping, audit log', true)
ON CONFLICT (id) DO UPDATE SET short_code = EXCLUDED.short_code, label = EXCLUDED.label, description = EXCLUDED.description;

INSERT INTO version (id, version_family_id, version_number, is_active)
VALUES (14, 14, '1.0.0', true)
ON CONFLICT (id) DO UPDATE SET version_family_id = EXCLUDED.version_family_id;

-- =============================================================================
-- 2. Insert all domain_family_map records
-- =============================================================================
\echo ''
\echo 'Inserting domain_family_map records...'


INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, database_level, is_active, created_by, updated_by)
VALUES
    ('public', 'audit_log',           14, 'Audit log',                             '00', TRUE, 2, 2),
    ('public', 'developer',           14, 'Developer registry',                    '00', TRUE, 2, 2),
    ('public', 'domain_family_map',   14, 'Domain-to-family mapping',              '00', TRUE, 2, 2),
    ('public', 'version',             14, 'Version records',                       '00', TRUE, 2, 2),
    ('public', 'version_family',      14, 'Version family definitions',            '00', TRUE, 2, 2),

    ('public', 'geometry_type',       11, 'Geometry type definitions',             '01', TRUE, 2, 2),
    ('public', 'hydrologic_region',   11, 'Hydrologic region lookup',              '01', TRUE, 2, 2),
    ('public', 'model_source',        11, 'Model source lookup',                   '01', TRUE, 2, 2),
    ('public', 'network_entity_type', 11, 'Network entity type definitions',       '01', TRUE, 2, 2),
    ('public', 'network_subtype',     11, 'Network subtype definitions',           '01', TRUE, 2, 2),
    ('public', 'network_type',        11, 'Network type definitions',              '01', TRUE, 2, 2),
    ('public', 'source',              11, 'Data source lookup',                    '01', TRUE, 2, 2),
    ('public', 'spatial_scale',       11, 'Spatial scale lookup',                  '01', TRUE, 2, 2),
    ('public', 'statistic_category',  11, 'Statistic category lookup',             '01', TRUE, 2, 2),
    ('public', 'statistic_type',      11, 'Statistic type lookup',                 '01', TRUE, 2, 2),
    ('public', 'temporal_scale',      11, 'Temporal scale lookup',                 '01', TRUE, 2, 2),
    ('public', 'unit',                11, 'Unit lookup',                            '01', TRUE, 2, 2),
    ('public', 'watershed',           11, 'Watershed lookup',                      '01', TRUE, 2, 2),

    ('public', 'network',             12, 'Network definitions',                   '02', TRUE, 2, 2),
    ('public', 'network_arc',         12, 'Network arc definitions',               '02', TRUE, 2, 2),
    ('public', 'network_node',        12, 'Network node definitions',              '02', TRUE, 2, 2),
    ('public', 'network_gis',          9, 'Network GIS data',                      '02', TRUE, 2, 2),

    ('public', 'reservoir',            9, 'Reservoir spatial data',                '03', TRUE, 2, 2),
    ('public', 'compliance_station',   9, 'Compliance station locations',          '03', TRUE, 2, 2),
    ('public', 'wba',                  9, 'Water budget areas',                    '03', TRUE, 2, 2),
    ('public', 'du_agriculture_entity',13, 'Agricultural demand unit entities',    '03', TRUE, 2, 2),
    ('public', 'du_urban_entity',     13, 'Urban demand unit entities',            '03', TRUE, 2, 2),
    ('public', 'du_refuge_entity',    13, 'Refuge demand unit entities',           '03', TRUE, 2, 2),
    ('public', 'reservoir_entity',    13, 'Reservoir entity definitions',          '03', TRUE, 2, 2),
    ('public', 'mi_contractor',       13, 'MI contractor entities',               '03', TRUE, 2, 2),
    ('public', 'channel_entity',      13, 'Channel entity definitions',           '03', TRUE, 2, 2),
    ('public', 'ag_aggregate_entity', 13, 'Agricultural aggregate entities',      '03', TRUE, 2, 2),
    ('public', 'cws_aggregate_entity',13, 'CWS aggregate entities',              '03', TRUE, 2, 2),
    ('public', 'du_urban_group',      13, 'Urban demand unit groups',             '03', TRUE, 2, 2),
    ('public', 'du_urban_group_member',13,'Urban demand unit group membership',   '03', TRUE, 2, 2),
    ('public', 'du_urban_delivery_arc',12,'Urban DU delivery arcs',              '03', TRUE, 2, 2),
    ('public', 'mi_contractor_delivery_arc',12,'MI contractor delivery arcs',    '03', TRUE, 2, 2),
    ('public', 'mi_contractor_group', 13, 'MI contractor groups',                '03', TRUE, 2, 2),
    ('public', 'mi_contractor_group_member',13,'MI contractor group membership', '03', TRUE, 2, 2),
    ('public', 'reservoir_group',     13, 'Reservoir group definitions',          '03', TRUE, 2, 2),
    ('public', 'reservoir_group_member',13,'Reservoir group membership',          '03', TRUE, 2, 2),

    ('public', 'calsim_model_variable_type', 6, 'CalSim model variable types',   '04', TRUE, 2, 2),
    ('public', 'derived_variable_type',      6, 'Derived variable types',         '04', TRUE, 2, 2),
    ('public', 'variable_type',              6, 'Variable type definitions',      '04', TRUE, 2, 2),
    ('public', 'channel_variable',           6, 'Channel variable definitions',   '04', TRUE, 2, 2),
    ('public', 'du_urban_variable',          6, 'Urban demand unit variables',    '04', TRUE, 2, 2),

    ('public', 'assumption_category',  3, 'Assumption category lookup',           '05', TRUE, 2, 2),
    ('public', 'assumption_definition',3, 'Assumption definitions',               '05', TRUE, 2, 2),
    ('public', 'operation_category',   4, 'Operation category lookup',            '05', TRUE, 2, 2),
    ('public', 'operation_definition', 4, 'Operation definitions',                '05', TRUE, 2, 2),
    ('public', 'scenario_key_assumption_link', 2, 'Scenario-assumption crosswalk','05', TRUE, 2, 2),
    ('public', 'scenario_key_operation_link',  2, 'Scenario-operation crosswalk', '05', TRUE, 2, 2),

    ('public', 'scenario',             2, 'Scenario definitions',                 '06', TRUE, 2, 2),
    ('public', 'scenario_author',      2, 'Scenario authorship',                  '06', TRUE, 2, 2),
    ('public', 'scenario_tag',         2, 'Scenario classification tags',         '06', TRUE, 2, 2),
    ('public', 'scenario_tag_link',    2, 'Scenario-tag crosswalk',               '06', TRUE, 2, 2),

    ('public', 'hydroclimate',         5, 'Hydroclimate conditions',              '07', TRUE, 2, 2),
    ('public', 'slr',                  5, 'Sea level rise scenarios',             '07', TRUE, 2, 2),

    ('public', 'theme',                1, 'Theme definitions',                    '08', TRUE, 2, 2),
    ('public', 'theme_scenario_link',  2, 'Theme-scenario relationships',         '08', TRUE, 2, 2),

    ('public', 'tier_definition',      8, 'Tier definitions',                     '10', TRUE, 2, 2),
    ('public', 'tier_result',          8, 'Tier results',                         '10', TRUE, 2, 2),
    ('public', 'tier_location_result', 8, 'Tier location results',               '10', TRUE, 2, 2),

    ('public', 'reservoir_monthly_percentile', 7, 'Reservoir monthly percentile statistics', '11', TRUE, 2, 2),
    ('public', 'reservoir_period_summary',     7, 'Reservoir period summary statistics',     '11', TRUE, 2, 2),
    ('public', 'reservoir_spill_monthly',      7, 'Reservoir monthly spill statistics',      '11', TRUE, 2, 2),
    ('public', 'reservoir_storage_monthly',    7, 'Reservoir monthly storage statistics',    '11', TRUE, 2, 2),

    ('public', 'du_delivery_monthly',           7, 'Demand unit monthly delivery statistics',    '12', TRUE, 2, 2),
    ('public', 'du_shortage_monthly',           7, 'Demand unit monthly shortage statistics',    '12', TRUE, 2, 2),
    ('public', 'du_period_summary',             7, 'Demand unit period summary',                 '12', TRUE, 2, 2),
    ('public', 'mi_delivery_monthly',           7, 'MI monthly delivery statistics',             '12', TRUE, 2, 2),
    ('public', 'mi_shortage_monthly',           7, 'MI monthly shortage statistics',             '12', TRUE, 2, 2),
    ('public', 'mi_contractor_period_summary',  7, 'MI contractor period summary',               '12', TRUE, 2, 2),
    ('public', 'cws_aggregate_monthly',         7, 'CWS aggregate monthly statistics',           '12', TRUE, 2, 2),
    ('public', 'cws_aggregate_period_summary',  7, 'CWS aggregate period summary',               '12', TRUE, 2, 2),

    ('public', 'ag_du_demand_monthly',          7, 'Agricultural DU demand monthly',             '13', TRUE, 2, 2),
    ('public', 'ag_du_sw_delivery_monthly',     7, 'Agricultural DU surface water delivery',     '13', TRUE, 2, 2),
    ('public', 'ag_du_gw_pumping_monthly',      7, 'Agricultural DU groundwater pumping',        '13', TRUE, 2, 2),
    ('public', 'ag_du_shortage_monthly',        7, 'Agricultural DU monthly shortage statistics', '13', TRUE, 2, 2),
    ('public', 'ag_du_period_summary',          7, 'Agricultural DU period summary',             '13', TRUE, 2, 2),
    ('public', 'ag_aggregate_monthly',          7, 'Agricultural aggregate monthly statistics',  '13', TRUE, 2, 2),
    ('public', 'ag_aggregate_period_summary',   7, 'Agricultural aggregate period summary',      '13', TRUE, 2, 2),

    ('public', 'refuge_du_delivery_monthly',    7, 'Refuge DU monthly delivery statistics',      '14', TRUE, 2, 2),
    ('public', 'refuge_du_shortage_monthly',    7, 'Refuge DU monthly shortage statistics',      '14', TRUE, 2, 2),
    ('public', 'refuge_du_period_summary',      7, 'Refuge DU period summary',                   '14', TRUE, 2, 2),
    ('public', 'env_flow_season',               7, 'Environmental flow season lookup',            '14', TRUE, 2, 2),
    ('public', 'env_flow_channel_monthly',      7, 'Environmental flow monthly statistics',       '14', TRUE, 2, 2),
    ('public', 'env_flow_channel_seasonal',     7, 'Environmental flow seasonal statistics',      '14', TRUE, 2, 2),
    ('public', 'env_flow_channel_period_summary',7,'Environmental flow period summary',           '14', TRUE, 2, 2),
    ('public', 'delta_monthly',                 7, 'Delta monthly statistics',                    '14', TRUE, 2, 2),
    ('public', 'delta_period_summary',          7, 'Delta period summary',                        '14', TRUE, 2, 2)

ON CONFLICT (schema_name, table_name)
DO UPDATE SET
    version_family_id = EXCLUDED.version_family_id,
    note = EXCLUDED.note,
    database_level = EXCLUDED.database_level,
    is_active = EXCLUDED.is_active,
    updated_at = NOW(),
    updated_by = 2;

-- =============================================================================
-- 3. Verify results
-- =============================================================================
\echo ''
\echo 'Version families:'
SELECT id, short_code, label, is_active FROM version_family ORDER BY id;

\echo ''
\echo 'Domain family map summary by database level:'
SELECT
    dfm.database_level,
    COUNT(dfm.table_name) AS table_count
FROM domain_family_map dfm
GROUP BY dfm.database_level
ORDER BY dfm.database_level;

\echo ''
\echo 'Domain family map summary by version family:'
SELECT
    vf.id,
    vf.short_code AS family,
    COUNT(dfm.table_name) AS table_count
FROM version_family vf
LEFT JOIN domain_family_map dfm ON dfm.version_family_id = vf.id
GROUP BY vf.id, vf.short_code
ORDER BY vf.id;

\echo ''
\echo 'Tables still NOT mapped:'
SELECT t.table_name
FROM information_schema.tables t
WHERE t.table_schema = 'public'
AND t.table_type = 'BASE TABLE'
AND t.table_name NOT IN (SELECT table_name FROM domain_family_map)
AND t.table_name NOT LIKE 'spatial_ref%'
ORDER BY t.table_name;

\echo ''
\echo '============================================================================'
\echo 'DOMAIN_FAMILY_MAP POPULATION COMPLETE'
\echo '============================================================================'
