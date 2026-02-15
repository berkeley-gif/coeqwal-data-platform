-- =============================================================================
-- 08_populate_domain_family_map.sql
-- Populates domain_family_map with all database tables mapped to version families
-- =============================================================================
-- Run in Cloud9: \i database/scripts/sql/00_versioning/08_populate_domain_family_map.sql
-- =============================================================================

\echo '============================================================================'
\echo 'POPULATING DOMAIN_FAMILY_MAP'
\echo '============================================================================'

-- =============================================================================
-- 1. Add infrastructure version family (id=14)
-- =============================================================================
\echo ''
\echo 'Adding infrastructure version family...'

INSERT INTO version_family (id, short_code, label, is_active)
VALUES (14, 'infrastructure', 'Infrastructure', true)
ON CONFLICT (id) DO UPDATE SET short_code = EXCLUDED.short_code, label = EXCLUDED.label;

INSERT INTO version (id, version_family_id, version_number, is_active)
VALUES (14, 14, '1.0.0', true)
ON CONFLICT (id) DO UPDATE SET version_family_id = EXCLUDED.version_family_id;

-- =============================================================================
-- 2. Insert all domain_family_map records
-- =============================================================================
\echo ''
\echo 'Inserting domain_family_map records...'

-- Use a single INSERT with ON CONFLICT to handle existing records
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note)
VALUES
    -- theme (1)
    ('public', 'theme', 1, 'Theme definitions'),
    
    -- scenario (2)
    ('public', 'scenario', 2, 'Scenario definitions'),
    ('public', 'scenario_author', 2, 'Scenario authorship'),
    ('public', 'theme_scenario_link', 2, 'Theme-scenario relationships'),
    ('public', 'scenario_key_assumption_link', 2, 'Scenario-assumption relationships'),
    ('public', 'scenario_key_operation_link', 2, 'Scenario-operation relationships'),
    
    -- assumption (3)
    ('public', 'assumption_definition', 3, 'Assumption definitions'),
    
    -- operation (4)
    ('public', 'operation_definition', 4, 'Operation definitions'),
    
    -- hydroclimate (5)
    ('public', 'hydroclimate', 5, 'Hydroclimate data'),
    
    -- variable (6)
    ('public', 'variable_type', 6, 'Variable type definitions'),
    ('public', 'du_urban_variable', 6, 'Urban demand unit variables'),
    
    -- statistics (7)
    ('public', 'reservoir_monthly_percentile', 7, 'Reservoir monthly percentile statistics'),
    ('public', 'reservoir_period_summary', 7, 'Reservoir period summary statistics'),
    ('public', 'reservoir_spill_monthly', 7, 'Reservoir monthly spill statistics'),
    ('public', 'reservoir_storage_monthly', 7, 'Reservoir monthly storage statistics'),
    ('public', 'ag_aggregate_monthly', 7, 'Agricultural aggregate monthly statistics'),
    ('public', 'ag_aggregate_period_summary', 7, 'Agricultural aggregate period summary'),
    ('public', 'ag_du_delivery_monthly', 7, 'Agricultural DU monthly delivery statistics'),
    ('public', 'ag_du_period_summary', 7, 'Agricultural DU period summary'),
    ('public', 'ag_du_shortage_monthly', 7, 'Agricultural DU monthly shortage statistics'),
    ('public', 'cws_aggregate_monthly', 7, 'CWS aggregate monthly statistics'),
    ('public', 'cws_aggregate_period_summary', 7, 'CWS aggregate period summary'),
    ('public', 'du_delivery_monthly', 7, 'Demand unit monthly delivery statistics'),
    ('public', 'du_period_summary', 7, 'Demand unit period summary'),
    ('public', 'du_shortage_monthly', 7, 'Demand unit monthly shortage statistics'),
    ('public', 'mi_delivery_monthly', 7, 'MI monthly delivery statistics'),
    ('public', 'mi_shortage_monthly', 7, 'MI monthly shortage statistics'),
    ('public', 'mi_contractor_period_summary', 7, 'MI contractor period summary'),
    
    -- tier (8)
    ('public', 'tier_definition', 8, 'Tier definitions'),
    ('public', 'tier_result', 8, 'Tier results'),
    ('public', 'tier_location_result', 8, 'Tier location results'),
    
    -- geospatial (9)
    ('public', 'network_gis', 9, 'Network GIS data'),
    ('public', 'wba', 9, 'Water budget areas'),
    ('public', 'compliance_stations', 9, 'Compliance station locations'),
    ('public', 'spatial_ref_sys', 9, 'PostGIS spatial reference systems'),
    
    -- metadata (11) - lookup tables
    ('public', 'geometry_type', 9, 'Geometry type definitions'),
    ('public', 'hydrologic_region', 11, 'Hydrologic region lookup'),
    ('public', 'model_source', 11, 'Model source lookup'),
    ('public', 'source', 11, 'Data source lookup'),
    ('public', 'spatial_scale', 11, 'Spatial scale lookup'),
    ('public', 'statistic_type', 11, 'Statistic type lookup'),
    ('public', 'temporal_scale', 11, 'Temporal scale lookup'),
    ('public', 'unit', 11, 'Unit lookup'),
    
    -- network (12)
    ('public', 'network_entity_type', 12, 'Network entity type definitions'),
    ('public', 'network', 12, 'Network definitions'),
    ('public', 'network_arc', 12, 'Network arc definitions'),
    ('public', 'network_node', 12, 'Network node definitions'),
    ('public', 'network_type', 12, 'Network type definitions'),
    ('public', 'network_subtype', 12, 'Network subtype definitions'),
    ('public', 'du_urban_delivery_arc', 12, 'Urban DU delivery arcs'),
    ('public', 'mi_contractor_delivery_arc', 12, 'MI contractor delivery arcs'),
    
    -- entity (13)
    ('public', 'reservoir_entity', 13, 'Reservoir entity definitions'),
    ('public', 'reservoir_group', 13, 'Reservoir group definitions'),
    ('public', 'reservoir_group_member', 13, 'Reservoir group membership'),
    ('public', 'reservoirs', 13, 'Reservoir base table'),
    ('public', 'ag_aggregate_entity', 13, 'Agricultural aggregate entities'),
    ('public', 'cws_aggregate_entity', 13, 'CWS aggregate entities'),
    ('public', 'du_agriculture_entity', 13, 'Agricultural demand unit entities'),
    ('public', 'du_urban_entity', 13, 'Urban demand unit entities'),
    ('public', 'du_urban_group', 13, 'Urban demand unit groups'),
    ('public', 'du_urban_group_member', 13, 'Urban demand unit group membership'),
    ('public', 'mi_contractor', 13, 'MI contractor entities'),
    ('public', 'mi_contractor_group', 13, 'MI contractor groups'),
    ('public', 'mi_contractor_group_member', 13, 'MI contractor group membership'),
    
    -- infrastructure (14)
    ('public', 'audit_log', 14, 'Audit log infrastructure'),
    ('public', 'developer', 14, 'Developer registry'),
    ('public', 'domain_family_map', 14, 'Domain-to-family mapping'),
    ('public', 'version', 14, 'Version records'),
    ('public', 'version_family', 14, 'Version family definitions')

ON CONFLICT (schema_name, table_name) 
DO UPDATE SET 
    version_family_id = EXCLUDED.version_family_id,
    note = EXCLUDED.note,
    updated_at = NOW(),
    updated_by = coeqwal_current_operator();

-- =============================================================================
-- 3. Verify results
-- =============================================================================
\echo ''
\echo 'Version families:'
SELECT id, short_code, label, is_active FROM version_family ORDER BY id;

\echo ''
\echo 'Domain family map summary by version family:'
SELECT 
    vf.id,
    vf.short_code as family,
    COUNT(dfm.table_name) as table_count
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
ORDER BY t.table_name;

\echo ''
\echo '============================================================================'
\echo 'DOMAIN_FAMILY_MAP POPULATION COMPLETE'
\echo '============================================================================'
