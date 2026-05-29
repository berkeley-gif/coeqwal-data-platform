-- UPDATE SOURCE TABLE FROM SEED DATA
-- Adds missing records from seed CSV to match complete lookup table

\echo ''
\echo '📊 UPDATING SOURCE FROM SEED DATA'
\echo '================================='

\echo ''
\echo '🔍 Current source records:'
SELECT id, source, description, is_active FROM source ORDER BY id;

\echo ''
\echo '➕ Adding missing sources from seed data...'

INSERT INTO source (source, description, is_active, created_by, updated_by) VALUES
('geopackage', 'CalSim3_GeoSchematic_20221227_COEQWAL_Revisions2024_corrected.gpkg', true, coeqwal_current_operator(), coeqwal_current_operator()),
('trend_report', 'Variables extracted from Gilbert team trend reports', true, coeqwal_current_operator(), coeqwal_current_operator()),
('metadata', 'Scenario metadata', true, coeqwal_current_operator(), coeqwal_current_operator()),
('cvm_docs', 'Central Valley Model documentation', true, coeqwal_current_operator(), coeqwal_current_operator()),
('network_schematic', 'Network schematic', true, coeqwal_current_operator(), coeqwal_current_operator()),
('manual', 'Manual insertion', true, coeqwal_current_operator(), coeqwal_current_operator())
ON CONFLICT (source) DO NOTHING;

\echo ''
\echo '✅ UPDATED SOURCE TABLE:'
\echo '======================='
SELECT id, source, description, is_active, created_by, updated_by 
FROM source 
ORDER BY id;

\echo ''
\echo '📊 SUMMARY:'
SELECT 
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE is_active = true) as active_records,
    COUNT(*) FILTER (WHERE created_by = coeqwal_current_operator()) as newly_added
FROM source;

\echo ''
\echo '🎉 SOURCE UPDATE COMPLETE!'
