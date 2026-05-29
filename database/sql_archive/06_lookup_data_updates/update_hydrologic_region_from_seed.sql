-- UPDATE HYDROLOGIC_REGION TABLE FROM SEED DATA
-- Adds missing records from seed CSV to match complete lookup table

\echo ''
\echo '📊 UPDATING HYDROLOGIC_REGION FROM SEED DATA'
\echo '============================================='

\echo ''
\echo '🔍 Current hydrologic_region records:'
SELECT id, short_code, label, is_active FROM hydrologic_region ORDER BY id;

\echo ''
\echo '➕ Adding missing hydrologic regions from seed data...'

INSERT INTO hydrologic_region (short_code, label, is_active, created_by, updated_by) VALUES
('TULARE', 'Tulare Basin', true, coeqwal_current_operator(), coeqwal_current_operator()),
('SOCAL', 'Southern California', true, coeqwal_current_operator(), coeqwal_current_operator()),
('EXTERNAL', 'External areas', true, coeqwal_current_operator(), coeqwal_current_operator())
ON CONFLICT (short_code) DO NOTHING;

\echo ''
\echo '✅ UPDATED HYDROLOGIC_REGION TABLE:'
\echo '=================================='
SELECT id, short_code, label, is_active, created_by, updated_by 
FROM hydrologic_region 
ORDER BY id;

\echo ''
\echo '📊 SUMMARY:'
SELECT 
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE is_active = true) as active_records,
    COUNT(*) FILTER (WHERE created_by = coeqwal_current_operator()) as newly_added
FROM hydrologic_region;

\echo ''
\echo '🎉 HYDROLOGIC_REGION UPDATE COMPLETE!'
