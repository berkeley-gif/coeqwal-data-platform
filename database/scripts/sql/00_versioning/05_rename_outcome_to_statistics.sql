-- =============================================================================
-- 05_rename_outcome_to_statistics.sql
-- Renames version_family 'outcome' to 'statistics' (id=7)
-- =============================================================================
-- Run in Cloud9: psql -f 05_rename_outcome_to_statistics.sql
-- =============================================================================

-- Show current state
\echo 'Current version_family id=7:'
SELECT id, short_code, label, is_active FROM version_family WHERE id = 7;

-- Update the short_code and label
UPDATE version_family 
SET 
    short_code = 'statistics',
    label = 'Statistics',
    updated_at = NOW()
WHERE id = 7;

-- Also update the corresponding version record
UPDATE version 
SET 
    updated_at = NOW()
WHERE version_family_id = 7;

-- Verify the change
\echo ''
\echo 'After update:'
SELECT id, short_code, label, is_active FROM version_family WHERE id = 7;

\echo ''
\echo 'Associated version record:'
SELECT v.id, v.version_number, vf.short_code, v.is_active
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE vf.id = 7;

\echo ''
\echo 'SUCCESS: version_family id=7 renamed from outcome to statistics';
