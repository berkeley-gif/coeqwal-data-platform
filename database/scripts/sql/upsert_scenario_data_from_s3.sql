-- UPSERT SCENARIO DATA FROM S3 (05_themes_scenarios directory)
-- Updates scenario, theme_scenario_link, scenario_key_assumption_link, scenario_key_operation_link
-- Created: December 2025

\echo ''
\echo '🔄 UPSERTING SCENARIO DATA FROM S3 (05_themes_scenarios)'
\echo '========================================================'

-- Check current data status
\echo ''
\echo '📋 Current scenario table status:'
SELECT 'scenario' as table_name, COUNT(*) as record_count FROM scenario
UNION ALL
SELECT 'theme_scenario_link' as table_name, COUNT(*) as record_count FROM theme_scenario_link
UNION ALL
SELECT 'scenario_key_assumption_link' as table_name, COUNT(*) as record_count FROM scenario_key_assumption_link
UNION ALL
SELECT 'scenario_key_operation_link' as table_name, COUNT(*) as record_count FROM scenario_key_operation_link;

-- =============================================================================
-- STEP 1: Clear link tables first (they reference scenario)
-- =============================================================================
\echo ''
\echo '🧹 Clearing link tables...'
TRUNCATE TABLE theme_scenario_link CASCADE;
TRUNCATE TABLE scenario_key_assumption_link CASCADE;
TRUNCATE TABLE scenario_key_operation_link CASCADE;
\echo '✅ Link tables cleared'

-- =============================================================================
-- STEP 2: Upsert scenario table (use temp table for upsert logic)
-- =============================================================================
\echo ''
\echo '📥 Loading scenarios from S3...'

-- Create temp table for staging
CREATE TEMP TABLE scenario_staging (
    id INTEGER,
    scenario_id VARCHAR,
    short_code VARCHAR,
    is_active INTEGER,
    name TEXT,
    subtitle TEXT,
    short_title TEXT,
    simple_description TEXT,
    description TEXT,
    narrative JSONB,
    baseline_scenario_id INTEGER,
    hydroclimate_id INTEGER,
    scenario_author_id INTEGER,
    scenario_version_id INTEGER,
    created_by INTEGER,
    updated_by INTEGER
);

-- Load from S3
SELECT aws_s3.table_import_from_s3(
    'scenario_staging',
    'id, scenario_id, short_code, is_active, name, subtitle, short_title, simple_description, description, narrative, baseline_scenario_id, hydroclimate_id, scenario_author_id, scenario_version_id, created_by, updated_by',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '05_themes_scenarios/scenario.csv',
    'us-west-2'
);

-- Upsert from staging to scenario
INSERT INTO scenario (
    id, scenario_id, short_code, is_active, name, subtitle, short_title, 
    simple_description, description, narrative, baseline_scenario_id, 
    hydroclimate_id, scenario_author_id, scenario_version_id, created_by, updated_by
)
SELECT 
    id, scenario_id, short_code, is_active::boolean, name, subtitle, short_title,
    simple_description, description, narrative, baseline_scenario_id,
    hydroclimate_id, scenario_author_id, scenario_version_id, 
    COALESCE(created_by, 1), COALESCE(updated_by, 1)
FROM scenario_staging
ON CONFLICT (id) DO UPDATE SET
    scenario_id = EXCLUDED.scenario_id,
    short_code = EXCLUDED.short_code,
    is_active = EXCLUDED.is_active,
    name = EXCLUDED.name,
    subtitle = EXCLUDED.subtitle,
    short_title = EXCLUDED.short_title,
    simple_description = EXCLUDED.simple_description,
    description = EXCLUDED.description,
    narrative = EXCLUDED.narrative,
    baseline_scenario_id = EXCLUDED.baseline_scenario_id,
    hydroclimate_id = EXCLUDED.hydroclimate_id,
    scenario_author_id = EXCLUDED.scenario_author_id,
    scenario_version_id = EXCLUDED.scenario_version_id,
    updated_by = EXCLUDED.updated_by,
    updated_at = NOW();

DROP TABLE scenario_staging;

\echo '✅ Scenarios upserted'

-- Reset sequence to max id + 1
SELECT setval('scenario_id_seq', COALESCE((SELECT MAX(id) FROM scenario), 0) + 1, false);
\echo '✅ Sequence reset'

-- =============================================================================
-- STEP 3: Load theme_scenario_link
-- =============================================================================
\echo ''
\echo '📥 Loading theme_scenario_link from S3...'

SELECT aws_s3.table_import_from_s3(
    'theme_scenario_link',
    'theme_id, scenario_id',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '05_themes_scenarios/theme_scenario_link.csv',
    'us-west-2'
);

\echo '✅ theme_scenario_link loaded'

-- =============================================================================
-- STEP 4: Load scenario_key_assumption_link
-- =============================================================================
\echo ''
\echo '📥 Loading scenario_key_assumption_link from S3...'

SELECT aws_s3.table_import_from_s3(
    'scenario_key_assumption_link',
    'scenario_id, assumption_id',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '05_themes_scenarios/scenario_key_assumption_link.csv',
    'us-west-2'
);

\echo '✅ scenario_key_assumption_link loaded'

-- =============================================================================
-- STEP 5: Load scenario_key_operation_link
-- =============================================================================
\echo ''
\echo '📥 Loading scenario_key_operation_link from S3...'

SELECT aws_s3.table_import_from_s3(
    'scenario_key_operation_link',
    'scenario_id, operation_id',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '05_themes_scenarios/scenario_key_operation_link.csv',
    'us-west-2'
);

\echo '✅ scenario_key_operation_link loaded'

-- =============================================================================
-- VERIFICATION
-- =============================================================================
\echo ''
\echo '🔍 VERIFICATION:'
\echo '================'

\echo ''
\echo '📊 Scenario summary:'
SELECT id, scenario_id, short_title, is_active, baseline_scenario_id
FROM scenario
ORDER BY id;

\echo ''
\echo '📊 Theme assignments:'
SELECT s.scenario_id, t.short_code as theme, t.name as theme_name
FROM theme_scenario_link tsl
JOIN scenario s ON tsl.scenario_id = s.id
JOIN theme t ON tsl.theme_id = t.id
ORDER BY s.id;

\echo ''
\echo '📊 Assumption assignments:'
SELECT s.scenario_id, ad.short_code as assumption, ad.category
FROM scenario_key_assumption_link skal
JOIN scenario s ON skal.scenario_id = s.id
JOIN assumption_definition ad ON skal.assumption_id = ad.id
ORDER BY s.id, ad.category;

\echo ''
\echo '📊 Operation assignments:'
SELECT s.scenario_id, od.short_code as operation, od.category
FROM scenario_key_operation_link skol
JOIN scenario s ON skol.scenario_id = s.id
JOIN operation_definition od ON skol.operation_id = od.id
ORDER BY s.id;

\echo ''
\echo '📊 Final counts:'
SELECT 
    (SELECT COUNT(*) FROM scenario) as scenarios,
    (SELECT COUNT(*) FROM theme_scenario_link) as theme_links,
    (SELECT COUNT(*) FROM scenario_key_assumption_link) as assumption_links,
    (SELECT COUNT(*) FROM scenario_key_operation_link) as operation_links;

\echo ''
\echo '🎉 SCENARIO DATA UPSERT COMPLETE!'
\echo '================================='

