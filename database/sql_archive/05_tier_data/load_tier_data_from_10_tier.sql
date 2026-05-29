-- LOAD TIER DATA FROM 10_tier S3 DIRECTORY
-- Handle existing tables and load new data with updated short codes

\echo ''
\echo '🎯 LOADING TIER DATA FROM 10_tier S3 DIRECTORY'
\echo '=============================================='

\echo ''
\echo '📋 Checking existing tier tables...'
SELECT 
    'tier_definition' as table_name,
    COUNT(*) as record_count
FROM tier_definition

UNION ALL

SELECT 
    'tier_result' as table_name,
    COUNT(*) as record_count  
FROM tier_result;

\echo ''
\echo '🧹 Clearing existing tier data...'
TRUNCATE TABLE tier_result CASCADE;
TRUNCATE TABLE tier_definition CASCADE;

\echo '✅ Existing data cleared'

\echo ''
\echo '📋 Getting tier version ID...'
SELECT v.id as tier_version_id, vf.short_code as family, v.version_number 
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE vf.short_code = 'tier';

\echo ''
\echo '🔧 Setting correct tier_version_id to 8...'
ALTER TABLE tier_definition ALTER COLUMN tier_version_id SET DEFAULT 8;
ALTER TABLE tier_result ALTER COLUMN tier_version_id SET DEFAULT 8;

\echo '✅ Updated defaults to tier_version_id = 8'

\echo ''
\echo '📥 Loading tier_definition from S3 (10_tier directory)...'

SELECT aws_s3.table_import_from_s3(
    'tier_definition',
    'short_code, name, description, tier_type, tier_count, is_active',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '10_tier/tier_definition.csv',
    'us-west-2'
);

\echo '✅ tier_definition loaded from S3'

\echo ''
\echo '📈 Loading tier_result from S3 (10_tier directory)...'

SELECT aws_s3.table_import_from_s3(
    'tier_result',
    'scenario_short_code, tier_short_code, tier_1_value, tier_2_value, tier_3_value, tier_4_value, norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4, total_value, single_tier_level',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '10_tier/tier_result.csv',
    'us-west-2'
);

\echo '✅ tier_result loaded from S3'

\echo ''
\echo '🔍 VERIFYING TIER DATA:'
\echo '======================'

\echo ''
\echo '📊 Tier definitions loaded:'
SELECT short_code, name, tier_type, tier_count, tier_version_id 
FROM tier_definition 
ORDER BY short_code;

\echo ''
\echo '📈 Tier results summary:'
SELECT 
    tier_short_code,
    COUNT(*) as scenario_count,
    COUNT(CASE WHEN tier_1_value IS NOT NULL THEN 1 END) as multi_value_count,
    COUNT(CASE WHEN single_tier_level IS NOT NULL THEN 1 END) as single_value_count
FROM tier_result 
GROUP BY tier_short_code 
ORDER BY tier_short_code;

\echo ''
\echo '🎯 Multi-value tier example (ENV_FLOWS):'
SELECT 
    scenario_short_code,
    ARRAY[tier_1_value, tier_2_value, tier_3_value, tier_4_value] as raw_values,
    ARRAY[norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4] as normalized_values,
    total_value
FROM tier_result 
WHERE tier_short_code = 'ENV_FLOWS'
ORDER BY scenario_short_code;

\echo ''
\echo '🎯 Single-value tier example (DELTA_ECO):'
SELECT 
    scenario_short_code,
    single_tier_level
FROM tier_result 
WHERE tier_short_code = 'DELTA_ECO'
ORDER BY scenario_short_code;

\echo ''
\echo '📊 All tier short codes:'
SELECT DISTINCT tier_short_code 
FROM tier_result 
ORDER BY tier_short_code;

\echo ''
\echo '🎉 TIER DATA SUCCESSFULLY LOADED FROM 10_tier!'
\echo '=============================================='
\echo 'Ready for D3 visualization with:'
\echo '• Updated short codes (DELTA_ECO, FW_EXP, RES_STOR, etc.)'
\echo '• Multi-value tiers: norm_tier_1 through norm_tier_4 (0-1 scale)'
\echo '• Single-value tiers: single_tier_level (1-4)'
\echo '• All data properly typed and normalized!'
