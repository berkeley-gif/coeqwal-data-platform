-- LOAD TIER TABLES USING AWS S3 EXTENSION
-- Uses aws_s3.table_import_from_s3 for direct S3 import
-- S3 bucket: coeqwal-seeds-dev (region us-west-2)
-- IAM role: AuroraS3ReadRole

\echo ''
\echo '🎯 LOADING TIER TABLES FROM S3 (AWS S3 EXTENSION)'
\echo '================================================'

\echo ''
\echo '📋 Finding tier version ID...'
SELECT v.id as tier_version_id, vf.short_code as family, v.version_number 
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE vf.short_code = 'tier';

\echo ''
\echo '🔧 Setting correct tier_version_id...'
DO $$
DECLARE
    tier_ver_id INTEGER;
BEGIN
    SELECT v.id INTO tier_ver_id
    FROM version v
    JOIN version_family vf ON v.version_family_id = vf.id
    WHERE vf.short_code = 'tier';
    
    IF tier_ver_id IS NOT NULL THEN
        EXECUTE format('ALTER TABLE tier_definition ALTER COLUMN tier_version_id SET DEFAULT %s', tier_ver_id);
        
        EXECUTE format('ALTER TABLE tier_result ALTER COLUMN tier_version_id SET DEFAULT %s', tier_ver_id);
        
        RAISE NOTICE '✅ Updated tier_version_id defaults to %', tier_ver_id;
    ELSE
        RAISE NOTICE '⚠️  Tier version not found, keeping default 9';
    END IF;
END $$;

\echo ''
\echo '📥 Loading tier_definition from S3...'

SELECT aws_s3.table_import_from_s3(
    'tier_definition',
    'short_code, name, description, tier_type, tier_count, is_active',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '06_tier/tier_definition.csv',
    'us-west-2'
);

\echo '✅ tier_definition loaded from S3'

\echo ''
\echo '📈 Loading tier_result from S3...'

SELECT aws_s3.table_import_from_s3(
    'tier_result',
    'scenario_short_code, tier_short_code, tier_1_value, tier_2_value, tier_3_value, tier_4_value, norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4, total_value, single_tier_level',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '06_tier/tier_result.csv',
    'us-west-2'
);

\echo '✅ tier_result loaded from S3'

\echo ''
\echo '🔍 VERIFYING TIER DATA:'
\echo '======================'

\echo ''
\echo '📊 Tier definitions loaded:'
SELECT short_code, name, tier_type, tier_count, tier_version_id, is_active 
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
\echo '🎯 Sample multi-value tier (Environmental flows):'
SELECT 
    scenario_short_code,
    ARRAY[tier_1_value, tier_2_value, tier_3_value, tier_4_value] as raw_values,
    ARRAY[norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4] as normalized_values,
    total_value
FROM tier_result 
WHERE tier_short_code = 'ENV_FLOWS'
ORDER BY scenario_short_code;

\echo ''
\echo '🎯 Sample single-value tier (Delta ecology):'
SELECT 
    scenario_short_code,
    single_tier_level,
    tier_version_id
FROM tier_result 
WHERE tier_short_code = 'DELTA_ECOLOGY'
ORDER BY scenario_short_code;

\echo ''
\echo '📊 Tier data statistics:'
SELECT 
    'Multi-value tiers' as tier_type,
    COUNT(DISTINCT tier_short_code) as count
FROM tier_result 
WHERE tier_1_value IS NOT NULL

UNION ALL

SELECT 
    'Single-value tiers' as tier_type,
    COUNT(DISTINCT tier_short_code) as count
FROM tier_result 
WHERE single_tier_level IS NOT NULL;

\echo ''
\echo '🎉 TIER TABLES LOADED AND VERIFIED!'
\echo '=================================='
\echo 'Ready for D3 visualization and tier reporting!'
\echo ''
\echo '🎯 D3 USAGE:'
\echo '• Multi-value tiers: Use norm_tier_1 through norm_tier_4 for bar widths'
\echo '• Single-value tiers: Use single_tier_level for tier display'
\echo '• All values pre-calculated and ready for charts!'
