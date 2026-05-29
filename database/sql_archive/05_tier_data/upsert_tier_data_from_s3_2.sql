-- ============================================
-- UPSERT TIER DATA FROM S3
-- ============================================

SELECT 'tier_definition' as table_name, COUNT(*) as record_count FROM tier_definition
UNION ALL
SELECT 'tier_result' as table_name, COUNT(*) as record_count FROM tier_result
UNION ALL
SELECT 'tier_location_result' as table_name, COUNT(*) as record_count FROM tier_location_result;

TRUNCATE TABLE tier_location_result;
TRUNCATE TABLE tier_result;

SELECT aws_s3.table_import_from_s3(
    'tier_result',
    'scenario_short_code, tier_short_code, tier_1_value, tier_2_value, tier_3_value, tier_4_value, norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4, total_value, single_tier_level',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '10_tier/tier_result.csv',
    'us-west-2'
);

SELECT aws_s3.table_import_from_s3(
    'tier_location_result',
    'scenario_short_code, tier_short_code, location_type, location_id, location_name, tier_level, tier_value, display_order',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '10_tier/tier_location_result.csv',
    'us-west-2'
);

SELECT 'tier_result' as table_name, COUNT(*) as record_count FROM tier_result
UNION ALL
SELECT 'tier_location_result' as table_name, COUNT(*) as record_count FROM tier_location_result;

SELECT scenario_short_code, COUNT(*) as indicators 
FROM tier_result 
GROUP BY scenario_short_code 
ORDER BY scenario_short_code;

SELECT scenario_short_code, tier_short_code, COUNT(*) as locations
FROM tier_location_result
GROUP BY scenario_short_code, tier_short_code
ORDER BY scenario_short_code, tier_short_code;