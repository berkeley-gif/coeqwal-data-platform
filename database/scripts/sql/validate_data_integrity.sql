-- ============================================================================
-- COEQWAL data integrity validation script
-- Run this script after ETL runs or periodically to verify data quality
-- ============================================================================

-- ============================================================================
-- 1. ORPHANED RECORDS CHECK
-- Records in statistics tables without matching scenario
-- ============================================================================

SELECT '=== ORPHANED RECORDS CHECK ===' as section;

SELECT 'reservoir_period_summary' as table_name, COUNT(*) as orphan_count
FROM reservoir_period_summary rps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = rps.scenario_id)
UNION ALL
SELECT 'reservoir_storage_monthly', COUNT(*)
FROM reservoir_storage_monthly rsm
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = rsm.scenario_id)
UNION ALL
SELECT 'mi_contractor_period_summary', COUNT(*)
FROM mi_contractor_period_summary mps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = mps.scenario_id)
UNION ALL
SELECT 'cws_aggregate_period_summary', COUNT(*)
FROM cws_aggregate_period_summary caps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = caps.scenario_id)
UNION ALL
SELECT 'ag_aggregate_period_summary', COUNT(*)
FROM ag_aggregate_period_summary aps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = aps.scenario_id);

-- ============================================================================
-- 2. SCENARIO COMPLETENESS CHECK
-- Verify each scenario has statistics data
-- ============================================================================

SELECT '=== SCENARIO COMPLETENESS CHECK ===' as section;

SELECT 
    s.id as scenario_id,
    s.name as scenario_name,
    (SELECT COUNT(*) FROM reservoir_period_summary rps WHERE rps.scenario_id = s.id) as reservoir_stats,
    (SELECT COUNT(*) FROM mi_contractor_period_summary mps WHERE mps.scenario_id = s.id) as mi_stats,
    (SELECT COUNT(*) FROM cws_aggregate_period_summary caps WHERE caps.scenario_id = s.id) as cws_stats,
    (SELECT COUNT(*) FROM ag_aggregate_period_summary aps WHERE aps.scenario_id = s.id) as ag_stats,
    CASE 
        WHEN (SELECT COUNT(*) FROM reservoir_period_summary rps WHERE rps.scenario_id = s.id) = 0 THEN 'MISSING RESERVOIR'
        WHEN (SELECT COUNT(*) FROM mi_contractor_period_summary mps WHERE mps.scenario_id = s.id) = 0 THEN 'MISSING MI'
        WHEN (SELECT COUNT(*) FROM ag_aggregate_period_summary aps WHERE aps.scenario_id = s.id) = 0 THEN 'MISSING AG'
        ELSE 'OK'
    END as status
FROM scenario s
ORDER BY s.id;

-- ============================================================================
-- 3. AUDIT FIELDS CHECK
-- Tables should have created_by populated
-- ============================================================================

SELECT '=== AUDIT FIELDS CHECK ===' as section;

SELECT 'reservoir_entity' as table_name, 
       COUNT(*) as total_rows,
       COUNT(*) FILTER (WHERE created_by IS NULL) as missing_created_by
FROM reservoir_entity
UNION ALL
SELECT 'du_urban_entity', COUNT(*), COUNT(*) FILTER (WHERE created_by IS NULL)
FROM du_urban_entity
UNION ALL
SELECT 'mi_contractor', COUNT(*), COUNT(*) FILTER (WHERE created_by IS NULL)
FROM mi_contractor
UNION ALL
SELECT 'du_agriculture_entity', COUNT(*), COUNT(*) FILTER (WHERE created_by IS NULL)
FROM du_agriculture_entity;

-- ============================================================================
-- 4. DATA RANGE VALIDATION
-- Check water_month and other constrained values
-- ============================================================================

SELECT '=== DATA RANGE VALIDATION ===' as section;

SELECT 'reservoir_monthly_percentile - invalid water_month' as check_name,
       COUNT(*) as invalid_count
FROM reservoir_monthly_percentile 
WHERE water_month NOT BETWEEN 1 AND 12
UNION ALL
SELECT 'du_delivery_monthly - invalid water_month', COUNT(*)
FROM du_delivery_monthly 
WHERE water_month NOT BETWEEN 1 AND 12
UNION ALL
SELECT 'mi_delivery_monthly - invalid water_month', COUNT(*)
FROM mi_delivery_monthly 
WHERE water_month NOT BETWEEN 1 AND 12
UNION ALL
SELECT 'ag_du_delivery_monthly - invalid water_month', COUNT(*)
FROM ag_du_delivery_monthly 
WHERE water_month NOT BETWEEN 1 AND 12;

-- ============================================================================
-- 5. RECORD COUNTS SUMMARY
-- Overview of table sizes
-- ============================================================================

SELECT '=== RECORD COUNTS SUMMARY ===' as section;

SELECT 'scenario' as table_name, COUNT(*) as record_count FROM scenario
UNION ALL SELECT 'reservoir_entity', COUNT(*) FROM reservoir_entity
UNION ALL SELECT 'reservoir_period_summary', COUNT(*) FROM reservoir_period_summary
UNION ALL SELECT 'reservoir_storage_monthly', COUNT(*) FROM reservoir_storage_monthly
UNION ALL SELECT 'du_urban_entity', COUNT(*) FROM du_urban_entity
UNION ALL SELECT 'mi_contractor', COUNT(*) FROM mi_contractor
UNION ALL SELECT 'mi_contractor_period_summary', COUNT(*) FROM mi_contractor_period_summary
UNION ALL SELECT 'cws_aggregate_entity', COUNT(*) FROM cws_aggregate_entity
UNION ALL SELECT 'cws_aggregate_period_summary', COUNT(*) FROM cws_aggregate_period_summary
UNION ALL SELECT 'du_agriculture_entity', COUNT(*) FROM du_agriculture_entity
UNION ALL SELECT 'ag_aggregate_period_summary', COUNT(*) FROM ag_aggregate_period_summary
ORDER BY table_name;

-- ============================================================================
-- 6. FOREIGN KEY INTEGRITY
-- Verify entity references are valid
-- ============================================================================

SELECT '=== FOREIGN KEY INTEGRITY ===' as section;

SELECT 'reservoir_period_summary - invalid reservoir_entity_id' as check_name,
       COUNT(*) as invalid_count
FROM reservoir_period_summary rps
WHERE NOT EXISTS (SELECT 1 FROM reservoir_entity re WHERE re.id = rps.reservoir_entity_id)
UNION ALL
SELECT 'mi_contractor_period_summary - invalid mi_contractor_id', COUNT(*)
FROM mi_contractor_period_summary mps
WHERE NOT EXISTS (SELECT 1 FROM mi_contractor mc WHERE mc.id = mps.mi_contractor_id)
UNION ALL
SELECT 'cws_aggregate_period_summary - invalid cws_aggregate_entity_id', COUNT(*)
FROM cws_aggregate_period_summary caps
WHERE NOT EXISTS (SELECT 1 FROM cws_aggregate_entity cae WHERE cae.id = caps.cws_aggregate_entity_id);

-- ============================================================================
-- SUMMARY
-- ============================================================================

SELECT '=== VALIDATION COMPLETE ===' as section;
SELECT 'Review results above. Any non-zero counts in checks indicate issues to investigate.' as note;
