-- ============================================================================
-- COEQWAL data integrity validation script
-- Run after ETL runs or periodically to verify data quality
-- Updated: March 2026 (post-migration 44)
-- ============================================================================

-- ============================================================================
-- 1. ORPHANED RECORDS CHECK
-- Statistics records whose scenario_short_code has no matching scenario
-- ============================================================================

SELECT '=== ORPHANED RECORDS CHECK ===' AS section;

SELECT 'reservoir_period_summary' AS table_name, COUNT(*) AS orphan_count
FROM reservoir_period_summary rps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.short_code = rps.scenario_short_code)
UNION ALL
SELECT 'reservoir_storage_monthly', COUNT(*)
FROM reservoir_storage_monthly rsm
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.short_code = rsm.scenario_short_code)
UNION ALL
SELECT 'mi_contractor_period_summary', COUNT(*)
FROM mi_contractor_period_summary mps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.short_code = mps.scenario_short_code)
UNION ALL
SELECT 'cws_aggregate_period_summary', COUNT(*)
FROM cws_aggregate_period_summary caps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.short_code = caps.scenario_short_code)
UNION ALL
SELECT 'ag_aggregate_period_summary', COUNT(*)
FROM ag_aggregate_period_summary aps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.short_code = aps.scenario_short_code)
UNION ALL
SELECT 'ag_du_demand_monthly', COUNT(*)
FROM ag_du_demand_monthly adm
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.short_code = adm.scenario_short_code)
UNION ALL
SELECT 'delta_period_summary', COUNT(*)
FROM delta_period_summary dps
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.short_code = dps.scenario_short_code);

-- ============================================================================
-- 2. SCENARIO COMPLETENESS CHECK
-- Verify each active scenario has statistics data
-- ============================================================================

SELECT '=== SCENARIO COMPLETENESS CHECK ===' AS section;

SELECT
    s.short_code,
    s.name AS scenario_name,
    (SELECT COUNT(*) FROM reservoir_period_summary rps WHERE rps.scenario_short_code = s.short_code) AS reservoir_stats,
    (SELECT COUNT(*) FROM mi_contractor_period_summary mps WHERE mps.scenario_short_code = s.short_code) AS mi_stats,
    (SELECT COUNT(*) FROM cws_aggregate_period_summary caps WHERE caps.scenario_short_code = s.short_code) AS cws_stats,
    (SELECT COUNT(*) FROM ag_aggregate_period_summary aps WHERE aps.scenario_short_code = s.short_code) AS ag_stats,
    (SELECT COUNT(*) FROM delta_period_summary dps WHERE dps.scenario_short_code = s.short_code) AS delta_stats,
    CASE
        WHEN (SELECT COUNT(*) FROM reservoir_period_summary rps WHERE rps.scenario_short_code = s.short_code) = 0 THEN 'MISSING RESERVOIR'
        WHEN (SELECT COUNT(*) FROM mi_contractor_period_summary mps WHERE mps.scenario_short_code = s.short_code) = 0 THEN 'MISSING MI'
        WHEN (SELECT COUNT(*) FROM ag_aggregate_period_summary aps WHERE aps.scenario_short_code = s.short_code) = 0 THEN 'MISSING AG'
        WHEN (SELECT COUNT(*) FROM delta_period_summary dps WHERE dps.scenario_short_code = s.short_code) = 0 THEN 'MISSING DELTA'
        ELSE 'OK'
    END AS status
FROM scenario s
WHERE s.is_active = TRUE
ORDER BY s.short_code;

-- ============================================================================
-- 3. AUDIT FIELDS CHECK
-- Tables should have created_by populated
-- ============================================================================

SELECT '=== AUDIT FIELDS CHECK ===' AS section;

SELECT 'reservoir_entity' AS table_name,
       COUNT(*) AS total_rows,
       COUNT(*) FILTER (WHERE created_by IS NULL) AS missing_created_by
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

SELECT '=== DATA RANGE VALIDATION ===' AS section;

SELECT 'reservoir_monthly_percentile - invalid water_month' AS check_name,
       COUNT(*) AS invalid_count
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
SELECT 'ag_du_sw_delivery_monthly - invalid water_month', COUNT(*)
FROM ag_du_sw_delivery_monthly
WHERE water_month NOT BETWEEN 1 AND 12
UNION ALL
SELECT 'ag_du_demand_monthly - invalid water_month', COUNT(*)
FROM ag_du_demand_monthly
WHERE water_month NOT BETWEEN 1 AND 12;

-- ============================================================================
-- 5. RECORD COUNTS SUMMARY
-- Overview of table sizes
-- ============================================================================

SELECT '=== RECORD COUNTS SUMMARY ===' AS section;

SELECT 'scenario' AS table_name, COUNT(*) AS record_count FROM scenario
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
UNION ALL SELECT 'delta_period_summary', COUNT(*) FROM delta_period_summary
UNION ALL SELECT 'env_flow_channel_period_summary', COUNT(*) FROM env_flow_channel_period_summary
ORDER BY table_name;

-- ============================================================================
-- 6. FOREIGN KEY INTEGRITY
-- Verify entity references are valid
-- ============================================================================

SELECT '=== FOREIGN KEY INTEGRITY ===' AS section;

SELECT 'reservoir_period_summary - invalid reservoir_entity_id' AS check_name,
       COUNT(*) AS invalid_count
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
-- 7. CROSSWALK INTEGRITY
-- Verify scenario link tables reference valid entities
-- ============================================================================

SELECT '=== CROSSWALK INTEGRITY ===' AS section;

SELECT 'theme_scenario_link - invalid scenario_id' AS check_name, COUNT(*) AS invalid_count
FROM theme_scenario_link tsl
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = tsl.scenario_id)
UNION ALL
SELECT 'scenario_key_operation_link - invalid scenario_id', COUNT(*)
FROM scenario_key_operation_link skol
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = skol.scenario_id)
UNION ALL
SELECT 'scenario_key_assumption_link - invalid scenario_id', COUNT(*)
FROM scenario_key_assumption_link skal
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = skal.scenario_id)
UNION ALL
SELECT 'scenario_tag_link - invalid scenario_id', COUNT(*)
FROM scenario_tag_link stl
WHERE NOT EXISTS (SELECT 1 FROM scenario s WHERE s.id = stl.scenario_id);

-- ============================================================================
-- SUMMARY
-- ============================================================================

SELECT '=== VALIDATION COMPLETE ===' AS section;
SELECT 'Review results above. Any non-zero counts in checks indicate issues to investigate.' AS note;
