-- =============================================================================
-- MIGRATION 52: Add s0070 and s0090 (eflowsV1 cc50/cc95 siblings of s0029)
-- =============================================================================
-- Inserts two inactive scenarios: the CC50 and CC95 hydroclimate siblings of
-- s0029 (functional environmental flows v1). Both are operationally identical
-- to s0029 but run under different climate assumptions.
--
-- s0029 is already in the database as inactive with its own entry in
-- scenario_hydroclimate_sibling. These two new rows join that sibling group.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/52_add_s0070_s0090.sql
-- =============================================================================

BEGIN;


ALTER TABLE scenario DISABLE TRIGGER audit_fields_scenario;


INSERT INTO scenario (
    short_code, run_name, is_active,
    hydroclimate_id, hydroclimate_sibling, scenario_version_id,
    scenario_author_id, model_source_id,
    created_by, updated_by
) VALUES
    ('s0070', 's0070_DCRadjHist_cc50_2020LU_eflowsV1', FALSE,
     3, 's0029', 1, 3, 1, 2, 2),
    ('s0090', 's0090_DCRadjHist_cc95_2020LU_eflowsV1_v0.2', FALSE,
     4, 's0029', 1, 3, 1, 2, 2);


ALTER TABLE scenario ENABLE TRIGGER audit_fields_scenario;


SELECT short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling,
       scenario_author_id, model_source_id, created_by, updated_by
FROM scenario
WHERE short_code IN ('s0029', 's0070', 's0090')
ORDER BY short_code;

SELECT 'scenario rows' AS check, COUNT(*) AS total,
       COUNT(*) FILTER (WHERE is_active) AS active,
       COUNT(*) FILTER (WHERE NOT is_active) AS inactive
FROM scenario;

SELECT hydroclimate_sibling, COUNT(*) AS members
FROM scenario
WHERE hydroclimate_sibling = 's0029'
GROUP BY hydroclimate_sibling;

COMMIT;
