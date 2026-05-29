-- Migration 40: Fix pre-existing data discrepancies in operation/assumption crosswalk tables
-- Run AFTER migrations 35-39 as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/40_fix_crosswalk_data.sql
--
-- Discrepancies identified by comparing the hand-verified frontend (opsIcons.tsx)
-- against the current database crosswalk tables and the scenario metadata document.
--
-- s0046 (id=21): wrong biops (standard→modified), wrong flow (no_min_flow→functional_flows), missing delta_regs_standard
-- s0065 (id=22): wrong biops (standard→modified)
-- s0035 (id=23): all operation and assumption links missing
-- s0036 (id=24): all operation and assumption links missing
-- s0037 (id=25): all operation and assumption links missing

BEGIN;

ALTER TABLE scenario_key_operation_link DISABLE TRIGGER USER;
ALTER TABLE scenario_key_assumption_link DISABLE TRIGGER USER;

UPDATE scenario_key_operation_link
SET    operation_id = 21, updated_at = NOW(), updated_by = 2
WHERE  scenario_id = 21 AND operation_id = 20;

UPDATE scenario_key_operation_link
SET    operation_id = 17, updated_at = NOW(), updated_by = 2
WHERE  scenario_id = 21 AND operation_id = 16;

INSERT INTO scenario_key_operation_link (scenario_id, operation_id, created_by, updated_by)
VALUES (21, 26, 2, 2);

UPDATE scenario_key_operation_link
SET    operation_id = 21, updated_at = NOW(), updated_by = 2
WHERE  scenario_id = 22 AND operation_id = 20;

INSERT INTO scenario_key_operation_link (scenario_id, operation_id, created_by, updated_by)
VALUES
    (23, 1,  2, 2),
    (23, 10, 2, 2),
    (23, 20, 2, 2),
    (23, 23, 2, 2),
    (23, 24, 2, 2),
    (23, 25, 2, 2),
    (23, 26, 2, 2);

INSERT INTO scenario_key_assumption_link (scenario_id, assumption_id, created_by, updated_by)
VALUES (23, 17, 2, 2);

INSERT INTO scenario_key_operation_link (scenario_id, operation_id, created_by, updated_by)
VALUES
    (24, 2,  2, 2),
    (24, 10, 2, 2),
    (24, 20, 2, 2),
    (24, 23, 2, 2),
    (24, 24, 2, 2),
    (24, 25, 2, 2),
    (24, 26, 2, 2);

INSERT INTO scenario_key_assumption_link (scenario_id, assumption_id, created_by, updated_by)
VALUES (24, 17, 2, 2);

INSERT INTO scenario_key_operation_link (scenario_id, operation_id, created_by, updated_by)
VALUES
    (25, 3,  2, 2),
    (25, 10, 2, 2),
    (25, 20, 2, 2),
    (25, 23, 2, 2),
    (25, 24, 2, 2),
    (25, 25, 2, 2),
    (25, 26, 2, 2);

INSERT INTO scenario_key_assumption_link (scenario_id, assumption_id, created_by, updated_by)
VALUES (25, 17, 2, 2);

ALTER TABLE scenario_key_operation_link ENABLE TRIGGER USER;
ALTER TABLE scenario_key_assumption_link ENABLE TRIGGER USER;

COMMIT;


\echo ''
\echo 'Operation link counts per fixed scenario:'
SELECT s.short_code, COUNT(ol.*) AS op_count
FROM scenario s
LEFT JOIN scenario_key_operation_link ol ON ol.scenario_id = s.id
WHERE s.id IN (21, 22, 23, 24, 25)
GROUP BY s.id, s.short_code
ORDER BY s.id;

\echo ''
\echo 'Assumption link counts per fixed scenario:'
SELECT s.short_code, COUNT(al.*) AS assumption_count
FROM scenario s
LEFT JOIN scenario_key_assumption_link al ON al.scenario_id = s.id
WHERE s.id IN (21, 22, 23, 24, 25)
GROUP BY s.id, s.short_code
ORDER BY s.id;

\echo ''
\echo 'Biops operations for s0046 and s0065 (should both be 21 = biops_modified_2019):'
SELECT s.short_code, ol.operation_id, od.short_code AS op_short_code
FROM scenario s
JOIN scenario_key_operation_link ol ON ol.scenario_id = s.id
JOIN operation_definition od ON od.id = ol.operation_id
JOIN operation_category oc ON oc.id = od.operation_category_id
WHERE s.id IN (21, 22) AND oc.short_code = 'biops'
ORDER BY s.id;

\echo ''
\echo 'Flow operations for s0046 (should be 17 = functional_flows):'
SELECT s.short_code, ol.operation_id, od.short_code AS op_short_code
FROM scenario s
JOIN scenario_key_operation_link ol ON ol.scenario_id = s.id
JOIN operation_definition od ON od.id = ol.operation_id
JOIN operation_category oc ON oc.id = od.operation_category_id
WHERE s.id = 21 AND oc.short_code = 'flow'
ORDER BY s.id;

\echo ''
\echo '40 CROSSWALK DATA FIX COMPLETE'
\echo '=============================='
