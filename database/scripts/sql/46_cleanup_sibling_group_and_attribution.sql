-- =============================================================================
-- MIGRATION 46: Cleanup sibling_group naming & fix created_by/updated_by
-- =============================================================================
--
-- 1. Replace decimal sibling_group values (e.g. '1.1') with the historical
--    hydrology scenario short_code (e.g. 's0020') — the canonical reference
--    for each operational configuration family.
--
-- 2. Fix created_by and updated_by to 2 (jfantauzza) for all rows where
--    they were incorrectly set to 1 (postgres) by the migration 45 run.
--
-- 3. Add FK constraint so sibling_group references scenario(short_code).
--
-- Safe to re-run: uses WHERE clauses that match only stale values.
-- =============================================================================

BEGIN;

-- ─── STEP 1: Rename sibling_group from decimal to hist_adj short_code ────────

UPDATE scenario SET sibling_group = CASE sibling_group
    WHEN '1.1' THEN 's0020'
    WHEN '1.2' THEN 's0011'
    WHEN '1.3' THEN 's0021'
    WHEN '1.4' THEN 's0024'
    WHEN '1.5' THEN 's0023'
    WHEN '2.1' THEN 's0025'
    WHEN '2.2' THEN 's0026'
    WHEN '2.3' THEN 's0027'
    WHEN '2.4' THEN 's0028'
    WHEN '3.1' THEN 's0030'
    WHEN '3.2' THEN 's0046'
    WHEN '3.3' THEN 's0032'
    WHEN '3.4' THEN 's0031'
    WHEN '3.5' THEN 's0033'
    WHEN '4.1' THEN 's0035'
    WHEN '4.2' THEN 's0036'
    WHEN '4.3' THEN 's0037'
    WHEN '5.1' THEN 's0040'
    WHEN '5.2' THEN 's0041'
    WHEN '5.3' THEN 's0042'
    WHEN '5.4' THEN 's0039'
    WHEN '6.1' THEN 's0044'
    WHEN '6.4' THEN 's0045'
    WHEN '7.4' THEN 's0065'
END
WHERE sibling_group IS NOT NULL;

-- ─── STEP 2: Fix attribution ─────────────────────────────────────────────────
-- Migration 45 ran as postgres (developer.id = 1). All rows touched by that
-- migration got updated_by = 1; new rows got created_by = 1. Fix to 2.

UPDATE scenario SET created_by = 2 WHERE created_by = 1;
UPDATE scenario SET updated_by = 2 WHERE updated_by = 1;

-- ─── STEP 3: Add FK constraint on sibling_group → scenario(short_code) ──────

ALTER TABLE scenario
    ADD CONSTRAINT fk_scenario_sibling_group
    FOREIGN KEY (sibling_group) REFERENCES scenario(short_code)
    ON UPDATE CASCADE ON DELETE RESTRICT;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. All sibling_group values should now be sNNNN format
SELECT 'sibling_group format check' AS check,
       COUNT(*) FILTER (WHERE sibling_group IS NOT NULL AND sibling_group !~ '^s\d{4}$') AS bad_format
FROM scenario;

-- 2. Every sibling_group should have exactly 3 members (hist_adj + cc50 + cc95)
SELECT sibling_group, COUNT(*) AS member_count
FROM scenario
WHERE sibling_group IS NOT NULL
GROUP BY sibling_group
ORDER BY sibling_group;

-- 3. No rows should have created_by = 1 or updated_by = 1
SELECT 'attribution check' AS check,
       COUNT(*) FILTER (WHERE created_by = 1) AS created_by_1,
       COUNT(*) FILTER (WHERE updated_by = 1) AS updated_by_1
FROM scenario;

-- 4. Sample: show sibling families with hydroclimate
SELECT s.short_code, s.sibling_group, h.short_code AS hydroclimate
FROM scenario s
JOIN hydroclimate h ON s.hydroclimate_id = h.id
WHERE s.sibling_group IS NOT NULL
ORDER BY s.sibling_group, h.id;

COMMIT;
