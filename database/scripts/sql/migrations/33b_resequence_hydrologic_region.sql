-- Migration 33b: Resequence hydrologic_region ids to contiguous 1-7
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/33b_resequence_hydrologic_region.sql
--
-- Current: 1=SAC, 2=SJR, 3=DELTA, 4=TULARE, 5=SOCAL, 8=EXPORT, 13=NC
-- Target:  1=SAC, 2=SJR, 3=DELTA, 4=TULARE, 5=SOCAL, 6=NC, 7=EXPORT
--
-- FK references to hydrologic_region(id):
--   du_agriculture_entity, du_urban_entity, network, wba, watershed
-- View dependency: env_flow_channel_full (via watershed)

BEGIN;

-- ── 1. Drop dependent view ──────────────────────────────────────────
DROP VIEW IF EXISTS env_flow_channel_full;

-- ── 2. Drop FK constraints from child tables ────────────────────────
ALTER TABLE du_agriculture_entity DROP CONSTRAINT IF EXISTS du_agriculture_entity_hydrologic_region_id_fkey;
ALTER TABLE du_urban_entity       DROP CONSTRAINT IF EXISTS du_urban_entity_hydrologic_region_id_fkey;
ALTER TABLE network               DROP CONSTRAINT IF EXISTS network_hydrologic_region_id_fkey;
ALTER TABLE wba                   DROP CONSTRAINT IF EXISTS wba_hydrologic_region_id_fkey;
ALTER TABLE watershed             DROP CONSTRAINT IF EXISTS watershed_new_hydrologic_region_id_fkey;

-- ── 3. Update ids: move 8→7 and 13→6 (use temp values to avoid PK clash) ──
UPDATE hydrologic_region SET id = -8  WHERE id = 8;
UPDATE hydrologic_region SET id = -13 WHERE id = 13;
UPDATE hydrologic_region SET id = 6   WHERE id = -13;
UPDATE hydrologic_region SET id = 7   WHERE id = -8;

-- ── 4. Update FK child tables ────────────────────────────────────────
UPDATE du_agriculture_entity SET hydrologic_region_id = 6 WHERE hydrologic_region_id = 13;
UPDATE du_agriculture_entity SET hydrologic_region_id = 7 WHERE hydrologic_region_id = 8;

UPDATE du_urban_entity SET hydrologic_region_id = 6 WHERE hydrologic_region_id = 13;
UPDATE du_urban_entity SET hydrologic_region_id = 7 WHERE hydrologic_region_id = 8;

UPDATE network SET hydrologic_region_id = 6 WHERE hydrologic_region_id = 13;
UPDATE network SET hydrologic_region_id = 7 WHERE hydrologic_region_id = 8;

UPDATE wba SET hydrologic_region_id = 6 WHERE hydrologic_region_id = 13;
UPDATE wba SET hydrologic_region_id = 7 WHERE hydrologic_region_id = 8;

UPDATE watershed SET hydrologic_region_id = 6 WHERE hydrologic_region_id = 13;
UPDATE watershed SET hydrologic_region_id = 7 WHERE hydrologic_region_id = 8;

-- reservoir_entity has INTEGER hydrologic_region_id (no FK) — only 1,2,4 used, but update for safety
UPDATE reservoir_entity SET hydrologic_region_id = 6 WHERE hydrologic_region_id = 13;
UPDATE reservoir_entity SET hydrologic_region_id = 7 WHERE hydrologic_region_id = 8;

-- ── 5. Re-add FK constraints (matching original rules) ──────────────
ALTER TABLE du_agriculture_entity
    ADD CONSTRAINT du_agriculture_entity_hydrologic_region_id_fkey
    FOREIGN KEY (hydrologic_region_id) REFERENCES hydrologic_region(id);

ALTER TABLE du_urban_entity
    ADD CONSTRAINT du_urban_entity_hydrologic_region_id_fkey
    FOREIGN KEY (hydrologic_region_id) REFERENCES hydrologic_region(id);

ALTER TABLE network
    ADD CONSTRAINT network_hydrologic_region_id_fkey
    FOREIGN KEY (hydrologic_region_id) REFERENCES hydrologic_region(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE wba
    ADD CONSTRAINT wba_hydrologic_region_id_fkey
    FOREIGN KEY (hydrologic_region_id) REFERENCES hydrologic_region(id);

ALTER TABLE watershed
    ADD CONSTRAINT watershed_hydrologic_region_id_fkey
    FOREIGN KEY (hydrologic_region_id) REFERENCES hydrologic_region(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

-- ── 6. Reset sequence ───────────────────────────────────────────────
SELECT setval('hydrologic_region_id_seq', 7);

-- ── 7. Recreate env_flow_channel_full view ──────────────────────────
CREATE VIEW env_flow_channel_full AS
SELECT
    ce.network_arc_id,
    ce.short_code                                       AS label,
    ce.channel_class,
    CASE ce.channel_class
        WHEN 'stream'             THEN 'Natural stream or river reach'
        WHEN 'canal'              THEN 'Constructed conveyance canal'
        WHEN 'reservoir_release'  THEN 'Regulated dam/reservoir outlet'
        ELSE ce.channel_class
    END                                                 AS channel_class_label,
    ce.watershed_short_code,
    w.name                                              AS watershed_name,
    hr.short_code                                       AS hydrologic_region,
    ce.unimp_sv_variable,
    ce.has_mif,
    ce.has_eflows,
    ce.from_node,
    ce.to_node,
    ce.hydrologic_region_id,
    ce.is_active
FROM channel_entity ce
LEFT JOIN watershed w ON w.short_code = ce.watershed_short_code
LEFT JOIN hydrologic_region hr ON hr.id = w.hydrologic_region_id
WHERE ce.is_active = TRUE
ORDER BY ce.channel_class NULLS LAST, ce.watershed_short_code NULLS LAST, ce.network_arc_id;

GRANT SELECT ON env_flow_channel_full TO jfantauzza;

COMMENT ON VIEW env_flow_channel_full IS
    'Denormalized channel entity view with watershed and environmental flow attributes. '
    'Use for API responses and frontend channel selection.';

COMMIT;

-- ── Verification ─────────────────────────────────────────────────────

SELECT 'resequenced_ids' AS check, id, short_code, label
FROM hydrologic_region ORDER BY id;

SELECT 'watershed_fk' AS check, w.short_code, w.hydrologic_region_id, hr.short_code AS region
FROM watershed w
LEFT JOIN hydrologic_region hr ON hr.id = w.hydrologic_region_id
ORDER BY w.id;

SELECT 'sequence_val' AS check, last_value FROM hydrologic_region_id_seq;

\echo
\echo '33b HYDROLOGIC_REGION RESEQUENCE COMPLETE'
\echo '=========================================='
