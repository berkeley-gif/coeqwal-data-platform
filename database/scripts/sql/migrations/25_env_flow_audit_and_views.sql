-- =============================================================================
-- 25_env_flow_audit_and_views.sql
-- Post-load audit, provenance, governance, and view creation for all tables
-- created or extended by migrations 23 and 24.
--
-- Run from the repository root with SUPERUSER_URL:
--   psql $SUPERUSER_URL -f database/scripts/sql/migrations/25_env_flow_audit_and_views.sql
--
-- Rubric checklist:
--   [x] 1. Developer attributed   — created_by/updated_by = 2 (jfantauzza) on all seed rows
--   [x] 2. Data source attributed — channel_entity/variable have source_ids; stats tables exempt
--   [x] 3. Version family linked  — channel_entity → 'entity', channel_variable → 'variable',
--                                    env_flow_season + 3 stats tables → 'statistics'
--   [x] 4. Appropriate lookups    — env_flow_season seeded; channel_class CHECK; watershed FK
--   [x] 5. Columns/types/FKs      — FK developer constraints added to all 6 new tables;
--                                    existing indices confirmed (created in 23/24)
--   [x] 6. Seed data aligned      — 669 channel_entity, 1352 channel_variable, 5 env_flow_season
--   [x] 7. Views created          — env_flow_channel_full (entity + watershed denorm)
-- =============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '============================================================'
\echo 'MIGRATION 25 — env flow audit, provenance, governance, views'
\echo '============================================================'


-- ─── 1. FK constraints → developer ──────────────────────────────────────────
-- All new tables have created_by/updated_by columns but were created without
-- the FK reference to developer.id that the ERD and migration 20 require.

\echo ''
\echo '1. Adding FK constraints to developer table...'

-- channel_entity
ALTER TABLE channel_entity
    ADD CONSTRAINT IF NOT EXISTS fk_channel_entity_created_by
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    ADD CONSTRAINT IF NOT EXISTS fk_channel_entity_updated_by
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;

-- channel_variable
ALTER TABLE channel_variable
    ADD CONSTRAINT IF NOT EXISTS fk_channel_variable_created_by
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    ADD CONSTRAINT IF NOT EXISTS fk_channel_variable_updated_by
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;

-- env_flow_season
ALTER TABLE env_flow_season
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_season_created_by
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_season_updated_by
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;

-- env_flow_channel_monthly
ALTER TABLE env_flow_channel_monthly
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_monthly_created_by
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_monthly_updated_by
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;

-- env_flow_channel_seasonal
ALTER TABLE env_flow_channel_seasonal
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_seasonal_created_by
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_seasonal_updated_by
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;

-- env_flow_channel_period_summary
ALTER TABLE env_flow_channel_period_summary
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_period_created_by
        FOREIGN KEY (created_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    ADD CONSTRAINT IF NOT EXISTS fk_env_flow_period_updated_by
        FOREIGN KEY (updated_by) REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE;

\echo '  ✅ FK constraints added'


-- ─── 2. Developer attribution — set created_by = 2 (jfantauzza) ─────────────
-- Seed rows were loaded with DEFAULT 1 (system user).
-- Disable USER triggers so we can write directly without the audit trigger
-- overwriting our explicit values.

\echo ''
\echo '2. Setting developer attribution (created_by = 2, jfantauzza)...'

ALTER TABLE channel_entity           DISABLE TRIGGER USER;
ALTER TABLE channel_variable         DISABLE TRIGGER USER;
ALTER TABLE env_flow_season          DISABLE TRIGGER USER;

UPDATE channel_entity    SET created_by = 2, updated_by = 2;
UPDATE channel_variable  SET created_by = 2, updated_by = 2;
UPDATE env_flow_season   SET created_by = 2, updated_by = 2;

ALTER TABLE channel_entity           ENABLE TRIGGER USER;
ALTER TABLE channel_variable         ENABLE TRIGGER USER;
ALTER TABLE env_flow_season          ENABLE TRIGGER USER;

-- Stats tables are empty; the ETL will write created_by = 2 directly.
\echo '  ✅ Developer attribution set (stats tables attributed by ETL)'


-- ─── 3. Register in domain_family_map ────────────────────────────────────────

\echo ''
\echo '3. Registering tables in domain_family_map...'

ALTER TABLE domain_family_map DISABLE TRIGGER USER;

-- channel_entity → 'entity' version family (Layer 03)
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
SELECT 'public', 'channel_entity', vf.id,
       'CalSim-III arc-level channel entities (Layer 03 ENTITY). ~669 rows.',
       2, 2
FROM version_family vf WHERE vf.short_code = 'entity'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id,
        note              = EXCLUDED.note,
        updated_at        = NOW(),
        updated_by        = 2;

-- channel_variable → 'variable' version family (Layer 04)
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note, created_by, updated_by)
SELECT 'public', 'channel_variable', vf.id,
       'CalSim-III channel/arc variable definitions (Layer 04 VARIABLE). ~1352 rows incl. 20 MIF.',
       2, 2
FROM version_family vf WHERE vf.short_code = 'variable'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id,
        note              = EXCLUDED.note,
        updated_at        = NOW(),
        updated_by        = 2;

-- env_flow tables already registered as 'statistics' in migration 24.
-- Update created_by/updated_by to 2 and add descriptive notes.
UPDATE domain_family_map SET updated_by = 2, updated_at = NOW(),
    note = 'CEFF 5-season calendar lookup for environmental flow statistics'
WHERE table_name = 'env_flow_season';

UPDATE domain_family_map SET updated_by = 2, updated_at = NOW(),
    note = 'Monthly % unimpaired and flow CV statistics per channel × scenario'
WHERE table_name = 'env_flow_channel_monthly';

UPDATE domain_family_map SET updated_by = 2, updated_at = NOW(),
    note = 'Seasonal % functional flow statistics per channel × scenario (5 CEFF seasons)'
WHERE table_name = 'env_flow_channel_seasonal';

UPDATE domain_family_map SET updated_by = 2, updated_at = NOW(),
    note = 'Period-of-record flow alteration index (Pearson r) per channel × scenario'
WHERE table_name = 'env_flow_channel_period_summary';

ALTER TABLE domain_family_map ENABLE TRIGGER USER;

\echo '  ✅ domain_family_map registrations complete'


-- ─── 4. Grants ───────────────────────────────────────────────────────────────

\echo ''
\echo '4. Granting permissions to jfantauzza...'

GRANT SELECT, INSERT, UPDATE, DELETE ON channel_entity                TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON channel_variable              TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON env_flow_season               TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON env_flow_channel_monthly      TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON env_flow_channel_seasonal     TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON env_flow_channel_period_summary TO jfantauzza;

GRANT USAGE, SELECT ON SEQUENCE channel_entity_id_seq    TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE channel_variable_id_seq  TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE env_flow_season_id_seq   TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE env_flow_channel_monthly_id_seq         TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE env_flow_channel_seasonal_id_seq        TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE env_flow_channel_period_summary_id_seq  TO jfantauzza;

\echo '  ✅ Grants applied'


-- ─── 5. View: env_flow_channel_full ──────────────────────────────────────────
-- Denormalizes channel_entity with watershed attributes for API consumption.
-- Analogous to refuge_du_full (migration 20).

\echo ''
\echo '5. Creating env_flow_channel_full view...'

DROP VIEW IF EXISTS env_flow_channel_full;

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

    -- Watershed linkage
    ce.watershed_short_code,
    w.name                                              AS watershed_name,
    w.hydrologic_region_short_code                      AS hydrologic_region,

    -- Environmental flow attributes
    ce.unimp_sv_variable,
    ce.has_mif,
    ce.has_eflows,

    -- Geometry / network metadata
    ce.from_node,
    ce.to_node,
    ce.hydrologic_region_id,

    ce.is_active
FROM channel_entity ce
LEFT JOIN watershed w ON w.short_code = ce.watershed_short_code
WHERE ce.is_active = TRUE
ORDER BY ce.channel_class NULLS LAST, ce.watershed_short_code NULLS LAST, ce.network_arc_id;

GRANT SELECT ON env_flow_channel_full TO jfantauzza;

COMMENT ON VIEW env_flow_channel_full IS
    'Denormalized channel entity view with watershed and environmental flow attributes. '
    'Use for API responses and frontend channel selection. '
    'Joins channel_entity → watershed. Filters to is_active = TRUE. '
    'Created by migration 25_env_flow_audit_and_views.sql.';

\echo '  ✅ env_flow_channel_full view created'


-- ─── 6. Verification ─────────────────────────────────────────────────────────

\echo ''
\echo '===== VERIFICATION ====='

\echo ''
\echo 'Developer attribution (all should show created_by = 2):'
SELECT 'channel_entity'   AS tbl, created_by FROM channel_entity   GROUP BY created_by
UNION ALL
SELECT 'channel_variable' AS tbl, created_by FROM channel_variable GROUP BY created_by
UNION ALL
SELECT 'env_flow_season'  AS tbl, created_by FROM env_flow_season  GROUP BY created_by
ORDER BY tbl;

\echo ''
\echo 'domain_family_map registrations (expect 6 env-flow rows):'
SELECT table_name, vf.short_code AS version_family, note
FROM domain_family_map dfm
JOIN version_family vf ON vf.id = dfm.version_family_id
WHERE table_name IN (
    'channel_entity', 'channel_variable',
    'env_flow_season', 'env_flow_channel_monthly',
    'env_flow_channel_seasonal', 'env_flow_channel_period_summary'
)
ORDER BY table_name;

\echo ''
\echo 'env_flow_channel_full (spot-check — DV channels only):'
SELECT network_arc_id, channel_class, watershed_short_code, unimp_sv_variable, has_mif, has_eflows
FROM env_flow_channel_full
WHERE channel_class IS NOT NULL
ORDER BY channel_class, network_arc_id
LIMIT 15;

\echo ''
\echo 'FK constraints on new tables:'
SELECT tc.table_name, tc.constraint_name, ccu.table_name AS references_table
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_name IN (
      'channel_entity', 'channel_variable', 'env_flow_season',
      'env_flow_channel_monthly', 'env_flow_channel_seasonal',
      'env_flow_channel_period_summary'
  )
  AND ccu.table_name = 'developer'
ORDER BY tc.table_name;

\echo ''
\echo '=== Migration 25 complete ==='
