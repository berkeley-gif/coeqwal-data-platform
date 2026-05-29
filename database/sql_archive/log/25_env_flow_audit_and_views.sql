-- =============================================================================
-- 25_env_flow_audit_and_views.sql
-- Post-load audit, provenance, governance, and view creation for all tables
-- created or extended by migrations 23 and 24.
--
-- Run from the repository root with SUPERUSER_URL:
--   psql $SUPERUSER_URL -f database/scripts/sql/migrations/25_env_flow_audit_and_views.sql
--
-- Rubric checklist:
--   [x] 1. Developer attributed    - created_by/updated_by = 2 (jfantauzza) on all seed rows
--   [x] 2. Data source attributed  - channel_entity/variable have source_ids; stats tables exempt
--   [x] 3. Version family linked   - channel_entity to 'entity', channel_variable to 'variable',
--                                    env_flow_season + 3 stats tables to 'statistics'
--   [x] 4. Appropriate lookups     - env_flow_season seeded; channel_class CHECK; watershed FK
--   [x] 5. Columns/types/FKs       - FK developer constraints added to all 6 new tables;
--                                    existing indices confirmed (created in 23/24)
--   [x] 6. Seed data aligned       - 669 channel_entity, 1352 channel_variable, 5 env_flow_season
--   [x] 7. Views created           - env_flow_channel_full (entity + watershed denorm)
-- =============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '============================================================'
\echo 'MIGRATION 25  - env flow audit, provenance, governance, views'
\echo '============================================================'


\echo ''
\echo '1. Adding FK constraints to developer table...'

DO $$
DECLARE
    r RECORD;
    constraints TEXT[][] := ARRAY[
        ARRAY['channel_entity',               'fk_channel_entity_created_by',         'created_by', 'developer'],
        ARRAY['channel_entity',               'fk_channel_entity_updated_by',         'updated_by', 'developer'],
        ARRAY['channel_variable',             'fk_channel_variable_created_by',       'created_by', 'developer'],
        ARRAY['channel_variable',             'fk_channel_variable_updated_by',       'updated_by', 'developer'],
        ARRAY['env_flow_season',              'fk_env_flow_season_created_by',        'created_by', 'developer'],
        ARRAY['env_flow_season',              'fk_env_flow_season_updated_by',        'updated_by', 'developer'],
        ARRAY['env_flow_channel_monthly',     'fk_env_flow_monthly_created_by',       'created_by', 'developer'],
        ARRAY['env_flow_channel_monthly',     'fk_env_flow_monthly_updated_by',       'updated_by', 'developer'],
        ARRAY['env_flow_channel_seasonal',    'fk_env_flow_seasonal_created_by',      'created_by', 'developer'],
        ARRAY['env_flow_channel_seasonal',    'fk_env_flow_seasonal_updated_by',      'updated_by', 'developer'],
        ARRAY['env_flow_channel_period_summary', 'fk_env_flow_period_created_by',     'created_by', 'developer'],
        ARRAY['env_flow_channel_period_summary', 'fk_env_flow_period_updated_by',     'updated_by', 'developer']
    ];
    c TEXT[];
BEGIN
    FOREACH c SLICE 1 IN ARRAY constraints LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = c[2] AND conrelid = c[1]::regclass
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I(id) ON DELETE RESTRICT ON UPDATE CASCADE',
                c[1], c[2], c[3], c[4]
            );
            RAISE NOTICE 'Added constraint % on %', c[2], c[1];
        ELSE
            RAISE NOTICE 'Constraint % already exists on %  - skipped', c[2], c[1];
        END IF;
    END LOOP;
END;
$$;

\echo '  ✅ FK constraints added'


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

\echo '  ✅ Developer attribution set (stats tables attributed by ETL)'


\echo ''
\echo '3. Registering tables in domain_family_map...'

ALTER TABLE domain_family_map DISABLE TRIGGER USER;

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

    ce.watershed_short_code,
    w.name                                              AS watershed_name,
    w.hydrologic_region_short_code                      AS hydrologic_region,

    ce.unimp_sv_variable,
    ce.has_mif,
    ce.has_eflows,

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
    'Joins channel_entity to watershed. Filters to is_active = TRUE. '
    'Created by migration 25_env_flow_audit_and_views.sql.';

\echo '  ✅ env_flow_channel_full view created'


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
\echo 'env_flow_channel_full (spot-check  - DV channels only):'
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
