-- CREATE SCENARIO_WATER_YEAR_TYPE TABLE
-- =====================================
-- Per-scenario water-year-type (WYT) series: one row per (scenario, water year)
-- giving the WYT classification for that year. Each scenario carries a full
-- timeline (~100 water years, 1921-2021), so s0020 has ~100 rows.
--
-- Layer 10+ (per-scenario results). Scenario is referenced by short_code -
-- the house convention for results tables (reservoir/ag/mi/cws/delta/
-- tier_location_result all key on scenario_short_code). Per SCHEMA_BACKLOG.md
-- the intended convention is a FORMAL FK to scenario(short_code); this new
-- table adopts that from the start.
--
-- Source: extracted from the CalSim trend-report export (WYT-prefixed
-- variables) via etl/data_in_depth/. Populated by ETL, not seeded.
--
-- Run from the repo root as superuser (DDL + FK + domain_family_map need
-- elevated privileges):
--   psql "$SUPERUSER_URL" -f database/scripts/sql/create_scenario_water_year_type_table.sql

\set ON_ERROR_STOP on

\echo ''
\echo 'CREATING SCENARIO_WATER_YEAR_TYPE TABLE'
\echo '======================================='

DROP TABLE IF EXISTS scenario_water_year_type CASCADE;

CREATE TABLE scenario_water_year_type (
    id                  SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR(20) NOT NULL,
    water_year          INTEGER     NOT NULL,
    wyt                 INTEGER     NOT NULL,

    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by          INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by          INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),

    -- Formal FK to the scenario definitions (house convention: short_code).
    CONSTRAINT scenario_water_year_type_scenario_fkey
        FOREIGN KEY (scenario_short_code) REFERENCES scenario (short_code)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT scenario_water_year_type_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer (id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT scenario_water_year_type_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer (id)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    -- One WYT value per scenario per water year.
    CONSTRAINT scenario_water_year_type_unique
        UNIQUE (scenario_short_code, water_year),

    -- Range guards (house pattern, cf. water_month BETWEEN 1 AND 12).
    CONSTRAINT chk_scenario_wyt_water_year CHECK (water_year BETWEEN 1900 AND 2100),
    CONSTRAINT chk_scenario_wyt_value      CHECK (wyt BETWEEN 1 AND 5)
);

\echo 'Table created.'

\echo 'Creating indexes...'
CREATE INDEX idx_scenario_wyt_scenario ON scenario_water_year_type (scenario_short_code);
CREATE INDEX idx_scenario_wyt_year     ON scenario_water_year_type (water_year);
CREATE INDEX idx_scenario_wyt_active   ON scenario_water_year_type (is_active) WHERE is_active = TRUE;
-- Cross-scenario filtering by WYT: leads with wyt so `WHERE wyt = X` (across all
-- scenarios) can use it; scenario_short_code trails to order/cover within a type.
CREATE INDEX idx_scenario_wyt_wyt      ON scenario_water_year_type (wyt, scenario_short_code);

\echo 'Attaching audit trigger...'
SELECT apply_audit_trigger_to_table('scenario_water_year_type');

\echo 'Registering in domain_family_map (scenario family)...'
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note)
SELECT 'public', 'scenario_water_year_type', vf.id,
       'Per-scenario water-year-type series (Layer 10, one row per scenario per water year)'
FROM version_family vf
WHERE vf.short_code = 'scenario'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id,
        note              = EXCLUDED.note;

\echo 'Granting to coeqwal_developer...'
GRANT SELECT, INSERT, UPDATE, DELETE ON scenario_water_year_type TO coeqwal_developer;
GRANT USAGE, SELECT ON SEQUENCE scenario_water_year_type_id_seq TO coeqwal_developer;

COMMENT ON TABLE scenario_water_year_type IS
    'Per-scenario water-year-type (WYT) series: one row per (scenario_short_code, water_year). '
    'Each scenario carries its full ~100-year timeline. Extracted from the CalSim trend-report '
    'export (WYT variables) via etl/data_in_depth/. Layer 10 results.';
COMMENT ON COLUMN scenario_water_year_type.scenario_short_code IS
    'Scenario identifier (e.g. s0020). FK to scenario.short_code.';
COMMENT ON COLUMN scenario_water_year_type.water_year IS
    'Water year (Oct-Sep). CalSim timeline spans ~1921-2021.';
COMMENT ON COLUMN scenario_water_year_type.wyt IS
    'Water-year-type classification (1=Wet, 2=Above Normal, 3=Below Normal, 4=Dry, 5=Critical). '
    'Confirm the coding matches the source before relaxing chk_scenario_wyt_value.';

\echo ''
\echo 'VERIFICATION:'
\echo '============='
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'scenario_water_year_type'
ORDER BY ordinal_position;

\echo ''
\echo 'Constraints:'
SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'scenario_water_year_type'::regclass
ORDER BY contype, conname;

\echo ''
\echo 'domain_family_map registration:'
SELECT table_name, version_family_id, note
FROM domain_family_map
WHERE table_name = 'scenario_water_year_type';

\echo ''
\echo 'SCENARIO_WATER_YEAR_TYPE TABLE CREATED.'
\echo '======================================='
