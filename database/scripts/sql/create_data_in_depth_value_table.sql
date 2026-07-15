-- CREATE DATA_IN_DEPTH_VALUE TABLE
-- =================================
-- Generic raw long-form value store for the data_in_depth extracts: one row per
-- (scenario, subject, source_variable, period, water_year, unit). Holds ONLY raw
-- per-year values (e.g. April & September reservoir storage). Everything
-- population-dependent — exceedance percentiles, mean, CV, box-plot quantiles —
-- is computed LIVE at query time so it stays correct under WYT filtering. Nothing
-- derived is stored here (no percentile column, no companion statistic table).
--
-- Generic across all extracts: reservoir storage, river flow, etc. differ only
-- by source_variable / period / unit. A measure stored in multiple units (e.g.
-- volume TAF + percent-of-capacity PCT_CAP; or flow CFS + TAF) is multiple rows,
-- so unit_id is part of the grain. Note PCT_CAP is a PER-ROW transform
-- (value/capacity*100), not a population statistic, so it is safe to store.
--
-- Run from repo root as superuser:
--   psql "$SUPERUSER_URL" -f database/scripts/sql/create_data_in_depth_value_table.sql

\set ON_ERROR_STOP on

\echo ''
\echo 'CREATING DATA_IN_DEPTH_VALUE TABLE'
\echo '=================================='

-- Ensure the percent-of-capacity unit exists (idempotent).
INSERT INTO unit (short_code, full_name, canonical_group)
VALUES ('PCT_CAP', 'percent of capacity', 'percent')
ON CONFLICT (short_code) DO NOTHING;

DROP TABLE IF EXISTS data_in_depth_value CASCADE;

CREATE TABLE data_in_depth_value (
    id                       SERIAL PRIMARY KEY,
    scenario_short_code      VARCHAR(20) NOT NULL,
    data_in_depth_subject_id INTEGER     NOT NULL,
    source_variable          VARCHAR     NOT NULL,   -- provenance, e.g. 'S_SHSTA'
    period                   VARCHAR     NOT NULL,   -- 'april','sept','annual',...
    water_year               INTEGER     NOT NULL,
    value                    NUMERIC     NOT NULL,
    unit_id                  INTEGER     NOT NULL,

    is_active                BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by               INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by               INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),

    CONSTRAINT data_in_depth_value_scenario_fkey
        FOREIGN KEY (scenario_short_code) REFERENCES scenario (short_code)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT data_in_depth_value_subject_fkey
        FOREIGN KEY (data_in_depth_subject_id) REFERENCES data_in_depth_subject (id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT data_in_depth_value_unit_fkey
        FOREIGN KEY (unit_id) REFERENCES unit (id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT data_in_depth_value_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT data_in_depth_value_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer (id) ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT data_in_depth_value_unique
        UNIQUE (scenario_short_code, data_in_depth_subject_id, source_variable, period, water_year, unit_id),
    CONSTRAINT chk_didv_water_year CHECK (water_year BETWEEN 1900 AND 2100)
);

\echo 'data_in_depth_value created.'

\echo 'Creating indexes...'
CREATE INDEX idx_didv_subject  ON data_in_depth_value (data_in_depth_subject_id);
CREATE INDEX idx_didv_scenario ON data_in_depth_value (scenario_short_code);
CREATE INDEX idx_didv_unit     ON data_in_depth_value (unit_id);
CREATE INDEX idx_didv_active   ON data_in_depth_value (is_active) WHERE is_active = TRUE;
-- Aggregation grain for exceedance / box-plot queries (GROUP BY series, PERCENTILE_CONT over value).
CREATE INDEX idx_didv_series
    ON data_in_depth_value (source_variable, period, unit_id, scenario_short_code, data_in_depth_subject_id);

SELECT apply_audit_trigger_to_table('data_in_depth_value');

\echo 'Registering in domain_family_map (scenario family)...'
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note)
SELECT 'public', 'data_in_depth_value', vf.id,
       'Generic raw long-form values for data_in_depth extracts (per scenario/subject/variable/period/year/unit)'
FROM version_family vf WHERE vf.short_code = 'scenario'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id, note = EXCLUDED.note;

\echo 'Granting to coeqwal_developer...'
GRANT SELECT, INSERT, UPDATE, DELETE ON data_in_depth_value TO coeqwal_developer;
GRANT USAGE, SELECT ON SEQUENCE data_in_depth_value_id_seq TO coeqwal_developer;

COMMENT ON TABLE data_in_depth_value IS
    'Generic raw long-form value store for data_in_depth extracts. One row per '
    '(scenario_short_code, subject, source_variable, period, water_year, unit). '
    'Raw annual samples; exceedance/box-plot quantiles derived at query time.';
COMMENT ON COLUMN data_in_depth_value.source_variable IS 'CalSim variable extracted, e.g. S_SHSTA. Provenance + disambiguates measures per subject.';
COMMENT ON COLUMN data_in_depth_value.period IS 'Temporal selector: april, sept, annual, ...';
COMMENT ON COLUMN data_in_depth_value.unit_id IS 'FK unit. A measure may exist in multiple units (TAF + PCT_CAP; CFS + TAF), so unit is part of the grain.';

\echo ''
\echo 'VERIFICATION:'
\echo '============='
SELECT column_name, data_type, is_nullable FROM information_schema.columns
WHERE table_name = 'data_in_depth_value' ORDER BY ordinal_position;
\echo ''
SELECT conname, contype FROM pg_constraint
WHERE conrelid = 'data_in_depth_value'::regclass ORDER BY contype, conname;
\echo ''
\echo 'DATA_IN_DEPTH_VALUE TABLE CREATED.'
\echo '=================================='
