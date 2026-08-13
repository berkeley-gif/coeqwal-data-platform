-- SEED DATA_IN_DEPTH_SUBJECT (salmon: WRLCM adult females)
-- =========================================================
-- WRLCM_ADULT_FEMALES is a METRIC subject (non-location): winter-run Chinook
-- natural-origin adult females, 3-year rolling average, from the WRLCM
-- (Winter-Run Life Cycle Model) tier output. Metric subjects carry NO
-- location_type/location_id (registry CHECK requires them NULL) - same
-- pattern as X2 (see seed_data_in_depth_delta_subjects.sql).
--
-- Source: data/raw/salmon/TIERS_WRLCM.csv. Extract variable metric_avg_roll,
-- new unit NOF_3YR_AVG (added to unit.csv + create_data_in_depth_value_table.sql).
-- ONE source_variable (METRIC_AVG_ROLL) under this ONE subject (see
-- extract_salmon.py).
--
-- Idempotent. DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_salmon_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql already applied.

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (WRLCM salmon adult-females metric)'
\echo '===================================================================='

INSERT INTO data_in_depth_subject (subject_kind, short_code, label)
VALUES ('metric', 'WRLCM_ADULT_FEMALES',
        'Metric of winter-run adundance')
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label, is_active = TRUE;

-- Verification --------------------------------------------------------------
\echo ''
\echo 'WRLCM_ADULT_FEMALES subject seeded (location_type/location_id must be NULL for a metric):'
SELECT subject_kind, short_code, label, location_type, location_id
FROM data_in_depth_subject
WHERE subject_kind = 'metric' AND short_code = 'WRLCM_ADULT_FEMALES';

\echo ''
\echo 'NOF_3YR_AVG unit present in lookup:'
SELECT short_code, full_name, canonical_group FROM unit WHERE short_code = 'NOF_3YR_AVG';

\echo ''
\echo 'WRLCM SALMON SUBJECT SEEDED.'
\echo '============================='
