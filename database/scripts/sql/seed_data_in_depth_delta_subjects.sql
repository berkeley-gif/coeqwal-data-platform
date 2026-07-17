-- SEED DATA_IN_DEPTH_SUBJECT (Delta salinity: X2)
-- ===============================================
-- X2 is a METRIC subject (non-location): the Delta 2-psu isohaline position,
-- in km upstream from the Golden Gate. Metric subjects carry NO
-- location_type/location_id (registry CHECK requires them NULL).
--
-- Extract variable: X2_PRV_KM (c-part X2-POSITION-PREV), unit km, 115 scenarios.
-- NOTE for the extractor: the trend report reports the unit as 'KM', but the DB
-- `unit` lookup short_code is lowercase 'km' — the extractor must emit 'km' so
-- the `JOIN unit ON short_code` resolves.
--
-- Idempotent. DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_delta_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql already applied.

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (X2 delta salinity metric)'
\echo '======================================================='

INSERT INTO data_in_depth_subject (subject_kind, short_code, label)
VALUES ('metric', 'X2', 'Delta salinity: X2 position (2 psu isohaline, km upstream from Golden Gate)')
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label, is_active = TRUE;

-- Verification --------------------------------------------------------------
\echo ''
\echo 'X2 subject seeded (location_type/location_id must be NULL for a metric):'
SELECT subject_kind, short_code, label, location_type, location_id
FROM data_in_depth_subject
WHERE subject_kind = 'metric' AND short_code = 'X2';

\echo ''
\echo 'km unit present in lookup:'
SELECT short_code, full_name FROM unit WHERE short_code = 'km';

\echo ''
\echo 'X2 SUBJECT SEEDED.'
\echo '=================='
