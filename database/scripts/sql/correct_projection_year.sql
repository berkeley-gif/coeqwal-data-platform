-- CORRECT PROJECTION_YEAR IN HYDROCLIMATE
-- update projection_year for 2 entries in the hydroclimate table that are set to floats
-- instead of ints.
--
-- Run from the repository root:
--   psql $DATABASE_URL -f database/scripts/sql/correct_projection_year.sql
--
-- Created by Eric Lehmer 5/29/2026

BEGIN;

UPDATE hydroclimate SET projection_year = 2043 WHERE id in (5,6);

COMMIT;