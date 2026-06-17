-- ADD NEW WARMER AND WETTER HYDROCLIMATE
-- inserts new record into hydroclimate table for Warmer and Wetter (EC-Earth3-Veg SSP370)
--
-- Run from the repository root:
--   psql $DATABASE_URL -f database/scripts/sql/1_add_warmer_wetter_hydroclimate.sql
--
-- Created by Eric Lehmer 5/29/2026

BEGIN;

INSERT INTO hydroclimate (
    short_code, name, simple_description,
    is_active, projection_year, source_id,
    notes, hydroclimate_version_id
) VALUES
    ('CMIP6_EC-Earth3-Veg_SSP370', 'Warmer and Wetter', '1.2°C temperature increase and 4% precipitation increase over CV inflow basins',
     1, '2043', 1,
     'HF statistics are used to adjust Historical temperature, precipitation, and streamflow using a quantile-mapping approach.', 1.0);

COMMIT;