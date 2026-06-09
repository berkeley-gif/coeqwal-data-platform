-- add_tier_continuous
--
-- Adds new continuous tier value field to be displayed on a per location basis
--
-- Created by Eric Lehmer 6/9/2026
BEGIN;

ALTER TABLE tier_location_result ADD COLUMN IF NOT EXISTS tier_continuous NUMERIC(5,2);

COMMIT;