-- add_total_count.sql
--
-- Add total_count field to tier_result that stores the count of tier locations. Used to calculate
-- along with total_value the average tier value that is returned by the API.
--
-- Created by Eric Lehmer 6/11/2026
BEGIN;

ALTER TABLE tier_result ADD COLUMN IF NOT EXISTS total_count INTEGER;

COMMIT;