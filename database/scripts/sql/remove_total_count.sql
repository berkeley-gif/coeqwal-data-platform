-- remove_total_count.sql
--
-- Remove added total_count as reverting to total_value
--
-- Created by Eric Lehmer 7/8/2026
BEGIN;

ALTER TABLE tier_result DROP COLUMN total_count;

COMMIT;