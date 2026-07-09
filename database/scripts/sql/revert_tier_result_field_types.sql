-- revert_tier_result_field_types.sql
--
-- Revert numeric fields to integer for continuous tier values
--
-- Created by Eric Lehmer 6/11/2026
BEGIN;

ALTER TABLE tier_result ALTER COLUMN tier_1_value TYPE INTEGER;
ALTER TABLE tier_result ALTER COLUMN tier_2_value TYPE INTEGER;
ALTER TABLE tier_result ALTER COLUMN tier_3_value TYPE INTEGER;
ALTER TABLE tier_result ALTER COLUMN tier_4_value TYPE INTEGER;
ALTER TABLE tier_result ALTER COLUMN total_value TYPE INTEGER;

COMMIT;