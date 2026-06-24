-- change_tier_result_field_types.sql
--
-- Change integer fields to numeric for continuous tier values
--
-- Created by Eric Lehmer 6/11/2026
BEGIN;

ALTER TABLE tier_result ALTER COLUMN tier_1_value TYPE NUMERIC(6,3);
ALTER TABLE tier_result ALTER COLUMN tier_2_value TYPE NUMERIC(6,3);
ALTER TABLE tier_result ALTER COLUMN tier_3_value TYPE NUMERIC(6,3);
ALTER TABLE tier_result ALTER COLUMN tier_4_value TYPE NUMERIC(6,3);
ALTER TABLE tier_result ALTER COLUMN total_value TYPE NUMERIC(6,3);

COMMIT;