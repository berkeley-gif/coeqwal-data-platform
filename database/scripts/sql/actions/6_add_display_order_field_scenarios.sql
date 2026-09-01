-- add_display_order_field_scenarios.sql
--
-- Add display_order field to scenario_hydroclimate_sibling to determine the order in which scenarios are displayed on website.
--
-- Created by Eric Lehmer 9/1/2026
BEGIN;

ALTER TABLE scenario_hydroclimate_sibling ADD COLUMN IF NOT EXISTS display_order INTEGER DEFAULT NULL;

COMMIT;