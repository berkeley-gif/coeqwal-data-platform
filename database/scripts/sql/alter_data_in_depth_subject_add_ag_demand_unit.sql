-- ALTER data_in_depth_subject: allow location_type 'ag_demand_unit'
-- =================================================================
-- Extends chk_dids_location_type so agricultural DUs can be typed distinctly
-- (they resolve to du_agriculture_entity, not du_urban_entity like 'demand_unit').
-- For live DBs where create_data_in_depth_subject_table.sql was already applied
-- (re-running the create would DROP the table + its data).
--
-- Run as superuser (DDL):
--   psql "$SUPERUSER_URL" -f database/scripts/sql/alter_data_in_depth_subject_add_ag_demand_unit.sql

\set ON_ERROR_STOP on

ALTER TABLE data_in_depth_subject DROP CONSTRAINT IF EXISTS chk_dids_location_type;
ALTER TABLE data_in_depth_subject ADD CONSTRAINT chk_dids_location_type
    CHECK (location_type IS NULL OR location_type IN
           ('network_node', 'wba', 'reservoir', 'compliance_station', 'region',
            'demand_unit', 'ag_demand_unit'));

\echo 'chk_dids_location_type now allows ag_demand_unit.'
