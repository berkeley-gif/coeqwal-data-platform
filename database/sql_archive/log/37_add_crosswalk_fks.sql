-- Migration 37: Add missing FK constraints to crosswalk tables
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/37_add_crosswalk_fks.sql
--
-- theme_scenario_link, scenario_key_assumption_link, scenario_key_operation_link
-- already have audit fields and indexes but are missing FK constraints on their
-- relationship columns.

BEGIN;

ALTER TABLE theme_scenario_link
    ADD CONSTRAINT theme_scenario_link_theme_id_fkey
    FOREIGN KEY (theme_id) REFERENCES theme(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE theme_scenario_link
    ADD CONSTRAINT theme_scenario_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_assumption_link
    ADD CONSTRAINT scenario_key_assumption_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_assumption_link
    ADD CONSTRAINT scenario_key_assumption_link_assumption_id_fkey
    FOREIGN KEY (assumption_id) REFERENCES assumption_definition(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_operation_link
    ADD CONSTRAINT scenario_key_operation_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_operation_link
    ADD CONSTRAINT scenario_key_operation_link_operation_id_fkey
    FOREIGN KEY (operation_id) REFERENCES operation_definition(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

COMMIT;


SELECT 'crosswalk_fks' AS check, conrelid::regclass AS table_name, conname
FROM pg_constraint
WHERE contype = 'f'
  AND conrelid IN (
      'theme_scenario_link'::regclass,
      'scenario_key_assumption_link'::regclass,
      'scenario_key_operation_link'::regclass
  )
ORDER BY conrelid::regclass::text, conname;

\echo
\echo '37 CROSSWALK FK CONSTRAINTS COMPLETE'
\echo '====================================='
