-- Migration 31: Fix developer attribution
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/31_fix_attribution.sql
--
-- All 38 tables currently have every row attributed to developer 1 (system).
-- Developer 2 (jfantauzza) initiated all ETL loads via S3 upload, so attribution
-- should reflect the actual operator.
--
-- Uses DISABLE TRIGGER USER to suppress all user-defined triggers regardless of
-- naming convention. Safe no-op on tables without triggers.

BEGIN;

ALTER TABLE ag_aggregate_entity          DISABLE TRIGGER USER;
ALTER TABLE ag_aggregate_monthly         DISABLE TRIGGER USER;
ALTER TABLE ag_aggregate_period_summary  DISABLE TRIGGER USER;
ALTER TABLE ag_du_demand_monthly         DISABLE TRIGGER USER;
ALTER TABLE ag_du_gw_pumping_monthly     DISABLE TRIGGER USER;
ALTER TABLE ag_du_period_summary         DISABLE TRIGGER USER;
ALTER TABLE ag_du_shortage_monthly       DISABLE TRIGGER USER;
ALTER TABLE ag_du_sw_delivery_monthly    DISABLE TRIGGER USER;
ALTER TABLE cws_aggregate_entity         DISABLE TRIGGER USER;
ALTER TABLE cws_aggregate_monthly        DISABLE TRIGGER USER;
ALTER TABLE cws_aggregate_period_summary DISABLE TRIGGER USER;
ALTER TABLE delta_monthly                DISABLE TRIGGER USER;
ALTER TABLE delta_period_summary         DISABLE TRIGGER USER;
ALTER TABLE du_agriculture_entity        DISABLE TRIGGER USER;
ALTER TABLE du_delivery_monthly          DISABLE TRIGGER USER;
ALTER TABLE du_period_summary            DISABLE TRIGGER USER;
ALTER TABLE du_shortage_monthly          DISABLE TRIGGER USER;
ALTER TABLE du_urban_delivery_arc        DISABLE TRIGGER USER;
ALTER TABLE du_urban_entity              DISABLE TRIGGER USER;
ALTER TABLE du_urban_group               DISABLE TRIGGER USER;
ALTER TABLE du_urban_group_member        DISABLE TRIGGER USER;
ALTER TABLE du_urban_variable            DISABLE TRIGGER USER;
ALTER TABLE hydroclimate                 DISABLE TRIGGER USER;
ALTER TABLE mi_contractor                DISABLE TRIGGER USER;
ALTER TABLE mi_contractor_delivery_arc   DISABLE TRIGGER USER;
ALTER TABLE mi_contractor_group          DISABLE TRIGGER USER;
ALTER TABLE mi_contractor_group_member   DISABLE TRIGGER USER;
ALTER TABLE mi_contractor_period_summary DISABLE TRIGGER USER;
ALTER TABLE mi_delivery_monthly          DISABLE TRIGGER USER;
ALTER TABLE mi_shortage_monthly          DISABLE TRIGGER USER;
ALTER TABLE reservoir_entity             DISABLE TRIGGER USER;
ALTER TABLE reservoir_group              DISABLE TRIGGER USER;
ALTER TABLE reservoir_group_member       DISABLE TRIGGER USER;
ALTER TABLE reservoir_monthly_percentile DISABLE TRIGGER USER;
ALTER TABLE reservoir_period_summary     DISABLE TRIGGER USER;
ALTER TABLE reservoir_spill_monthly      DISABLE TRIGGER USER;
ALTER TABLE reservoir_storage_monthly    DISABLE TRIGGER USER;
ALTER TABLE theme_scenario_link          DISABLE TRIGGER USER;

UPDATE ag_aggregate_entity          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_aggregate_monthly         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_aggregate_period_summary  SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_demand_monthly         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_gw_pumping_monthly     SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_period_summary         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_shortage_monthly       SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_sw_delivery_monthly    SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE cws_aggregate_entity         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE cws_aggregate_monthly        SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE cws_aggregate_period_summary SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE delta_monthly                SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE delta_period_summary         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_agriculture_entity        SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_delivery_monthly          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_period_summary            SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_shortage_monthly          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_urban_delivery_arc        SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_urban_entity              SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_urban_group               SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_urban_group_member        SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE du_urban_variable            SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE hydroclimate                 SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE mi_contractor                SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE mi_contractor_delivery_arc   SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE mi_contractor_group          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE mi_contractor_group_member   SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE mi_contractor_period_summary SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE mi_delivery_monthly          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE mi_shortage_monthly          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE reservoir_entity             SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE reservoir_group              SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE reservoir_group_member       SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE reservoir_monthly_percentile SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE reservoir_period_summary     SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE reservoir_spill_monthly      SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE reservoir_storage_monthly    SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE theme_scenario_link          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;

ALTER TABLE ag_aggregate_entity          ENABLE TRIGGER USER;
ALTER TABLE ag_aggregate_monthly         ENABLE TRIGGER USER;
ALTER TABLE ag_aggregate_period_summary  ENABLE TRIGGER USER;
ALTER TABLE ag_du_demand_monthly         ENABLE TRIGGER USER;
ALTER TABLE ag_du_gw_pumping_monthly     ENABLE TRIGGER USER;
ALTER TABLE ag_du_period_summary         ENABLE TRIGGER USER;
ALTER TABLE ag_du_shortage_monthly       ENABLE TRIGGER USER;
ALTER TABLE ag_du_sw_delivery_monthly    ENABLE TRIGGER USER;
ALTER TABLE cws_aggregate_entity         ENABLE TRIGGER USER;
ALTER TABLE cws_aggregate_monthly        ENABLE TRIGGER USER;
ALTER TABLE cws_aggregate_period_summary ENABLE TRIGGER USER;
ALTER TABLE delta_monthly                ENABLE TRIGGER USER;
ALTER TABLE delta_period_summary         ENABLE TRIGGER USER;
ALTER TABLE du_agriculture_entity        ENABLE TRIGGER USER;
ALTER TABLE du_delivery_monthly          ENABLE TRIGGER USER;
ALTER TABLE du_period_summary            ENABLE TRIGGER USER;
ALTER TABLE du_shortage_monthly          ENABLE TRIGGER USER;
ALTER TABLE du_urban_delivery_arc        ENABLE TRIGGER USER;
ALTER TABLE du_urban_entity              ENABLE TRIGGER USER;
ALTER TABLE du_urban_group               ENABLE TRIGGER USER;
ALTER TABLE du_urban_group_member        ENABLE TRIGGER USER;
ALTER TABLE du_urban_variable            ENABLE TRIGGER USER;
ALTER TABLE hydroclimate                 ENABLE TRIGGER USER;
ALTER TABLE mi_contractor                ENABLE TRIGGER USER;
ALTER TABLE mi_contractor_delivery_arc   ENABLE TRIGGER USER;
ALTER TABLE mi_contractor_group          ENABLE TRIGGER USER;
ALTER TABLE mi_contractor_group_member   ENABLE TRIGGER USER;
ALTER TABLE mi_contractor_period_summary ENABLE TRIGGER USER;
ALTER TABLE mi_delivery_monthly          ENABLE TRIGGER USER;
ALTER TABLE mi_shortage_monthly          ENABLE TRIGGER USER;
ALTER TABLE reservoir_entity             ENABLE TRIGGER USER;
ALTER TABLE reservoir_group              ENABLE TRIGGER USER;
ALTER TABLE reservoir_group_member       ENABLE TRIGGER USER;
ALTER TABLE reservoir_monthly_percentile ENABLE TRIGGER USER;
ALTER TABLE reservoir_period_summary     ENABLE TRIGGER USER;
ALTER TABLE reservoir_spill_monthly      ENABLE TRIGGER USER;
ALTER TABLE reservoir_storage_monthly    ENABLE TRIGGER USER;
ALTER TABLE theme_scenario_link          ENABLE TRIGGER USER;

COMMIT;

SELECT 'system_rows_remaining' AS check,
       COUNT(*) FILTER (WHERE created_by = 1) AS system_rows,
       COUNT(*) FILTER (WHERE created_by = 2) AS developer_rows
FROM (
    SELECT created_by FROM ag_aggregate_entity
    UNION ALL SELECT created_by FROM reservoir_storage_monthly
    UNION ALL SELECT created_by FROM du_urban_entity
    UNION ALL SELECT created_by FROM mi_contractor
    UNION ALL SELECT created_by FROM theme_scenario_link
    UNION ALL SELECT created_by FROM delta_monthly
    UNION ALL SELECT created_by FROM hydroclimate
) t;

\echo
\echo 'Disabled triggers (should be empty):'
SELECT c.relname AS table_name, t.tgname AS trigger_name, t.tgenabled AS state
FROM pg_trigger t
JOIN pg_class c ON t.tgrelid = c.oid
WHERE t.tgenabled = 'D'
  AND NOT t.tgisinternal
  AND c.relname IN (
    'ag_aggregate_entity', 'ag_aggregate_monthly', 'ag_aggregate_period_summary',
    'ag_du_demand_monthly', 'ag_du_gw_pumping_monthly',
    'ag_du_period_summary', 'ag_du_shortage_monthly', 'ag_du_sw_delivery_monthly',
    'cws_aggregate_entity', 'cws_aggregate_monthly', 'cws_aggregate_period_summary',
    'delta_monthly', 'delta_period_summary',
    'du_agriculture_entity', 'du_delivery_monthly', 'du_period_summary',
    'du_shortage_monthly', 'du_urban_delivery_arc', 'du_urban_entity',
    'du_urban_group', 'du_urban_group_member', 'du_urban_variable',
    'hydroclimate', 'mi_contractor', 'mi_contractor_delivery_arc',
    'mi_contractor_group', 'mi_contractor_group_member', 'mi_contractor_period_summary',
    'mi_delivery_monthly', 'mi_shortage_monthly',
    'reservoir_entity', 'reservoir_group', 'reservoir_group_member',
    'reservoir_monthly_percentile', 'reservoir_period_summary',
    'reservoir_spill_monthly', 'reservoir_storage_monthly', 'theme_scenario_link'
  )
ORDER BY c.relname;

\echo
\echo '31 ATTRIBUTION FIX COMPLETE'
\echo '==========================='
