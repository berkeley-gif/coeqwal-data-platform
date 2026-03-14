-- Migration 31: Fix developer attribution
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/31_fix_attribution.sql
--
-- All 38 tables currently have every row attributed to developer 1 (system).
-- Developer 2 (jfantauzza) initiated all ETL loads via S3 upload, so attribution
-- should reflect the actual operator.
--
-- Tables WITHOUT audit triggers (4): update directly
-- Tables WITH audit triggers (34): disable trigger → update → re-enable

BEGIN;

-- ── Disable audit triggers on the 34 tables that have them ───────────
ALTER TABLE ag_aggregate_entity             DISABLE TRIGGER audit_fields_ag_aggregate_entity;
ALTER TABLE ag_aggregate_monthly            DISABLE TRIGGER audit_fields_ag_aggregate_monthly;
ALTER TABLE ag_aggregate_period_summary     DISABLE TRIGGER audit_fields_ag_aggregate_period_summary;
ALTER TABLE ag_du_demand_monthly            DISABLE TRIGGER audit_fields_ag_du_demand_monthly;
ALTER TABLE ag_du_period_summary            DISABLE TRIGGER audit_fields_ag_du_period_summary;
ALTER TABLE ag_du_shortage_monthly          DISABLE TRIGGER audit_fields_ag_du_shortage_monthly;
ALTER TABLE cws_aggregate_entity            DISABLE TRIGGER audit_fields_cws_aggregate_entity;
ALTER TABLE cws_aggregate_monthly           DISABLE TRIGGER audit_fields_cws_aggregate_monthly;
ALTER TABLE cws_aggregate_period_summary    DISABLE TRIGGER audit_fields_cws_aggregate_period_summary;
ALTER TABLE du_agriculture_entity           DISABLE TRIGGER audit_fields_du_agriculture_entity;
ALTER TABLE du_delivery_monthly             DISABLE TRIGGER audit_fields_du_delivery_monthly;
ALTER TABLE du_period_summary               DISABLE TRIGGER audit_fields_du_period_summary;
ALTER TABLE du_shortage_monthly             DISABLE TRIGGER audit_fields_du_shortage_monthly;
ALTER TABLE du_urban_delivery_arc           DISABLE TRIGGER audit_fields_du_urban_delivery_arc;
ALTER TABLE du_urban_entity                 DISABLE TRIGGER audit_fields_du_urban_entity;
ALTER TABLE du_urban_group                  DISABLE TRIGGER audit_fields_du_urban_group;
ALTER TABLE du_urban_group_member           DISABLE TRIGGER audit_fields_du_urban_group_member;
ALTER TABLE du_urban_variable               DISABLE TRIGGER audit_fields_du_urban_variable;
ALTER TABLE hydroclimate                    DISABLE TRIGGER audit_fields_hydroclimate;
ALTER TABLE mi_contractor                   DISABLE TRIGGER audit_fields_mi_contractor;
ALTER TABLE mi_contractor_delivery_arc      DISABLE TRIGGER audit_fields_mi_contractor_delivery_arc;
ALTER TABLE mi_contractor_group             DISABLE TRIGGER audit_fields_mi_contractor_group;
ALTER TABLE mi_contractor_group_member      DISABLE TRIGGER audit_fields_mi_contractor_group_member;
ALTER TABLE mi_contractor_period_summary    DISABLE TRIGGER audit_fields_mi_contractor_period_summary;
ALTER TABLE mi_delivery_monthly             DISABLE TRIGGER audit_fields_mi_delivery_monthly;
ALTER TABLE mi_shortage_monthly             DISABLE TRIGGER audit_fields_mi_shortage_monthly;
ALTER TABLE reservoir_entity                DISABLE TRIGGER audit_fields_reservoir_entity;
ALTER TABLE reservoir_group                 DISABLE TRIGGER audit_fields_reservoir_group;
ALTER TABLE reservoir_group_member          DISABLE TRIGGER audit_fields_reservoir_group_member;
ALTER TABLE reservoir_monthly_percentile    DISABLE TRIGGER audit_fields_reservoir_monthly_percentile;
ALTER TABLE reservoir_period_summary        DISABLE TRIGGER audit_fields_reservoir_period_summary;
ALTER TABLE reservoir_spill_monthly         DISABLE TRIGGER audit_fields_reservoir_spill_monthly;
ALTER TABLE reservoir_storage_monthly       DISABLE TRIGGER audit_fields_reservoir_storage_monthly;
ALTER TABLE theme_scenario_link             DISABLE TRIGGER audit_fields_theme_scenario_link;

-- ── Update attribution on all 38 tables (created_by 1→2, updated_by 1→2) ──
-- Tables WITH triggers (34):
UPDATE ag_aggregate_entity          SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_aggregate_monthly         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_aggregate_period_summary  SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_demand_monthly         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_period_summary         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_shortage_monthly       SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE cws_aggregate_entity         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE cws_aggregate_monthly        SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE cws_aggregate_period_summary SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
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

-- Tables WITHOUT triggers (4):
UPDATE ag_du_gw_pumping_monthly     SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE ag_du_sw_delivery_monthly    SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE delta_monthly                SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;
UPDATE delta_period_summary         SET created_by = 2, updated_by = 2, updated_at = NOW() WHERE created_by = 1;

-- ── Re-enable audit triggers ─────────────────────────────────────────
ALTER TABLE ag_aggregate_entity             ENABLE TRIGGER audit_fields_ag_aggregate_entity;
ALTER TABLE ag_aggregate_monthly            ENABLE TRIGGER audit_fields_ag_aggregate_monthly;
ALTER TABLE ag_aggregate_period_summary     ENABLE TRIGGER audit_fields_ag_aggregate_period_summary;
ALTER TABLE ag_du_demand_monthly            ENABLE TRIGGER audit_fields_ag_du_demand_monthly;
ALTER TABLE ag_du_period_summary            ENABLE TRIGGER audit_fields_ag_du_period_summary;
ALTER TABLE ag_du_shortage_monthly          ENABLE TRIGGER audit_fields_ag_du_shortage_monthly;
ALTER TABLE cws_aggregate_entity            ENABLE TRIGGER audit_fields_cws_aggregate_entity;
ALTER TABLE cws_aggregate_monthly           ENABLE TRIGGER audit_fields_cws_aggregate_monthly;
ALTER TABLE cws_aggregate_period_summary    ENABLE TRIGGER audit_fields_cws_aggregate_period_summary;
ALTER TABLE du_agriculture_entity           ENABLE TRIGGER audit_fields_du_agriculture_entity;
ALTER TABLE du_delivery_monthly             ENABLE TRIGGER audit_fields_du_delivery_monthly;
ALTER TABLE du_period_summary               ENABLE TRIGGER audit_fields_du_period_summary;
ALTER TABLE du_shortage_monthly             ENABLE TRIGGER audit_fields_du_shortage_monthly;
ALTER TABLE du_urban_delivery_arc           ENABLE TRIGGER audit_fields_du_urban_delivery_arc;
ALTER TABLE du_urban_entity                 ENABLE TRIGGER audit_fields_du_urban_entity;
ALTER TABLE du_urban_group                  ENABLE TRIGGER audit_fields_du_urban_group;
ALTER TABLE du_urban_group_member           ENABLE TRIGGER audit_fields_du_urban_group_member;
ALTER TABLE du_urban_variable               ENABLE TRIGGER audit_fields_du_urban_variable;
ALTER TABLE hydroclimate                    ENABLE TRIGGER audit_fields_hydroclimate;
ALTER TABLE mi_contractor                   ENABLE TRIGGER audit_fields_mi_contractor;
ALTER TABLE mi_contractor_delivery_arc      ENABLE TRIGGER audit_fields_mi_contractor_delivery_arc;
ALTER TABLE mi_contractor_group             ENABLE TRIGGER audit_fields_mi_contractor_group;
ALTER TABLE mi_contractor_group_member      ENABLE TRIGGER audit_fields_mi_contractor_group_member;
ALTER TABLE mi_contractor_period_summary    ENABLE TRIGGER audit_fields_mi_contractor_period_summary;
ALTER TABLE mi_delivery_monthly             ENABLE TRIGGER audit_fields_mi_delivery_monthly;
ALTER TABLE mi_shortage_monthly             ENABLE TRIGGER audit_fields_mi_shortage_monthly;
ALTER TABLE reservoir_entity                ENABLE TRIGGER audit_fields_reservoir_entity;
ALTER TABLE reservoir_group                 ENABLE TRIGGER audit_fields_reservoir_group;
ALTER TABLE reservoir_group_member          ENABLE TRIGGER audit_fields_reservoir_group_member;
ALTER TABLE reservoir_monthly_percentile    ENABLE TRIGGER audit_fields_reservoir_monthly_percentile;
ALTER TABLE reservoir_period_summary        ENABLE TRIGGER audit_fields_reservoir_period_summary;
ALTER TABLE reservoir_spill_monthly         ENABLE TRIGGER audit_fields_reservoir_spill_monthly;
ALTER TABLE reservoir_storage_monthly       ENABLE TRIGGER audit_fields_reservoir_storage_monthly;
ALTER TABLE theme_scenario_link             ENABLE TRIGGER audit_fields_theme_scenario_link;

COMMIT;

-- ── Verification ─────────────────────────────────────────────────────
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
\echo '31 ATTRIBUTION FIX COMPLETE'
\echo '==========================='
