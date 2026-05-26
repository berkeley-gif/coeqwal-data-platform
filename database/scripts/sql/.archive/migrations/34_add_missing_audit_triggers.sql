-- Migration 34: Add missing audit triggers
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/34_add_missing_audit_triggers.sql
--
-- These 6 tables have audit columns (created_at, created_by, etc.) but
-- no set_audit_fields() trigger, so inserts/updates bypass the automatic
-- audit field population.

BEGIN;

CREATE TRIGGER audit_fields_ag_du_gw_pumping_monthly
    BEFORE INSERT OR UPDATE ON ag_du_gw_pumping_monthly
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

CREATE TRIGGER audit_fields_ag_du_sw_delivery_monthly
    BEFORE INSERT OR UPDATE ON ag_du_sw_delivery_monthly
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

CREATE TRIGGER audit_fields_channel_entity
    BEFORE INSERT OR UPDATE ON channel_entity
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

CREATE TRIGGER audit_fields_channel_variable
    BEFORE INSERT OR UPDATE ON channel_variable
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

CREATE TRIGGER audit_fields_delta_monthly
    BEFORE INSERT OR UPDATE ON delta_monthly
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

CREATE TRIGGER audit_fields_delta_period_summary
    BEFORE INSERT OR UPDATE ON delta_period_summary
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMIT;

-- ── Verification ─────────────────────────────────────────────────────

SELECT 'new_triggers' AS check, tgrelid::regclass AS table_name, tgname, tgenabled
FROM pg_trigger
WHERE tgname IN (
    'audit_fields_ag_du_gw_pumping_monthly',
    'audit_fields_ag_du_sw_delivery_monthly',
    'audit_fields_channel_entity',
    'audit_fields_channel_variable',
    'audit_fields_delta_monthly',
    'audit_fields_delta_period_summary'
)
ORDER BY tgrelid::regclass::text;

\echo
\echo '34 MISSING AUDIT TRIGGERS ADDED'
\echo '================================'
