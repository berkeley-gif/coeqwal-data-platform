-- =============================================================================
-- 01_create_audit_trigger_function.sql
-- Creates the trigger function for automatic audit field population
-- =============================================================================

DROP FUNCTION IF EXISTS set_audit_fields() CASCADE;

CREATE OR REPLACE FUNCTION set_audit_fields()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.created_at := COALESCE(NEW.created_at, NOW());
        NEW.created_by := COALESCE(NEW.created_by, coeqwal_current_operator());
        NEW.updated_at := NOW();
        NEW.updated_by := coeqwal_current_operator();
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.updated_at := NOW();
        NEW.updated_by := coeqwal_current_operator();
        NEW.created_at := OLD.created_at;
        NEW.created_by := OLD.created_by;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_audit_fields() IS 
'Trigger function that automatically sets audit fields:
- INSERT: Sets created_at, created_by, updated_at, updated_by
- UPDATE: Sets updated_at, updated_by (preserves created_* fields)
Uses coeqwal_current_operator() to identify the current user.';

SELECT 'set_audit_fields() function created successfully' AS status;
