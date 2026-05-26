-- =============================================================================
-- 03_apply_audit_triggers.sql
-- Applies audit triggers to all tables with audit columns
-- =============================================================================
-- Run in Cloud9: psql -f 03_apply_audit_triggers.sql
-- =============================================================================

-- This script applies the set_audit_fields() trigger to all tables that have
-- the required audit columns (created_at, created_by, updated_at, updated_by).

-- =============================================================================
-- Create helper function to apply triggers dynamically
-- =============================================================================
CREATE OR REPLACE FUNCTION apply_audit_trigger_to_table(p_table_name TEXT)
RETURNS TEXT AS $$
DECLARE
    trigger_name TEXT;
    has_all_columns BOOLEAN;
BEGIN
    trigger_name := 'audit_fields_' || p_table_name;
    
    -- Check if table has all required audit columns
    SELECT COUNT(*) = 4 INTO has_all_columns
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = p_table_name
    AND column_name IN ('created_at', 'created_by', 'updated_at', 'updated_by');
    
    IF NOT has_all_columns THEN
        RETURN 'SKIPPED: ' || p_table_name || ' (missing audit columns)';
    END IF;
    
    -- Drop existing trigger if it exists
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, p_table_name);
    
    -- Create new trigger
    EXECUTE format(
        'CREATE TRIGGER %I
         BEFORE INSERT OR UPDATE ON %I
         FOR EACH ROW EXECUTE FUNCTION set_audit_fields()',
        trigger_name, p_table_name
    );
    
    RETURN 'SUCCESS: ' || p_table_name;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Apply triggers to all public tables (except system tables)
-- =============================================================================
DO $$
DECLARE
    tbl RECORD;
    result TEXT;
BEGIN
    RAISE NOTICE '=== Applying audit triggers to all tables ===';
    
    FOR tbl IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name NOT IN ('spatial_ref_sys', 'geography_columns', 'geometry_columns')
        ORDER BY table_name
    LOOP
        result := apply_audit_trigger_to_table(tbl.table_name);
        RAISE NOTICE '%', result;
    END LOOP;
    
    RAISE NOTICE '=== Trigger application complete ===';
END $$;

-- =============================================================================
-- Create audit logging trigger function (records changes to audit_log table)
-- =============================================================================
CREATE OR REPLACE FUNCTION log_audit_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_old_values JSONB;
    v_new_values JSONB;
    v_changed_fields TEXT[];
    v_record_id INTEGER;
    v_key TEXT;
BEGIN
    -- Get record ID (assumes 'id' column, or NULL if not present)
    IF TG_OP = 'DELETE' THEN
        BEGIN
            v_record_id := OLD.id;
        EXCEPTION WHEN undefined_column THEN
            v_record_id := NULL;
        END;
    ELSE
        BEGIN
            v_record_id := NEW.id;
        EXCEPTION WHEN undefined_column THEN
            v_record_id := NULL;
        END;
    END IF;
    
    IF TG_OP = 'INSERT' THEN
        v_new_values := to_jsonb(NEW);
        INSERT INTO audit_log (table_name, record_id, operation, new_values, changed_by)
        VALUES (TG_TABLE_NAME, v_record_id, 'INSERT', v_new_values, NEW.created_by);
        
    ELSIF TG_OP = 'UPDATE' THEN
        v_old_values := to_jsonb(OLD);
        v_new_values := to_jsonb(NEW);
        
        -- Find changed fields
        SELECT array_agg(key) INTO v_changed_fields
        FROM jsonb_each(v_old_values) AS old_kv(key, value)
        WHERE v_new_values->key IS DISTINCT FROM old_kv.value;
        
        INSERT INTO audit_log (table_name, record_id, operation, old_values, new_values, changed_fields, changed_by)
        VALUES (TG_TABLE_NAME, v_record_id, 'UPDATE', v_old_values, v_new_values, v_changed_fields, NEW.updated_by);
        
    ELSIF TG_OP = 'DELETE' THEN
        v_old_values := to_jsonb(OLD);
        INSERT INTO audit_log (table_name, record_id, operation, old_values, changed_by)
        VALUES (TG_TABLE_NAME, v_record_id, 'DELETE', v_old_values, coeqwal_current_operator());
    END IF;
    
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION log_audit_changes() IS 
'Trigger function that logs all changes to the audit_log table.
Records old/new values as JSONB and tracks which fields changed.';

-- =============================================================================
-- Helper function to apply audit logging trigger
-- =============================================================================
CREATE OR REPLACE FUNCTION apply_audit_log_trigger_to_table(p_table_name TEXT)
RETURNS TEXT AS $$
DECLARE
    trigger_name TEXT;
BEGIN
    trigger_name := 'audit_log_' || p_table_name;
    
    -- Drop existing trigger if it exists
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, p_table_name);
    
    -- Create new trigger (AFTER to capture final values)
    EXECUTE format(
        'CREATE TRIGGER %I
         AFTER INSERT OR UPDATE OR DELETE ON %I
         FOR EACH ROW EXECUTE FUNCTION log_audit_changes()',
        trigger_name, p_table_name
    );
    
    RETURN 'SUCCESS: audit_log trigger on ' || p_table_name;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Apply audit logging triggers (uncomment to enable full audit logging)
-- =============================================================================
-- Note: Audit logging can generate a lot of data. Enable selectively.
-- To enable for a specific table, run:
--   SELECT apply_audit_log_trigger_to_table('your_table_name');

-- Example: Enable audit logging for critical tables
-- SELECT apply_audit_log_trigger_to_table('developer');
-- SELECT apply_audit_log_trigger_to_table('version');
-- SELECT apply_audit_log_trigger_to_table('version_family');

-- =============================================================================
-- Verify triggers
-- =============================================================================
SELECT 'Triggers applied successfully' AS status;

SELECT 
    trigger_name,
    event_object_table AS table_name,
    event_manipulation AS event,
    action_timing AS timing
FROM information_schema.triggers
WHERE trigger_schema = 'public'
AND trigger_name LIKE 'audit_%'
ORDER BY event_object_table, trigger_name;
