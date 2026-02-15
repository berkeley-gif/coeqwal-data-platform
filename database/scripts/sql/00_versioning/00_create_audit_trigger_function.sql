-- =============================================================================
-- 00_create_audit_trigger_function.sql
-- Creates the trigger function for automatic audit field population
-- =============================================================================
-- Run in Cloud9: psql -f 00_create_audit_trigger_function.sql
-- =============================================================================

-- Drop existing function if it exists (to allow updates)
DROP FUNCTION IF EXISTS set_audit_fields() CASCADE;

-- Create the audit trigger function
-- This function automatically populates audit fields on INSERT and UPDATE
CREATE OR REPLACE FUNCTION set_audit_fields()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- On INSERT: set all audit fields
        -- Use COALESCE to allow explicit values to override defaults
        NEW.created_at := COALESCE(NEW.created_at, NOW());
        NEW.created_by := COALESCE(NEW.created_by, coeqwal_current_operator());
        NEW.updated_at := NOW();
        NEW.updated_by := coeqwal_current_operator();
    ELSIF TG_OP = 'UPDATE' THEN
        -- On UPDATE: only update the updated_* fields
        -- Preserve original created_* fields (prevent tampering)
        NEW.updated_at := NOW();
        NEW.updated_by := coeqwal_current_operator();
        NEW.created_at := OLD.created_at;
        NEW.created_by := OLD.created_by;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add documentation
COMMENT ON FUNCTION set_audit_fields() IS 
'Trigger function that automatically sets audit fields:
- INSERT: Sets created_at, created_by, updated_at, updated_by
- UPDATE: Sets updated_at, updated_by (preserves created_* fields)
Uses coeqwal_current_operator() to identify the current user.';

-- Verify creation
SELECT 'set_audit_fields() function created successfully' AS status;
