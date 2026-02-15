-- =============================================================================
-- 01_create_audit_log_table.sql
-- Creates the audit_log table for tracking all database changes
-- =============================================================================
-- Run in Cloud9: psql -f 01_create_audit_log_table.sql
-- =============================================================================

-- Create audit_log table for comprehensive change tracking
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    
    -- What was changed
    table_name TEXT NOT NULL,
    record_id INTEGER,                    -- Primary key of affected record (if applicable)
    record_key JSONB,                     -- For composite keys or non-integer PKs
    
    -- What operation
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    
    -- What changed
    old_values JSONB,                     -- Previous values (UPDATE/DELETE)
    new_values JSONB,                     -- New values (INSERT/UPDATE)
    changed_fields TEXT[],                -- List of fields that changed (UPDATE only)
    
    -- Who and when
    changed_by INTEGER REFERENCES developer(id),
    changed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    -- Context
    session_user_name TEXT DEFAULT session_user,
    application_name TEXT DEFAULT current_setting('application_name', true),
    client_addr INET DEFAULT inet_client_addr()
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_audit_log_table_name ON audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_record ON audit_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON audit_log(changed_at);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_by ON audit_log(changed_by);
CREATE INDEX IF NOT EXISTS idx_audit_log_operation ON audit_log(operation);

-- Add documentation
COMMENT ON TABLE audit_log IS 
'Audit log tracking all INSERT, UPDATE, DELETE operations.
Stores old/new values as JSONB for flexibility.
Query examples:
  - All changes to a table: SELECT * FROM audit_log WHERE table_name = ''scenario'';
  - Changes by user: SELECT * FROM audit_log WHERE changed_by = 2;
  - Recent changes: SELECT * FROM audit_log WHERE changed_at > NOW() - INTERVAL ''7 days'';';

COMMENT ON COLUMN audit_log.record_id IS 'Integer primary key of affected record (NULL for composite keys)';
COMMENT ON COLUMN audit_log.record_key IS 'JSONB representation of primary key (for composite or non-integer PKs)';
COMMENT ON COLUMN audit_log.changed_fields IS 'Array of column names that changed (UPDATE operations only)';

-- Verify creation
SELECT 'audit_log table created successfully' AS status;
SELECT COUNT(*) AS existing_records FROM audit_log;
