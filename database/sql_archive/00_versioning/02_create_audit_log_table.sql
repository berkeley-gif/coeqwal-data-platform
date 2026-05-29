-- =============================================================================
-- 02_create_audit_log_table.sql
-- Creates the audit_log table for tracking all database changes
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    
    table_name TEXT NOT NULL,
    record_id INTEGER,
    record_key JSONB,
    
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    
    old_values JSONB,
    new_values JSONB,
    changed_fields TEXT[],
    
    changed_by INTEGER REFERENCES developer(id),
    changed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    
    session_user_name TEXT DEFAULT session_user,
    application_name TEXT DEFAULT current_setting('application_name', true),
    client_addr INET DEFAULT inet_client_addr()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_table_name ON audit_log(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_record ON audit_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON audit_log(changed_at);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_by ON audit_log(changed_by);
CREATE INDEX IF NOT EXISTS idx_audit_log_operation ON audit_log(operation);

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

SELECT 'audit_log table created successfully' AS status;
SELECT COUNT(*) AS existing_records FROM audit_log;
