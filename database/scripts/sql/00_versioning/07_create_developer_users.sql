-- =============================================================================
-- 07_create_developer_users.sql
-- Utility function for creating and registering database users
-- =============================================================================

\echo '============================================================================'
\echo 'CREATING DEVELOPER USER MANAGEMENT FUNCTION'
\echo '============================================================================'

-- =============================================================================
-- Function: register_developer(username, email, display_name, password, role)
-- Creates a PostgreSQL user and registers them in the developer table
-- =============================================================================

CREATE OR REPLACE FUNCTION register_developer(
    p_username TEXT,
    p_email TEXT,
    p_display_name TEXT,
    p_password TEXT,
    p_role TEXT DEFAULT 'developer'
)
RETURNS TEXT AS $$
DECLARE
    v_user_exists BOOLEAN;
    v_dev_exists BOOLEAN;
    v_dev_id INTEGER;
BEGIN
    -- Check if PostgreSQL user already exists
    SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = p_username) INTO v_user_exists;
    
    -- Check if developer record already exists
    SELECT EXISTS(SELECT 1 FROM developer WHERE email = p_email) INTO v_dev_exists;
    
    -- Create PostgreSQL user if doesn't exist
    IF NOT v_user_exists THEN
        EXECUTE format('CREATE USER %I WITH PASSWORD %L', p_username, p_password);
        RAISE NOTICE 'Created PostgreSQL user: %', p_username;
    ELSE
        RAISE NOTICE 'PostgreSQL user % already exists', p_username;
    END IF;
    
    -- Grant permissions
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), p_username);
    EXECUTE format('GRANT USAGE ON SCHEMA public TO %I', p_username);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %I', p_username);
    EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %I', p_username);
    RAISE NOTICE 'Granted permissions to %', p_username;
    
    -- Create or update developer record
    IF NOT v_dev_exists THEN
        INSERT INTO developer (email, display_name, aws_sso_username, role, is_active, created_at, updated_at)
        VALUES (p_email, p_display_name, p_username, p_role, true, NOW(), NOW())
        RETURNING id INTO v_dev_id;
        RAISE NOTICE 'Created developer record (id=%)', v_dev_id;
    ELSE
        UPDATE developer 
        SET aws_sso_username = p_username,
            display_name = p_display_name,
            role = p_role,
            updated_at = NOW()
        WHERE email = p_email
        RETURNING id INTO v_dev_id;
        RAISE NOTICE 'Updated developer record (id=%)', v_dev_id;
    END IF;
    
    RETURN format('SUCCESS: User %s registered (developer.id=%s). Connect with: psql -U %s -d %s', 
                  p_username, v_dev_id, p_username, current_database());
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION register_developer(TEXT, TEXT, TEXT, TEXT, TEXT) IS 
'Creates a PostgreSQL user and registers them in the developer table for audit tracking.
Usage: SELECT register_developer(''username'', ''email@example.com'', ''Display Name'', ''password'', ''role'');
Roles: admin, developer (default)';

-- =============================================================================
-- Function: list_developers()
-- Shows all registered developers and their database users
-- =============================================================================

CREATE OR REPLACE FUNCTION list_developers()
RETURNS TABLE (
    id INTEGER,
    email TEXT,
    display_name TEXT,
    db_username TEXT,
    role TEXT,
    is_active BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT d.id, d.email, d.display_name, d.aws_sso_username, d.role, d.is_active
    FROM developer d
    ORDER BY d.id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION list_developers() IS 'Lists all registered developers and their database usernames';

-- =============================================================================
-- Verify function created
-- =============================================================================

\echo ''
\echo 'Functions created:'
\echo '  - register_developer(username, email, display_name, password, role)'
\echo '  - list_developers()'
\echo ''
\echo '============================================================================'
\echo 'USAGE EXAMPLES'
\echo '============================================================================'
\echo ''
\echo 'Register a new developer (run as postgres):'
\echo '  SELECT register_developer('
\echo '      ''jdoe'','
\echo '      ''jdoe@berkeley.edu'','
\echo '      ''Jane Doe'','
\echo '      ''secure_password_here'','
\echo '      ''developer'''
\echo '  );'
\echo ''
\echo 'List all developers:'
\echo '  SELECT * FROM list_developers();'
\echo ''
\echo 'Change a user password:'
\echo '  ALTER USER jdoe WITH PASSWORD ''new_password'';'
\echo ''
\echo '============================================================================'
