-- =============================================================================
-- 04_update_coeqwal_operator.sql
-- Updates coeqwal_current_operator() to use proper SSO detection
-- =============================================================================
-- Run in Cloud9: psql -f 04_update_coeqwal_operator.sql
-- =============================================================================

-- First, check the current function definition
\echo 'Current function definition:'
SELECT prosrc FROM pg_proc WHERE proname = 'coeqwal_current_operator';

-- Check if developer table has aws_sso_username column
\echo ''
\echo 'Checking developer table columns:'
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'developer' 
ORDER BY ordinal_position;

-- Add aws_sso_username column if it doesn't exist
ALTER TABLE developer 
    ADD COLUMN IF NOT EXISTS aws_sso_username TEXT;

-- Add name column if it doesn't exist (used in Strategy 3)
ALTER TABLE developer 
    ADD COLUMN IF NOT EXISTS name TEXT;


-- Update the function to use proper SSO detection
CREATE OR REPLACE FUNCTION coeqwal_current_operator()
RETURNS INTEGER AS $$
DECLARE
    dev_id INTEGER;
    current_db_user TEXT;
BEGIN
    -- Get current database user
    current_db_user := current_user;
    
    -- Special case: postgres superuser maps to system account
    -- This allows administrative operations while maintaining audit trail
    IF current_db_user = 'postgres' THEN
        RETURN 1;  -- system@coeqwal.local (id=1)
    END IF;
    
    -- Try multiple strategies to find the developer
    
    -- Strategy 1: Match by AWS SSO username
    SELECT id INTO dev_id
    FROM developer 
    WHERE aws_sso_username = current_db_user
    AND is_active = true
    LIMIT 1;
    
    -- Strategy 2: Match by email containing database username
    IF dev_id IS NULL THEN
        SELECT id INTO dev_id
        FROM developer 
        WHERE email LIKE '%' || current_db_user || '%'
        AND is_active = true
        LIMIT 1;
    END IF;
    
    -- Strategy 3: Match by name containing database username
    IF dev_id IS NULL THEN
        SELECT id INTO dev_id
        FROM developer 
        WHERE LOWER(name) LIKE '%' || LOWER(current_db_user) || '%'
        AND is_active = true
        LIMIT 1;
    END IF;
    
    -- Strategy 4: Match by display_name containing database username
    IF dev_id IS NULL THEN
        SELECT id INTO dev_id
        FROM developer 
        WHERE LOWER(display_name) LIKE '%' || LOWER(current_db_user) || '%'
        AND is_active = true
        LIMIT 1;
    END IF;
    
    -- STRICT MODE: Fail if we can't identify the operator
    -- This prevents unverified users from making database changes
    IF dev_id IS NULL THEN
        RAISE EXCEPTION 'UNAUTHORIZED: Cannot identify current operator (db_user: %). You must be registered in the developer table with a matching aws_sso_username or email before making database changes.', current_db_user;
    END IF;
    
    RETURN dev_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

COMMENT ON FUNCTION coeqwal_current_operator() IS 
'Returns developer.id for current session user. STRICT MODE: Fails if user cannot be identified.

Special cases:
- postgres superuser -> returns 1 (system@coeqwal.local)

Detection strategies (in order):
1. Match aws_sso_username column
2. Match email containing database username
3. Match name containing database username
4. Match display_name containing database username
5. RAISE EXCEPTION if no match found

To be authorized for database changes:
- Use postgres superuser (maps to system account), OR
- Register in developer table with aws_sso_username = your db username, OR
- Register with email containing your db username';

-- =============================================================================
-- Verify the update
-- =============================================================================
\echo ''
\echo 'Updated function definition:'
SELECT prosrc FROM pg_proc WHERE proname = 'coeqwal_current_operator';

\echo ''
\echo 'Testing current operator detection:'
SELECT 
    coeqwal_current_operator() as developer_id,
    d.email,
    d.display_name,
    current_user as database_user
FROM developer d 
WHERE d.id = coeqwal_current_operator();
