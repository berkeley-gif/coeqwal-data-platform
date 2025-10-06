-- ============================================================================
-- REGISTER DEVELOPER (IF NEEDED)
-- ============================================================================
-- This script registers you as a developer if you're not already in the system
-- ============================================================================

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'REGISTERING DEVELOPER (IF NEEDED)'
\echo '============================================================================'

-- Check if you're already registered
\echo ''
\echo 'Checking existing developer registration:'

DO $$
DECLARE
    dev_count INTEGER;
    dev_id INTEGER;
BEGIN
    -- Check if developer exists
    SELECT COUNT(*), MAX(id) INTO dev_count, dev_id
    FROM developer 
    WHERE email LIKE '%jfantauzza%' OR email LIKE '%berkeley%';
    
    IF dev_count > 0 THEN
        RAISE NOTICE 'Developer already registered with ID: %', dev_id;
        
        -- Show existing registration
        PERFORM pg_sleep(0.1); -- Small delay for output ordering
        
    ELSE
        RAISE NOTICE 'No existing developer found. Registering new developer...';
        
        -- Insert new developer
        INSERT INTO developer (
            email,
            display_name,
            username,
            is_active,
            role,
            is_admin,
            aws_sso_username,
            created_at,
            created_by,
            updated_at,
            updated_by
        ) VALUES (
            'jfantauzza@berkeley.edu',
            'Jill Fantauzza',
            'jfantauzza',
            true,
            'admin',
            true,
            'jfantauzza',
            now(),
            1, -- system user creates the first admin
            now(),
            1
        )
        ON CONFLICT (email) DO NOTHING;
        
        RAISE NOTICE 'Developer registration complete.';
    END IF;
END $$;

\echo ''
\echo 'Current developer registration:'
SELECT 
    id,
    email,
    display_name,
    username,
    role,
    is_active,
    is_admin,
    created_at
FROM developer 
WHERE email LIKE '%jfantauzza%' OR email LIKE '%berkeley%'
ORDER BY id;

\echo ''
\echo 'Testing current operator function:'
SELECT 
    coeqwal_current_operator() as current_operator_id,
    d.email,
    d.display_name,
    d.role
FROM developer d 
WHERE d.id = coeqwal_current_operator();

\echo ''
\echo 'DEVELOPER REGISTRATION COMPLETE'
\echo '============================================================================'
