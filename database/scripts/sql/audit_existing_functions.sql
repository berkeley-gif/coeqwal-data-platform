-- AUDIT EXISTING FUNCTIONS IN DATABASE
-- Check what functions are in db

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'AUDITING EXISTING FUNCTIONS IN DATABASE'
\echo '============================================================================'

-- Check database connection
\echo ''
\echo '1. DATABASE CONNECTION:'
\echo '----------------------'
SELECT 
    current_database() as database_name,
    current_user as connected_as,
    current_schema() as current_schema;

-- Check for COEQWAL-specific functions
\echo ''
\echo '2. CHECKING FOR COEQWAL HELPER FUNCTIONS:'
\echo '----------------------------------------'

SELECT 
    routine_name as function_name,
    routine_type,
    data_type as return_type,
    routine_definition as function_body
FROM information_schema.routines 
WHERE routine_name LIKE '%coeqwal%' 
   OR routine_name LIKE '%get_active_version%'
   OR routine_name LIKE '%get_source_id%'
ORDER BY routine_name;

-- If no results, functions don't exist yet

-- Check for any custom functions (non-system)
\echo ''
\echo '3. ALL CUSTOM FUNCTIONS IN DATABASE:'
\echo '-----------------------------------'

SELECT 
    n.nspname as schema_name,
    p.proname as function_name,
    pg_get_function_result(p.oid) as return_type,
    pg_get_function_arguments(p.oid) as arguments,
    CASE 
        WHEN p.prosecdef THEN 'SECURITY DEFINER'
        ELSE 'SECURITY INVOKER'
    END as security_type
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
  AND p.prokind = 'f'  -- Functions only (not procedures)
ORDER BY n.nspname, p.proname;

-- Test if helper functions work (if they exist)
\echo ''
\echo '4. TESTING HELPER FUNCTIONS (IF THEY EXIST):'
\echo '--------------------------------------------'

-- Test coeqwal_current_operator (will fail if doesn't exist)
\echo 'Testing coeqwal_current_operator():'
DO $$
BEGIN
    BEGIN
        PERFORM coeqwal_current_operator();
        RAISE NOTICE 'coeqwal_current_operator() EXISTS and returns: %', coeqwal_current_operator();
        
        -- Show developer details
        PERFORM pg_sleep(0.1);
        RAISE NOTICE 'Developer details:';
        
    EXCEPTION WHEN undefined_function THEN
        RAISE NOTICE 'coeqwal_current_operator() does NOT exist - need to create it';
    END;
END $$;

-- Test get_active_version (will fail if doesn't exist)
\echo ''
\echo 'Testing get_active_version():'
DO $$
BEGIN
    BEGIN
        RAISE NOTICE 'get_active_version(''network'') returns: %', get_active_version('network');
        RAISE NOTICE 'get_active_version(''entity'') returns: %', get_active_version('entity');
        
    EXCEPTION WHEN undefined_function THEN
        RAISE NOTICE 'get_active_version() does NOT exist - need to create it';
    END;
END $$;

-- Show current developer registration
\echo ''
\echo '5. CURRENT DEVELOPER REGISTRATION:'
\echo '---------------------------------'

SELECT 
    id,
    email,
    display_name,
    role,
    aws_sso_username,
    is_active,
    last_login
FROM developer 
WHERE email LIKE '%jfantauzza%' OR email LIKE '%berkeley%'
ORDER BY id;

-- Show version information
\echo ''
\echo '6. ACTIVE VERSIONS:'
\echo '------------------'

SELECT 
    vf.short_code as family,
    v.id as version_id,
    v.version_number,
    v.is_active
FROM version v
JOIN version_family vf ON v.version_family_id = vf.id
WHERE v.is_active = true
ORDER BY vf.short_code;

\echo ''
\echo 'FUNCTION AUDIT COMPLETE'
\echo '============================================================================'
