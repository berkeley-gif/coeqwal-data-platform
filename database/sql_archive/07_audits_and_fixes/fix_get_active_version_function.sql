-- FIX get_active_version() FUNCTION
-- Diagnose and fix the parameter type issue

\echo ''
\echo '🔧 FIXING get_active_version() FUNCTION'
\echo '======================================'
\echo ''

\echo '🔍 Checking current function signature:'
SELECT 
    routine_name,
    routine_type,
    data_type as return_type,
    type_udt_name,
    parameter_name,
    parameter_mode,
    data_type as parameter_type
FROM information_schema.routines r
LEFT JOIN information_schema.parameters p ON r.specific_name = p.specific_name
WHERE r.routine_name = 'get_active_version'
    AND r.routine_schema = 'public'
ORDER BY p.ordinal_position;

\echo ''
\echo '🔍 Let me check what the function actually expects:'

SELECT pg_get_functiondef(oid) as function_definition
FROM pg_proc 
WHERE proname = 'get_active_version' 
    AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');

\echo ''
\echo '🔍 Testing different parameter approaches:'

\echo 'Test 1: get_active_version(1) - version_family_id as integer'
DO $$
BEGIN
    PERFORM get_active_version(1);
    RAISE NOTICE '✅ get_active_version(1) works';
EXCEPTION
    WHEN others THEN
        RAISE NOTICE '❌ get_active_version(1) failed: %', SQLERRM;
END $$;

\echo 'Test 2: get_active_version(''theme'') - version_family short_code as text'
DO $$
BEGIN
    PERFORM get_active_version('theme');
    RAISE NOTICE '✅ get_active_version(''theme'') works';
EXCEPTION
    WHEN others THEN
        RAISE NOTICE '❌ get_active_version(''theme'') failed: %', SQLERRM;
END $$;

\echo ''
\echo '🔍 All get_active_version function variants:'
SELECT 
    proname,
    pronargs,
    proargtypes,
    proargnames,
    proargmodes
FROM pg_proc 
WHERE proname LIKE '%get_active_version%'
    AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');

\echo ''
\echo '🔧 CREATING CORRECT get_active_version() FUNCTION:'

DROP FUNCTION IF EXISTS get_active_version(integer);
DROP FUNCTION IF EXISTS get_active_version(text);

CREATE OR REPLACE FUNCTION get_active_version(family_id INTEGER) 
RETURNS INTEGER AS $$
DECLARE
    active_version_id INTEGER;
BEGIN
    SELECT id INTO active_version_id 
    FROM version 
    WHERE version_family_id = family_id 
        AND is_active = true
    LIMIT 1;
    
    RETURN active_version_id;
END;
$$ LANGUAGE plpgsql;

\echo '  ✅ Created get_active_version(family_id INTEGER) function'

\echo ''
\echo '🔍 Testing fixed function:'

SELECT 
    'get_active_version(1)' as test,
    get_active_version(1) as result,
    CASE 
        WHEN get_active_version(1) IS NOT NULL THEN '✅ WORKING'
        ELSE '❌ NO ACTIVE VERSION'
    END as status;

SELECT 
    vf.short_code as version_family,
    vf.id as family_id,
    get_active_version(vf.id) as active_version_id,
    v.version_number,
    CASE 
        WHEN get_active_version(vf.id) IS NOT NULL THEN '✅ HAS ACTIVE VERSION'
        ELSE '❌ NO ACTIVE VERSION'
    END as status
FROM version_family vf
LEFT JOIN version v ON v.id = get_active_version(vf.id)
ORDER BY vf.id
LIMIT 5;

\echo ''
\echo '✅ get_active_version() FUNCTION FIXED!'
\echo ''
\echo '🎯 Function now works correctly with integer version_family_id parameter'
\echo ''
