-- CREATE HELPER FUNCTIONS FOR SSO INTEGRATION
-- Foundation functions for automatic developer ID and version ID population
-- To run first before any other scripts that use these functions, naturally.

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'CREATING COEQWAL HELPER FUNCTIONS'
\echo '============================================================================'

-- ============================================================================
-- FUNCTION: coeqwal_current_operator()
-- Returns the developer.id for the current database session user
-- ============================================================================

CREATE OR REPLACE FUNCTION coeqwal_current_operator()
RETURNS INTEGER AS $$
DECLARE
    dev_id INTEGER;
    current_db_user TEXT;
BEGIN
    -- Get current database user
    current_db_user := current_user;
    
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
    
    -- Strategy 4: If connected as 'postgres', find jfantauzza
    IF dev_id IS NULL AND current_db_user = 'postgres' THEN
        SELECT id INTO dev_id
        FROM developer 
        WHERE email LIKE '%jfantauzza%' OR email LIKE '%berkeley%'
        AND is_active = true
        LIMIT 1;
    END IF;
    
    -- Fallback: Use system user if nothing else works
    IF dev_id IS NULL THEN
        dev_id := 1; -- system@coeqwal.local
        RAISE WARNING 'Could not determine current operator, using system user (ID=1)';
    END IF;
    
    RETURN dev_id;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER
COMMENT ON FUNCTION coeqwal_current_operator() IS 'Returns developer.id for current session user with SSO integration';

-- ============================================================================
-- FUNCTION: get_active_version(version_family_short_code)
-- Returns the active version.id for a given version family
-- ============================================================================

CREATE OR REPLACE FUNCTION get_active_version(family_short_code TEXT)
RETURNS INTEGER AS $$
DECLARE
    version_id INTEGER;
BEGIN
    -- Find active version for the specified family
    SELECT v.id INTO version_id
    FROM version v
    JOIN version_family vf ON v.version_family_id = vf.id
    WHERE vf.short_code = family_short_code 
    AND v.is_active = true
    LIMIT 1;
    
    -- Raise warning if no active version found
    IF version_id IS NULL THEN
        RAISE WARNING 'No active version found for family: %', family_short_code;
    END IF;
    
    RETURN version_id;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER
COMMENT ON FUNCTION get_active_version(TEXT) IS 'Returns active version.id for specified version family';

-- ============================================================================
-- FUNCTION: get_source_id(source_short_code)
-- Returns source.id for a given source short_code
-- ============================================================================

CREATE OR REPLACE FUNCTION get_source_id(source_short_code TEXT)
RETURNS INTEGER AS $$
DECLARE
    src_id INTEGER;
BEGIN
    SELECT id INTO src_id
    FROM source 
    WHERE short_code = source_short_code
    AND is_active = true
    LIMIT 1;
    
    IF src_id IS NULL THEN
        RAISE WARNING 'Source not found: %', source_short_code;
    END IF;
    
    RETURN src_id;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER
COMMENT ON FUNCTION get_source_id(TEXT) IS 'Returns source.id for specified source short_code';

-- ============================================================================
-- TEST THE FUNCTIONS
-- ============================================================================

\echo ''
\echo 'Testing helper functions:'
\echo '------------------------'

-- Test current operator detection
\echo 'Current operator:'
SELECT 
    coeqwal_current_operator() as developer_id,
    d.email,
    d.display_name,
    current_user as database_user
FROM developer d 
WHERE d.id = coeqwal_current_operator();

-- Test version functions
\echo ''
\echo 'Active versions:'
SELECT 
    'network' as family,
    get_active_version('network') as version_id,
    v.version_number
FROM version v 
WHERE v.id = get_active_version('network')

UNION ALL

SELECT 
    'entity' as family,
    get_active_version('entity') as version_id,
    v.version_number
FROM version v 
WHERE v.id = get_active_version('entity')

UNION ALL

SELECT 
    'tier' as family,
    get_active_version('tier') as version_id,
    v.version_number
FROM version v 
WHERE v.id = get_active_version('tier');

-- Test source function
\echo ''
\echo 'Source lookup:'
SELECT 
    get_source_id('geopackage') as geopackage_id,
    get_source_id('xml_schematic') as xml_id,
    get_source_id('calsim_model_run') as calsim_id;

\echo ''
\echo 'HELPER FUNCTIONS CREATED SUCCESSFULLY'
\echo '============================================================================'
