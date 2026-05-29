-- COMPREHENSIVE AUDIT: 00_VERSIONING & 01_LOOKUP CONSTRAINTS AND INDEXES
-- Compare ERD documentation with actual database constraints and indexes

\echo ''
\echo 'COMPREHENSIVE 00_VERSIONING & 01_LOOKUP CONSTRAINTS AND INDEXES AUDIT'
\echo '===================================================================='
\echo ''

-- ============================================================================
-- 1. CHECK ALL CONSTRAINTS ON 00_VERSIONING TABLES
-- ============================================================================

\echo '1. CONSTRAINTS ON 00_VERSIONING TABLES'
\echo '======================================'

\echo ''
\echo 'version_family constraints:'
SELECT 
    constraint_name,
    constraint_type,
    column_name,
    is_deferrable,
    initially_deferred
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'version_family'
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
ORDER BY constraint_type, constraint_name;

\echo ''
\echo 'version constraints:'
SELECT 
    constraint_name,
    constraint_type,
    column_name,
    is_deferrable,
    initially_deferred
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'version'
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
ORDER BY constraint_type, constraint_name;

\echo ''
\echo 'developer constraints:'
SELECT 
    constraint_name,
    constraint_type,
    column_name,
    is_deferrable,
    initially_deferred
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'developer'
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
ORDER BY constraint_type, constraint_name;

\echo ''
\echo 'domain_family_map constraints:'
SELECT 
    constraint_name,
    constraint_type,
    column_name,
    is_deferrable,
    initially_deferred
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'domain_family_map'
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
ORDER BY constraint_type, constraint_name;

-- ============================================================================
-- 2. CHECK ALL CONSTRAINTS ON 01_LOOKUP TABLES
-- ============================================================================

\echo ''
\echo '2. CONSTRAINTS ON 01_LOOKUP TABLES'
\echo '=================================='

\echo ''
\echo 'hydrologic_region constraints:'
SELECT 
    constraint_name,
    constraint_type,
    column_name
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'hydrologic_region'
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
ORDER BY constraint_type, constraint_name;

\echo ''
\echo 'source constraints:'
SELECT 
    constraint_name,
    constraint_type,
    column_name
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'source'
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
ORDER BY constraint_type, constraint_name;

\echo ''
\echo 'model_source constraints:'
SELECT 
    constraint_name,
    constraint_type,
    column_name
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'model_source'
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
ORDER BY constraint_type, constraint_name;

\echo ''
\echo 'Other lookup tables constraints summary:'
SELECT 
    tc.table_name,
    tc.constraint_type,
    COUNT(*) as constraint_count
FROM information_schema.table_constraints tc
WHERE tc.table_name IN ('geometry_type', 'spatial_scale', 'temporal_scale', 'statistic_type', 'unit')
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY')
GROUP BY tc.table_name, tc.constraint_type
ORDER BY tc.table_name, tc.constraint_type;

-- ============================================================================
-- 3. COMPARE ERD DOCUMENTATION WITH DATABASE REALITY
-- ============================================================================

\echo ''
\echo '3. ERD vs DATABASE CONSTRAINT COMPARISON'
\echo '======================================='

\echo ''
\echo 'ERD CONSTRAINT DOCUMENTATION ANALYSIS:'
\echo ''
\echo 'Tables that show constraints in ERD:'
\echo '• version: UNIQUE(version_family_id, version_number) + business rule'
\echo '• network (future): UNIQUE(short_code) + CHECK constraints'
\echo '• network_gis (future): CHECK precision_level + CASCADE DELETE'
\echo '• Future tables: Various constraints documented'
\echo ''
\echo 'Tables that do NOT show constraints in ERD:'
\echo '• version_family: No constraints documented'
\echo '• developer: No constraints documented'
\echo '• domain_family_map: No constraints documented'
\echo '• Most 01_lookup tables: No constraints documented'
\echo ''

\echo 'INCONSISTENCY IDENTIFIED:'
\echo '• Some implemented tables show constraints, others do not'
\echo '• Future tables show detailed constraints'
\echo '• No clear pattern for when to document constraints'
\echo ''

\echo 'RECOMMENDATION:'
\echo '• Either document ALL constraints consistently'
\echo '• OR document NO constraints (focus on columns only)'
\echo '• Current mixed approach is confusing'
\echo ''
