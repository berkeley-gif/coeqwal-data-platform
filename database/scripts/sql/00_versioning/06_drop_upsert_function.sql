-- =============================================================================
-- 06_drop_upsert_function.sql
-- Removes the inconsistent upsert_reservoir_percentile function
-- =============================================================================
-- Run in Cloud9: psql -f 06_drop_upsert_function.sql
-- =============================================================================
-- 
-- RATIONALE: This function is inconsistent with the rest of the codebase.
-- We use direct INSERT...ON CONFLICT statements instead of wrapper functions.
-- The function logic will be moved inline into the loading script.
-- =============================================================================

-- Check if function exists and show its definition
\echo 'Current function (if exists):'
SELECT 
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as arguments,
    obj_description(p.oid, 'pg_proc') as description
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE p.proname = 'upsert_reservoir_percentile'
AND n.nspname = 'public';

-- Drop the function
DROP FUNCTION IF EXISTS upsert_reservoir_percentile(
    character varying, integer, integer, 
    numeric, numeric, numeric, numeric, numeric, numeric, numeric, 
    numeric, integer
);

-- Also try without the default parameter
DROP FUNCTION IF EXISTS upsert_reservoir_percentile(
    character varying, integer, integer, 
    numeric, numeric, numeric, numeric, numeric, numeric, numeric, 
    numeric
);

-- Verify it's gone
\echo ''
\echo 'After drop (should be empty):'
SELECT proname FROM pg_proc WHERE proname = 'upsert_reservoir_percentile';

\echo ''
\echo 'SUCCESS: upsert_reservoir_percentile function removed';
\echo '';
\echo 'NOTE: Use direct INSERT...ON CONFLICT in your scripts instead:';
\echo '';
\echo 'INSERT INTO reservoir_monthly_percentile (...) VALUES (...)';
\echo 'ON CONFLICT (scenario_short_code, reservoir_entity_id, water_month)';
\echo 'DO UPDATE SET ...';
