-- CREATE TIER_LOCATION_RESULT TABLE
-- ====================================
-- Stores tier assignments at specific geographic locations
-- Enables map-based tier visualization
-- Links to network nodes, WBAs, reservoirs, or compliance stations

\echo ''
\echo '📍 CREATING TIER_LOCATION_RESULT TABLE'
\echo '======================================='

-- Drop if exists
DROP TABLE IF EXISTS tier_location_result CASCADE;

-- Create table
\echo ''
\echo '🏗️  Creating tier_location_result table...'

CREATE TABLE tier_location_result (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR NOT NULL,
    tier_short_code VARCHAR NOT NULL,
    location_type VARCHAR NOT NULL,
    location_id VARCHAR NOT NULL,
    location_name VARCHAR,
    tier_level INTEGER,
    tier_value INTEGER,
    display_order INTEGER DEFAULT 1,
    tier_version_id INTEGER NOT NULL DEFAULT 8,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    
    -- Ensure unique locations per scenario/tier combination
    UNIQUE(scenario_short_code, tier_short_code, location_id, tier_version_id),
    
    -- Validate location_type values
    CHECK (location_type IN ('network_node', 'wba', 'reservoir', 'compliance_station', 'region', 'demand_unit')),
    
    -- Validate tier_level range
    CHECK (tier_level BETWEEN 1 AND 4 OR tier_level IS NULL)
);

\echo '✅ tier_location_result table created'

-- Add foreign keys
\echo '🔗 Adding foreign key constraints...'

ALTER TABLE tier_location_result 
ADD CONSTRAINT tier_location_result_tier_short_code_fkey 
FOREIGN KEY (tier_short_code) 
REFERENCES tier_definition(short_code) 
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE tier_location_result 
ADD CONSTRAINT tier_location_result_tier_version_id_fkey 
FOREIGN KEY (tier_version_id) 
REFERENCES version(id) 
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE tier_location_result 
ADD CONSTRAINT tier_location_result_created_by_fkey 
FOREIGN KEY (created_by) 
REFERENCES developer(id) 
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE tier_location_result 
ADD CONSTRAINT tier_location_result_updated_by_fkey 
FOREIGN KEY (updated_by) 
REFERENCES developer(id) 
ON DELETE RESTRICT ON UPDATE CASCADE;

\echo '✅ Foreign key constraints added'

-- Create indexes
\echo '📊 Creating indexes...'

CREATE UNIQUE INDEX idx_tier_location_unique 
    ON tier_location_result(scenario_short_code, tier_short_code, location_id, tier_version_id);

CREATE INDEX idx_tier_location_scenario 
    ON tier_location_result(scenario_short_code);

CREATE INDEX idx_tier_location_tier 
    ON tier_location_result(tier_short_code);

CREATE INDEX idx_tier_location_type 
    ON tier_location_result(location_type);

CREATE INDEX idx_tier_location_level 
    ON tier_location_result(tier_level);

CREATE INDEX idx_tier_location_combined 
    ON tier_location_result(scenario_short_code, tier_short_code);

\echo '✅ Indexes created'

-- Load data from S3
\echo ''
\echo '📥 Loading tier_location_result from S3...'

SELECT aws_s3.table_import_from_s3(
    'tier_location_result',
    'scenario_short_code, tier_short_code, location_type, location_id, location_name, tier_level, tier_value, display_order',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '10_tier/tier_location_result.csv',
    'us-west-2'
);

\echo '✅ tier_location_result data loaded'

-- Verification
\echo ''
\echo '🔍 VERIFYING TIER_LOCATION_RESULT DATA'
\echo '======================================'

\echo ''
\echo '📊 Total location records:'
SELECT COUNT(*) as total_locations FROM tier_location_result;

\echo ''
\echo '📊 By tier:'
SELECT 
    tier_short_code,
    COUNT(*) as location_count,
    COUNT(DISTINCT scenario_short_code) as scenario_count
FROM tier_location_result
GROUP BY tier_short_code
ORDER BY tier_short_code;

\echo ''
\echo '📊 By location type:'
SELECT 
    location_type,
    COUNT(*) as count
FROM tier_location_result
GROUP BY location_type
ORDER BY count DESC;

\echo ''
\echo '📊 ENV_FLOWS example (17 locations expected):'
SELECT 
    scenario_short_code,
    location_id,
    location_name,
    tier_level
FROM tier_location_result
WHERE tier_short_code = 'ENV_FLOWS'
  AND scenario_short_code = 's0011'
ORDER BY display_order;

\echo ''
\echo '📊 RES_STOR example (7 reservoirs expected):'
SELECT 
    scenario_short_code,
    location_id,
    location_name,
    tier_level
FROM tier_location_result
WHERE tier_short_code = 'RES_STOR'
  AND scenario_short_code = 's0011'
ORDER BY display_order;

\echo ''
\echo '📊 Validation - Compare with tier_result aggregates:'
SELECT 
    tlr.scenario_short_code,
    tlr.tier_short_code,
    COUNT(*) FILTER (WHERE tlr.tier_level = 1) as tier_1_locations,
    tr.tier_1_value as tier_1_aggregate,
    COUNT(*) FILTER (WHERE tlr.tier_level = 2) as tier_2_locations,
    tr.tier_2_value as tier_2_aggregate,
    COUNT(*) FILTER (WHERE tlr.tier_level = 3) as tier_3_locations,
    tr.tier_3_value as tier_3_aggregate,
    COUNT(*) FILTER (WHERE tlr.tier_level = 4) as tier_4_locations,
    tr.tier_4_value as tier_4_aggregate,
    COUNT(*) as total_locations,
    tr.total_value as total_aggregate
FROM tier_location_result tlr
LEFT JOIN tier_result tr 
    ON tlr.scenario_short_code = tr.scenario_short_code 
    AND tlr.tier_short_code = tr.tier_short_code
WHERE tr.tier_1_value IS NOT NULL  -- Multi-value tiers only
GROUP BY tlr.scenario_short_code, tlr.tier_short_code, 
         tr.tier_1_value, tr.tier_2_value, tr.tier_3_value, tr.tier_4_value, tr.total_value
ORDER BY tlr.scenario_short_code, tlr.tier_short_code;

\echo ''
\echo '🎉 TIER_LOCATION_RESULT TABLE SUCCESSFULLY CREATED!'
\echo '==================================================='
\echo 'Ready for tier map visualization API integration!'

