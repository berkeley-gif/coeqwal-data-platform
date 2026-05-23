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

\echo 'Indexes created'

\echo ''
\echo 'tier_location_result table is now ready for the ETL loader.'
\echo 'Populate it by running:'
\echo '  python etl/tier_data/scripts/load_all_tier_results.py --output-sql all_tiers.sql'
\echo '  psql $DATABASE_URL -f etl/tier_data/output/all_tiers.sql'
\echo 'See etl/tier_data/README.md for the full workflow.'

