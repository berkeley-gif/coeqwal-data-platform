-- LOAD TIER TABLES FROM S3
-- Creates and populates tier_definition and tier_result tables
-- Includes audit metadata, versioning, and indexes

\echo ''
\echo '🎯 LOADING TIER TABLES FROM S3'
\echo '=============================='

\echo ''
\echo '📋 Getting tier version ID...'
SELECT id as tier_version_id, short_code, label 
FROM version 
WHERE short_code = 'tier';

\echo ''
\echo '📊 Creating tier_definition table...'

CREATE TABLE IF NOT EXISTS tier_definition (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR UNIQUE NOT NULL,
    name VARCHAR NOT NULL,
    description TEXT,
    tier_type VARCHAR NOT NULL CHECK (tier_type IN ('multi_value', 'single_value')),
    tier_count INTEGER NOT NULL CHECK (tier_count IN (1, 4)),
    tier_version_id INTEGER NOT NULL DEFAULT (SELECT id FROM version WHERE short_code = 'tier'),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'version') THEN
        ALTER TABLE tier_definition 
        ADD CONSTRAINT fk_tier_definition_version 
        FOREIGN KEY (tier_version_id) REFERENCES version(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'developer') THEN
        ALTER TABLE tier_definition 
        ADD CONSTRAINT fk_tier_definition_created_by 
        FOREIGN KEY (created_by) REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE tier_definition 
        ADD CONSTRAINT fk_tier_definition_updated_by 
        FOREIGN KEY (updated_by) REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_tier_definition_tier_type ON tier_definition(tier_type);
CREATE INDEX IF NOT EXISTS idx_tier_definition_version ON tier_definition(tier_version_id);
CREATE INDEX IF NOT EXISTS idx_tier_definition_active ON tier_definition(is_active);

\echo '✅ tier_definition table created with indexes'

\echo ''
\echo '📈 Creating tier_result table...'

CREATE TABLE IF NOT EXISTS tier_result (
    id SERIAL PRIMARY KEY,
    scenario_short_code VARCHAR NOT NULL,
    tier_short_code VARCHAR NOT NULL,
    
    tier_1_value INTEGER,
    tier_2_value INTEGER,
    tier_3_value INTEGER,
    tier_4_value INTEGER,
    
    norm_tier_1 NUMERIC(5,3),
    norm_tier_2 NUMERIC(5,3),
    norm_tier_3 NUMERIC(5,3),
    norm_tier_4 NUMERIC(5,3),
    
    total_value INTEGER,
    single_tier_level INTEGER,
    
    tier_version_id INTEGER NOT NULL DEFAULT (SELECT id FROM version WHERE short_code = 'tier'),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    
    UNIQUE(scenario_short_code, tier_short_code, tier_version_id),
    CHECK (
        (tier_1_value IS NOT NULL AND single_tier_level IS NULL) OR
        (tier_1_value IS NULL AND single_tier_level IS NOT NULL)
    ),
    CHECK (single_tier_level BETWEEN 1 AND 4 OR single_tier_level IS NULL)
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'tier_definition') THEN
        ALTER TABLE tier_result 
        ADD CONSTRAINT fk_tier_result_tier_definition 
        FOREIGN KEY (tier_short_code) REFERENCES tier_definition(short_code) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'version') THEN
        ALTER TABLE tier_result 
        ADD CONSTRAINT fk_tier_result_version 
        FOREIGN KEY (tier_version_id) REFERENCES version(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'developer') THEN
        ALTER TABLE tier_result 
        ADD CONSTRAINT fk_tier_result_created_by 
        FOREIGN KEY (created_by) REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
        
        ALTER TABLE tier_result 
        ADD CONSTRAINT fk_tier_result_updated_by 
        FOREIGN KEY (updated_by) REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_tier_result_scenario ON tier_result(scenario_short_code);
CREATE INDEX IF NOT EXISTS idx_tier_result_tier ON tier_result(tier_short_code);
CREATE INDEX IF NOT EXISTS idx_tier_result_scenario_tier ON tier_result(scenario_short_code, tier_short_code);
CREATE INDEX IF NOT EXISTS idx_tier_result_version ON tier_result(tier_version_id);
CREATE INDEX IF NOT EXISTS idx_tier_result_active ON tier_result(is_active);

\echo '✅ tier_result table created with indexes'

\echo ''
\echo '📥 Loading tier_definition from S3...'

\! aws s3 cp s3://coeqwal-seeds-dev/06_tier/tier_definition.csv /tmp/tier_definition.csv

\copy tier_definition (short_code, name, description, tier_type, tier_count, is_active) FROM '/tmp/tier_definition.csv' WITH CSV HEADER

\echo '✅ tier_definition loaded'

\echo ''
\echo '📈 Loading tier_result from S3...'

\! aws s3 cp s3://coeqwal-seeds-dev/06_tier/tier_result.csv /tmp/tier_result.csv

\copy tier_result (scenario_short_code, tier_short_code, tier_1_value, tier_2_value, tier_3_value, tier_4_value, norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4, total_value, single_tier_level) FROM '/tmp/tier_result.csv' WITH CSV HEADER

\echo '✅ tier_result loaded'

\echo ''
\echo '🔍 VERIFYING TIER DATA:'
\echo '======================'

\echo ''
\echo '📊 Tier definitions:'
SELECT short_code, name, tier_type, tier_count, is_active 
FROM tier_definition 
ORDER BY short_code;

\echo ''
\echo '📈 Tier results summary:'
SELECT 
    tier_short_code,
    COUNT(*) as scenario_count,
    COUNT(CASE WHEN tier_1_value IS NOT NULL THEN 1 END) as multi_value_count,
    COUNT(CASE WHEN single_tier_level IS NOT NULL THEN 1 END) as single_value_count
FROM tier_result 
GROUP BY tier_short_code 
ORDER BY tier_short_code;

\echo ''
\echo '🎯 Sample normalized values:'
SELECT 
    scenario_short_code,
    tier_short_code,
    ARRAY[tier_1_value, tier_2_value, tier_3_value, tier_4_value] as raw_values,
    ARRAY[norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4] as normalized_values,
    total_value,
    single_tier_level
FROM tier_result 
WHERE tier_short_code IN ('ENV_FLOWS', 'DELTA_ECOLOGY')
ORDER BY tier_short_code, scenario_short_code;

\! rm -f /tmp/tier_definition.csv /tmp/tier_result.csv

\echo ''
\echo '🎉 TIER TABLES LOADED SUCCESSFULLY!'
\echo '=================================='
\echo 'Ready for D3 visualization and tier reporting!'
