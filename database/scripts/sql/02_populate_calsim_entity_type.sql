-- Populate calsim_entity_type table

\set ON_ERROR_STOP on

\echo '============================================================================'
\echo 'POPULATING CALSIM_ENTITY_TYPE TABLE'
\echo '============================================================================'

-- Check current state
\echo ''
\echo '1. CURRENT STATE:'
\echo '----------------'
SELECT 
    'calsim_entity_type' as table_name,
    COUNT(*) as current_records
FROM calsim_entity_type;

-- Add missing audit fields if they don't exist
\echo ''
\echo '2. ADDING MISSING AUDIT FIELDS:'
\echo '------------------------------'

-- Add created_by if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'calsim_entity_type' AND column_name = 'created_by'
    ) THEN
        ALTER TABLE calsim_entity_type ADD COLUMN created_by INTEGER;
        RAISE NOTICE 'Added created_by column';
    ELSE
        RAISE NOTICE 'created_by column already exists';
    END IF;
END $$;

-- Add created_at if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'calsim_entity_type' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE calsim_entity_type ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW();
        RAISE NOTICE 'Added created_at column';
    ELSE
        RAISE NOTICE 'created_at column already exists';
    END IF;
END $$;

-- Add updated_by if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'calsim_entity_type' AND column_name = 'updated_by'
    ) THEN
        ALTER TABLE calsim_entity_type ADD COLUMN updated_by INTEGER;
        RAISE NOTICE 'Added updated_by column';
    ELSE
        RAISE NOTICE 'updated_by column already exists';
    END IF;
END $$;

-- Add updated_at if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'calsim_entity_type' AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE calsim_entity_type ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();
        RAISE NOTICE 'Added updated_at column';
    ELSE
        RAISE NOTICE 'updated_at column already exists';
    END IF;
END $$;

-- Add network_entity_type_id column to link to new architecture
\echo ''
\echo '3. ADDING NETWORK_ENTITY_TYPE_ID COLUMN:'
\echo '---------------------------------------'

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'calsim_entity_type' AND column_name = 'network_entity_type_id'
    ) THEN
        ALTER TABLE calsim_entity_type ADD COLUMN network_entity_type_id INTEGER;
        RAISE NOTICE 'Added network_entity_type_id column';
    ELSE
        RAISE NOTICE 'network_entity_type_id column already exists';
    END IF;
END $$;

-- Populate the table with data from seed file structure
\echo ''
\echo '4. POPULATING WITH ENTITY TYPE DATA:'
\echo '-----------------------------------'

-- Insert all entity types
INSERT INTO calsim_entity_type (type, network_entity_type_id, description, key_dynamic, is_active, created_by, updated_by) VALUES
('reservoir', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Reservoir or lake for water storage', 'storage levels', true, coeqwal_current_operator(), coeqwal_current_operator()),
('channel', (SELECT id FROM network_entity_type WHERE short_code = 'arc'), 'River channel or canal for water conveyance', 'flow amounts and rates', true, coeqwal_current_operator(), coeqwal_current_operator()),
('inflow', (SELECT id FROM network_entity_type WHERE short_code = 'arc'), 'Rim flow to reservoirs', 'inflow amounts and rates', true, coeqwal_current_operator(), coeqwal_current_operator()),
('demand_unit_agriculture', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Agriculture water delivery district', 'delivery patterns', true, coeqwal_current_operator(), coeqwal_current_operator()),
('demand_unit_urban', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Community water delivery district', 'delivery patterns', true, coeqwal_current_operator(), coeqwal_current_operator()),
('demand_unit_refuge', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Federal wetlands water delivery district', 'delivery patterns', true, coeqwal_current_operator(), coeqwal_current_operator()),
('groundwater', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Groundwater aquifer', 'groundwater volume levels', true, coeqwal_current_operator(), coeqwal_current_operator()),
('salinity_node', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Delta salinity node', 'salinity', true, coeqwal_current_operator(), coeqwal_current_operator()),
('delta_outflow', (SELECT id FROM network_entity_type WHERE short_code = 'arc'), 'Delta outflow', 'Delta outflow', true, coeqwal_current_operator(), coeqwal_current_operator()),
('delta_export', (SELECT id FROM network_entity_type WHERE short_code = 'arc'), 'Delta export', 'Delta export divided between CVP and SWP', true, coeqwal_current_operator(), coeqwal_current_operator()),
('infrastructure', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Infrastructure node', 'infrastructure', true, coeqwal_current_operator(), coeqwal_current_operator()),
('junction', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Junction node', 'junction', true, coeqwal_current_operator(), coeqwal_current_operator()),
('flow_management', (SELECT id FROM network_entity_type WHERE short_code = 'node'), 'Flow management node', 'flow management', true, coeqwal_current_operator(), coeqwal_current_operator())
ON CONFLICT (type) DO NOTHING;

-- Add foreign key constraints
\echo ''
\echo '5. ADDING FOREIGN KEY CONSTRAINTS:'
\echo '---------------------------------'

-- Add FK to network_entity_type
ALTER TABLE calsim_entity_type ADD CONSTRAINT fk_calsim_entity_type_network_entity_type
    FOREIGN KEY (network_entity_type_id) REFERENCES network_entity_type(id) ON DELETE RESTRICT ON UPDATE CASCADE;

-- Add FK to developer for audit fields
ALTER TABLE calsim_entity_type ADD CONSTRAINT fk_calsim_entity_type_created_by
    FOREIGN KEY (created_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;
    
ALTER TABLE calsim_entity_type ADD CONSTRAINT fk_calsim_entity_type_updated_by
    FOREIGN KEY (updated_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;

-- Verify the results
\echo ''
\echo '6. VERIFICATION:'
\echo '---------------'

SELECT 
    cet.id,
    cet.type,
    net.short_code as maps_to_network_type,
    net.label as network_type_label,
    cet.description,
    cet.is_active,
    (SELECT display_name FROM developer WHERE id = cet.created_by) as created_by_name
FROM calsim_entity_type cet
LEFT JOIN network_entity_type net ON cet.network_entity_type_id = net.id
ORDER BY cet.id;

-- Show count
SELECT COUNT(*) as total_entity_types FROM calsim_entity_type;

\echo ''
\echo 'CALSIM_ENTITY_TYPE TABLE POPULATED SUCCESSFULLY'
\echo '============================================================================'
