-- =============================================================================
-- 02_add_missing_audit_columns.sql
-- Adds missing audit columns to tables that need them
-- =============================================================================
-- Run in Cloud9: psql -f 02_add_missing_audit_columns.sql
-- =============================================================================

-- Tables identified as missing audit columns:
-- 1. developer (has created_at, updated_at; missing created_by, updated_by)
-- 2. domain_family_map (missing all 4)
-- 3. geometry_type (missing all 4)
-- 4. variable_type (missing all 4)
-- 5. theme_scenario_link (missing all 4)
-- 6. scenario_key_assumption_link (missing all 4)
-- 7. scenario_key_operation_link (missing all 4)
-- Note: spatial_ref_sys is a PostGIS system table - do not modify

-- =============================================================================
-- 1. developer table
-- =============================================================================
ALTER TABLE developer 
    ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES developer(id),
    ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES developer(id);

-- Set default for existing records (bootstrap user created by system)
UPDATE developer 
SET created_by = 1, updated_by = 1 
WHERE created_by IS NULL;

-- =============================================================================
-- 2. domain_family_map table
-- =============================================================================
ALTER TABLE domain_family_map 
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES developer(id) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES developer(id) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- Set timestamps for existing records
UPDATE domain_family_map 
SET created_at = NOW(), updated_at = NOW(), created_by = 1, updated_by = 1
WHERE created_at IS NULL;

-- =============================================================================
-- 3. geometry_type table
-- =============================================================================
ALTER TABLE geometry_type 
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES developer(id) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES developer(id) DEFAULT 1;

UPDATE geometry_type 
SET created_at = NOW(), updated_at = NOW(), created_by = 1, updated_by = 1
WHERE created_at IS NULL;

-- =============================================================================
-- 4. variable_type table
-- =============================================================================
ALTER TABLE variable_type 
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES developer(id) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES developer(id) DEFAULT 1;

UPDATE variable_type 
SET created_at = NOW(), updated_at = NOW(), created_by = 1, updated_by = 1
WHERE created_at IS NULL;

-- =============================================================================
-- 5. theme_scenario_link table
-- =============================================================================
ALTER TABLE theme_scenario_link 
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES developer(id) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES developer(id) DEFAULT 1;

UPDATE theme_scenario_link 
SET created_at = NOW(), updated_at = NOW(), created_by = 1, updated_by = 1
WHERE created_at IS NULL;

-- =============================================================================
-- 6. scenario_key_assumption_link table
-- =============================================================================
ALTER TABLE scenario_key_assumption_link 
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES developer(id) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES developer(id) DEFAULT 1;

UPDATE scenario_key_assumption_link 
SET created_at = NOW(), updated_at = NOW(), created_by = 1, updated_by = 1
WHERE created_at IS NULL;

-- =============================================================================
-- 7. scenario_key_operation_link table
-- =============================================================================
ALTER TABLE scenario_key_operation_link 
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES developer(id) DEFAULT 1,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES developer(id) DEFAULT 1;

UPDATE scenario_key_operation_link 
SET created_at = NOW(), updated_at = NOW(), created_by = 1, updated_by = 1
WHERE created_at IS NULL;

-- =============================================================================
-- Verify changes
-- =============================================================================
SELECT 'Audit columns added successfully' AS status;

-- Show tables that now have all audit columns
SELECT 
    t.table_name,
    MAX(CASE WHEN c.column_name = 'created_at' THEN 'YES' ELSE 'NO' END) as has_created_at,
    MAX(CASE WHEN c.column_name = 'created_by' THEN 'YES' ELSE 'NO' END) as has_created_by,
    MAX(CASE WHEN c.column_name = 'updated_at' THEN 'YES' ELSE 'NO' END) as has_updated_at,
    MAX(CASE WHEN c.column_name = 'updated_by' THEN 'YES' ELSE 'NO' END) as has_updated_by
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_name = c.table_name AND t.table_schema = c.table_schema
WHERE t.table_schema = 'public' 
AND t.table_type = 'BASE TABLE'
AND t.table_name IN ('developer', 'domain_family_map', 'geometry_type', 'variable_type', 
                     'theme_scenario_link', 'scenario_key_assumption_link', 'scenario_key_operation_link')
GROUP BY t.table_name
ORDER BY t.table_name;
