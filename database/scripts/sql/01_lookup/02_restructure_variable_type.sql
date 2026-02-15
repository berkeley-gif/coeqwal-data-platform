-- =============================================================================
-- 02_restructure_variable_type.sql
-- Restructure variable_type into calsim_variable_type and new variable_type
-- =============================================================================
-- Run in Cloud9: \i database/scripts/sql/01_lookup/02_restructure_variable_type.sql
-- =============================================================================
--
-- Changes:
--   1. Rename variable_type -> calsim_variable_type
--   2. Update values: output, state, decision (rename 'input' to 'state')
--   3. Create new variable_type table for colloquial/domain types
--   4. Populate new variable_type with: delivery, gw_pumping, PA, PR, PU, unknown
--   5. Update FK references in domain_family_map
--   6. Apply audit triggers to new table
-- =============================================================================

\echo '============================================================================'
\echo 'RESTRUCTURING VARIABLE_TYPE'
\echo '============================================================================'

-- =============================================================================
-- 1. RENAME variable_type TO calsim_variable_type
-- =============================================================================
\echo ''
\echo '1. Renaming variable_type to calsim_variable_type...'

-- Rename the table
ALTER TABLE variable_type RENAME TO calsim_variable_type;

-- Rename the sequence
ALTER SEQUENCE variable_type_id_seq RENAME TO calsim_variable_type_id_seq;

-- Rename the primary key constraint
ALTER TABLE calsim_variable_type RENAME CONSTRAINT variable_type_pkey TO calsim_variable_type_pkey;

-- Rename the unique constraint on short_code (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'variable_type_short_code_key') THEN
        ALTER TABLE calsim_variable_type RENAME CONSTRAINT variable_type_short_code_key TO calsim_variable_type_short_code_key;
    END IF;
END $$;

-- Rename FK constraints
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'variable_type_created_by_fkey') THEN
        ALTER TABLE calsim_variable_type RENAME CONSTRAINT variable_type_created_by_fkey TO calsim_variable_type_created_by_fkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'variable_type_updated_by_fkey') THEN
        ALTER TABLE calsim_variable_type RENAME CONSTRAINT variable_type_updated_by_fkey TO calsim_variable_type_updated_by_fkey;
    END IF;
END $$;

-- Rename the audit trigger
DROP TRIGGER IF EXISTS audit_fields_variable_type ON calsim_variable_type;
CREATE TRIGGER audit_fields_calsim_variable_type
    BEFORE INSERT OR UPDATE ON calsim_variable_type
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

\echo 'Table renamed to calsim_variable_type'

-- =============================================================================
-- 2. UPDATE calsim_variable_type VALUES
-- =============================================================================
\echo ''
\echo '2. Updating calsim_variable_type values...'

-- Rename 'input' to 'state'
UPDATE calsim_variable_type 
SET short_code = 'state', 
    label = 'State',
    description = 'State variable'
WHERE short_code = 'input';

-- Verify
SELECT id, short_code, label, description FROM calsim_variable_type ORDER BY id;

-- =============================================================================
-- 3. CREATE NEW variable_type TABLE
-- =============================================================================
\echo ''
\echo '3. Creating new variable_type table...'

CREATE TABLE IF NOT EXISTS variable_type (
    id SERIAL PRIMARY KEY,
    short_code TEXT UNIQUE NOT NULL,
    label TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL REFERENCES developer(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL REFERENCES developer(id)
);

COMMENT ON TABLE variable_type IS 'Colloquial/domain variable types for user-friendly classification';

\echo 'New variable_type table created'

-- =============================================================================
-- 4. POPULATE NEW variable_type
-- =============================================================================
\echo ''
\echo '4. Populating new variable_type...'

INSERT INTO variable_type (short_code, label, description, is_active, created_by, updated_by)
VALUES 
    ('delivery', 'delivery', 'water delivery', true, coeqwal_current_operator(), coeqwal_current_operator()),
    ('gw_pumping', 'groundwater pumping', 'Groundwater pumping', true, coeqwal_current_operator(), coeqwal_current_operator()),
    ('PA', 'Project agricultural', 'project agricultural water use', true, coeqwal_current_operator(), coeqwal_current_operator()),
    ('PR', 'Project wildlife refuge', 'project wildlife refuge water use', true, coeqwal_current_operator(), coeqwal_current_operator()),
    ('PU', 'Project community water system', 'Project community water system (M&I)', true, coeqwal_current_operator(), coeqwal_current_operator()),
    ('unknown', 'Unknown', 'Unknown or unclassified variable type', true, coeqwal_current_operator(), coeqwal_current_operator())
ON CONFLICT (short_code) DO NOTHING;

-- Verify
SELECT id, short_code, label, description FROM variable_type ORDER BY id;

-- =============================================================================
-- 5. UPDATE domain_family_map
-- =============================================================================
\echo ''
\echo '5. Updating domain_family_map...'

-- Update existing variable_type entry to calsim_variable_type
UPDATE domain_family_map 
SET table_name = 'calsim_variable_type',
    note = 'CalSim technical variable type (output, state, decision)',
    updated_at = NOW(),
    updated_by = coeqwal_current_operator()
WHERE table_name = 'variable_type';

-- Add new variable_type entry
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note)
VALUES ('public', 'variable_type', 6, 'Colloquial/domain variable type (delivery, gw_pumping, PA, PR, PU)')
ON CONFLICT (schema_name, table_name) DO UPDATE SET
    version_family_id = EXCLUDED.version_family_id,
    note = EXCLUDED.note,
    updated_at = NOW(),
    updated_by = coeqwal_current_operator();

-- Verify
SELECT table_name, vf.short_code as version_family, note
FROM domain_family_map dfm
JOIN version_family vf ON dfm.version_family_id = vf.id
WHERE table_name IN ('calsim_variable_type', 'variable_type')
ORDER BY table_name;

-- =============================================================================
-- 6. APPLY AUDIT TRIGGER TO NEW TABLE
-- =============================================================================
\echo ''
\echo '6. Applying audit trigger to new variable_type...'

SELECT apply_audit_trigger_to_table('variable_type');

-- Verify trigger exists
SELECT trigger_name, event_object_table 
FROM information_schema.triggers 
WHERE event_object_table IN ('calsim_variable_type', 'variable_type')
AND trigger_name LIKE 'audit_fields_%';

-- =============================================================================
-- 7. UPDATE REFERENCING TABLES (rename FK columns)
-- =============================================================================
\echo ''
\echo '7. Checking tables that reference variable_type...'

-- Find tables with variable_type_id FK
SELECT 
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS referenced_table
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu 
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
AND ccu.table_name = 'calsim_variable_type';

\echo ''
\echo 'NOTE: Tables referencing calsim_variable_type may need column renamed to calsim_variable_type_id'
\echo 'and a new variable_type_id column added for the colloquial type.'

-- =============================================================================
-- SUMMARY
-- =============================================================================
\echo ''
\echo '============================================================================'
\echo 'RESTRUCTURING COMPLETE'
\echo '============================================================================'
\echo ''
\echo 'Changes made:'
\echo '  - variable_type renamed to calsim_variable_type'
\echo '  - calsim_variable_type values: output, state, decision'
\echo '  - New variable_type table created'
\echo '  - variable_type values: delivery, gw_pumping, PA, PR, PU, unknown'
\echo '  - domain_family_map updated for both tables'
\echo '  - Audit triggers applied'
\echo ''
\echo 'Next steps:'
\echo '  1. Rename FK columns from variable_type_id to calsim_variable_type_id where appropriate'
\echo '  2. Add new variable_type_id columns where needed'
\echo '  3. Update ERD documentation'
\echo '============================================================================'
