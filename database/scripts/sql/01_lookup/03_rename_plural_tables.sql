-- =============================================================================
-- 03_rename_plural_tables.sql
-- Rename plural table names to singular for consistency
-- =============================================================================
-- Run in Cloud9: \i database/scripts/sql/01_lookup/03_rename_plural_tables.sql
-- =============================================================================
--
-- Tables to rename:
--   reservoirs -> reservoir
--   compliance_stations -> compliance_station
-- =============================================================================

\echo '============================================================================'
\echo 'RENAMING PLURAL TABLES TO SINGULAR'
\echo '============================================================================'

-- =============================================================================
-- 1. CHECK FK DEPENDENCIES
-- =============================================================================
\echo ''
\echo '1. Checking FK dependencies...'

SELECT 
    tc.table_name as referencing_table,
    kcu.column_name,
    ccu.table_name as referenced_table,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu 
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
AND ccu.table_name IN ('compliance_stations', 'reservoirs')
ORDER BY ccu.table_name, tc.table_name;

-- =============================================================================
-- 2. RENAME reservoirs -> reservoir
-- =============================================================================
\echo ''
\echo '2. Renaming reservoirs to reservoir...'

-- Rename the table
ALTER TABLE IF EXISTS reservoirs RENAME TO reservoir;

-- Rename the sequence (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'reservoirs_id_seq') THEN
        ALTER SEQUENCE reservoirs_id_seq RENAME TO reservoir_id_seq;
    END IF;
END $$;

-- Rename primary key constraint
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'reservoirs_pkey') THEN
        ALTER TABLE reservoir RENAME CONSTRAINT reservoirs_pkey TO reservoir_pkey;
    END IF;
END $$;

-- Rename audit trigger
DROP TRIGGER IF EXISTS audit_fields_reservoirs ON reservoir;
CREATE TRIGGER audit_fields_reservoir
    BEFORE INSERT OR UPDATE ON reservoir
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

-- Update domain_family_map
UPDATE domain_family_map 
SET table_name = 'reservoir',
    updated_at = NOW(),
    updated_by = coeqwal_current_operator()
WHERE table_name = 'reservoirs';

\echo 'reservoirs renamed to reservoir'

-- =============================================================================
-- 3. RENAME compliance_stations -> compliance_station
-- =============================================================================
\echo ''
\echo '3. Renaming compliance_stations to compliance_station...'

-- Rename the table
ALTER TABLE IF EXISTS compliance_stations RENAME TO compliance_station;

-- Rename the sequence (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'compliance_stations_id_seq') THEN
        ALTER SEQUENCE compliance_stations_id_seq RENAME TO compliance_station_id_seq;
    END IF;
END $$;

-- Rename primary key constraint
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'compliance_stations_pkey') THEN
        ALTER TABLE compliance_station RENAME CONSTRAINT compliance_stations_pkey TO compliance_station_pkey;
    END IF;
END $$;

-- Rename audit trigger
DROP TRIGGER IF EXISTS audit_fields_compliance_stations ON compliance_station;
CREATE TRIGGER audit_fields_compliance_station
    BEFORE INSERT OR UPDATE ON compliance_station
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

-- Update domain_family_map
UPDATE domain_family_map 
SET table_name = 'compliance_station',
    updated_at = NOW(),
    updated_by = coeqwal_current_operator()
WHERE table_name = 'compliance_stations';

\echo 'compliance_stations renamed to compliance_station'

-- =============================================================================
-- 4. VERIFY
-- =============================================================================
\echo ''
\echo '4. Verifying renames...'

-- Check tables exist with new names
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('reservoir', 'compliance_station', 'reservoirs', 'compliance_stations')
ORDER BY table_name;

-- Check triggers
SELECT trigger_name, event_object_table 
FROM information_schema.triggers 
WHERE event_object_table IN ('reservoir', 'compliance_station')
AND trigger_name LIKE 'audit_fields_%';

-- Check domain_family_map
SELECT table_name, note 
FROM domain_family_map 
WHERE table_name IN ('reservoir', 'compliance_station', 'reservoirs', 'compliance_stations');

-- =============================================================================
-- SUMMARY
-- =============================================================================
\echo ''
\echo '============================================================================'
\echo 'RENAME COMPLETE'
\echo '============================================================================'
\echo ''
\echo 'Tables renamed:'
\echo '  - reservoirs -> reservoir'
\echo '  - compliance_stations -> compliance_station'
\echo ''
\echo 'Updated:'
\echo '  - Primary key constraints'
\echo '  - Sequences (if existed)'
\echo '  - Audit triggers'
\echo '  - domain_family_map entries'
\echo '============================================================================'
