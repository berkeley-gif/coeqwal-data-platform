-- MIGRATION: Convert reservoir_code (S_SHSTA) to reservoir_entity_id
-- This script converts statistics tables from variable-centric to entity-centric storage
--
-- Prerequisites:
--   1. reservoir_entity table must exist and be populated (92 reservoirs)
--   2. Statistics tables must have data with S_* prefixed reservoir_code values
--
-- Run with: psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f 07_migrate_to_entity_ids.sql

\echo ''
\echo '========================================='
\echo 'MIGRATING STATISTICS TABLES TO ENTITY IDs'
\echo '========================================='
\echo ''
\echo 'This migration converts reservoir_code (S_SHSTA) to reservoir_entity_id'
\echo ''

-- ============================================
-- VERIFICATION: Check reservoir_entity exists
-- ============================================
\echo 'Verifying reservoir_entity table...'
SELECT COUNT(*) as reservoir_count FROM reservoir_entity;

-- ============================================
-- 1. MIGRATE reservoir_storage_monthly
-- ============================================
\echo ''
\echo '1. Migrating reservoir_storage_monthly...'

-- Add new column
ALTER TABLE reservoir_storage_monthly ADD COLUMN IF NOT EXISTS reservoir_entity_id INTEGER;

-- Populate from reservoir_entity lookup (strip S_ prefix)
UPDATE reservoir_storage_monthly rsm
SET reservoir_entity_id = re.id
FROM reservoir_entity re
WHERE re.short_code = SUBSTRING(rsm.reservoir_code, 3);

-- Verify no nulls
\echo 'Checking for unmapped records...'
SELECT COUNT(*) as missing_mappings
FROM reservoir_storage_monthly
WHERE reservoir_entity_id IS NULL;

-- Make NOT NULL
ALTER TABLE reservoir_storage_monthly
ALTER COLUMN reservoir_entity_id SET NOT NULL;

-- Add FK constraint
ALTER TABLE reservoir_storage_monthly
ADD CONSTRAINT fk_storage_monthly_reservoir_entity
FOREIGN KEY (reservoir_entity_id) REFERENCES reservoir_entity(id)
ON DELETE RESTRICT ON UPDATE CASCADE;

-- Drop old unique constraint (if exists)
ALTER TABLE reservoir_storage_monthly
DROP CONSTRAINT IF EXISTS uq_storage_monthly;

-- Add new unique constraint
ALTER TABLE reservoir_storage_monthly
ADD CONSTRAINT uq_storage_monthly
UNIQUE(scenario_short_code, reservoir_entity_id, water_month);

-- Drop old indexes
DROP INDEX IF EXISTS idx_storage_monthly_reservoir;
DROP INDEX IF EXISTS idx_storage_monthly_combined;

-- Create new indexes
CREATE INDEX IF NOT EXISTS idx_storage_monthly_entity
ON reservoir_storage_monthly(reservoir_entity_id);
CREATE INDEX IF NOT EXISTS idx_storage_monthly_combined
ON reservoir_storage_monthly(scenario_short_code, reservoir_entity_id);

-- Drop old column
ALTER TABLE reservoir_storage_monthly DROP COLUMN IF EXISTS reservoir_code;

\echo '  reservoir_storage_monthly migrated'

-- ============================================
-- 2. MIGRATE reservoir_spill_monthly
-- ============================================
\echo ''
\echo '2. Migrating reservoir_spill_monthly...'

-- Add new column
ALTER TABLE reservoir_spill_monthly ADD COLUMN IF NOT EXISTS reservoir_entity_id INTEGER;

-- Populate from reservoir_entity lookup (strip S_ prefix)
UPDATE reservoir_spill_monthly rsm
SET reservoir_entity_id = re.id
FROM reservoir_entity re
WHERE re.short_code = SUBSTRING(rsm.reservoir_code, 3);

-- Verify no nulls
\echo 'Checking for unmapped records...'
SELECT COUNT(*) as missing_mappings
FROM reservoir_spill_monthly
WHERE reservoir_entity_id IS NULL;

-- Make NOT NULL
ALTER TABLE reservoir_spill_monthly
ALTER COLUMN reservoir_entity_id SET NOT NULL;

-- Add FK constraint
ALTER TABLE reservoir_spill_monthly
ADD CONSTRAINT fk_spill_monthly_reservoir_entity
FOREIGN KEY (reservoir_entity_id) REFERENCES reservoir_entity(id)
ON DELETE RESTRICT ON UPDATE CASCADE;

-- Drop old unique constraint (if exists)
ALTER TABLE reservoir_spill_monthly
DROP CONSTRAINT IF EXISTS uq_spill_monthly;

-- Add new unique constraint
ALTER TABLE reservoir_spill_monthly
ADD CONSTRAINT uq_spill_monthly
UNIQUE(scenario_short_code, reservoir_entity_id, water_month);

-- Drop old indexes
DROP INDEX IF EXISTS idx_spill_monthly_reservoir;
DROP INDEX IF EXISTS idx_spill_monthly_combined;

-- Create new indexes
CREATE INDEX IF NOT EXISTS idx_spill_monthly_entity
ON reservoir_spill_monthly(reservoir_entity_id);
CREATE INDEX IF NOT EXISTS idx_spill_monthly_combined
ON reservoir_spill_monthly(scenario_short_code, reservoir_entity_id);

-- Drop old column
ALTER TABLE reservoir_spill_monthly DROP COLUMN IF EXISTS reservoir_code;

\echo '  reservoir_spill_monthly migrated'

-- ============================================
-- 3. MIGRATE reservoir_period_summary
-- ============================================
\echo ''
\echo '3. Migrating reservoir_period_summary...'

-- Add new column
ALTER TABLE reservoir_period_summary ADD COLUMN IF NOT EXISTS reservoir_entity_id INTEGER;

-- Populate from reservoir_entity lookup (strip S_ prefix)
UPDATE reservoir_period_summary rps
SET reservoir_entity_id = re.id
FROM reservoir_entity re
WHERE re.short_code = SUBSTRING(rps.reservoir_code, 3);

-- Verify no nulls
\echo 'Checking for unmapped records...'
SELECT COUNT(*) as missing_mappings
FROM reservoir_period_summary
WHERE reservoir_entity_id IS NULL;

-- Make NOT NULL
ALTER TABLE reservoir_period_summary
ALTER COLUMN reservoir_entity_id SET NOT NULL;

-- Add FK constraint
ALTER TABLE reservoir_period_summary
ADD CONSTRAINT fk_period_summary_reservoir_entity
FOREIGN KEY (reservoir_entity_id) REFERENCES reservoir_entity(id)
ON DELETE RESTRICT ON UPDATE CASCADE;

-- Drop old unique constraint (if exists)
ALTER TABLE reservoir_period_summary
DROP CONSTRAINT IF EXISTS uq_period_summary;

-- Add new unique constraint
ALTER TABLE reservoir_period_summary
ADD CONSTRAINT uq_period_summary
UNIQUE(scenario_short_code, reservoir_entity_id);

-- Drop old indexes
DROP INDEX IF EXISTS idx_period_summary_reservoir;

-- Create new indexes
CREATE INDEX IF NOT EXISTS idx_period_summary_entity
ON reservoir_period_summary(reservoir_entity_id);

-- Drop old column
ALTER TABLE reservoir_period_summary DROP COLUMN IF EXISTS reservoir_code;

\echo '  reservoir_period_summary migrated'

-- ============================================
-- 4. MIGRATE reservoir_monthly_percentile
-- ============================================
\echo ''
\echo '4. Checking reservoir_monthly_percentile...'

-- Check if this table needs migration (may already have reservoir_entity_id)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reservoir_monthly_percentile'
        AND column_name = 'reservoir_code'
    ) THEN
        RAISE NOTICE 'reservoir_monthly_percentile has reservoir_code - migrating...';

        -- Add new column if needed
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'reservoir_monthly_percentile'
            AND column_name = 'reservoir_entity_id'
        ) THEN
            ALTER TABLE reservoir_monthly_percentile ADD COLUMN reservoir_entity_id INTEGER;
        END IF;

        -- Populate from reservoir_entity lookup
        UPDATE reservoir_monthly_percentile rmp
        SET reservoir_entity_id = re.id
        FROM reservoir_entity re
        WHERE re.short_code = SUBSTRING(rmp.reservoir_code, 3)
        AND rmp.reservoir_entity_id IS NULL;

        -- Make NOT NULL
        ALTER TABLE reservoir_monthly_percentile
        ALTER COLUMN reservoir_entity_id SET NOT NULL;

        -- Drop old column
        ALTER TABLE reservoir_monthly_percentile DROP COLUMN reservoir_code;

        RAISE NOTICE 'reservoir_monthly_percentile migrated';
    ELSE
        RAISE NOTICE 'reservoir_monthly_percentile already uses reservoir_entity_id - skipping';
    END IF;
END $$;

\echo '  reservoir_monthly_percentile checked'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Table row counts:'
SELECT 'reservoir_storage_monthly' as table_name, COUNT(*) as rows FROM reservoir_storage_monthly
UNION ALL
SELECT 'reservoir_spill_monthly', COUNT(*) FROM reservoir_spill_monthly
UNION ALL
SELECT 'reservoir_period_summary', COUNT(*) FROM reservoir_period_summary
UNION ALL
SELECT 'reservoir_monthly_percentile', COUNT(*) FROM reservoir_monthly_percentile;

\echo ''
\echo 'Sample joined data (storage_monthly):'
SELECT rsm.scenario_short_code, re.short_code, rsm.water_month, rsm.storage_avg_taf
FROM reservoir_storage_monthly rsm
JOIN reservoir_entity re ON rsm.reservoir_entity_id = re.id
WHERE rsm.scenario_short_code = 's0020'
ORDER BY re.short_code, rsm.water_month
LIMIT 5;

\echo ''
\echo 'Sample joined data (spill_monthly):'
SELECT rsm.scenario_short_code, re.short_code, rsm.water_month, rsm.spill_frequency_pct
FROM reservoir_spill_monthly rsm
JOIN reservoir_entity re ON rsm.reservoir_entity_id = re.id
WHERE rsm.scenario_short_code = 's0020'
ORDER BY re.short_code, rsm.water_month
LIMIT 5;

\echo ''
\echo 'Sample joined data (period_summary):'
SELECT rps.scenario_short_code, re.short_code, rps.spill_frequency_pct, rps.storage_exc_p50
FROM reservoir_period_summary rps
JOIN reservoir_entity re ON rps.reservoir_entity_id = re.id
WHERE rps.scenario_short_code = 's0020'
ORDER BY re.short_code
LIMIT 5;

\echo ''
\echo 'Migration complete!'
\echo ''
\echo 'Statistics tables now use reservoir_entity_id FK instead of reservoir_code VARCHAR.'
\echo 'API queries should JOIN on reservoir_entity to get short_code.'
