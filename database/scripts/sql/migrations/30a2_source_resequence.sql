-- Migration 30a2: Source resequence — run as postgres
--
-- Run from Cloud9 (after 30a, before 30b):
--   psql $SUPERUSER_URL -f database/scripts/sql/migrations/30a2_source_resequence.sql
--
-- The source table has a UNIQUE constraint on the text "source" column.
-- Resequencing IDs requires INSERT+DELETE which temporarily duplicates
-- those text values. This must run as postgres to drop/recreate the constraint.
-- (The source audit trigger is already disabled by 30a.)

BEGIN;

-- Fix record 35 attribution before resequencing
UPDATE source
SET    created_by = 2,
       updated_by = 2,
       updated_at = NOW()
WHERE  id = 35;

-- Temporarily drop the UNIQUE constraint on source.source
-- (required so INSERT of new rows doesn't collide with old rows' text values)
ALTER TABLE source DROP CONSTRAINT source_source_key;

-- Insert new rows with correct IDs (9-12) copying data from old rows (32-35)
INSERT INTO source (id, source, description, is_active, created_at, created_by, updated_at, updated_by)
SELECT 9,  source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 32
UNION ALL
SELECT 10, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 33
UNION ALL
SELECT 11, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 34
UNION ALL
SELECT 12, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 35;

-- Update all child FK references
UPDATE network_type       SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_subtype    SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_arc        SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_node       SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_gis        SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE reservoir          SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE compliance_station SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE wba                SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE hydroclimate       SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);

-- Delete old rows
DELETE FROM source WHERE id IN (32, 33, 34, 35);

-- Recreate the UNIQUE constraint
ALTER TABLE source ADD CONSTRAINT source_source_key UNIQUE (source);

-- Reset sequence
SELECT setval('source_id_seq', (SELECT MAX(id) FROM source));

COMMIT;

\echo ''
\echo '30a2 SOURCE RESEQUENCE COMPLETE'
\echo '==============================='
\echo 'Now run 30b_data_changes.sql as your own role ($DATABASE_URL).'
