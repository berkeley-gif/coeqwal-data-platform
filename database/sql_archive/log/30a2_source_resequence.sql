BEGIN;

UPDATE source
SET    created_by = 2,
       updated_by = 2,
       updated_at = NOW()
WHERE  id = 35;

ALTER TABLE assumption_definition DROP CONSTRAINT fk_assumption_definition_source;
ALTER TABLE operation_definition  DROP CONSTRAINT fk_operation_definition_source;
ALTER TABLE slr                   DROP CONSTRAINT fk_slr_source;
ALTER TABLE theme                 DROP CONSTRAINT fk_theme_source;

ALTER TABLE source DROP CONSTRAINT source_source_key;

INSERT INTO source (id, source, description, is_active, created_at, created_by, updated_at, updated_by)
SELECT 9,  source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 32
UNION ALL
SELECT 10, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 33
UNION ALL
SELECT 11, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 34
UNION ALL
SELECT 12, source, description, is_active, created_at, created_by, NOW(), updated_by FROM source WHERE id = 35;

UPDATE network_type       SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_subtype    SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_arc        SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_node       SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE network_gis        SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE reservoir          SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE compliance_station SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE wba                SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);
UPDATE hydroclimate       SET source_id = CASE source_id WHEN 32 THEN 9 WHEN 33 THEN 10 WHEN 34 THEN 11 WHEN 35 THEN 12 ELSE source_id END WHERE source_id IN (32,33,34,35);

DELETE FROM source WHERE id IN (32, 33, 34, 35);

ALTER TABLE source ADD CONSTRAINT source_source_key UNIQUE (source);

ALTER TABLE assumption_definition ADD CONSTRAINT fk_assumption_definition_source FOREIGN KEY (source) REFERENCES source(source);
ALTER TABLE operation_definition  ADD CONSTRAINT fk_operation_definition_source  FOREIGN KEY (source) REFERENCES source(source);
ALTER TABLE slr                   ADD CONSTRAINT fk_slr_source                   FOREIGN KEY (source) REFERENCES source(source);
ALTER TABLE theme                 ADD CONSTRAINT fk_theme_source                 FOREIGN KEY (source) REFERENCES source(source);

SELECT setval('source_id_seq', (SELECT MAX(id) FROM source));

COMMIT;

\echo ''
\echo '30a2 SOURCE RESEQUENCE COMPLETE'
\echo '==============================='
\echo 'Now run 30b_data_changes.sql as your own role ($DATABASE_URL).'
