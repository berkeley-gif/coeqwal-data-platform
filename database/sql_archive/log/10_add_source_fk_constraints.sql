-- =============================================================================
-- Migration 10: Add FK constraints on source column to source lookup table
-- =============================================================================
-- assumption_definition, operation_definition, and slr all have a source TEXT
-- column referencing the source lookup table, but without a FK constraint.
-- This migration enforces referential integrity on all three.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/10_add_source_fk_constraints.sql
-- =============================================================================

ALTER TABLE assumption_definition
    ADD CONSTRAINT fk_assumption_definition_source
    FOREIGN KEY (source) REFERENCES source(source)
    ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE operation_definition
    ADD CONSTRAINT fk_operation_definition_source
    FOREIGN KEY (source) REFERENCES source(source)
    ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE slr
    ADD CONSTRAINT fk_slr_source
    FOREIGN KEY (source) REFERENCES source(source)
    ON UPDATE CASCADE ON DELETE RESTRICT;


SELECT tc.table_name, tc.constraint_name, kcu.column_name,
       ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND kcu.column_name = 'source'
  AND tc.table_name IN ('assumption_definition', 'operation_definition', 'slr')
ORDER BY tc.table_name;
