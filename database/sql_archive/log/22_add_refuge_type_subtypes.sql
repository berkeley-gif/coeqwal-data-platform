-- =============================================================================
-- 22_add_refuge_type_subtypes.sql
-- Adds PR and NR node subtypes to network_subtype, then corrects
-- subtype_ids on all 18 wildlife refuge network nodes.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/22_add_refuge_type_subtypes.sql
--
-- Background:
--   network_subtype previously had only `R` (generic Refuge) for wildlife
--   refuge demand nodes.  CalSim 3 distinguishes two types:
--     PR  - Project Refuge:     receives CVP (Central Valley Project) contract deliveries
--     NR  - Non-project Refuge: served by water rights only, no CVP deliveries
--   Additionally, 9 refuge nodes (08N_PR1, 08N_PR2, and all SJR refuges
--   except 91_PR) were incorrectly tagged with subtype U (Urban, id=25).
--   The remaining 9 SAC-region PR nodes and 17N_NR used the generic R (id=20).
--   This migration replaces all refuge subtype assignments with the new
--   specific PR (id=27) and NR (id=28) values.
-- =============================================================================

\set ON_ERROR_STOP on

\echo ''
\echo '============================================='
\echo 'MIGRATION 22  - Add PR/NR network_subtype rows + fix refuge node subtypes'
\echo '============================================='


-- =============================================================================
-- 1. Insert PR and NR subtypes into network_subtype
-- =============================================================================

\echo ''
\echo 'Inserting PR and NR into network_subtype...'

ALTER TABLE network_subtype DISABLE TRIGGER USER;

INSERT INTO network_subtype
    (short_code, label, description, network_entity_type_id, type_id, model_source_id, source_id, is_active,
     created_by, updated_by)
SELECT 'PR', 'Project Refuge',
     'CalSim project refuge demand node  - receives CVP (Central Valley Project) contract deliveries',
     2,
     (SELECT id FROM network_type WHERE short_code = 'X'),
     1, 4, TRUE, 2, 2
WHERE NOT EXISTS (SELECT 1 FROM network_subtype WHERE short_code = 'PR');

INSERT INTO network_subtype
    (short_code, label, description, network_entity_type_id, type_id, model_source_id, source_id, is_active,
     created_by, updated_by)
SELECT 'NR', 'Non-project Refuge',
     'CalSim non-project refuge demand node  - served by water rights only (no CVP contract deliveries)',
     2,
     (SELECT id FROM network_type WHERE short_code = 'X'),
     1, 4, TRUE, 2, 2
WHERE NOT EXISTS (SELECT 1 FROM network_subtype WHERE short_code = 'NR');

ALTER TABLE network_subtype ENABLE TRIGGER USER;

\echo 'PR and NR subtypes inserted.'


-- =============================================================================
-- 2. Verify new subtype IDs
-- =============================================================================

\echo ''
\echo 'New subtype IDs (expect PR=27, NR=28 if these are the first inserts):'
SELECT id, short_code, label
FROM network_subtype
WHERE short_code IN ('PR', 'NR')
ORDER BY id;


-- =============================================================================
-- 3. Update refuge network nodes to use correct subtype IDs
-- =============================================================================

\echo ''
\echo 'Updating subtype_ids on refuge network nodes...'

ALTER TABLE network DISABLE TRIGGER USER;

UPDATE network
SET
    subtype_ids = ARRAY[(SELECT id FROM network_subtype WHERE short_code = 'PR')],
    updated_at  = NOW(),
    updated_by  = 2
WHERE short_code IN (
    '08N_PR1', '08N_PR2', '08S_PR', '09_PR', '11_PR',
    '17N_PR',  '17S_PR',
    '63_PR1',  '63_PR2',  '63_PR3',
    '72_PR1',  '72_PR2',  '72_PR3',  '72_PR4',  '72_PR5',  '72_PR6',
    '91_PR'
);

UPDATE network
SET
    subtype_ids = ARRAY[(SELECT id FROM network_subtype WHERE short_code = 'NR')],
    updated_at  = NOW(),
    updated_by  = 2
WHERE short_code IN ('17N_NR');

ALTER TABLE network ENABLE TRIGGER USER;

\echo 'Refuge node subtypes updated.'


-- =============================================================================
-- 4. Verification
-- =============================================================================

\echo ''
\echo '===== VERIFICATION ====='

\echo ''
\echo 'network_subtype  - refuge-related entries (R, PR, NR):'
SELECT id, short_code, label, description
FROM network_subtype
WHERE short_code IN ('R', 'PR', 'NR')
ORDER BY id;

\echo ''
\echo 'Refuge network nodes  - short_code and resolved subtypes:'
SELECT
    n.short_code,
    n.subtype_ids,
    ns.short_code   AS subtype,
    ns.label        AS subtype_label
FROM network n
JOIN network_subtype ns ON ns.id = ANY(n.subtype_ids)
WHERE n.short_code IN (
    '08N_PR1', '08N_PR2', '08S_PR', '09_PR', '11_PR',
    '17N_NR',
    '17N_PR',  '17S_PR',
    '63_PR1',  '63_PR2',  '63_PR3',
    '72_PR1',  '72_PR2',  '72_PR3',  '72_PR4',  '72_PR5',  '72_PR6',
    '91_PR'
)
ORDER BY n.short_code;

\echo ''
\echo 'Confirm no refuge nodes still have old generic R (id=20) or Urban (id=25) subtype:'
SELECT short_code, subtype_ids
FROM network
WHERE short_code IN (
    '08N_PR1', '08N_PR2', '08S_PR', '09_PR', '11_PR',
    '17N_NR',
    '17N_PR',  '17S_PR',
    '63_PR1',  '63_PR2',  '63_PR3',
    '72_PR1',  '72_PR2',  '72_PR3',  '72_PR4',  '72_PR5',  '72_PR6',
    '91_PR'
)
AND (20 = ANY(subtype_ids) OR 25 = ANY(subtype_ids));

\echo ''
\echo '=== Migration 22 complete ==='
