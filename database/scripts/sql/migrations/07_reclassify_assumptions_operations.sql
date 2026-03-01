-- =============================================================================
-- Migration 07: Reclassify assumptions → operations
-- =============================================================================
-- Requires: superuser (for DISABLE TRIGGER USER DDL on RDS)
--           Set DATABASE_URL to jfantauzza connection for audit attribution.
--
-- What this migration does:
--   1. Inserts new operation_category rows: tucp, gw_restrictions,
--      infrastructure, flow, biops
--   2. Moves rows from assumption_definition to operation_definition:
--        TUCP_TUCO, SGMA_SJV/SAC/CV, DCP_6000/Bethany,
--        no_min_flow/functional_flows/salmon_flows, biops_2024
--   3. Migrates scenario_key_assumption_link → scenario_key_operation_link
--      for moved assumption rows
--   4. Removes moved rows + SLR rows from assumption_definition
--   5. Removes TUCP_TUCO, gw_restrictions, infrastructure, flow, biops, slr
--      rows from assumption_category
--   6. Adds new land use assumption rows:
--        lu_2020_landiq, lu_2020_landiq_reduced_ag
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/07_reclassify_assumptions_operations.sql
-- =============================================================================

BEGIN;

-- ─── 1. New operation_category rows ──────────────────────────────────────

ALTER TABLE operation_category DISABLE TRIGGER USER;

INSERT INTO operation_category (short_code, name, description, is_active, created_by, updated_by)
VALUES
    ('tucp',                 'TUCP / TUCO',               'Temporary Urgency Change Petitions and Temporary Urgency Change Orders', TRUE, 2, 2),
    ('gw_restrictions',      'Groundwater Restrictions',  'SGMA-type groundwater pumping restrictions', TRUE, 2, 2),
    ('infrastructure',       'Infrastructure',            'Water infrastructure configuration (tunnels, reservoirs)', TRUE, 2, 2),
    ('flow',                 'Flow Requirements',         'Instream flow and minimum flow objectives', TRUE, 2, 2),
    ('biops',                'Biological Opinions',       'NMFS and USFWS biological opinions (USBR LTO)', TRUE, 2, 2)
ON CONFLICT (short_code) DO NOTHING;

ALTER TABLE operation_category ENABLE TRIGGER USER;

-- ─── 2. Move rows from assumption_definition → operation_definition ───────
-- Use a CTE with RETURNING to capture the mapping from old assumption IDs
-- to new operation IDs, then use it to migrate the link table.

ALTER TABLE operation_definition DISABLE TRIGGER USER;
ALTER TABLE scenario_key_operation_link DISABLE TRIGGER USER;

-- Insert moved rows into operation_definition; capture new ids
WITH moved AS (
    INSERT INTO operation_definition (
        short_code, name, short_title, subtitle, simple_description,
        description, narrative, category, is_active, notes,
        operation_version_id, created_by, updated_by, created_at, updated_at
    )
    SELECT
        ad.short_code,
        ad.name,
        ad.short_title,
        ad.subtitle,
        ad.simple_description,
        ad.description,
        ad.narrative,
        CASE ad.category
            WHEN 'TUCP_TUCO'      THEN 'tucp'
            WHEN 'gw_restrictions' THEN 'gw_restrictions'
            WHEN 'infrastructure' THEN 'infrastructure'
            WHEN 'flow'           THEN 'flow'
            WHEN 'biops'          THEN 'biops'
            ELSE ad.category
        END AS category,
        ad.is_active,
        ad.notes,
        ad.assumptions_version_id,
        2,
        2,
        ad.created_at,
        ad.updated_at
    FROM assumption_definition ad
    WHERE ad.short_code IN (
        'TUCP_TUCO',
        'SGMA_SJV', 'SGMA_SAC', 'SGMA_CV',
        'DCP_6000', 'DCP_Bethany',
        'no_min_flow', 'functional_flows', 'salmon_flows',
        'biops_2024'
    )
    ON CONFLICT (short_code) DO NOTHING
    RETURNING id, short_code
),
-- Build mapping: old assumption_definition.id → new operation_definition.id
mapping AS (
    SELECT
        ad.id AS old_assumption_id,
        moved.id AS new_operation_id
    FROM moved
    JOIN assumption_definition ad ON ad.short_code = moved.short_code
)
-- Migrate scenario_key_assumption_link rows for moved assumptions
INSERT INTO scenario_key_operation_link (
    scenario_id, operation_id, created_at, created_by, updated_at, updated_by
)
SELECT
    skal.scenario_id,
    m.new_operation_id,
    COALESCE(skal.created_at, NOW()),
    2,
    COALESCE(skal.updated_at, NOW()),
    2
FROM scenario_key_assumption_link skal
JOIN mapping m ON skal.assumption_id = m.old_assumption_id
ON CONFLICT (scenario_id, operation_id) DO NOTHING;

ALTER TABLE operation_definition ENABLE TRIGGER USER;
ALTER TABLE scenario_key_operation_link ENABLE TRIGGER USER;

-- ─── 3. Remove migrated links from scenario_key_assumption_link ───────────

DELETE FROM scenario_key_assumption_link
WHERE assumption_id IN (
    SELECT id FROM assumption_definition
    WHERE short_code IN (
        'TUCP_TUCO',
        'SGMA_SJV', 'SGMA_SAC', 'SGMA_CV',
        'DCP_6000', 'DCP_Bethany',
        'no_min_flow', 'functional_flows', 'salmon_flows',
        'biops_2024'
    )
);

-- ─── 4. Remove moved + SLR rows from assumption_definition ───────────────

ALTER TABLE assumption_definition DISABLE TRIGGER USER;

DELETE FROM assumption_definition
WHERE short_code IN (
    -- Moved to operation_definition
    'TUCP_TUCO',
    'SGMA_SJV', 'SGMA_SAC', 'SGMA_CV',
    'DCP_6000', 'DCP_Bethany',
    'no_min_flow', 'functional_flows', 'salmon_flows',
    'biops_2024',
    -- SLR moved to slr table
    'slr_15', 'slr_30'
);

-- ─── 5. Add new land use assumption rows ─────────────────────────────────

INSERT INTO assumption_definition (
    short_code, name, short_title, category, is_active,
    assumptions_version_id, created_by, updated_by
)
VALUES
    (
        'lu_2020_landiq',
        '2020 LandIQ land use',
        '2020 LandIQ',
        'land_use',
        TRUE,
        1, 2, 2
    ),
    (
        'lu_2020_landiq_reduced_ag',
        '2020 LandIQ land use with reduced agricultural acreage',
        '2020 LandIQ (reduced ag)',
        'land_use',
        TRUE,
        1, 2, 2
    )
ON CONFLICT (short_code) DO NOTHING;

ALTER TABLE assumption_definition ENABLE TRIGGER USER;

-- ─── 6. Clean up assumption_category ─────────────────────────────────────
-- Remove categories that are now represented by operation_category rows
-- or by dedicated tables (slr).

DELETE FROM assumption_category
WHERE short_code IN (
    'TUCP_TUCO',
    'gw_restrictions',
    'infrastructure',
    'flow',
    'biops',
    'slr'
);

COMMIT;

-- ─── Verify ───────────────────────────────────────────────────────────────

SELECT 'assumption_definition rows remaining' AS check_name,
       short_code, category
FROM assumption_definition
ORDER BY category, short_code;

SELECT 'operation_definition rows (full set)' AS check_name,
       od.short_code, od.category
FROM operation_definition od
ORDER BY od.category, od.short_code;

SELECT 'assumption_category rows remaining' AS check_name,
       short_code
FROM assumption_category;

SELECT 'operation_category rows (full set)' AS check_name,
       short_code
FROM operation_category;
