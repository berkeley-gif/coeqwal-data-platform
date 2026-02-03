-- LOAD DU_URBAN VARIABLE CATEGORY GROUPS
-- Adds variable extraction category groups to du_urban_group
-- and populates memberships in du_urban_group_member
--
-- This normalizes the variable_type information from du_urban_variable
-- into the standard group/member pattern used throughout the database.
--
-- Groups added:
--   - var_wba (40 members): WBA-style with DL_* delivery variables
--   - var_gw_only (3 members): Groundwater only, no surface delivery
--   - var_swp_contractor (11 members): SWP contractor PMI deliveries
--   - var_named_locality (15 members): Named locality arc deliveries
--   - var_missing (2 members): No CalSim variables found
--
-- Prerequisites:
--   1. Run 01_create_du_urban_entity.sql
--   2. Run 01b_load_du_urban_entity_from_s3.sql
--   3. Run 02b_create_du_urban_group_tables.sql
--   4. Run 02c_load_du_urban_group_from_s3.sql (loads existing groups)
--   5. Run 01c_create_du_urban_variable.sql
--   6. Run 01d_load_du_urban_variable.sql
--
-- Run with: psql -f 02d_load_du_variable_groups.sql

\echo ''
\echo '========================================='
\echo 'LOADING DU VARIABLE CATEGORY GROUPS'
\echo '========================================='

-- ============================================
-- GET NEXT GROUP ID
-- ============================================
\echo ''
\echo 'Determining next group ID...'

-- Get max existing ID (should be 6)
DO $$
DECLARE
    max_id INTEGER;
BEGIN
    SELECT COALESCE(MAX(id), 0) INTO max_id FROM du_urban_group;
    RAISE NOTICE 'Current max group ID: %', max_id;
END $$;

-- ============================================
-- INSERT VARIABLE CATEGORY GROUPS
-- ============================================
\echo ''
\echo 'Inserting variable category groups...'

INSERT INTO du_urban_group (id, short_code, label, description, display_order, is_active, created_at, created_by, updated_at, updated_by)
VALUES
    (7, 'var_wba', 'WBA Delivery Units', 
     'Demand units using WBA-style DL_* total delivery variables. These have pre-calculated total delivery in CalSim output.',
     10, TRUE, NOW(), 1, NOW(), 1),
    
    (8, 'var_gw_only', 'Groundwater Only Units',
     'Demand units with no surface water delivery. Only have GP_* groundwater pumping and GW_SHORT_* restriction shortage variables.',
     11, TRUE, NOW(), 1, NOW(), 1),
    
    (9, 'var_swp_contractor', 'SWP Contractor Units',
     'Demand units receiving water via SWP contractor delivery arcs (D_*_PMI). Shortage tracked via SHORT_D_*_PMI.',
     12, TRUE, NOW(), 1, NOW(), 1),
    
    (10, 'var_named_locality', 'Named Locality Units',
     'Named localities with specific delivery arcs (D_*). Some require summing multiple arcs. Includes water treatment plant deliveries.',
     13, TRUE, NOW(), 1, NOW(), 1),
    
    (11, 'var_missing', 'Missing Variable Units',
     'Demand units in the canonical tier matrix but with no corresponding CalSim output variables found.',
     14, TRUE, NOW(), 1, NOW(), 1)
ON CONFLICT (id) DO UPDATE SET
    short_code = EXCLUDED.short_code,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    display_order = EXCLUDED.display_order,
    updated_at = NOW();

\echo '  Inserted 5 variable category groups (IDs 7-11)'

-- ============================================
-- INSERT GROUP MEMBERSHIPS
-- ============================================
\echo ''
\echo 'Inserting group memberships...'

-- Clear any existing variable group memberships (IDs 7-11)
DELETE FROM du_urban_group_member WHERE du_urban_group_id >= 7;

-- ----------------------------------------
-- GROUP 7: var_wba (40 members)
-- WBA-style units with DL_* delivery variables
-- ----------------------------------------
\echo '  Inserting var_wba memberships (40 units)...'

INSERT INTO du_urban_group_member (du_urban_group_id, du_id, display_order, is_active, created_at, created_by, updated_at, updated_by)
SELECT 7, du_id, ROW_NUMBER() OVER (ORDER BY du_id), TRUE, NOW(), 1, NOW(), 1
FROM du_urban_variable
WHERE delivery_variable LIKE 'DL_%'
ORDER BY du_id;

-- ----------------------------------------
-- GROUP 8: var_gw_only (3 members)
-- Groundwater-only units with GP_* variables
-- ----------------------------------------
\echo '  Inserting var_gw_only memberships (3 units)...'

INSERT INTO du_urban_group_member (du_urban_group_id, du_id, display_order, is_active, created_at, created_by, updated_at, updated_by)
VALUES
    (8, '71_NU', 1, TRUE, NOW(), 1, NOW(), 1),
    (8, '72_NU', 2, TRUE, NOW(), 1, NOW(), 1),
    (8, '72_PU', 3, TRUE, NOW(), 1, NOW(), 1);

-- ----------------------------------------
-- GROUP 9: var_swp_contractor (11 members)
-- SWP contractor units with D_*_PMI variables
-- ----------------------------------------
\echo '  Inserting var_swp_contractor memberships (11 units)...'

INSERT INTO du_urban_group_member (du_urban_group_id, du_id, display_order, is_active, created_at, created_by, updated_at, updated_by)
VALUES
    (9, 'CSB038', 1, TRUE, NOW(), 1, NOW(), 1),
    (9, 'CSB103', 2, TRUE, NOW(), 1, NOW(), 1),
    (9, 'CSTIC', 3, TRUE, NOW(), 1, NOW(), 1),
    (9, 'ESB324', 4, TRUE, NOW(), 1, NOW(), 1),
    (9, 'ESB347', 5, TRUE, NOW(), 1, NOW(), 1),
    (9, 'ESB414', 6, TRUE, NOW(), 1, NOW(), 1),
    (9, 'ESB415', 7, TRUE, NOW(), 1, NOW(), 1),
    (9, 'ESB420', 8, TRUE, NOW(), 1, NOW(), 1),
    (9, 'SBA029', 9, TRUE, NOW(), 1, NOW(), 1),
    (9, 'SBA036', 10, TRUE, NOW(), 1, NOW(), 1),
    (9, 'SCVWD', 11, TRUE, NOW(), 1, NOW(), 1);

-- ----------------------------------------
-- GROUP 10: var_named_locality (15 members)
-- Named localities with D_* arc variables
-- ----------------------------------------
\echo '  Inserting var_named_locality memberships (15 units)...'

INSERT INTO du_urban_group_member (du_urban_group_id, du_id, display_order, is_active, created_at, created_by, updated_at, updated_by)
VALUES
    (10, 'AMADR', 1, TRUE, NOW(), 1, NOW(), 1),
    (10, 'AMCYN', 2, TRUE, NOW(), 1, NOW(), 1),
    (10, 'ANTOC', 3, TRUE, NOW(), 1, NOW(), 1),
    (10, 'BNCIA', 4, TRUE, NOW(), 1, NOW(), 1),
    (10, 'CCWD', 5, TRUE, NOW(), 1, NOW(), 1),
    (10, 'FRFLD', 6, TRUE, NOW(), 1, NOW(), 1),
    (10, 'GRSVL', 7, TRUE, NOW(), 1, NOW(), 1),
    (10, 'MWD', 8, TRUE, NOW(), 1, NOW(), 1),
    (10, 'NAPA', 9, TRUE, NOW(), 1, NOW(), 1),
    (10, 'NAPA2', 10, TRUE, NOW(), 1, NOW(), 1),
    (10, 'PLMAS', 11, TRUE, NOW(), 1, NOW(), 1),
    (10, 'SUISN', 12, TRUE, NOW(), 1, NOW(), 1),
    (10, 'TVAFB', 13, TRUE, NOW(), 1, NOW(), 1),
    (10, 'VLLJO', 14, TRUE, NOW(), 1, NOW(), 1),
    (10, 'WLDWD', 15, TRUE, NOW(), 1, NOW(), 1);

-- ----------------------------------------
-- GROUP 11: var_missing (2 members)
-- Units with no CalSim variables
-- ----------------------------------------
\echo '  Inserting var_missing memberships (2 units)...'

INSERT INTO du_urban_group_member (du_urban_group_id, du_id, display_order, is_active, created_at, created_by, updated_at, updated_by)
VALUES
    (11, 'JLIND', 1, TRUE, NOW(), 1, NOW(), 1),
    (11, 'UPANG', 2, TRUE, NOW(), 1, NOW(), 1);

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'All groups with member counts:'
SELECT g.id, g.short_code, g.label,
       COUNT(gm.id) as member_count
FROM du_urban_group g
LEFT JOIN du_urban_group_member gm ON g.id = gm.du_urban_group_id
GROUP BY g.id, g.short_code, g.label
ORDER BY g.display_order;

\echo ''
\echo 'Variable category groups breakdown:'
SELECT g.short_code, g.label, COUNT(gm.id) as members
FROM du_urban_group g
JOIN du_urban_group_member gm ON g.id = gm.du_urban_group_id
WHERE g.short_code LIKE 'var_%'
GROUP BY g.short_code, g.label
ORDER BY g.id;

\echo ''
\echo 'Total memberships in variable groups:'
SELECT COUNT(*) as total_var_memberships
FROM du_urban_group_member gm
JOIN du_urban_group g ON gm.du_urban_group_id = g.id
WHERE g.short_code LIKE 'var_%';

\echo ''
\echo 'Sample: var_wba members (first 10):'
SELECT gm.du_id, v.delivery_variable, v.shortage_variable
FROM du_urban_group_member gm
JOIN du_urban_group g ON gm.du_urban_group_id = g.id
JOIN du_urban_variable v ON gm.du_id = v.du_id
WHERE g.short_code = 'var_wba'
ORDER BY gm.display_order
LIMIT 10;

\echo ''
\echo 'Sample: var_gw_only members:'
SELECT gm.du_id, v.delivery_variable, v.shortage_variable, v.notes
FROM du_urban_group_member gm
JOIN du_urban_group g ON gm.du_urban_group_id = g.id
JOIN du_urban_variable v ON gm.du_id = v.du_id
WHERE g.short_code = 'var_gw_only'
ORDER BY gm.display_order;

\echo ''
\echo '✅ Variable category groups loaded successfully'
\echo ''
\echo 'API can now query demand units by category:'
\echo '  - GET /demand-units?group=var_wba'
\echo '  - GET /demand-units?group=var_swp_contractor'
\echo '  - etc.'
