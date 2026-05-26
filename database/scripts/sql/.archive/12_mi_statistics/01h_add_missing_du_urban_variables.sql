-- ADD DU_URBAN_VARIABLE RECORDS FOR NEW ENTITIES
-- Creates variable mappings for entities added in 01g_add_missing_du_urban_entities.sql
-- Run AFTER 01g script
-- Run with: psql "$DATABASE_URL" -f database/scripts/sql/12_mi_statistics/01h_add_missing_du_urban_variables.sql

\echo ''
\echo '========================================='
\echo 'ADDING DU_URBAN_VARIABLE RECORDS FOR NEW ENTITIES'
\echo '========================================='

BEGIN;

-- ============================================================================
-- PROJECT URBAN (PU) VARIABLES
-- ============================================================================

\echo ''
\echo 'Adding variable mappings for Project Urban (PU) entities...'

-- 60N_PU1
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('60N_PU1', 'D_FSC025_60N_PU', 'UD_60N_PU', NULL, 'PU', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    demand_variable = EXCLUDED.demand_variable;

-- 60S_PU (no direct delivery arcs found - may be groundwater only)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('60S_PU', 'NOT_FOUND', 'UD_60S_PU', NULL, 'PU', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable;

-- 61_PU1 - has many delivery arcs
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('61_PU1', 'NOT_FOUND', 'UD_61_PU', NULL, 'PU', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

-- Add delivery arcs for 61_PU1 (selecting key urban WTPs)
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('61_PU1', 'D_WTPMOD_61_NU1', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('61_PU1', 'D_WTPDGT_61_NU2', 2) ON CONFLICT DO NOTHING;

-- 61_PU2 - has many delivery arcs
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('61_PU2', 'NOT_FOUND', 'UD_61_PU2', NULL, 'PU', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

-- Add delivery arcs for 61_PU2
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('61_PU2', 'D_WTPMOD_61_NU1', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('61_PU2', 'D_WTPDGT_61_NU2', 2) ON CONFLICT DO NOTHING;

-- 64_PU (no direct delivery arcs found)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('64_PU', 'NOT_FOUND', 'UD_64_PU', NULL, 'PU', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable;

-- 65_PU (no direct delivery arcs found)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('65_PU', 'NOT_FOUND', 'UD_65_PU', NULL, 'PU', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable;

-- 70_PU1 - has 11 delivery arcs
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('70_PU1', 'NOT_FOUND', 'UD_70_PU', NULL, 'PU', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

-- Add delivery arcs for 70_PU1 (key arcs)
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('70_PU1', 'D_CAA069_DMC070', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('70_PU1', 'D_DMC070_CAA069', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('70_PU1', 'D_PTH070_PUTAH', 3) ON CONFLICT DO NOTHING;

-- 72_PU1 - has 17 delivery arcs
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('72_PU1', 'NOT_FOUND', 'UD_72_PU', NULL, 'PU', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

-- Add delivery arcs for 72_PU1 (key arcs)
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('72_PU1', 'D_XCC025_72_PA', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('72_PU1', 'D_VLW008_72_PR1', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('72_PU1', 'D_ARY010_72_PR3', 3) ON CONFLICT DO NOTHING;

-- ============================================================================
-- PROJECT AGRICULTURAL (PA) VARIABLES
-- ============================================================================

\echo ''
\echo 'Adding variable mappings for Project Agricultural (PA) entities...'

-- 50_PA1
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('50_PA1', 'NOT_FOUND', 'UD_50_PA1', NULL, 'PA', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('50_PA1', 'D_DMC021_50_PA1', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('50_PA1', 'D_RFS71A_50_PA1', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('50_PA1', 'D_SJR062_50_PA1', 3) ON CONFLICT DO NOTHING;

-- 50_PA2
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('50_PA2', 'D_CAA002_50_PA2', 'UD_50_PA2', NULL, 'PA', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    demand_variable = EXCLUDED.demand_variable;

-- 60N_PA (no delivery arcs found)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('60N_PA', 'NOT_FOUND', 'UD_60N_PA', NULL, 'PA', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable;

-- 60S_PA
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('60S_PA', 'NOT_FOUND', 'UD_60S_PA', NULL, 'PA', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('60S_PA', 'D_CLV026_60S_PA1', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('60S_PA', 'D_LJC010_60S_PA2', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('60S_PA', 'D_LJC022_60S_PA1', 3) ON CONFLICT DO NOTHING;

-- 61_PA
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('61_PA', 'NOT_FOUND', 'UD_61_PA', NULL, 'PA', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('61_PA', 'D_OAK020_61_PA2', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('61_PA', 'D_SSJ004_61_PA1', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('61_PA', 'D_WDWRD_61_PA3', 3) ON CONFLICT DO NOTHING;

-- 63_PA (no delivery arcs found)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('63_PA', 'NOT_FOUND', 'UD_63_PA', NULL, 'PA', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable;

-- 63_PR (Project Refuge)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('63_PR', 'NOT_FOUND', 'UD_63_PR', NULL, 'PR', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('63_PR', 'D_DED010_63_PR2', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('63_PR', 'D_EBP048_63_PR3', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('63_PR', 'D_ESC005_63_PR1', 3) ON CONFLICT DO NOTHING;

-- 64_PA
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('64_PA', 'NOT_FOUND', 'UD_64_PA', NULL, 'PA', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

-- Add key delivery arcs for 64_PA (many arcs available)
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('64_PA', 'D_DBC014_64_PA1', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('64_PA', 'D_FRS036_64_PA1', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('64_PA', 'D_MDC006_64_PA1', 3) ON CONFLICT DO NOTHING;

-- 65_PA (no delivery arcs found)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('65_PA', 'NOT_FOUND', 'UD_65_PA', NULL, 'PA', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable;

-- 70_PA (no delivery arcs found)
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('70_PA', 'NOT_FOUND', 'UD_70_PA', NULL, 'PA', FALSE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable;

-- 71_PA
INSERT INTO du_urban_variable (du_id, delivery_variable, demand_variable, shortage_variable, variable_type, requires_sum)
VALUES ('71_PA', 'NOT_FOUND', 'UD_71_PA', NULL, 'PA', TRUE)
ON CONFLICT (du_id) DO UPDATE SET
    demand_variable = EXCLUDED.demand_variable,
    requires_sum = TRUE;

-- Add key delivery arcs for 71_PA (many arcs available)
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('71_PA', 'D_DMC030_71_PA1', 1) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('71_PA', 'D_DMC034_71_PA2', 2) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('71_PA', 'D_DMC044_71_PA4', 3) ON CONFLICT DO NOTHING;
INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES ('71_PA', 'D_CAA046_71_PA7', 4) ON CONFLICT DO NOTHING;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'New variable mappings added:'
SELECT du_id, delivery_variable, demand_variable, variable_type, requires_sum
FROM du_urban_variable
WHERE du_id IN (
    '60N_PU1', '60S_PU', '61_PU1', '61_PU2', '64_PU', '65_PU', '70_PU1', '72_PU1',
    '50_PA1', '50_PA2', '60N_PA', '60S_PA', '61_PA', '63_PA', '63_PR', '64_PA', '65_PA', '70_PA', '71_PA'
)
ORDER BY du_id;

\echo ''
\echo 'Delivery arcs for new entities:'
SELECT du_id, delivery_arc, arc_order
FROM du_urban_delivery_arc
WHERE du_id IN (
    '60N_PU1', '60S_PU', '61_PU1', '61_PU2', '64_PU', '65_PU', '70_PU1', '72_PU1',
    '50_PA1', '50_PA2', '60N_PA', '60S_PA', '61_PA', '63_PA', '63_PR', '64_PA', '65_PA', '70_PA', '71_PA'
)
ORDER BY du_id, arc_order;

\echo ''
\echo 'Total variable mappings:'
SELECT COUNT(*) as total FROM du_urban_variable;

COMMIT;

\echo ''
\echo '✅ Variable mappings for new entities added successfully'
