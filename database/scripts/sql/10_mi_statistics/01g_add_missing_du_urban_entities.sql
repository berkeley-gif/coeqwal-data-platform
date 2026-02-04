-- ADD MISSING DU_URBAN_ENTITY RECORDS
-- These DUs exist in CalSim output with delivery arcs but were not in the original entity table
-- Run with: psql "$DATABASE_URL" -f database/scripts/sql/10_mi_statistics/01g_add_missing_du_urban_entities.sql

\echo ''
\echo '========================================='
\echo 'ADDING MISSING DU_URBAN_ENTITY RECORDS'
\echo '========================================='

BEGIN;

-- ============================================================================
-- PROJECT URBAN (PU) - Urban water service areas within project boundaries
-- ============================================================================

\echo ''
\echo 'Adding missing Project Urban (PU) demand units...'

-- 60N_PU1 - Mokelumne River area Project Urban
-- Has delivery arc: D_FSC025_60N_PU
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type, 
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '60N_PU1', '60N', 'SJR', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '0',
    'Mokelumne River Area Project Urban', 'Folsom South Canal'
) ON CONFLICT (du_id) DO NOTHING;

-- 60S_PU - Calaveras/Stanislaus Area Project Urban  
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency
) VALUES (
    '60S_PU', '60S', 'SJR', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '0',
    'Calaveras/Stanislaus Area Project Urban'
) ON CONFLICT (du_id) DO NOTHING;

-- 61_PU1 - Tuolumne/Merced Area Project Urban 1
-- Has 17 delivery arcs including D_WTPMOD_61_NU1, D_WTPDGT_61_NU2
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '61_PU1', '61', 'SJR', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '0',
    'Tuolumne/Merced Area Project Urban (Modesto, Turlock)', 
    'Multiple WTPs: Modesto, Don Pedro, Turlock'
) ON CONFLICT (du_id) DO NOTHING;

-- 61_PU2 - Tuolumne/Merced Area Project Urban 2
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '61_PU2', '61', 'SJR', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '0',
    'Tuolumne/Merced Area Project Urban 2',
    'Multiple WTPs'
) ON CONFLICT (du_id) DO NOTHING;

-- 64_PU - Fresno/Kings Area Project Urban
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency
) VALUES (
    '64_PU', '64', 'TULARE', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '1',
    'Fresno/Kings Area Project Urban'
) ON CONFLICT (du_id) DO NOTHING;

-- 65_PU - Kaweah/Tule Area Project Urban
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency
) VALUES (
    '65_PU', '65', 'TULARE', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '1',
    'Kaweah/Tule Area Project Urban'
) ON CONFLICT (du_id) DO NOTHING;

-- 70_PU1 - Kern County Area Project Urban
-- Has 11 delivery arcs
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '70_PU1', '70', 'TULARE', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '1',
    'Kern County Area Project Urban (Bakersfield metro)',
    'Cross Valley Canal, California Aqueduct, Kern River'
) ON CONFLICT (du_id) DO NOTHING;

-- 72_PU1 - Cross Valley/Tulare Lake Area Project Urban 1
-- Has 17 delivery arcs
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '72_PU1', '72', 'TULARE', 0, 'Urban', 'PU',
    'calsim3', FALSE, '1', '1',
    'Cross Valley/Tulare Lake Area Project Urban 1',
    'Cross Valley Canal, Arvin-Edison Canal'
) ON CONFLICT (du_id) DO NOTHING;

-- ============================================================================
-- PROJECT AGRICULTURAL (PA) - Agricultural water within project boundaries
-- Including for completeness, as they have delivery arcs in CalSim
-- ============================================================================

\echo ''
\echo 'Adding missing Project Agricultural (PA) demand units...'

-- 50_PA1 - San Joaquin Exchange Contractors Area 1
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '50_PA1', '50', 'SJR', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'San Joaquin Exchange Contractors Area 1',
    'Delta-Mendota Canal, San Joaquin River'
) ON CONFLICT (du_id) DO NOTHING;

-- 50_PA2 - San Joaquin Exchange Contractors Area 2
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '50_PA2', '50', 'SJR', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'San Joaquin Exchange Contractors Area 2',
    'California Aqueduct'
) ON CONFLICT (du_id) DO NOTHING;

-- 60N_PA - Mokelumne River Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency
) VALUES (
    '60N_PA', '60N', 'SJR', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Mokelumne River Area Project Agricultural'
) ON CONFLICT (du_id) DO NOTHING;

-- 60S_PA - Calaveras/Stanislaus Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '60S_PA', '60S', 'SJR', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Calaveras/Stanislaus Area Project Agricultural',
    'Calaveras River, Littlejohns Creek'
) ON CONFLICT (du_id) DO NOTHING;

-- 61_PA - Tuolumne/Merced Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '61_PA', '61', 'SJR', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Tuolumne/Merced Area Project Agricultural',
    'Oakdale Irrigation, Woodward Reservoir'
) ON CONFLICT (du_id) DO NOTHING;

-- 63_PA - Upper San Joaquin Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency
) VALUES (
    '63_PA', '63', 'SJR', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Upper San Joaquin Area Project Agricultural'
) ON CONFLICT (du_id) DO NOTHING;

-- 63_PR - Upper San Joaquin Area Project Refuge (Wildlife)
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '63_PR', '63', 'SJR', 0, 'Refuge', 'PR',
    'calsim3', FALSE, '1', '0',
    'Upper San Joaquin Area Project Refuge (Wildlife)',
    'Deadman Creek, Eastside Bypass'
) ON CONFLICT (du_id) DO NOTHING;

-- 64_PA - Fresno/Kings Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '64_PA', '64', 'TULARE', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Fresno/Kings Area Project Agricultural (Westlands, etc.)',
    'San Luis Canal, Mendota Pool, Fresno Slough'
) ON CONFLICT (du_id) DO NOTHING;

-- 65_PA - Kaweah/Tule Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency
) VALUES (
    '65_PA', '65', 'TULARE', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Kaweah/Tule Area Project Agricultural'
) ON CONFLICT (du_id) DO NOTHING;

-- 70_PA - Kern County Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency
) VALUES (
    '70_PA', '70', 'TULARE', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Kern County Area Project Agricultural'
) ON CONFLICT (du_id) DO NOTHING;

-- 71_PA - Delta-Mendota Canal Area Project Agricultural
INSERT INTO du_urban_entity (
    du_id, wba_id, hydrologic_region, dups, du_class, cs3_type,
    model_source, has_gis_data, sw, gw,
    community_agency, point_of_diversion
) VALUES (
    '71_PA', '71', 'TULARE', 0, 'Agricultural', 'PA',
    'calsim3', FALSE, '1', '1',
    'Delta-Mendota Canal Area Project Agricultural',
    'Delta-Mendota Canal, California Aqueduct'
) ON CONFLICT (du_id) DO NOTHING;

-- ============================================================================
-- SET AUDIT FIELDS
-- ============================================================================

\echo ''
\echo 'Setting audit fields for new records...'

UPDATE du_urban_entity SET
    is_active = TRUE,
    created_at = NOW(),
    created_by = 1,
    updated_at = NOW(),
    updated_by = 1
WHERE created_by IS NULL OR updated_at < NOW() - INTERVAL '1 minute';

-- ============================================================================
-- VERIFICATION
-- ============================================================================

\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'New records added:'
SELECT du_id, wba_id, hydrologic_region, cs3_type, du_class, community_agency
FROM du_urban_entity
WHERE du_id IN (
    '60N_PU1', '60S_PU', '61_PU1', '61_PU2', '64_PU', '65_PU', '70_PU1', '72_PU1',
    '50_PA1', '50_PA2', '60N_PA', '60S_PA', '61_PA', '63_PA', '63_PR', '64_PA', '65_PA', '70_PA', '71_PA'
)
ORDER BY du_id;

\echo ''
\echo 'Total record count by type:'
SELECT cs3_type, COUNT(*) as count
FROM du_urban_entity
GROUP BY cs3_type
ORDER BY cs3_type;

\echo ''
\echo 'Total records:'
SELECT COUNT(*) as total_records FROM du_urban_entity;

COMMIT;

\echo ''
\echo '✅ Missing du_urban_entity records added successfully'
