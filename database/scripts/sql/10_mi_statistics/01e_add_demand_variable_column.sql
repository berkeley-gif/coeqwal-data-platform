-- ADD DEMAND_VARIABLE COLUMN TO DU_URBAN_VARIABLE
-- Adds the demand variable mapping to enable calculation of % demand met
--
-- Demand variables come from the separate *_demand.csv files which contain:
--   - UD_*           : Urban demand variables (from SV DSS file)
--   - DEM_D_*_PMI    : SWP contractor demand variables (from DV DSS file)
--
-- Prerequisites:
--   1. Run 01d_load_du_urban_variable.sql first
--
-- Run with: psql -f 01e_add_demand_variable_column.sql

\echo ''
\echo '========================================='
\echo 'ADDING DEMAND_VARIABLE COLUMN'
\echo '========================================='

-- ============================================
-- ADD COLUMN IF NOT EXISTS
-- ============================================
\echo ''
\echo 'Adding demand_variable column...'

ALTER TABLE du_urban_variable
ADD COLUMN IF NOT EXISTS demand_variable VARCHAR(100);

COMMENT ON COLUMN du_urban_variable.demand_variable IS 'CalSim demand variable from *_demand.csv. UD_* for urban demands, DEM_D_*_PMI for SWP contractors.';

-- ============================================
-- CATEGORY 1: WBA-Style Units
-- Demand variable pattern: UD_{du_id}
-- ============================================
\echo ''
\echo 'Updating Category 1: WBA-style units (UD_{du_id})...'

UPDATE du_urban_variable SET demand_variable = 'UD_02_PU' WHERE du_id = '02_PU';
UPDATE du_urban_variable SET demand_variable = 'UD_02_SU' WHERE du_id = '02_SU';
UPDATE du_urban_variable SET demand_variable = 'UD_03_PU1' WHERE du_id = '03_PU1';
UPDATE du_urban_variable SET demand_variable = 'UD_03_PU2' WHERE du_id = '03_PU2';
UPDATE du_urban_variable SET demand_variable = 'UD_03_SU' WHERE du_id = '03_SU';
UPDATE du_urban_variable SET demand_variable = 'UD_11_NU1' WHERE du_id = '11_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_12_NU1' WHERE du_id = '12_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_13_NU1' WHERE du_id = '13_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_16_PU' WHERE du_id = '16_PU';
UPDATE du_urban_variable SET demand_variable = 'UD_20_NU1' WHERE du_id = '20_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_21_PU' WHERE du_id = '21_PU';
UPDATE du_urban_variable SET demand_variable = 'UD_24_NU1' WHERE du_id = '24_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_24_NU2' WHERE du_id = '24_NU2';
UPDATE du_urban_variable SET demand_variable = 'UD_24_NU3' WHERE du_id = '24_NU3';
UPDATE du_urban_variable SET demand_variable = 'UD_25_PU' WHERE du_id = '25_PU';
UPDATE du_urban_variable SET demand_variable = 'UD_26N_NU1' WHERE du_id = '26N_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_26N_NU2' WHERE du_id = '26N_NU2';
UPDATE du_urban_variable SET demand_variable = 'UD_26N_NU3' WHERE du_id = '26N_NU3';
UPDATE du_urban_variable SET demand_variable = 'UD_26N_PU1' WHERE du_id = '26N_PU1';
UPDATE du_urban_variable SET demand_variable = 'UD_26N_PU2' WHERE du_id = '26N_PU2';
UPDATE du_urban_variable SET demand_variable = 'UD_26N_PU3' WHERE du_id = '26N_PU3';
UPDATE du_urban_variable SET demand_variable = 'UD_26S_NU1' WHERE du_id = '26S_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_26S_NU3' WHERE du_id = '26S_NU3';
UPDATE du_urban_variable SET demand_variable = 'UD_26S_PU1' WHERE du_id = '26S_PU1';
UPDATE du_urban_variable SET demand_variable = 'UD_26S_PU2' WHERE du_id = '26S_PU2';
UPDATE du_urban_variable SET demand_variable = 'UD_26S_PU4' WHERE du_id = '26S_PU4';
UPDATE du_urban_variable SET demand_variable = 'UD_26S_PU5' WHERE du_id = '26S_PU5';
UPDATE du_urban_variable SET demand_variable = 'UD_26S_PU6' WHERE du_id = '26S_PU6';
UPDATE du_urban_variable SET demand_variable = 'UD_50_PU' WHERE du_id = '50_PU';
UPDATE du_urban_variable SET demand_variable = 'UD_60N_NU1' WHERE du_id = '60N_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_60N_NU2' WHERE du_id = '60N_NU2';
UPDATE du_urban_variable SET demand_variable = 'UD_60S_NU1' WHERE du_id = '60S_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_61_NU2' WHERE du_id = '61_NU2';
UPDATE du_urban_variable SET demand_variable = 'UD_62_NU' WHERE du_id = '62_NU';
UPDATE du_urban_variable SET demand_variable = 'UD_90_PU' WHERE du_id = '90_PU';
UPDATE du_urban_variable SET demand_variable = 'UD_ELDID_NU1' WHERE du_id = 'ELDID_NU1';
UPDATE du_urban_variable SET demand_variable = 'UD_ELDID_NU2' WHERE du_id = 'ELDID_NU2';
UPDATE du_urban_variable SET demand_variable = 'UD_ELDID_NU3' WHERE du_id = 'ELDID_NU3';
UPDATE du_urban_variable SET demand_variable = 'UD_GDPUD_NU' WHERE du_id = 'GDPUD_NU';
UPDATE du_urban_variable SET demand_variable = 'UD_PCWA3' WHERE du_id = 'PCWA3';

\echo '  Updated 40 WBA-style units'

-- ============================================
-- CATEGORY 2: Groundwater-Only Units
-- These have UD_* demand variables in the demand CSV
-- ============================================
\echo ''
\echo 'Updating Category 2: Groundwater-only units...'

UPDATE du_urban_variable SET demand_variable = 'UD_71_NU' WHERE du_id = '71_NU';
UPDATE du_urban_variable SET demand_variable = 'UD_72_NU' WHERE du_id = '72_NU';
UPDATE du_urban_variable SET demand_variable = 'UD_72_PU' WHERE du_id = '72_PU';

\echo '  Updated 3 groundwater-only units'

-- ============================================
-- CATEGORY 3: SWP Contractor Deliveries
-- Demand variable pattern: DEM_D_{arc}_PMI
-- ============================================
\echo ''
\echo 'Updating Category 3: SWP contractor units (DEM_D_*_PMI)...'

UPDATE du_urban_variable SET demand_variable = 'DEM_D_CSB038_OBISPO_PMI' WHERE du_id = 'CSB038';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_CSB103_BRBRA_PMI' WHERE du_id = 'CSB103';
UPDATE du_urban_variable SET demand_variable = 'DEM_VNTRA_PMI' WHERE du_id = 'CSTIC';  -- Ventura County WPD demand
UPDATE du_urban_variable SET demand_variable = 'DEM_D_ESB324_AVEK_PMI' WHERE du_id = 'ESB324';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_ESB347_PLMDL_PMI' WHERE du_id = 'ESB347';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_ESB414_BRDNO_PMI' WHERE du_id = 'ESB414';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_ESB415_GABRL_PMI' WHERE du_id = 'ESB415';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_ESB420_GRGNO_PMI' WHERE du_id = 'ESB420';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_SBA029_ACWD_PMI' WHERE du_id = 'SBA029';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_SBA036_SCVWD_PMI' WHERE du_id = 'SBA036';
UPDATE du_urban_variable SET demand_variable = 'DEM_D_SBA036_SCVWD_PMI' WHERE du_id = 'SCVWD';  -- Alias

\echo '  Updated 11 SWP contractor units'

-- ============================================
-- CATEGORY 4: Named Localities
-- Demand variable pattern: UD_{du_id} for most
-- ============================================
\echo ''
\echo 'Updating Category 4: Named localities...'

UPDATE du_urban_variable SET demand_variable = 'UD_BNCIA' WHERE du_id = 'BNCIA';
UPDATE du_urban_variable SET demand_variable = 'DEMAND_D420_' WHERE du_id = 'CCWD';  -- CCWD uses DEMAND_D420_
UPDATE du_urban_variable SET demand_variable = 'UD_GRSVL' WHERE du_id = 'GRSVL';
-- MWD uses TABLEA_CONTRACT_MWD which is manually added in demand CSV
UPDATE du_urban_variable SET demand_variable = 'TABLEA_CONTRACT_MWD' WHERE du_id = 'MWD';
UPDATE du_urban_variable SET demand_variable = 'UD_NAPA' WHERE du_id = 'NAPA';
UPDATE du_urban_variable SET demand_variable = 'UD_NAPA2' WHERE du_id = 'NAPA2';
UPDATE du_urban_variable SET demand_variable = 'UD_PLMAS' WHERE du_id = 'PLMAS';
UPDATE du_urban_variable SET demand_variable = 'UD_SUISN' WHERE du_id = 'SUISN';
UPDATE du_urban_variable SET demand_variable = 'UD_TVAFB' WHERE du_id = 'TVAFB';
UPDATE du_urban_variable SET demand_variable = 'UD_VLLJO' WHERE du_id = 'VLLJO';
UPDATE du_urban_variable SET demand_variable = 'UD_WLDWD' WHERE du_id = 'WLDWD';
UPDATE du_urban_variable SET demand_variable = 'UD_AMADR_NU' WHERE du_id = 'AMADR';
UPDATE du_urban_variable SET demand_variable = 'UD_AMCYN' WHERE du_id = 'AMCYN';
UPDATE du_urban_variable SET demand_variable = 'UD_ANTOC' WHERE du_id = 'ANTOC';
UPDATE du_urban_variable SET demand_variable = 'UD_FRFLD' WHERE du_id = 'FRFLD';

\echo '  Updated 15 named locality units'

-- ============================================
-- CATEGORY 5: Units without demand data
-- ============================================
\echo ''
\echo 'Updating Category 5: Units without demand data...'

UPDATE du_urban_variable SET demand_variable = 'UD_JLIND' WHERE du_id = 'JLIND';  -- Manually added in demand CSV
UPDATE du_urban_variable SET demand_variable = 'D_ANC000_ANGLS_DEM' WHERE du_id = 'UPANG';  -- Manually added demand for Angels Camp area

\echo '  Updated 2 units'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Total units with demand_variable mapped:'
SELECT COUNT(*) as with_demand FROM du_urban_variable WHERE demand_variable IS NOT NULL;

\echo ''
\echo 'Total units WITHOUT demand_variable:'
SELECT COUNT(*) as without_demand FROM du_urban_variable WHERE demand_variable IS NULL;

\echo ''
\echo 'Units still missing demand_variable (may need manual review):'
SELECT du_id, delivery_variable, variable_type, notes
FROM du_urban_variable
WHERE demand_variable IS NULL
ORDER BY du_id;

\echo ''
\echo 'NOTE: If any units show as missing, check the demand CSV for:'
\echo '  - UD_{du_id} pattern'
\echo '  - DEM_D_{arc}_PMI pattern'
\echo '  - CALCULATED or MANUAL-ADD columns'

\echo ''
\echo 'Sample mappings with demand_variable:'
SELECT du_id, delivery_variable, demand_variable, shortage_variable, variable_type
FROM du_urban_variable
WHERE demand_variable IS NOT NULL
ORDER BY du_id
LIMIT 20;

\echo ''
\echo 'Demand variable patterns:'
SELECT 
    CASE 
        WHEN demand_variable LIKE 'UD_%' THEN 'UD_* (Urban Demand)'
        WHEN demand_variable LIKE 'DEM_D_%' THEN 'DEM_D_*_PMI (SWP Contractor)'
        WHEN demand_variable = 'TABLEA_CONTRACT_MWD' THEN 'MWD Table A Contract'
        ELSE 'Other'
    END as pattern,
    COUNT(*) as count
FROM du_urban_variable
WHERE demand_variable IS NOT NULL
GROUP BY 1
ORDER BY count DESC;

\echo ''
\echo '✅ demand_variable column added and populated successfully'
