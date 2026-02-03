-- LOAD DU_URBAN_VARIABLE SEED DATA
-- Populates variable mappings for the 71 canonical CWS demand units
--
-- Variable mapping source: Analysis of s0020_coeqwal_calsim_output.csv
-- See: etl/pipelines/CWS/all_scenarios_tier_matrix.csv for canonical list
--
-- Prerequisites:
--   1. Run 01_create_du_urban_entity.sql
--   2. Run 01b_load_du_urban_entity_from_s3.sql  
--   3. Run 01c_create_du_urban_variable.sql
--
-- Run with: psql -f 01d_load_du_urban_variable.sql

\echo ''
\echo '========================================='
\echo 'LOADING DU_URBAN_VARIABLE SEED DATA'
\echo '========================================='

-- ============================================
-- ENSURE REQUIRED DU_IDS EXIST
-- ============================================
\echo ''
\echo 'Ensuring all canonical du_ids exist in du_urban_entity...'

-- Insert 72_PU if it doesn't exist (entity table has 72_PU2 but CalSim uses 72_PU)
INSERT INTO du_urban_entity (du_id, wba_id, hydrologic_region, du_class, cs3_type, model_source, has_gis_data)
VALUES ('72_PU', '72', 'SJR', 'Urban', 'PU', 'calsim3', FALSE)
ON CONFLICT (du_id) DO NOTHING;

-- Insert JLIND if it doesn't exist (canonical unit - Calaveras County WD area)
INSERT INTO du_urban_entity (du_id, wba_id, hydrologic_region, du_class, cs3_type, model_source, has_gis_data)
VALUES ('JLIND', NULL, 'SJR', 'Urban', 'NU', 'tier_matrix', FALSE)
ON CONFLICT (du_id) DO NOTHING;

-- Insert UPANG if it doesn't exist (canonical unit - Union PUD area)
INSERT INTO du_urban_entity (du_id, wba_id, hydrologic_region, du_class, cs3_type, model_source, has_gis_data)
VALUES ('UPANG', NULL, 'SJR', 'Urban', 'NU', 'tier_matrix', FALSE)
ON CONFLICT (du_id) DO NOTHING;

\echo '  Ensured 72_PU, JLIND, UPANG exist'

-- ============================================
-- CLEAR EXISTING DATA
-- ============================================
\echo ''
\echo 'Clearing existing data...'

TRUNCATE TABLE du_urban_delivery_arc CASCADE;
TRUNCATE TABLE du_urban_variable CASCADE;

-- ============================================
-- CATEGORY 1: WBA-Style Units with DL_* delivery variables
-- These have well-defined DL_* (total delivery) and SHRTG_* (shortage) vars
-- ============================================
\echo ''
\echo 'Inserting Category 1: WBA-style units with DL_* delivery...'

INSERT INTO du_urban_variable (du_id, delivery_variable, shortage_variable, variable_type, requires_sum, notes) VALUES
-- Sacramento Region (02-03)
('02_PU', 'DL_02_PU', 'SHRTG_02_PU', 'DL', FALSE, 'Zone 02 Project Urban'),
('02_SU', 'DL_02_SU', 'SHRTG_02_SU', 'DL', FALSE, 'Zone 02 Settlement Urban'),
('03_PU1', 'DL_03_PU1', 'SHRTG_03_PU1', 'DL', FALSE, 'Zone 03 Project Urban 1'),
('03_PU2', 'DL_03_PU2', 'SHRTG_03_PU2', 'DL', FALSE, 'Zone 03 Project Urban 2'),
('03_SU', 'DL_03_SU', 'SHRTG_03_SU', 'DL', FALSE, 'Zone 03 Settlement Urban'),
-- Feather/Yuba Region (11-16)
('11_NU1', 'DL_11_NU1', 'SHRTG_11_NU1', 'DL', FALSE, 'Zone 11 Non-project Urban 1'),
('12_NU1', 'DL_12_NU1', 'SHRTG_12_NU1', 'DL', FALSE, 'Zone 12 Non-project Urban 1'),
('13_NU1', 'DL_13_NU1', 'SHRTG_13_NU1', 'DL', FALSE, 'Zone 13 Non-project Urban 1'),
('16_PU', 'DL_16_PU', 'SHRTG_16_PU', 'DL', FALSE, 'Zone 16 Project Urban'),
-- Sacramento Metro Region (20-26)
('20_NU1', 'DL_20_NU1', 'SHRTG_20_NU1', 'DL', FALSE, 'Zone 20 Non-project Urban 1'),
('21_PU', 'DL_21_PU', 'SHRTG_21_PU', 'DL', FALSE, 'Zone 21 Project Urban'),
('24_NU1', 'DL_24_NU1', 'SHRTG_24_NU1', 'DL', FALSE, 'Zone 24 Non-project Urban 1'),
('24_NU2', 'DL_24_NU2', 'SHRTG_24_NU2', 'DL', FALSE, 'Zone 24 Non-project Urban 2'),
('24_NU3', 'DL_24_NU3', 'SHRTG_24_NU3', 'DL', FALSE, 'Zone 24 Non-project Urban 3'),
('25_PU', 'DL_25_PU', 'SHRTG_25_PU', 'DL', FALSE, 'Zone 25 Project Urban'),
('26N_NU1', 'DL_26N_NU1', 'SHRTG_26N_NU1', 'DL', FALSE, 'Zone 26N Non-project Urban 1'),
('26N_NU2', 'DL_26N_NU2', 'SHRTG_26N_NU2', 'DL', FALSE, 'Zone 26N Non-project Urban 2'),
('26N_NU3', 'DL_26N_NU3', 'SHRTG_26N_NU3', 'DL', FALSE, 'Zone 26N Non-project Urban 3'),
('26N_PU1', 'DL_26N_PU1', 'SHRTG_26N_PU1', 'DL', FALSE, 'Zone 26N Project Urban 1'),
('26N_PU2', 'DL_26N_PU2', 'SHRTG_26N_PU2', 'DL', FALSE, 'Zone 26N Project Urban 2'),
('26N_PU3', 'DL_26N_PU3', 'SHRTG_26N_PU3', 'DL', FALSE, 'Zone 26N Project Urban 3'),
('26S_NU1', 'DL_26S_NU1', 'SHRTG_26S_NU1', 'DL', FALSE, 'Zone 26S Non-project Urban 1'),
('26S_NU3', 'DL_26S_NU3', 'SHRTG_26S_NU3', 'DL', FALSE, 'Zone 26S Non-project Urban 3'),
('26S_PU1', 'DL_26S_PU1', 'SHRTG_26S_PU1', 'DL', FALSE, 'Zone 26S Project Urban 1'),
('26S_PU2', 'DL_26S_PU2', 'SHRTG_26S_PU2', 'DL', FALSE, 'Zone 26S Project Urban 2'),
('26S_PU4', 'DL_26S_PU4', 'SHRTG_26S_PU4', 'DL', FALSE, 'Zone 26S Project Urban 4'),
('26S_PU5', 'DL_26S_PU5', 'SHRTG_26S_PU5', 'DL', FALSE, 'Zone 26S Project Urban 5'),
('26S_PU6', 'DL_26S_PU6', 'SHRTG_26S_PU6', 'DL', FALSE, 'Zone 26S Project Urban 6'),
-- San Joaquin Region (50-62)
('50_PU', 'DL_50_PU', 'GW_SHORT_50_PU', 'DL', FALSE, 'Zone 50 Project Urban - uses GW_SHORT'),
('60N_NU1', 'DL_60N_NU1', 'SHRTG_60N_NU1', 'DL', FALSE, 'Zone 60N Non-project Urban 1'),
('60N_NU2', 'DL_60N_NU2', 'GW_SHORT_60N_NU2', 'DL', FALSE, 'Zone 60N Non-project Urban 2 - uses GW_SHORT'),
('60S_NU1', 'DL_60S_NU1', 'GW_SHORT_60S_NU1', 'DL', FALSE, 'Zone 60S Non-project Urban 1 - uses GW_SHORT'),
('61_NU2', 'DL_61_NU2', 'GW_SHORT_61_NU2', 'DL', FALSE, 'Zone 61 Non-project Urban 2 - uses GW_SHORT'),
('62_NU', 'DL_62_NU', 'GW_SHORT_62_NU', 'DL', FALSE, 'Zone 62 Non-project Urban - uses GW_SHORT'),
-- Tulare Region (90)
('90_PU', 'DL_90_PU', 'GW_SHORT_90_PU', 'DL', FALSE, 'Zone 90 Project Urban - uses GW_SHORT'),
-- El Dorado (ELDID)
('ELDID_NU1', 'DL_ELDID_NU1', 'SHRTG_ELDID_NU1', 'DL', FALSE, 'El Dorado ID Non-project Urban 1'),
('ELDID_NU2', 'DL_ELDID_NU2', 'SHRTG_ELDID_NU2', 'DL', FALSE, 'El Dorado ID Non-project Urban 2'),
('ELDID_NU3', 'DL_ELDID_NU3', NULL, 'DL', FALSE, 'El Dorado ID Non-project Urban 3 - no shortage var found'),
-- Other named units with DL_*
('GDPUD_NU', 'DL_GDPUD_NU', 'SHRTG_GDPUD_NU', 'DL', FALSE, 'Georgetown Divide PUD Non-project Urban'),
('PCWA3', 'DL_PCWA3', 'SHRTG_PCWA3', 'DL', FALSE, 'Placer County Water Agency 3')
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    shortage_variable = EXCLUDED.shortage_variable,
    variable_type = EXCLUDED.variable_type,
    requires_sum = EXCLUDED.requires_sum,
    notes = EXCLUDED.notes,
    updated_at = NOW();

\echo '  Inserted 40 WBA-style units'

-- ============================================
-- CATEGORY 2: Groundwater-Only Units (no surface delivery)
-- These have ONLY groundwater pumping (GP_*) and GW_SHORT_* shortage
-- ============================================
\echo ''
\echo 'Inserting Category 2: Groundwater-only units...'

INSERT INTO du_urban_variable (du_id, delivery_variable, shortage_variable, variable_type, requires_sum, notes) VALUES
('71_NU', 'GP_71_NU', 'GW_SHORT_71_NU', 'GP', FALSE, 'Zone 71 Non-project Urban - groundwater only, no surface delivery'),
('72_NU', 'GP_72_NU', 'GW_SHORT_72_NU', 'GP', FALSE, 'Zone 72 Non-project Urban - groundwater only, no surface delivery'),
('72_PU', 'GP_72_PU', 'GW_SHORT_72_PU', 'GP', FALSE, 'Zone 72 Project Urban - groundwater only, no surface delivery')
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    shortage_variable = EXCLUDED.shortage_variable,
    variable_type = EXCLUDED.variable_type,
    requires_sum = EXCLUDED.requires_sum,
    notes = EXCLUDED.notes,
    updated_at = NOW();

\echo '  Inserted 3 groundwater-only units'

-- ============================================
-- CATEGORY 3: SWP Contractor Deliveries (D_*_PMI variables)
-- These use D_*_PMI for delivery and SHORT_D_*_PMI for shortage
-- ============================================
\echo ''
\echo 'Inserting Category 3: SWP contractor deliveries...'

INSERT INTO du_urban_variable (du_id, delivery_variable, shortage_variable, variable_type, requires_sum, notes) VALUES
('CSB038', 'D_CSB038_OBISPO_PMI', 'SHORT_D_CSB038_OBISPO_PMI', 'D', FALSE, 'San Luis Obispo - SWP contractor'),
('CSB103', 'D_CSB103_BRBRA_PMI', 'SHORT_D_CSB103_BRBRA_PMI', 'D', FALSE, 'Santa Barbara - SWP contractor'),
('CSTIC', 'D_CSTIC_VNTRA_PMI', 'SHORT_D_CSTIC_VNTRA_PMI', 'D', FALSE, 'Ventura (Castaic) - SWP contractor'),
('ESB324', 'D_ESB324_AVEK_PMI', 'SHORT_D_ESB324_AVEK_PMI', 'D', FALSE, 'Antelope Valley-East Kern - SWP contractor'),
('ESB347', 'D_ESB347_PLMDL_PMI', 'SHORT_D_ESB347_PLMDL_PMI', 'D', FALSE, 'Palmdale - SWP contractor'),
('ESB414', 'D_ESB414_BRDNO_PMI', 'SHORT_D_ESB414_BRDNO_PMI', 'D', FALSE, 'San Bernardino - SWP contractor'),
('ESB415', 'D_ESB415_GABRL_PMI', 'SHORT_D_ESB415_GABRL_PMI', 'D', FALSE, 'San Gabriel - SWP contractor'),
('ESB420', 'D_ESB420_GRGNO_PMI', 'SHORT_D_ESB420_GRGNO_PMI', 'D', FALSE, 'San Gorgonio - SWP contractor'),
('SBA029', 'D_SBA029_ACWD_PMI', 'SHORT_D_SBA029_ACWD_PMI', 'D', FALSE, 'Alameda County Water District - SWP contractor'),
('SBA036', 'D_SBA036_SCVWD_PMI', 'SHORT_D_SBA036_SCVWD_PMI', 'D', FALSE, 'Santa Clara Valley WD - SWP contractor'),
('SCVWD', 'D_SBA036_SCVWD_PMI', 'SHORT_D_SBA036_SCVWD_PMI', 'D', FALSE, 'Santa Clara Valley WD - alias for SBA036')
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    shortage_variable = EXCLUDED.shortage_variable,
    variable_type = EXCLUDED.variable_type,
    requires_sum = EXCLUDED.requires_sum,
    notes = EXCLUDED.notes,
    updated_at = NOW();

\echo '  Inserted 11 SWP contractor units'

-- ============================================
-- CATEGORY 4: Named localities with water treatment plant deliveries
-- These use D_WTP*_ or other arc patterns, some need summing
-- ============================================
\echo ''
\echo 'Inserting Category 4: Named localities with arc deliveries...'

-- Units with single delivery arc
INSERT INTO du_urban_variable (du_id, delivery_variable, shortage_variable, variable_type, requires_sum, notes) VALUES
('BNCIA', 'D_WTPBNC_BNCIA', NULL, 'D', FALSE, 'Benicia - water treatment plant delivery'),
('CCWD', 'D_CCC019_CCWD', NULL, 'D', FALSE, 'Contra Costa WD - single arc delivery'),
('GRSVL', 'D_CSD014_GRSVL', 'SHRTG_GRSVL', 'D', TRUE, 'Grass Valley - multiple arcs, has shortage var'),
('MWD', 'DEL_SWP_MWD', 'SHORT_SWP_MWD', 'D', FALSE, 'Metropolitan Water District - aggregate variable'),
('NAPA', 'D_BKR004_NBA009_NAPA_PMI', NULL, 'D', FALSE, 'Napa - NBA arc PMI delivery'),
('NAPA2', 'D_BRYSA_NAPA2', NULL, 'D', FALSE, 'Napa 2 - BRYSA arc delivery'),
('PLMAS', 'D_BGC002_PLMAS', NULL, 'D', FALSE, 'Plumas - BGC arc delivery'),
('SUISN', 'D_WTPCMT_SUISN', NULL, 'D', FALSE, 'Suisun - CMT water treatment plant'),
('TVAFB', 'D_WTPTAB_TVAFB', NULL, 'D', FALSE, 'Travis AFB - TAB water treatment plant'),
('VLLJO', 'D_WTPFMH_VLLJO', NULL, 'D', FALSE, 'Vallejo - FMH water treatment plant'),
('WLDWD', 'D_NWT013_WLDWD', 'SHRTG_WLDWD', 'D', FALSE, 'Wildwood - NWT arc delivery')
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    shortage_variable = EXCLUDED.shortage_variable,
    variable_type = EXCLUDED.variable_type,
    requires_sum = EXCLUDED.requires_sum,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- Units that require summing multiple arcs
INSERT INTO du_urban_variable (du_id, delivery_variable, shortage_variable, variable_type, requires_sum, notes) VALUES
('AMADR', 'D_TBAUD_AMADR_NU', NULL, 'D', TRUE, 'Amador - sum of TBAUD and TGC003 arcs'),
('AMCYN', 'D_WTPAMC_AMCYN', NULL, 'D', TRUE, 'American Canyon - sum of WTPAMC and WTPJAC arcs'),
('ANTOC', 'D_CCC007_ANTOC', NULL, 'D', TRUE, 'Antioch - sum of CCC007 and SJR006 arcs'),
('FRFLD', 'D_WTPNBR_FRFLD', NULL, 'D', TRUE, 'Fairfield - sum of WTPNBR and WTPWMN arcs')
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    shortage_variable = EXCLUDED.shortage_variable,
    variable_type = EXCLUDED.variable_type,
    requires_sum = EXCLUDED.requires_sum,
    notes = EXCLUDED.notes,
    updated_at = NOW();

\echo '  Inserted 15 named locality units'

-- ============================================
-- CATEGORY 5: Units NOT FOUND in CalSim output
-- These are in the canonical list but have no matching variables
-- ============================================
\echo ''
\echo 'Inserting Category 5: Units without CalSim variables...'

INSERT INTO du_urban_variable (du_id, delivery_variable, shortage_variable, variable_type, requires_sum, notes) VALUES
('JLIND', 'NOT_FOUND', NULL, 'MISSING', FALSE, 'No matching CalSim variables found in s0020 output'),
('UPANG', 'NOT_FOUND', NULL, 'MISSING', FALSE, 'No matching CalSim variables found in s0020 output')
ON CONFLICT (du_id) DO UPDATE SET
    delivery_variable = EXCLUDED.delivery_variable,
    shortage_variable = EXCLUDED.shortage_variable,
    variable_type = EXCLUDED.variable_type,
    requires_sum = EXCLUDED.requires_sum,
    notes = EXCLUDED.notes,
    updated_at = NOW();

\echo '  Inserted 2 units without CalSim variables'

-- ============================================
-- LOAD DELIVERY ARCS FOR MULTI-ARC UNITS
-- ============================================
\echo ''
\echo 'Inserting delivery arcs for multi-arc units...'

INSERT INTO du_urban_delivery_arc (du_id, delivery_arc, arc_order) VALUES
-- AMADR: sum of 2 arcs
('AMADR', 'D_TBAUD_AMADR_NU', 1),
('AMADR', 'D_TGC003_AMADR_NU', 2),
-- AMCYN: sum of 2 arcs
('AMCYN', 'D_WTPAMC_AMCYN', 1),
('AMCYN', 'D_WTPJAC_AMCYN', 2),
-- ANTOC: sum of 2 arcs
('ANTOC', 'D_CCC007_ANTOC', 1),
('ANTOC', 'D_SJR006_ANTOC', 2),
-- FRFLD: sum of 2 arcs
('FRFLD', 'D_WTPNBR_FRFLD', 1),
('FRFLD', 'D_WTPWMN_FRFLD', 2),
-- GRSVL: sum of 2 arcs
('GRSVL', 'D_CSD014_GRSVL', 1),
('GRSVL', 'D_DES006_GRSVL', 2)
ON CONFLICT (du_id, delivery_arc) DO NOTHING;

\echo '  Inserted 10 delivery arcs for 5 multi-arc units'

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Total records by variable_type:'
SELECT variable_type, COUNT(*) as count
FROM du_urban_variable
GROUP BY variable_type
ORDER BY count DESC;

\echo ''
\echo 'Units with shortage variables:'
SELECT COUNT(*) as with_shortage FROM du_urban_variable WHERE shortage_variable IS NOT NULL;

\echo ''
\echo 'Units requiring multi-arc sum:'
SELECT COUNT(*) as requires_sum FROM du_urban_variable WHERE requires_sum = TRUE;

\echo ''
\echo 'Delivery arcs count:'
SELECT COUNT(*) as arc_count FROM du_urban_delivery_arc;

\echo ''
\echo 'Sample mappings:'
SELECT du_id, delivery_variable, shortage_variable, variable_type, requires_sum
FROM du_urban_variable
ORDER BY du_id
LIMIT 15;

\echo ''
\echo '✅ du_urban_variable seed data loaded successfully'
