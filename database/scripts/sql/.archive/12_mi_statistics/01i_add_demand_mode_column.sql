-- ADD DEMAND_MODE COLUMN TO DU_URBAN_VARIABLE
-- Specifies how demand is computed for each DU, matching COEQWAL_V3 logic.
--
-- Demand modes:
--   'sv'           : Read UD_* from SV CSV (TAF). Default for WBA DUs.
--   'perdv'        : Compute (delivery+shortage)/PERDV_SWP_X from DV. For SWP contractors.
--   'table_a'      : MWD Table A contract constant (1911.5 TAF/yr).
--   'constant_cfs' : Flat CFS constant converted to TAF. Stores CFS value in demand_params.
--   'dv_sum'       : Sum of DV columns. Stores column list in demand_params.
--   NULL           : Demand unavailable (V3 uses complex formula not yet replicated).
--
-- demand_params stores JSON metadata needed for non-SV modes, e.g.:
--   perdv:        {"perdv_vars": ["PERDV_SWP_4"]}
--   constant_cfs: {"cfs_value": 25}
--   dv_sum:       {"columns": ["DEMAND_AMADR_CAWP_", "DEMAND_AMADR_AWS_"]}
--
-- Source of truth: COEQWAL_V3/notebooks/coeqwalpackage/DataExtraction.py
--
-- Run with: psql -f 01i_add_demand_mode_column.sql

\echo ''
\echo '========================================='
\echo 'ADDING DEMAND_MODE + DEMAND_PARAMS COLUMNS'
\echo '========================================='

ALTER TABLE du_urban_variable
ADD COLUMN IF NOT EXISTS demand_mode VARCHAR(20);

ALTER TABLE du_urban_variable
ADD COLUMN IF NOT EXISTS demand_params JSONB;

COMMENT ON COLUMN du_urban_variable.demand_mode IS 'How demand is computed: sv (SV CSV), perdv (PERDV division), table_a (MWD constant), constant_cfs, dv_sum, or NULL (unavailable).';
COMMENT ON COLUMN du_urban_variable.demand_params IS 'JSON parameters for demand computation (perdv_vars, cfs_value, columns, etc.)';

-- ============================================
-- CATEGORY 1 + 2: WBA + GW-only DUs → 'sv'
-- ============================================
\echo 'Setting demand_mode = sv for WBA and GW-only DUs...'

UPDATE du_urban_variable SET demand_mode = 'sv'
WHERE demand_variable LIKE 'UD_%'
  AND du_id NOT IN ('NAPA', 'AMCYN', 'AMADR', 'ANTOC', 'JLIND', 'PLMAS');

-- ============================================
-- CATEGORY 3: SWP Contractors → 'perdv'
-- PERDV mappings verified against V3 DataExtraction.py lines 1061-1333
-- ============================================
\echo 'Setting demand_mode = perdv for SWP contractors...'

UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_35"]}'::jsonb WHERE du_id = 'CSB038';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_34"]}'::jsonb WHERE du_id = 'CSB103';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_39", "PERDV_SWP_38"]}'::jsonb WHERE du_id = 'CSTIC';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_4"]}'::jsonb WHERE du_id = 'ESB324';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_29"]}'::jsonb WHERE du_id = 'ESB347';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_30"]}'::jsonb WHERE du_id = 'ESB414';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_31"]}'::jsonb WHERE du_id = 'ESB415';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_32"]}'::jsonb WHERE du_id = 'ESB420';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_3"]}'::jsonb WHERE du_id = 'SBA029';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_35"]}'::jsonb WHERE du_id = 'SBA036';
UPDATE du_urban_variable SET demand_mode = 'perdv',
  demand_params = '{"perdv_vars": ["PERDV_SWP_35"]}'::jsonb WHERE du_id = 'SCVWD';

-- ============================================
-- MWD → 'table_a'
-- V3 DataExtraction.py line 914: MWD_yearly_taf_value = 1911.5
-- ============================================
\echo 'Setting demand_mode = table_a for MWD...'

UPDATE du_urban_variable SET demand_mode = 'table_a' WHERE du_id = 'MWD';

-- ============================================
-- ANTOC → 'constant_cfs'
-- V3 DataExtraction.py line 915: ANTOC_monthly_cfs_value = 25
-- ============================================
\echo 'Setting demand_mode = constant_cfs for ANTOC...'

UPDATE du_urban_variable SET demand_mode = 'constant_cfs',
  demand_params = '{"cfs_value": 25}'::jsonb WHERE du_id = 'ANTOC';

-- ============================================
-- AMADR → 'dv_sum'
-- V3 DataExtraction.py line 1047-1059: DEMAND_AMADR_CAWP_ + DEMAND_AMADR_AWS_
-- ============================================
\echo 'Setting demand_mode = dv_sum for AMADR...'

UPDATE du_urban_variable SET demand_mode = 'dv_sum',
  demand_params = '{"columns": ["DEMAND_AMADR_CAWP_", "DEMAND_AMADR_AWS_"]}'::jsonb WHERE du_id = 'AMADR';

-- ============================================
-- Complex V3 formulas → NULL (not yet replicated)
-- These require monthly lookup tables or multi-step formulas from V3.
-- ============================================
\echo 'Leaving demand_mode NULL for complex V3 formulas...'

-- NAPA: V3 sums SWP_CO_NAPA + SWP_IN_NAPA + SWP_TA_NAPA + D_BKR004_NBA009_NAPA_PLS
-- AMCYN: V3 computes (D_BKR004_NBA009_NAPA - D_BKR004_NBA009_NAPA_PLS) * 0.179
-- JLIND: V3 uses monthly lookup table from Calaveras County
-- PLMAS: V3 uses monthly lookup table from UF_MFFDelivery
-- UPANG: V3 uses monthly CFS lookup table for Angels Camp
-- CCWD: V3 uses DEMAND_D420_ which is not in raw DV/SV
-- TLMNE, UNION: No variables found in DV or SV

UPDATE du_urban_variable SET demand_mode = NULL, demand_variable = NULL
WHERE du_id IN ('NAPA', 'AMCYN', 'JLIND', 'PLMAS', 'UPANG', 'CCWD', 'TLMNE', 'UNION');

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'VERIFICATION:'

\echo ''
\echo 'Demand mode distribution:'
SELECT COALESCE(demand_mode, '(null)') as mode, COUNT(*) as count
FROM du_urban_variable
GROUP BY demand_mode
ORDER BY count DESC;

\echo ''
\echo 'DUs with NULL demand_mode (V3 complex formulas not yet replicated):'
SELECT du_id, delivery_variable, notes
FROM du_urban_variable
WHERE demand_mode IS NULL
ORDER BY du_id;

\echo ''
\echo 'SWP contractors with PERDV params:'
SELECT du_id, demand_mode, demand_params
FROM du_urban_variable
WHERE demand_mode = 'perdv'
ORDER BY du_id;
