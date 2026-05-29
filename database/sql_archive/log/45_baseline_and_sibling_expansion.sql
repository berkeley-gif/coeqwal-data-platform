-- =============================================================================
-- MIGRATION: Baseline Resolution & Hydroclimate Sibling Expansion
-- =============================================================================
--
-- Adds sibling_group column, fixes baseline_scenario_id for existing scenarios,
-- inserts 2 inactive agency baselines (s0022, s0038), and 48 new scenarios
-- (24 CC50 + 24 CC95 siblings of the existing hist_adj set).
--
-- Source data: database/reference/COEQWAL_Completed_Scenario_Listing.xlsx
--   - HistHydro_20260223.csv
--   - CC50Hydro_20260225.csv
--   - CC95Hydro_20260323.csv
--   - coeqwal_cs3_scenario_listing - scenario_list.csv
-- =============================================================================

BEGIN;

-- =============================================================================
-- STEP 1: Add sibling_group column
-- =============================================================================

ALTER TABLE scenario ADD COLUMN IF NOT EXISTS sibling_group VARCHAR;
CREATE INDEX IF NOT EXISTS idx_scenario_sibling_group ON scenario (sibling_group);

-- =============================================================================
-- STEP 2: Insert inactive agency baselines (s0022, s0038)
-- =============================================================================

INSERT INTO scenario (short_code, run_name, is_active, name, short_description, hydroclimate_id, baseline_scenario_id, scenario_version_id, scenario_author_id, model_source_id)
VALUES
  ('s0022', 's0022_USBR24Alt2V1_adjHist_0413LU_woTUCP', FALSE,
   'USBR 2024 LTO Alt2V1',
   'Baseline - USBR 2024 LTO alternative Alt2V1 with DWR adjusted historical hydrology',
   2, NULL, 1, 3, 1),
  ('s0038', 's0038_USBRAlt3_DCRadjBL_DeltaOut65', FALSE,
   'USBR 2021 LTO Alt3',
   'USBR Alt3 run with DWR adjusted historical hydrology and 2004-2013 land use',
   2, NULL, 1, 3, 1);

-- =============================================================================
-- STEP 3: Fix existing hist_adj baseline_scenario_id values
-- =============================================================================

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0011')
WHERE short_code = 's0020';

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0022')
WHERE short_code = 's0023';

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0023')
WHERE short_code = 's0024';

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0031')
WHERE short_code = 's0033';

UPDATE scenario
SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0020'),
    hydroclimate_id = 2
WHERE short_code IN ('s0035', 's0036', 's0037');

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0038')
WHERE short_code IN ('s0039', 's0040', 's0041', 's0042');

UPDATE scenario SET baseline_scenario_id = id
WHERE short_code = 's0065';

-- =============================================================================
-- STEP 4: Set sibling_group for all 24 active hist_adj scenarios
-- =============================================================================

UPDATE scenario SET sibling_group = CASE short_code
  WHEN 's0011' THEN '1.2'
  WHEN 's0020' THEN '1.1'
  WHEN 's0021' THEN '1.3'
  WHEN 's0023' THEN '1.5'
  WHEN 's0024' THEN '1.4'
  WHEN 's0025' THEN '2.1'
  WHEN 's0026' THEN '2.2'
  WHEN 's0027' THEN '2.3'
  WHEN 's0028' THEN '2.4'
  WHEN 's0030' THEN '3.1'
  WHEN 's0031' THEN '3.4'
  WHEN 's0032' THEN '3.3'
  WHEN 's0033' THEN '3.5'
  WHEN 's0035' THEN '4.1'
  WHEN 's0036' THEN '4.2'
  WHEN 's0037' THEN '4.3'
  WHEN 's0039' THEN '5.4'
  WHEN 's0040' THEN '5.1'
  WHEN 's0041' THEN '5.2'
  WHEN 's0042' THEN '5.3'
  WHEN 's0044' THEN '6.1'
  WHEN 's0045' THEN '6.4'
  WHEN 's0046' THEN '3.2'
  WHEN 's0065' THEN '7.4'
END
WHERE short_code IN (
  's0011','s0020','s0021','s0023','s0024','s0025','s0026','s0027','s0028',
  's0030','s0031','s0032','s0033','s0035','s0036','s0037','s0039','s0040',
  's0041','s0042','s0044','s0045','s0046','s0065'
);

-- =============================================================================
-- STEP 5: Insert 24 CC50 scenarios (hydroclimate_id = 3)
-- Names copied from hist_adj siblings.
-- =============================================================================

INSERT INTO scenario (short_code, run_name, is_active, name, short_description, hydroclimate_id, baseline_scenario_id, scenario_version_id, scenario_author_id, model_source_id, sibling_group)
VALUES
  ('s0048', 's0048_adjBL_cc50_wTUCP', TRUE,
   'DWR Historical Adjusted Baseline with TUCPs',
   'DWR 2023 Baseline', 3, NULL, 1, 3, 1, '1.2'),
  ('s0047', 's0047_DCRcc50_2020LU_wTUCP', TRUE,
   'DWR Adjusted Hydrology with 2020 Land Use and TUCPs',
   'DWR 2023 Baseline with TUCPs & 2020 land use', 3, NULL, 1, 3, 1, '1.1'),
  ('s0049', 's0049_DCRadjBL_cc50_2020LU_woTUCP', TRUE,
   'DWR Adjusted Hydrology with 2020 Land Use without TUCPs',
   'DWR 2023 Baseline without TUCPs & 2020 land use', 3, NULL, 1, 3, 1, '1.3'),
  ('s0051', 's0051_USBR24Alt2V1_DCRcc50_2020LU_woTUCP', TRUE,
   'USBR 2024 LTO Alt2v1 with 2020 Land Use without TUCPs',
   'USBR 2024 Alt2V1 with 2020 land use', 3, NULL, 1, 3, 1, '1.5'),
  ('s0050', 's0050_USBR24Alt2V1_cc50_adjHist_2020LU_wTUCP', TRUE,
   'USBR 2024 LTO Alt2v1 with 2020 Land Use with TUCPs',
   'USBR 2024 Alt2V1 with 2020 land use & TUCPs', 3, NULL, 1, 3, 1, '1.4'),
  ('s0067', 's0067_SJVgwLimit_cc50__20LU_wTUCP', TRUE,
   'Limited San Joaquin GW Pumping with 2020 Land Use and TUCPs',
   'Limiting groundwater pumping in the San Joaquin', 3, NULL, 1, 3, 1, '2.1'),
  ('s0068', 's0068_SJVgwLimit_cc50_SGMALU_wTUCP', TRUE,
   'Reduced Ag Irrigated Acreage in SJV',
   'Reducing irrigated acreage in the San Joaquin', 3, NULL, 1, 3, 1, '2.2'),
  ('s0062', 's0062_CVgwLimit_cc50_2020LU_wTUCP', TRUE,
   'Limited Central Valley GW Pumping with 2020 Land Use and TUCPs',
   'Limiting groundwater pumping in the Central Valley', 3, NULL, 1, 3, 1, '2.3'),
  ('s0069', 's0069_CVgwLimit_cc50_SGMALU_wTUCP', TRUE,
   'CV-wide Reduced Irrigated Acreage for Improved Groundwater Sustainability',
   'Reducing irrigated acreage in the Central Valley', 3, NULL, 1, 3, 1, '2.4'),
  ('s0071', 's0071_DCRadjHist_cc50_2020LU_NoFlowReqt', TRUE,
   'Removal of Central Valley Minimum Flows',
   'Remove minimum flow requirements throughout CV', 3, NULL, 1, 3, 1, '3.1'),
  ('s0072', 's0072_DCRadjHist_cc50_2020LU_salmonflows', TRUE,
   'Salmon Flows',
   'Salmon-friendly flows on the Sac & SJR with Shasta storage protections', 3, NULL, 1, 3, 1, '3.4'),
  ('s0073', 's0073_DCRadjHist_cc50_CVgwLU_eflows', TRUE,
   'Functional Flows with Reduced Irrigated Acreage',
   'Functional flows for all sites with reduced irrigated acreage for CV', 3, NULL, 1, 3, 1, '3.3'),
  ('s0074', 's0074_DCRadjHist_cc50_CVgwLU_salmonflows', TRUE,
   'Salmon Flows with Reduced Irrigated Acreage',
   'Salmon-friendly flows on the Sac & SJR with Shasta storage protections and reduced irrigated acreage for CV', 3, NULL, 1, 3, 1, '3.5'),
  ('s0075', 's0075_DCRadjBL_cc50_2020LU_PriorityHHS', TRUE,
   'Prioritizing Health & Human Safety Deliveries to CWS',
   'Prioritizing health and human safety surface delivery levels to community water systems', 3, NULL, 1, 3, 1, '4.1'),
  ('s0076', 's0076_DCRadjBL_cc50_2020LU_PriorityFuncCWN', TRUE,
   'Prioritizing Functional Deliveries to CWS',
   'Prioritizing functional delivery levels to community water systems', 3, NULL, 1, 3, 1, '4.2'),
  ('s0077', 's0077_DCRadjBL_cc50_2020LU_PriorityFullCWN', TRUE,
   'Prioritizing Full Contract Deliveries to CWS',
   'Prioritizing full contract or demand levels to community water systems', 3, NULL, 1, 3, 1, '4.3'),
  ('s0078', 's0078_USBRAlt3_cc50_2020LU_DeltaOut65', TRUE,
   'USBR Alt3 with 65% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 65% unimpaired Delta outflow with upstream actions to protect storage', 3, NULL, 1, 3, 1, '5.4'),
  ('s0079', 's0079_USBRAlt3_cc50_2020LU_DeltaOut35', TRUE,
   'USBR Alt3 with 35% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 35% unimpaired Delta outflow with upstream actions to protect storage', 3, NULL, 1, 3, 1, '5.1'),
  ('s0080', 's0080_USBRAlt3_cc50_2020LU_DeltaOut45', TRUE,
   'USBR Alt3 with 45% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 45% unimpaired Delta outflow with upstream actions to protect storage', 3, NULL, 1, 3, 1, '5.2'),
  ('s0081', 's0081_USBRAlt3_cc50_2020LU_DeltaOut55', TRUE,
   'USBR Alt3 with 55% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 55% unimpaired Delta outflow with upstream actions to protect storage', 3, NULL, 1, 3, 1, '5.3'),
  ('s0082', 's0082_DCRadjHist_cc50_2020LU_ShastaCarryover20', TRUE,
   'Increase Shasta Carryover 20%',
   'Target an increase in Shasta carryover of 20% compared to s0020 baseline', 3, NULL, 1, 3, 1, '6.1'),
  ('s0083', 's0083_DCRadjBL_cc50_2020LU_RelaxFallX2', TRUE,
   'Remove Fall X2 Requirements',
   'Remove the fall X2 requirement in the Delta', 3, NULL, 1, 3, 1, '6.4'),
  ('s0084', 's0084_DCRadjHist_2020LU_EflowsV2', TRUE,
   'CV Functional Flows without Downstream and Delta Locations',
   'Variation of functional flows that removes the downstream Sac, SJR, and Delta flow requirements', 3, NULL, 1, 3, 1, '3.2'),
  ('s0085', 's0085_DWRadapt25_cc50_2020LU_DCP', TRUE,
   'Delta Conveyance Project - DWR 2025 Version',
   'An updated version of the Delta Conveyance Project with 2020 land use', 3, NULL, 1, 3, 1, '7.4');


UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0048')
WHERE short_code = 's0047';

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0047')
WHERE short_code IN ('s0049', 's0062', 's0067', 's0068', 's0069', 's0071', 's0072', 's0073', 's0075', 's0076', 's0077', 's0082', 's0083', 's0084');

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0051')
WHERE short_code = 's0050';

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0072')
WHERE short_code = 's0074';

UPDATE scenario SET baseline_scenario_id = id
WHERE short_code = 's0085';

-- =============================================================================
-- STEP 6: Insert 24 CC95 scenarios (hydroclimate_id = 4)
-- Same pattern: insert with NULL baselines, then set them.
-- =============================================================================

INSERT INTO scenario (short_code, run_name, is_active, name, short_description, hydroclimate_id, baseline_scenario_id, scenario_version_id, scenario_author_id, model_source_id, sibling_group)
VALUES
  ('s0057', 's0057_adjBL_cc95_wTUCP', TRUE,
   'DWR Historical Adjusted Baseline with TUCPs',
   'DWR 2023 Baseline', 4, NULL, 1, 3, 1, '1.2'),
  ('s0056', 's0056_DCRcc95_2020LU_wTUCP', TRUE,
   'DWR Adjusted Hydrology with 2020 Land Use and TUCPs',
   'DWR 2023 Baseline with TUCPs & 2020 land use', 4, NULL, 1, 3, 1, '1.1'),
  ('s0058', 's0058_DCRadjBL_cc95_2020LU_woTUCP', TRUE,
   'DWR Adjusted Hydrology with 2020 Land Use without TUCPs',
   'DWR 2023 Baseline without TUCPs & 2020 land use', 4, NULL, 1, 3, 1, '1.3'),
  ('s0060', 's0060_USBR24Alt2V1_adjBL_cc95_2020LU_woTUCP', TRUE,
   'USBR 2024 LTO Alt2v1 with 2020 Land Use without TUCPs',
   'USBR 2024 Alt2V1 with 2020 land use', 4, NULL, 1, 3, 1, '1.5'),
  ('s0059', 's0059_USBR24Alt2V1_adjBL_cc95_2020LU_woTUCP', TRUE,
   'USBR 2024 LTO Alt2v1 with 2020 Land Use with TUCPs',
   'USBR 2024 Alt2V1 with 2020 land use & TUCPs', 4, NULL, 1, 3, 1, '1.4'),
  ('s0087', 's0087_CC95_SJVgwLimit_20LU_wTUCP', TRUE,
   'Limited San Joaquin GW Pumping with 2020 Land Use and TUCPs',
   'Limiting groundwater pumping in the San Joaquin', 4, NULL, 1, 3, 1, '2.1'),
  ('s0088', 's0088_SJVgwLimit_cc95_SGMALU_wTUCP', TRUE,
   'Reduced Ag Irrigated Acreage in SJV',
   'Reducing irrigated acreage in the San Joaquin', 4, NULL, 1, 3, 1, '2.2'),
  ('s0063', 's0063_CVgwLimit_cc95_2020LU_wTUCP', TRUE,
   'Limited Central Valley GW Pumping with 2020 Land Use and TUCPs',
   'Limiting groundwater pumping in the Central Valley', 4, NULL, 1, 3, 1, '2.3'),
  ('s0089', 's0089_CVgwLimit_cc95_SGMALU_wTUCP', TRUE,
   'CV-wide Reduced Irrigated Acreage for Improved Groundwater Sustainability',
   'Reducing irrigated acreage in the Central Valley', 4, NULL, 1, 3, 1, '2.4'),
  ('s0091', 's0091_DCRadjHist_cc95_2020LU_NoFlowReqt', TRUE,
   'Removal of Central Valley Minimum Flows',
   'Remove minimum flow requirements throughout CV', 4, NULL, 1, 3, 1, '3.1'),
  ('s0092', 's0092_DCRadjHist_cc95_2020LU_salmonflows', TRUE,
   'Salmon Flows',
   'Salmon-friendly flows on the Sac & SJR with Shasta storage protections', 4, NULL, 1, 3, 1, '3.4'),
  ('s0093', 's0093_DCRadjHist_cc95_CVgwLU_eflows', TRUE,
   'Functional Flows with Reduced Irrigated Acreage',
   'Functional flows for all sites with reduced irrigated acreage for CV', 4, NULL, 1, 3, 1, '3.3'),
  ('s0094', 's0094_DCRadjHist_cc95_CVgwLU_salmonflows', TRUE,
   'Salmon Flows with Reduced Irrigated Acreage',
   'Salmon-friendly flows on the Sac & SJR with Shasta storage protections and reduced irrigated acreage for CV', 4, NULL, 1, 3, 1, '3.5'),
  ('s0095', 's0095_DCRadjBL_cc95_2020LU_PriorityHHS', TRUE,
   'Prioritizing Health & Human Safety Deliveries to CWS',
   'Prioritizing health and human safety surface delivery levels to community water systems', 4, NULL, 1, 3, 1, '4.1'),
  ('s0096', 's0096_DCRadjBL_cc95_2020LU_PriorityFuncCWN', TRUE,
   'Prioritizing Functional Deliveries to CWS',
   'Prioritizing functional delivery levels to community water systems', 4, NULL, 1, 3, 1, '4.2'),
  ('s0097', 's0097_DCRadjBL_cc95_2020LU_PriorityFullCWN', TRUE,
   'Prioritizing Full Contract Deliveries to CWS',
   'Prioritizing full contract or demand levels to community water systems', 4, NULL, 1, 3, 1, '4.3'),
  ('s0098', 's0098_USBRAlt3_cc95_2020LU_DeltaOut65', TRUE,
   'USBR Alt3 with 65% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 65% unimpaired Delta outflow with upstream actions to protect storage', 4, NULL, 1, 3, 1, '5.4'),
  ('s0099', 's0099_USBRAlt3_cc95_2020LU_DeltaOut35', TRUE,
   'USBR Alt3 with 35% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 35% unimpaired Delta outflow with upstream actions to protect storage', 4, NULL, 1, 3, 1, '5.1'),
  ('s0100', 's0100_USBRAlt3_cc95_2020LU_DeltaOut45', TRUE,
   'USBR Alt3 with 45% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 45% unimpaired Delta outflow with upstream actions to protect storage', 4, NULL, 1, 3, 1, '5.2'),
  ('s0101', 's0101_USBRAlt3_cc95_2020LU_DeltaOut55', TRUE,
   'USBR Alt3 with 55% Unimpaired Delta Outflow',
   'USBR Alt3 with 2020 land use - 55% unimpaired Delta outflow with upstream actions to protect storage', 4, NULL, 1, 3, 1, '5.3'),
  ('s0102', 's0102_DCRadjHist_cc95_2020LU_ShastaCarryover20', TRUE,
   'Increase Shasta Carryover 20%',
   'Target an increase in Shasta carryover of 20% compared to s0020 baseline', 4, NULL, 1, 3, 1, '6.1'),
  ('s0103', 's0103_DCRadjBL_cc95_2020LU_RelaxFallX2', TRUE,
   'Remove Fall X2 Requirements',
   'Remove the fall X2 requirement in the Delta', 4, NULL, 1, 3, 1, '6.4'),
  ('s0104', 's0104_DCRadjHist_cc95_2020LU_EflowsV2', TRUE,
   'CV Functional Flows without Downstream and Delta Locations',
   'Variation of functional flows that removes the downstream Sac, SJR, and Delta flow requirements', 4, NULL, 1, 3, 1, '3.2'),
  ('s0105', 's0105_DWRadapt25_cc95_2020LU_DCP', TRUE,
   'Delta Conveyance Project - DWR 2025 Version',
   'An updated version of the Delta Conveyance Project with 2020 land use', 4, NULL, 1, 3, 1, '7.4');


UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0057')
WHERE short_code = 's0056';

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0056')
WHERE short_code IN ('s0058', 's0063', 's0087', 's0088', 's0089', 's0091', 's0092', 's0093', 's0095', 's0096', 's0097', 's0102', 's0103', 's0104');

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0060')
WHERE short_code = 's0059';

UPDATE scenario SET baseline_scenario_id = (SELECT id FROM scenario WHERE short_code = 's0092')
WHERE short_code = 's0094';

UPDATE scenario SET baseline_scenario_id = id
WHERE short_code = 's0105';

-- =============================================================================
-- STEP 7: Enrich hydroclimate metadata
-- =============================================================================

UPDATE hydroclimate SET
  description = 'DWR adjusted historical hydrology (DWR_DCR2023_AdjHydrology): historical time series (Oct 1921 - Sep 2021) modified such that the more distant past is statistically similar to the recent past, an effort to correct for climate warming that has already occurred in the 20th century.',
  simple_description = 'Adjusted historical hydrology correcting for 20th century climate warming'
WHERE id = 2;

UPDATE hydroclimate SET
  description = 'DWR hydroclimate future based on an estimated 50% chance of exceedance (DWR_DCR2023_cc50), i.e. a median future climate condition, based on DWR''s analysis of quasi-probabilities or "levels of concern." Represents a 1.5C temperature increase and 3% precipitation decrease over Central Valley inflow basins projected to 2043.',
  simple_description = 'Median future climate (50% exceedance): +1.5C, -3% precipitation',
  projection_year = 2043
WHERE id = 3;

UPDATE hydroclimate SET
  description = 'DWR hydroclimate future based on an estimated 95% chance of exceedance (DWR_DCR2023_cc95), i.e. the hydroclimate condition yielding 8-river flows that would be expected to be exceeded (a better/wetter outcome) 95% of the time, based on DWR''s analysis of quasi-probabilities or "levels of concern." The most extreme (hot/dry) hydroclimate future DWR created for the 2023 DCR. Represents a 1.8C temperature increase and 9% precipitation decrease over Central Valley inflow basins projected to 2043.',
  simple_description = 'Extreme hot/dry future climate (95% exceedance): +1.8C, -9% precipitation',
  projection_year = 2043
WHERE id = 4;

-- =============================================================================
-- STEP 8: Copy tag/theme/assumption/operation links for cc50/cc95
-- Uses sibling_group to map new scenarios to their hist_adj sibling's links.
-- =============================================================================

INSERT INTO scenario_tag_link (scenario_id, tag_id, created_by, updated_by)
SELECT new_s.id, stl.tag_id, stl.created_by, stl.updated_by
FROM scenario new_s
JOIN scenario hist_s ON hist_s.sibling_group = new_s.sibling_group
                    AND hist_s.hydroclimate_id = 2
                    AND hist_s.is_active = TRUE
JOIN scenario_tag_link stl ON stl.scenario_id = hist_s.id
WHERE new_s.hydroclimate_id IN (3, 4)
ON CONFLICT (scenario_id, tag_id) DO NOTHING;

INSERT INTO theme_scenario_link (theme_id, scenario_id, created_by, updated_by)
SELECT tsl.theme_id, new_s.id, tsl.created_by, tsl.updated_by
FROM scenario new_s
JOIN scenario hist_s ON hist_s.sibling_group = new_s.sibling_group
                    AND hist_s.hydroclimate_id = 2
                    AND hist_s.is_active = TRUE
JOIN theme_scenario_link tsl ON tsl.scenario_id = hist_s.id
WHERE new_s.hydroclimate_id IN (3, 4)
ON CONFLICT (theme_id, scenario_id) DO NOTHING;

INSERT INTO scenario_key_assumption_link (scenario_id, assumption_id, created_by, updated_by)
SELECT new_s.id, skal.assumption_id, skal.created_by, skal.updated_by
FROM scenario new_s
JOIN scenario hist_s ON hist_s.sibling_group = new_s.sibling_group
                    AND hist_s.hydroclimate_id = 2
                    AND hist_s.is_active = TRUE
JOIN scenario_key_assumption_link skal ON skal.scenario_id = hist_s.id
WHERE new_s.hydroclimate_id IN (3, 4)
ON CONFLICT (scenario_id, assumption_id) DO NOTHING;

INSERT INTO scenario_key_operation_link (scenario_id, operation_id, created_by, updated_by)
SELECT new_s.id, skol.operation_id, skol.created_by, skol.updated_by
FROM scenario new_s
JOIN scenario hist_s ON hist_s.sibling_group = new_s.sibling_group
                    AND hist_s.hydroclimate_id = 2
                    AND hist_s.is_active = TRUE
JOIN scenario_key_operation_link skol ON skol.scenario_id = hist_s.id
WHERE new_s.hydroclimate_id IN (3, 4)
ON CONFLICT (scenario_id, operation_id) DO NOTHING;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

\echo ''
\echo '=== VERIFICATION ==='
\echo ''

\echo 'Total scenarios by hydroclimate and active status:'
SELECT
  h.short_code AS hydroclimate,
  s.is_active,
  COUNT(*) AS count
FROM scenario s
LEFT JOIN hydroclimate h ON h.id = s.hydroclimate_id
GROUP BY h.short_code, s.is_active
ORDER BY h.short_code, s.is_active;

\echo ''
\echo 'Scenarios with NULL baseline_scenario_id (should only be roots):'
SELECT short_code, hydroclimate_id, is_active, sibling_group
FROM scenario
WHERE baseline_scenario_id IS NULL
ORDER BY short_code;

\echo ''
\echo 'Scenarios with NULL hydroclimate_id (should be zero):'
SELECT short_code FROM scenario WHERE hydroclimate_id IS NULL;

\echo ''
\echo 'Sibling group coverage (should be 24 groups, each with 3 members):'
SELECT sibling_group, COUNT(*) AS members,
  string_agg(short_code, ', ' ORDER BY short_code) AS scenarios
FROM scenario
WHERE sibling_group IS NOT NULL
GROUP BY sibling_group
ORDER BY sibling_group;

\echo ''
\echo 'Baseline chain for hist_adj scenarios:'
SELECT s.short_code, s.baseline_scenario_id, b.short_code AS baseline_short_code
FROM scenario s
LEFT JOIN scenario b ON b.id = s.baseline_scenario_id
WHERE s.hydroclimate_id = 2
ORDER BY s.short_code;

\echo ''
\echo 'Link table counts for new scenarios:'
SELECT 'scenario_tag_link' AS link_table, COUNT(*) FROM scenario_tag_link WHERE scenario_id IN (SELECT id FROM scenario WHERE hydroclimate_id IN (3,4))
UNION ALL
SELECT 'theme_scenario_link', COUNT(*) FROM theme_scenario_link WHERE scenario_id IN (SELECT id FROM scenario WHERE hydroclimate_id IN (3,4))
UNION ALL
SELECT 'scenario_key_assumption_link', COUNT(*) FROM scenario_key_assumption_link WHERE scenario_id IN (SELECT id FROM scenario WHERE hydroclimate_id IN (3,4))
UNION ALL
SELECT 'scenario_key_operation_link', COUNT(*) FROM scenario_key_operation_link WHERE scenario_id IN (SELECT id FROM scenario WHERE hydroclimate_id IN (3,4));

COMMIT;
