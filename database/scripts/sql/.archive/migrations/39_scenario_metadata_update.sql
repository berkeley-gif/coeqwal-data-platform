-- Migration 39: Update scenario metadata from the scenario description document
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/39_scenario_metadata_update.sql
--
-- Updates run_name and name fields for all 25 existing scenarios.
-- Sets is_active = FALSE for s0029.
-- Does NOT insert new scenarios (s0002, s0010 are not in the DB).

BEGIN;

ALTER TABLE scenario DISABLE TRIGGER USER;

-- ── s0011 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0011_adjBL_wTUCP',
    name              = 'DWR Historical Adjusted Baseline with TUCPs',
    short_description = 'DWR DCR2023 baseline with adjusted historical hydrology and TUCP actions active.',
    scenario_author_id = 3
WHERE short_code = 's0011';

-- ── s0020 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0020_DCRadjBL_2020LU_wTUCP',
    name              = 'DWR Adjusted Hydrology with 2020 Land Use and TUCPs',
    short_description = 'DWR DCR2023 baseline with adjusted historical hydrology, 2020 LandIQ land use, and TUCP actions active.',
    scenario_author_id = 3
WHERE short_code = 's0020';

-- ── s0021 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0021_DCRadjBL_2020LU_woTUCP',
    name              = 'DWR Adjusted Hydrology with 2020 Land Use without TUCPs',
    short_description = 'Same as s0020 but with TUCP actions deactivated.',
    scenario_author_id = 3
WHERE short_code = 's0021';

-- ── s0022 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0022_USBR24Alt2V1_adjHist_0413LU_woTUCP',
    name              = 'USBR 2024 LTO Alt2v1 with 2004-2013 Land Use without TUCPs',
    short_description = 'USBR 2024 LTO proposed action with DWR adjusted hydrology and conventional 2004-2013 land use.',
    scenario_author_id = 3
WHERE short_code = 's0022';

-- ── s0023 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0023_USBR24Alt2V1_adjBL_2020LU_woTUCP',
    name              = 'USBR 2024 LTO Alt2v1 with 2020 Land Use without TUCPs',
    short_description = 'USBR 2024 LTO proposed action with DWR adjusted hydrology and 2020 LandIQ land use.',
    scenario_author_id = 3
WHERE short_code = 's0023';

-- ── s0024 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0024_USBR24Alt2V1_adjHist_2020LU_wTUCP',
    name              = 'USBR 2024 LTO Alt2v1 with 2020 Land Use with TUCPs',
    short_description = 'USBR 2024 LTO proposed action with 2020 LandIQ land use and TUCP actions active.',
    scenario_author_id = 3
WHERE short_code = 's0024';

-- ── s0025 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0025_SJVgwLimit_2020LU_wTUCP_v0.2',
    name              = 'Limited San Joaquin GW Pumping with 2020 Land Use and TUCPs',
    short_description = 'Groundwater pumping limits for San Joaquin Valley demand units based on sustainable levels.',
    scenario_author_id = 3
WHERE short_code = 's0025';

-- ── s0026 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0026_SJVgwLimit_SGMALU_wTUCP',
    name              = 'Reduced Ag Irrigated Acreage in SJV',
    short_description = 'Reduction in irrigated acreage in the San Joaquin Valley to reduce long-term groundwater decline.',
    scenario_author_id = 3
WHERE short_code = 's0026';

-- ── s0027 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0027_CVgwLimit_2020LU_wTUCP_v0.2',
    name              = 'Limited Central Valley GW Pumping with 2020 Land Use and TUCPs',
    short_description = 'Groundwater pumping limits for entire Central Valley demand units based on sustainable levels.',
    scenario_author_id = 3
WHERE short_code = 's0027';

-- ── s0028 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0028_CVgwLimit_SGMALU_wTUCP',
    name              = 'CV-wide Reduced Irrigated Acreage for Improved Groundwater Sustainability',
    short_description = 'Reduced irrigated acreage across the entire Central Valley for improved groundwater sustainability.',
    scenario_author_id = 3
WHERE short_code = 's0028';

-- ── s0029 (deactivated) ─────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0029_DCRadjHist_2020LU_eflowsV1_v0.2_20251014',
    name              = 'Functional Flows on Tributaries and Delta (eflows)',
    short_description = 'Functional flow requirements at 17 locations including tributaries and Delta outflow.',
    is_active         = FALSE,
    scenario_author_id = 3
WHERE short_code = 's0029';

-- ── s0030 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0030_DCRadjHist_2020LU_NoFlowReqt',
    name              = 'Removal of Central Valley Minimum Flows',
    short_description = 'Removes minimum flow requirements on CV rivers and streams; Delta outflow requirements maintained.',
    scenario_author_id = 3
WHERE short_code = 's0030';

-- ── s0031 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0031_DCRadjHist_2020LU_salmonflows',
    name              = 'Salmon Flows',
    short_description = 'Sacramento River flows assigned to improve salmon survival.',
    scenario_author_id = 3
WHERE short_code = 's0031';

-- ── s0032 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0032_DCRadjHist_2020LU_eflows_aggw',
    name              = 'Functional Flows with Reduced Irrigated Acreage',
    short_description = 'Combines functional flow requirements of s0029 with reduced irrigated acreage of s0028.',
    scenario_author_id = 3
WHERE short_code = 's0032';

-- ── s0033 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0033_DCRadjHist_2020LU_salmonflows_aggw',
    name              = 'Salmon Flows with Reduced Irrigated Acreage',
    short_description = 'Combines salmon flow requirements with reduced irrigated acreage for improved groundwater.',
    scenario_author_id = 3
WHERE short_code = 's0033';

-- ── s0035 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0035_DCRadjBL_2020LU_PriorityHHS',
    name              = 'Prioritizing Health & Human Safety Deliveries to CWS',
    short_description = 'Prioritizes surface water allocation to M&I contractors at health and human safety levels.',
    scenario_author_id = 3
WHERE short_code = 's0035';

-- ── s0036 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0036_DCRadjBL_2020LU_PriorityFuncCWN',
    name              = 'Prioritizing Functional Deliveries to CWS',
    short_description = 'Prioritizes surface water allocation to M&I contractors at 70% of contract entitlement.',
    scenario_author_id = 3
WHERE short_code = 's0036';

-- ── s0037 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0037_DCRadjBL_2020LU_PriorityFullCWN',
    name              = 'Prioritizing Full Contract Deliveries to CWS',
    short_description = 'Prioritizes surface water allocation to M&I contractors at full contract entitlement levels.',
    scenario_author_id = 3
WHERE short_code = 's0037';

-- ── s0039 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0039_USBRAlt3_2020LU_DeltaOut65',
    name              = 'USBR Alt3 with 65% Unimpaired Delta Outflow',
    short_description = 'USBR Alt3 with 65% unimpaired Delta outflow requirement (Dec-May) and upstream actions.',
    scenario_author_id = 3
WHERE short_code = 's0039';

-- ── s0040 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0040_USBRAlt3_2020LU_DeltaOut35',
    name              = 'USBR Alt3 with 35% Unimpaired Delta Outflow',
    short_description = 'USBR Alt3 with 35% unimpaired Delta outflow requirement (Dec-May).',
    scenario_author_id = 3
WHERE short_code = 's0040';

-- ── s0041 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0041_USBRAlt3_2020LU_DeltaOut45',
    name              = 'USBR Alt3 with 45% Unimpaired Delta Outflow',
    short_description = 'USBR Alt3 with 45% unimpaired Delta outflow requirement (Dec-May).',
    scenario_author_id = 3
WHERE short_code = 's0041';

-- ── s0042 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0042_USBRAlt3_2020LU_DeltaOut55',
    name              = 'USBR Alt3 with 55% Unimpaired Delta Outflow',
    short_description = 'USBR Alt3 with 55% unimpaired Delta outflow requirement (Dec-May).',
    scenario_author_id = 3
WHERE short_code = 's0042';

-- ── s0044 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0044_DCRadjHist_2020LU_ShastaCarryOver20',
    name              = 'Increase Shasta Carryover 20%',
    short_description = 'CVP allocations reduced in spring to increase Shasta carryover by 20% relative to s0020.',
    scenario_author_id = 3
WHERE short_code = 's0044';

-- ── s0045 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0045_DCRadjBL_2020LU_RelaxFallX2',
    name              = 'Remove Fall X2 Requirements',
    short_description = 'Removes fall X2 salinity and flow requirements (Aug-Oct) from biological opinions.',
    scenario_author_id = 3
WHERE short_code = 's0045';

-- ── s0046 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0046_DCRadjHist_2020LU_EflowsV2',
    name              = 'CV Functional Flows without Downstream and Delta Locations',
    short_description = 'Functional flows for CV tributary locations but excluding downstream Sac/SJ and Delta outflow.',
    scenario_author_id = 3
WHERE short_code = 's0046';

-- ── s0065 ────────────────────────────────────────────────────────────
UPDATE scenario SET
    run_name          = 's0065_DWRadapt25_2020LU_DCP',
    name              = 'Delta Conveyance Project - DWR 2025 Version',
    short_description = 'DWR 2025 climate adaptation model with Delta Conveyance Project (6000 cfs NDD) and voluntary agreements.',
    scenario_author_id = 3
WHERE short_code = 's0065';

ALTER TABLE scenario ENABLE TRIGGER USER;

COMMIT;

-- ── Verification ─────────────────────────────────────────────────────

SELECT 'scenario_metadata' AS check, short_code, run_name, name, is_active
FROM scenario ORDER BY short_code;

SELECT 'inactive_check' AS check, short_code, is_active
FROM scenario WHERE is_active = FALSE;

SELECT 'update_counts' AS check,
       count(*) AS total,
       count(*) FILTER (WHERE name IS NOT NULL) AS with_name,
       count(*) FILTER (WHERE short_description IS NOT NULL) AS with_short_desc,
       count(*) FILTER (WHERE run_name IS NOT NULL) AS with_run_name
FROM scenario;

\echo
\echo '39 SCENARIO METADATA UPDATE COMPLETE'
\echo '======================================'
