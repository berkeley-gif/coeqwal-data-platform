-- ADD NEW SCENARIOS THAT WERE LEFT OFF INITIAL LIST
-- inserts new records into scenario table for new set of scenarios.
-- Additions needed as spreadsheet has changed since last addition.
--
-- Run from the repository root:
--   psql $DATABASE_URL -f database/scripts/sql/actions/3_add_additional_scenarios.sql
--
-- Created by Eric Lehmer 6/17/2026

BEGIN;

INSERT INTO scenario (
    short_code, run_name, is_active,
    hydroclimate_id, hydroclimate_sibling, scenario_version_id,
    scenario_author_id, model_source_id
) VALUES
    ('s0133', 's0133_adjBL_cqlECV_wTUCP', FALSE, 7, 's0011', 1, 3, 1),
    ('s0134', 's0134_DCRbl_cqlECV_2020LU_wTUCP', FALSE, 7, 's0020', 1, 3, 1),
    ('s0135', 's0135_DCRbl_cqlECV_2020LU_woTUCP', FALSE, 7, 's0021', 1, 3, 1),
    ('s0136', 's0136_USBR24Alt2V1_cqlECV_2020LU_woTUCP', FALSE, 7, 's0023', 1, 3, 1),
    ('s0137', 's0137_USBR24Alt2V1_cqlECV_2020LU_wTUCP', FALSE, 7, 's0024', 1, 3, 1),
    ('s0138', 's0138_SJVgwLimit_cqlECV_2020LU_wTUCP', FALSE, 7, 's0025', 1, 3, 1),
    ('s0139', 's0139_SJVgwLimit_cqlECV_SGMALU_wTUCP', FALSE, 7, 's0026', 1, 3, 1),
    ('s0140', 's0140_CVgwLimit_cqlECV_2020LU_wTUCP', FALSE, 7, 's0027', 1, 3, 1),
    ('s0143', 's0143_DCRadjHist_cqlECV_2020LU_NoFlowReqt', FALSE, 7, 's0030', 1, 3, 1),
    ('s0144', 's0144_DCRadjHist_cqlECV_2020LU_salmonflows', FALSE, 7, 's0031', 1, 3, 1),
    ('s0145', 's0145_DCRadjHist_cqlECV_CVgwLU_eflows', FALSE, 7, 's0032', 1, 3, 1),
    ('s0146', 's0146_DCRadjHist_cqlECV_CVgwLU_salmonflows', FALSE, 7, 's0033', 1, 3, 1),
    ('s0147', 's0147_DCRadjBL_cqlECV_2020LU_PriorityHHS', FALSE, 7, 's0035', 1, 3, 1),
    ('s0149', 's0149_DCRadjBL_cqlECV_2020LU_PriorityFullCWN', FALSE, 7, 's0037', 1, 3, 1),
    ('s0151', 's0151_USBRAlt3_cqlECV_2020LU_DeltaOut35', FALSE, 7, 's0040', 1, 3, 1),
    ('s0152', 's0152_USBRAlt3_cqlECV_2020LU_DeltaOut45', FALSE, 7, 's0041', 1, 3, 1),
    ('s0153', 's0153_USBRAlt3_cqlECV_2020LU_DeltaOut55', FALSE, 7, 's0042', 1, 3, 1),
    ('s0154', 's0154_DCRadjHist_cqlECV_2020LU_ShastaCarryover20', FALSE, 7, 's0044', 1, 3, 1),
    ('s0155', 's0155_DCRadjBL_cqlECV_2020LU_RelaxFallX2', FALSE, 7, 's0045', 1, 3, 1),
    ('s0157', 's0157_DWRadapt25_cqlECV_2020LU_DCP', FALSE, 7, 's0065', 1, 3, 1);

COMMIT;
