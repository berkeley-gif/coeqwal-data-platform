-- ADD NEW SCENARIOS s0107-s0156
-- inserts new records into scenario table for new set of scenarios: s0107 through s0156
--
-- Run from the repository root:
--   psql $DATABASE_URL -f database/scripts/sql/add_s0107-s0156_scenarios.sql
--
-- Created by Eric Lehmer 5/29/2026

BEGIN;

INSERT INTO scenario (
    short_code, run_name, is_active,
    hydroclimate_id, hydroclimate_sibling, scenario_version_id,
    scenario_author_id, model_source_id
) VALUES
    ('s0107', 's0107_adjBL_cqlTAI_wTUCP', FALSE, 5, 's0011', 1, 3, 1),
    ('s0108', 's0108_DCRbl_cqlTAI_2020LU_wTUCP', FALSE, 5, 's0020', 1, 3, 1),
    ('s0109', 's0109_DCRbl_cqlTAI_2020LU_woTUCP', FALSE, 5, 's0021', 1, 3, 1),
    ('s0110', 's0110_USBR24Alt2V1_cqlTAI_2020LU_woTUCP', FALSE, 5, 's0023', 1, 3, 1),
    ('s0111', 's0111_USBR24Alt2V1_cqlTAI_2020LU_wTUCP', FALSE, 5, 's0024', 1, 3, 1),
    ('s0112', 's0112_SJVgwLimit_cqlTAI_2020LU_wTUCP', FALSE, 5, 's0025', 1, 3, 1),
    ('s0113', 's0113_SJVgwLimit_cqlTAI_SGMALU_wTUCP', FALSE, 5, 's0026', 1, 3, 1),
    ('s0114', 's0114_CVgwLimit_cqlTAI_2020LU_wTUCP', FALSE, 5, 's0027', 1, 3, 1),
    ('s0115', 's0115_CVgwLimit_cqlTAI_SGMALU_wTUCP', FALSE, 5, 's0028', 1, 3, 1),
    ('s0117', 's0117_DCRadjHist_cqlTAI_2020LU_NoFlowReqt', FALSE, 5, 's0030', 1, 3, 1),
    ('s0118', 's0118_DCRadjHist_cqlTAI_2020LU_salmonflows', FALSE, 5, 's0031', 1, 3, 1),
    ('s0119', 's0119_DCRadjHist_cqlTAI_CVgwLU_eflows', FALSE, 5, 's0032', 1, 3, 1),
    ('s0120', 's0120_DCRadjHist_cqlTAI_CVgwLU_salmonflows', FALSE, 5, 's0033', 1, 3, 1),
    ('s0121', 's0121_DCRadjBL_cqlTAI_2020LU_PriorityHHS', FALSE, 5, 's0035', 1, 3, 1),
    ('s0123', 's0123_DCRadjBL_cqlTAI_2020LU_PriorityFullCWN', FALSE, 5, 's0037', 1, 3, 1),
    ('s0124', 's0124_USBRAlt3_cqlTAI_2020LU_DeltaOut65', FALSE, 5, 's0039', 1, 3, 1),
    ('s0125', 's0125_USBRAlt3_cqlTAI_2020LU_DeltaOut35', FALSE, 5, 's0040', 1, 3, 1),
    ('s0126', 's0126_USBRAlt3_cqlTAI_2020LU_DeltaOut45', FALSE, 5, 's0041', 1, 3, 1),
    ('s0127', 's0127_USBRAlt3_cqlTAI_2020LU_DeltaOut55', FALSE, 5, 's0042', 1, 3, 1),
    ('s0128', 's0128_DCRadjHist_TAIESM1_2020LU_ShastaCarryover20', FALSE, 5, 's0044', 1, 3, 1),
    ('s0129', 's0129_DCRadjBL_cqlTAI_2020LU_RelaxFallX2', FALSE, 5, 's0045', 1, 3, 1),
    ('s0130', 's0130_DCRadjHist_cqlTAI_2020LU_EflowsV2', FALSE, 5, 's0046', 1, 3, 1),
    ('s0131', 's0131_DWRadapt25_cqlTAI_2020LU_DCP', FALSE, 5, 's0065', 1, 3, 1),
    ('s0134', 's0134_DCRbl_cqlECV_2020LU_wTUCP', FALSE, 7, 's0020', 1, 3, 1),
    ('s0141', 's0141_CVgwLimit_cqlECV_SGMALU_wTUCP', FALSE, 7, 's0028', 1, 3, 1),
    ('s0150', 's0150_USBRAlt3_cqlECV_2020LU_DeltaOut65', FALSE, 7, 's0039', 1, 3, 1),
    ('s0156', 's0156_DCRadjHist_cqlECV_2020LU_EflowsV2', FALSE, 7, 's0046', 1, 3, 1);

COMMIT;
