-- =============================================================================
-- Migration 13: Add standard and named operation_definition rows
-- =============================================================================
-- The scenario link tables require an explicit operation row for every
-- parameter category on every scenario. This migration adds the missing
-- "standard" and "named" operation variants so that each of the 9 operation
-- categories has a full vocabulary:
--
--   biops            +  biops_standard       (2019/2020 ITP for SWP)
--                    +  biops_modified_2019  (Modified versions of 2019)
--   tucp             +  tucp_not_active      (TUCPs not active)
--   gw_restrictions  +  gw_none              (No SGMA restrictions)
--   infrastructure   +  infra_standard       (Standard / no DCP)
--   flow             +  flow_standard        (Standard / existing min flows)
--   delta_outflow    +  delta_regs_standard  (Standard D1641 delta regs)
--   comm_delivery    +  alloc_standard       (Standard CVP/SWP allocation)
--                    +  cvp_settlement_to_zero  (CVP Settlement to 0%  - Alt3)
--
-- All rows: source = james_gilbert, created_by = 2, is_active = 1
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/13_add_standard_operations.sql
-- =============================================================================

BEGIN;


ALTER TABLE operation_definition DISABLE TRIGGER USER;

INSERT INTO operation_definition
    (id, short_code, name, short_title, simple_description, category,
     source, is_active, operation_version_id, created_by, updated_by,
     created_at, updated_at)
VALUES
    (20, 'biops_standard',
     '2019 Biological Opinions / 2020 ITP for SWP',
     '2019 BiOps / 2020 ITP',
     'Standard 2019 NMFS/USFWS Biological Opinions and 2020 SWP Incidental Take Permit  - baseline operating rules for the Central Valley water system',
     'biops', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (21, 'biops_modified_2019',
     'Modified versions of 2019 Biological Opinions (2020 ITP for SWP)',
     '2019 BiOps modified',
     'Scenario-specific modifications to the 2019 Biological Opinions and 2020 SWP ITP  - changes include modified Shasta storage targets, export limits, or relaxed Delta salinity requirements depending on the scenario',
     'biops', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (22, 'tucp_not_active',
     'Temporary Urgency Change Petitions and Orders not active',
     'TUCPs not active',
     'TUCP/TUCO actions are deactivated  - the variable simulateTUCP is set to 0 in the main WRESL file',
     'tucp', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (23, 'gw_none',
     'No groundwater restrictions',
     'No GW restrictions',
     'No SGMA-type groundwater pumping limits or irrigated acreage restrictions applied  - groundwater use follows baseline CalSim3 assumptions',
     'gw_restrictions', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (24, 'infra_standard',
     'Standard infrastructure  - no Delta Conveyance Project',
     'Standard infrastructure',
     'Existing Central Valley water infrastructure configuration with no Delta Conveyance Project (tunnel) modifications',
     'infrastructure', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (25, 'flow_standard',
     'Standard minimum flow requirements',
     'Standard flow requirements',
     'Existing minimum instream flow requirements on Sacramento and San Joaquin tributaries as defined in CalSim3 baseline  - no modifications to flow requirement switches',
     'flow', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (26, 'delta_regs_standard',
     'Standard Delta regulations  - D1641 requirements',
     'Standard Delta regs',
     'Standard Delta outflow and water quality requirements as set by State Water Board Decision D-1641 and applicable Biological Opinions  - no modified unimpaired flow percentage requirements',
     'delta_outflow', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (27, 'alloc_standard',
     'Standard CVP/SWP allocation priorities',
     'Standard allocation',
     'Standard Central Valley Project and State Water Project allocation rules and priorities  - no modifications to settlement contractor floors or carryover targets',
     'comm_delivery', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00'),

    (28, 'cvp_settlement_to_zero',
     'CVP Settlement contractor allocations reduced to 0%',
     'CVP Settlement to 0%',
     'CVP Settlement contractor allocations are allowed to be reduced to 0% as needed to support increased Delta outflow requirements  - used in USBR Alt3-based scenarios',
     'comm_delivery', 'james_gilbert', 1, 1, 2, 2,
     '2025-01-01 00:00:00+00', '2025-01-01 00:00:00+00');

ALTER TABLE operation_definition ENABLE TRIGGER USER;

SELECT setval('operation_definition_id_seq', (SELECT MAX(id) FROM operation_definition));


SELECT id, short_code, category, source, created_by, updated_by
FROM operation_definition
WHERE id >= 20
ORDER BY id;

COMMIT;
