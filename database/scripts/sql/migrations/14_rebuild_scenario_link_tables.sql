-- =============================================================================
-- Migration 14: Rebuild scenario link tables from data document
-- =============================================================================
-- Completely replaces both link tables with the correct operation and
-- assumption links for all 22 active scenarios, derived from the COEQWAL
-- scenario data document (Feb 2026).
--
-- Key changes from the previous state:
--   • delta_outflow_45 (id=5) removed from all non-Alt3 scenarios.
--     It now only links to s0041 (45% unimpaired Delta outflow).
--     All other scenarios use delta_regs_standard (id=26).
--   • All 22 active scenarios have a full set of operation links (7–8 per
--     scenario covering biops, tucp, gw_restrictions, infrastructure, flow,
--     delta_outflow, and comm_delivery categories).
--   • s0011 gains its missing land use assumption (lu_2004_2013, id=2).
--   • All 2020 LandIQ links now use lu_2020_landiq (id=17) instead of
--     lu_updated (id=3); reduced-ag scenarios use lu_2020_landiq_reduced_ag
--     (id=18) instead of lu_proj_reductions (id=4).
--   • Placeholder scenarios s0035, s0036, s0037 remain unlinked.
--
-- Scenario id → scenario_id mapping (from scenario table):
--    1=s0011  2=s0020  3=s0021  4=s0023  5=s0024  6=s0025  7=s0027
--    8=s0029  9=s0026 10=s0028 11=s0030 12=s0031 13=s0032 14=s0033
--   15=s0039 16=s0040 17=s0041 18=s0042 19=s0044 20=s0045 21=s0046
--   22=s0065
--
-- Operation id → short_code mapping (operation_definition):
--    1=comm_delivery_HHS        2=comm_delivery_functional   3=comm_delivery_full
--    4=delta_outflow_35         5=delta_outflow_45           6=delta_outflow_55
--    7=delta_outflow_65         8=increase_Shasta_co         9=delta_salinity_standards
--   10=TUCP_TUCO               11=SGMA_SJV                  12=SGMA_SAC
--   13=SGMA_CV                 14=DCP_6000                  15=DCP_Bethany
--   16=no_min_flow             17=functional_flows          18=salmon_flows
--   19=biops_2024              20=biops_standard            21=biops_modified_2019
--   22=tucp_not_active         23=gw_none                   24=infra_standard
--   25=flow_standard           26=delta_regs_standard       27=alloc_standard
--   28=cvp_settlement_to_zero
--
-- Assumption id → short_code mapping (assumption_definition):
--    2=lu_2004_2013  17=lu_2020_landiq  18=lu_2020_landiq_reduced_ag
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/14_rebuild_scenario_link_tables.sql
-- =============================================================================

BEGIN;

-- ─── 1. Clear existing link tables ───────────────────────────────────────────

DELETE FROM scenario_key_operation_link;
DELETE FROM scenario_key_assumption_link;

-- ─── 2. Insert operation links ────────────────────────────────────────────────
-- Columns: (scenario_id, operation_id)
-- Categories per row (left to right): biops, tucp, gw_restrictions,
-- infrastructure, flow, delta_outflow, comm_delivery, [regulatory_salinity]
--
-- Legend for delta_outflow column:
--   delta_regs_standard (26) = D1641 standard
--   delta_outflow_35/45/55/65 (4/5/6/7) = Alt3 unimpaired flow variants
--
-- Legend for comm_delivery column:
--   alloc_standard (27) = standard CVP/SWP allocation
--   cvp_settlement_to_zero (28) = Alt3 (CVP Settlement reduced to 0%)
--   increase_Shasta_co (8) = s0044 (allocations reduced for Shasta carryover)

INSERT INTO scenario_key_operation_link (scenario_id, operation_id)
VALUES
    -- ── s0011 (id=1): DWR DCR2023 adj hist baseline with TUCPs ──────────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (1, 20), -- biops_standard
    (1, 10), -- TUCP_TUCO
    (1, 23), -- gw_none
    (1, 24), -- infra_standard
    (1, 25), -- flow_standard
    (1, 26), -- delta_regs_standard
    (1, 27), -- alloc_standard

    -- ── s0020 (id=2): DCR2023 adj hist + 2020 LU + TUCPs ───────────────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (2, 20), -- biops_standard
    (2, 10), -- TUCP_TUCO
    (2, 23), -- gw_none
    (2, 24), -- infra_standard
    (2, 25), -- flow_standard
    (2, 26), -- delta_regs_standard
    (2, 27), -- alloc_standard

    -- ── s0021 (id=3): DCR2023 adj hist + 2020 LU without TUCPs ─────────────
    -- BiOps: 2019/2020 ITP | TUCP: Not active | GW: None | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (3, 20), -- biops_standard
    (3, 22), -- tucp_not_active
    (3, 23), -- gw_none
    (3, 24), -- infra_standard
    (3, 25), -- flow_standard
    (3, 26), -- delta_regs_standard
    (3, 27), -- alloc_standard

    -- ── s0023 (id=4): USBR 2024 LTO Alt2V1 + adj hist + 2020 LU, no TUCPs ─
    -- BiOps: 2024 USBR PA | TUCP: Not active | GW: None | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (4, 19), -- biops_2024
    (4, 22), -- tucp_not_active
    (4, 23), -- gw_none
    (4, 24), -- infra_standard
    (4, 25), -- flow_standard
    (4, 26), -- delta_regs_standard
    (4, 27), -- alloc_standard

    -- ── s0024 (id=5): USBR 2024 LTO Alt2V1 + adj hist + 2020 LU, with TUCPs
    -- BiOps: 2024 USBR PA | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (5, 19), -- biops_2024
    (5, 10), -- TUCP_TUCO
    (5, 23), -- gw_none
    (5, 24), -- infra_standard
    (5, 25), -- flow_standard
    (5, 26), -- delta_regs_standard
    (5, 27), -- alloc_standard

    -- ── s0025 (id=6): SJV GW pumping limits + 2020 LU + TUCPs ─────────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: SGMA SJV | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (6, 20), -- biops_standard
    (6, 10), -- TUCP_TUCO
    (6, 11), -- SGMA_SJV
    (6, 24), -- infra_standard
    (6, 25), -- flow_standard
    (6, 26), -- delta_regs_standard
    (6, 27), -- alloc_standard

    -- ── s0027 (id=7): CV-wide GW pumping limits + 2020 LU + TUCPs ──────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: SGMA CV | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (7, 20), -- biops_standard
    (7, 10), -- TUCP_TUCO
    (7, 13), -- SGMA_CV
    (7, 24), -- infra_standard
    (7, 25), -- flow_standard
    (7, 26), -- delta_regs_standard
    (7, 27), -- alloc_standard

    -- ── s0029 (id=8): Functional flows on tribs + Delta, 2020 LU [inactive] ─
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: None | Infra: Std
    -- Flow: Functional flows | Delta: D1641 Std | Alloc: Std
    (8, 20), -- biops_standard
    (8, 10), -- TUCP_TUCO
    (8, 23), -- gw_none
    (8, 24), -- infra_standard
    (8, 17), -- functional_flows
    (8, 26), -- delta_regs_standard
    (8, 27), -- alloc_standard

    -- ── s0026 (id=9): Reduced SJV irrigated acreage for GW sustainability ───
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: SGMA SJV | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (9, 20), -- biops_standard
    (9, 10), -- TUCP_TUCO
    (9, 11), -- SGMA_SJV
    (9, 24), -- infra_standard
    (9, 25), -- flow_standard
    (9, 26), -- delta_regs_standard
    (9, 27), -- alloc_standard

    -- ── s0028 (id=10): CV-wide reduced irrigated acreage for GW ─────────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: SGMA CV | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (10, 20), -- biops_standard
    (10, 10), -- TUCP_TUCO
    (10, 13), -- SGMA_CV
    (10, 24), -- infra_standard
    (10, 25), -- flow_standard
    (10, 26), -- delta_regs_standard
    (10, 27), -- alloc_standard

    -- ── s0030 (id=11): Remove CV min flows; keep Delta outflow reqs ──────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: None | Infra: Std
    -- Flow: No min flow | Delta: D1641 Std (Delta reqs maintained) | Alloc: Std
    (11, 20), -- biops_standard
    (11, 10), -- TUCP_TUCO
    (11, 23), -- gw_none
    (11, 24), -- infra_standard
    (11, 16), -- no_min_flow
    (11, 26), -- delta_regs_standard
    (11, 27), -- alloc_standard

    -- ── s0031 (id=12): Salmon-friendly flow requirements ────────────────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: None | Infra: Std
    -- Flow: Salmon flows | Delta: D1641 Std | Alloc: Std
    (12, 20), -- biops_standard
    (12, 10), -- TUCP_TUCO
    (12, 23), -- gw_none
    (12, 24), -- infra_standard
    (12, 18), -- salmon_flows
    (12, 26), -- delta_regs_standard
    (12, 27), -- alloc_standard

    -- ── s0032 (id=13): Functional flows + reduced CV-wide ag acreage ─────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: SGMA CV (LU) | Infra: Std
    -- Flow: Functional flows | Delta: D1641 Std | Alloc: Std
    (13, 20), -- biops_standard
    (13, 10), -- TUCP_TUCO
    (13, 13), -- SGMA_CV
    (13, 24), -- infra_standard
    (13, 17), -- functional_flows
    (13, 26), -- delta_regs_standard
    (13, 27), -- alloc_standard

    -- ── s0033 (id=14): Salmon flows + reduced CV-wide ag acreage ────────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: SGMA CV (LU) | Infra: Std
    -- Flow: Salmon flows | Delta: D1641 Std | Alloc: Std
    (14, 20), -- biops_standard
    (14, 10), -- TUCP_TUCO
    (14, 13), -- SGMA_CV
    (14, 24), -- infra_standard
    (14, 18), -- salmon_flows
    (14, 26), -- delta_regs_standard
    (14, 27), -- alloc_standard

    -- ── s0039 (id=15): USBR Alt3 + 65% unimpaired Delta outflow ─────────────
    -- BiOps: Modified 2019 | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: 65% unimpaired | Alloc: CVP Settlement to 0%
    (15, 21), -- biops_modified_2019
    (15, 10), -- TUCP_TUCO
    (15, 23), -- gw_none
    (15, 24), -- infra_standard
    (15, 25), -- flow_standard
    (15,  7), -- delta_outflow_65
    (15, 28), -- cvp_settlement_to_zero

    -- ── s0040 (id=16): USBR Alt3 + 35% unimpaired Delta outflow ─────────────
    -- BiOps: Modified 2019 | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: 35% unimpaired | Alloc: CVP Settlement to 0%
    (16, 21), -- biops_modified_2019
    (16, 10), -- TUCP_TUCO
    (16, 23), -- gw_none
    (16, 24), -- infra_standard
    (16, 25), -- flow_standard
    (16,  4), -- delta_outflow_35
    (16, 28), -- cvp_settlement_to_zero

    -- ── s0041 (id=17): USBR Alt3 + 45% unimpaired Delta outflow ─────────────
    -- BiOps: Modified 2019 | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: 45% unimpaired | Alloc: CVP Settlement to 0%
    (17, 21), -- biops_modified_2019
    (17, 10), -- TUCP_TUCO
    (17, 23), -- gw_none
    (17, 24), -- infra_standard
    (17, 25), -- flow_standard
    (17,  5), -- delta_outflow_45
    (17, 28), -- cvp_settlement_to_zero

    -- ── s0042 (id=18): USBR Alt3 + 55% unimpaired Delta outflow ─────────────
    -- BiOps: Modified 2019 | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: 55% unimpaired | Alloc: CVP Settlement to 0%
    (18, 21), -- biops_modified_2019
    (18, 10), -- TUCP_TUCO
    (18, 23), -- gw_none
    (18, 24), -- infra_standard
    (18, 25), -- flow_standard
    (18,  6), -- delta_outflow_55
    (18, 28), -- cvp_settlement_to_zero

    -- ── s0044 (id=19): Increase Shasta carryover target by 20% ─────────────
    -- BiOps: Modified 2019 | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: CVP Settlement reduced (carryover)
    (19, 21), -- biops_modified_2019
    (19, 10), -- TUCP_TUCO
    (19, 23), -- gw_none
    (19, 24), -- infra_standard
    (19, 25), -- flow_standard
    (19, 26), -- delta_regs_standard
    (19,  8), -- increase_Shasta_co (allocation is non-standard; captured by this)

    -- ── s0045 (id=20): Remove fall X2 salinity requirement ──────────────────
    -- BiOps: Modified 2019 | TUCP: Active | GW: None | Infra: Std
    -- Flow: Std | Delta: D1641 Std | Alloc: Std | Reg. Salinity: Relax Fall X2
    (20, 21), -- biops_modified_2019
    (20, 10), -- TUCP_TUCO
    (20, 23), -- gw_none
    (20, 24), -- infra_standard
    (20, 25), -- flow_standard
    (20, 26), -- delta_regs_standard
    (20, 27), -- alloc_standard
    (20,  9), -- delta_salinity_standards (relax Fall X2 — extra link in regulatory_salinity)

    -- ── s0046 (id=21): Remove Delta + lower Sac/SJR flow requirements ───────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: None | Infra: Std
    -- Flow: No min flow | Delta: no delta outflow reqs (absent) | Alloc: Std
    -- Note: delta_regs not linked — both CV min flows and Delta outflow reqs removed
    (21, 20), -- biops_standard
    (21, 10), -- TUCP_TUCO
    (21, 23), -- gw_none
    (21, 24), -- infra_standard
    (21, 16), -- no_min_flow (CV + lower river flows removed)
    (21, 27), -- alloc_standard

    -- ── s0065 (id=22): DWR 2025 DCP scenario, 2020 LU ───────────────────────
    -- BiOps: 2019/2020 ITP | TUCP: Active | GW: None | Infra: DCP 6000 CFS
    -- Flow: Std | Delta: D1641 Std | Alloc: Std
    (22, 20), -- biops_standard
    (22, 10), -- TUCP_TUCO
    (22, 23), -- gw_none
    (22, 14), -- DCP_6000
    (22, 25), -- flow_standard
    (22, 26), -- delta_regs_standard
    (22, 27); -- alloc_standard

-- ─── 3. Insert assumption links ───────────────────────────────────────────────
-- Columns: (scenario_id, assumption_id)
-- One land use assumption per scenario.
-- Uses lu_2020_landiq (id=17) for all 2020 LandIQ scenarios;
-- lu_2020_landiq_reduced_ag (id=18) for reduced-acreage SGMA scenarios;
-- lu_2004_2013 (id=2) for s0011.
-- Deprecated: lu_updated (id=3) and lu_proj_reductions (id=4) are no longer
-- linked; they remain in assumption_definition for historical reference.

INSERT INTO scenario_key_assumption_link (scenario_id, assumption_id)
VALUES
    (1,  2),  -- s0011 → lu_2004_2013       (2004–2013 average land use)
    (2, 17),  -- s0020 → lu_2020_landiq
    (3, 17),  -- s0021 → lu_2020_landiq
    (4, 17),  -- s0023 → lu_2020_landiq
    (5, 17),  -- s0024 → lu_2020_landiq
    (6, 17),  -- s0025 → lu_2020_landiq
    (7, 17),  -- s0027 → lu_2020_landiq
    (8, 17),  -- s0029 → lu_2020_landiq
    (9, 18),  -- s0026 → lu_2020_landiq_reduced_ag (SJV SGMA land use)
   (10, 18),  -- s0028 → lu_2020_landiq_reduced_ag (CV-wide SGMA land use)
   (11, 17),  -- s0030 → lu_2020_landiq
   (12, 17),  -- s0031 → lu_2020_landiq
   (13, 18),  -- s0032 → lu_2020_landiq_reduced_ag (functional flows + SGMA LU)
   (14, 18),  -- s0033 → lu_2020_landiq_reduced_ag (salmon flows + SGMA LU)
   (15, 17),  -- s0039 → lu_2020_landiq
   (16, 17),  -- s0040 → lu_2020_landiq
   (17, 17),  -- s0041 → lu_2020_landiq
   (18, 17),  -- s0042 → lu_2020_landiq
   (19, 17),  -- s0044 → lu_2020_landiq
   (20, 17),  -- s0045 → lu_2020_landiq
   (21, 17),  -- s0046 → lu_2020_landiq
   (22, 17);  -- s0065 → lu_2020_landiq

-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT
    s.scenario_id,
    COUNT(kol.operation_id) AS operation_count,
    COUNT(kal.assumption_id) AS assumption_count
FROM scenario s
LEFT JOIN scenario_key_operation_link kol ON kol.scenario_id = s.id
LEFT JOIN scenario_key_assumption_link kal ON kal.scenario_id = s.id
WHERE s.id <= 22
GROUP BY s.id, s.scenario_id
ORDER BY s.id;

-- Show operation assignments per category to verify correctness
SELECT
    s.scenario_id,
    oc.short_code AS category,
    od.short_code AS operation
FROM scenario s
JOIN scenario_key_operation_link kol ON kol.scenario_id = s.id
JOIN operation_definition od ON od.id = kol.operation_id
JOIN operation_category oc ON oc.short_code = od.category
WHERE s.id <= 22
ORDER BY s.id, oc.short_code;

COMMIT;
