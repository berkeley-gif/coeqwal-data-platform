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
--   • All 22 active scenarios have a full set of operation links (7 -8 per
--     scenario covering biops, tucp, gw_restrictions, infrastructure, flow,
--     delta_outflow, and comm_delivery categories).
--   • s0011 gains its missing land use assumption (lu_2004_2013, id=2).
--   • All 2020 LandIQ links now use lu_2020_landiq (id=17) instead of
--     lu_updated (id=3); reduced-ag scenarios use lu_2020_landiq_reduced_ag
--     (id=18) instead of lu_proj_reductions (id=4).
--   • Placeholder scenarios s0035, s0036, s0037 remain unlinked.
--
-- Scenario id to scenario_id mapping (from scenario table):
--    1=s0011  2=s0020  3=s0021  4=s0023  5=s0024  6=s0025  7=s0027
--    8=s0029  9=s0026 10=s0028 11=s0030 12=s0031 13=s0032 14=s0033
--   15=s0039 16=s0040 17=s0041 18=s0042 19=s0044 20=s0045 21=s0046
--   22=s0065
--
-- Operation id to short_code mapping (operation_definition):
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
-- Assumption id to short_code mapping (assumption_definition):
--    2=lu_2004_2013  17=lu_2020_landiq  18=lu_2020_landiq_reduced_ag
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/14_rebuild_scenario_link_tables.sql
-- =============================================================================

BEGIN;


DELETE FROM scenario_key_operation_link;
DELETE FROM scenario_key_assumption_link;


ALTER TABLE scenario_key_operation_link DISABLE TRIGGER USER;

INSERT INTO scenario_key_operation_link (scenario_id, operation_id)
VALUES
    (1, 20),
    (1, 10),
    (1, 23),
    (1, 24),
    (1, 25),
    (1, 26),
    (1, 27),

    (2, 20),
    (2, 10),
    (2, 23),
    (2, 24),
    (2, 25),
    (2, 26),
    (2, 27),

    (3, 20),
    (3, 22),
    (3, 23),
    (3, 24),
    (3, 25),
    (3, 26),
    (3, 27),

    (4, 19),
    (4, 22),
    (4, 23),
    (4, 24),
    (4, 25),
    (4, 26),
    (4, 27),

    (5, 19),
    (5, 10),
    (5, 23),
    (5, 24),
    (5, 25),
    (5, 26),
    (5, 27),

    (6, 20),
    (6, 10),
    (6, 11),
    (6, 24),
    (6, 25),
    (6, 26),
    (6, 27),

    (7, 20),
    (7, 10),
    (7, 13),
    (7, 24),
    (7, 25),
    (7, 26),
    (7, 27),

    (8, 20),
    (8, 10),
    (8, 23),
    (8, 24),
    (8, 17),
    (8, 26),
    (8, 27),

    (9, 20),
    (9, 10),
    (9, 11),
    (9, 24),
    (9, 25),
    (9, 26),
    (9, 27),

    (10, 20),
    (10, 10),
    (10, 13),
    (10, 24),
    (10, 25),
    (10, 26),
    (10, 27),

    (11, 20),
    (11, 10),
    (11, 23),
    (11, 24),
    (11, 16),
    (11, 26),
    (11, 27),

    (12, 20),
    (12, 10),
    (12, 23),
    (12, 24),
    (12, 18),
    (12, 26),
    (12, 27),

    (13, 20),
    (13, 10),
    (13, 13),
    (13, 24),
    (13, 17),
    (13, 26),
    (13, 27),

    (14, 20),
    (14, 10),
    (14, 13),
    (14, 24),
    (14, 18),
    (14, 26),
    (14, 27),

    (15, 21),
    (15, 10),
    (15, 23),
    (15, 24),
    (15, 25),
    (15,  7),
    (15, 28),

    (16, 21),
    (16, 10),
    (16, 23),
    (16, 24),
    (16, 25),
    (16,  4),
    (16, 28),

    (17, 21),
    (17, 10),
    (17, 23),
    (17, 24),
    (17, 25),
    (17,  5),
    (17, 28),

    (18, 21),
    (18, 10),
    (18, 23),
    (18, 24),
    (18, 25),
    (18,  6),
    (18, 28),

    (19, 21),
    (19, 10),
    (19, 23),
    (19, 24),
    (19, 25),
    (19, 26),
    (19,  8),

    (20, 21),
    (20, 10),
    (20, 23),
    (20, 24),
    (20, 25),
    (20, 26),
    (20, 27),
    (20,  9),

    (21, 20),
    (21, 10),
    (21, 23),
    (21, 24),
    (21, 16),
    (21, 27),

    (22, 20),
    (22, 10),
    (22, 23),
    (22, 14),
    (22, 25),
    (22, 26),
    (22, 27);

ALTER TABLE scenario_key_operation_link ENABLE TRIGGER USER;


ALTER TABLE scenario_key_assumption_link DISABLE TRIGGER USER;

INSERT INTO scenario_key_assumption_link (scenario_id, assumption_id)
VALUES
    (1,  2),
    (2, 17),
    (3, 17),
    (4, 17),
    (5, 17),
    (6, 17),
    (7, 17),
    (8, 17),
    (9, 18),
   (10, 18),
   (11, 17),
   (12, 17),
   (13, 18),
   (14, 18),
   (15, 17),
   (16, 17),
   (17, 17),
   (18, 17),
   (19, 17),
   (20, 17),
   (21, 17),
   (22, 17);

ALTER TABLE scenario_key_assumption_link ENABLE TRIGGER USER;


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
