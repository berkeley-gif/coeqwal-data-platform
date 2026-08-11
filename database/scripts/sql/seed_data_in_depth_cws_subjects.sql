-- SEED DATA_IN_DEPTH_SUBJECT (community water system DUs)
-- =======================================================
-- Seeds 106 CWS ENTITY subjects + 2 AGGREGATE subjects (NOD_CWS/SOD_CWS, with
-- membership) for the community-water-system extract. Each entity is a
-- CalSim urban demand unit addressed by location_type='demand_unit' + location_id
-- = the dwuc/DU code, resolved via LOCATION_ENTITY_MAP (demand_unit ->
-- du_urban_entity.du_id), the same path the ENV/tier CWS_DEL indicator uses.
--
-- Labels come from du_urban_entity.community_agency where present (it is messy),
-- else the code.
--
-- NOTE: several codes are NOT in the du_urban_entity seed CSV
-- (72_PU, ACFC, KCWA, MHILL_NU, SBCWD, SVWRD, TLMNE, UNION, ESB355, WSB032) —
-- mostly named contractors; they seed with label=code and will not resolve to a
-- richer label/geometry unless the live du_urban_entity has them. ESB355 and
-- WSB032 are present in the CWS data files (data/raw/cws), so they are seeded so
-- their extract rows are not dropped. The verification query below reports
-- actual resolution against the live table.
--
-- 29 codes added 2026-08-05 (below the original 77, marked) for the new
-- data/raw/cws/DUs_allscs_welfare_outcomes.xlsx source (welfare_loss/shortage
-- measures - see extract_cws.py). All 29 resolve cleanly against
-- du_urban_entity except 26N_NU5 (blank community_agency -> label=code).
-- NOT yet added to NOD_CWS/SOD_CWS membership - no NOD/SOD mapping given yet
-- for these 29; open question, see open_issues.md.
--
-- Idempotent (ON CONFLICT). DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_cws_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql already applied.

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (106 CWS demand units)'
\echo '====================================================='

INSERT INTO data_in_depth_subject (subject_kind, short_code, label, location_type, location_id)
SELECT 'entity', c.code, COALESCE(NULLIF(du.community_agency, ''), c.code), 'demand_unit', c.code
FROM (VALUES
    -- original 77 (CWS delivery/pct_demand_met source)
    ('02_PU'),('02_SU'),('03_PU1'),('03_PU2'),('03_SU'),('11_NU1'),('12_NU1'),('13_NU1'),
    ('16_PU'),('20_NU1'),('21_PU'),('24_NU1'),('24_NU2'),('24_NU3'),('25_PU'),('26N_NU1'),
    ('26N_NU2'),('26N_NU3'),('26N_PU1'),('26N_PU2'),('26N_PU3'),('26S_NU1'),('26S_PU1'),
    ('26S_PU2'),('26S_PU4'),('26S_PU5'),('26S_PU6'),('50_PU'),('60N_NU2'),('60S_NU1'),
    ('61_NU2'),('62_NU'),('72_PU'),('90_PU'),('ACFC'),('AMADR'),('AMCYN'),('ANTOC'),
    ('BNCIA'),('CCWD'),('CSB038'),('CSB103'),('CSPSO'),('CSTIC'),('EBMUD'),('ELDID_NU1'),
    ('ELDID_NU2'),('ELDID_NU3'),('ESB324'),('ESB347'),('ESB414'),('ESB420'),('FRFLD'),
    ('GDPUD_NU'),('GRSVL'),('JLIND'),('KCWA'),('MHILL_NU'),('MWD'),('NAPA'),('NAPA2'),
    ('PCWA3'),('PLMAS'),('SBA029'),('SBA036'),('SBCWD'),('SCVWD'),('SUISN'),('SVWRD'),
    ('TLMNE'),('TVAFB'),('UNION'),('UPANG'),('VLLJO'),('WLDWD'),('ESB355'),('WSB032'),
    -- +29 new (welfare_outcomes source, added 2026-08-05)
    ('02_NU'),('03_PU3'),('04_NU1'),('04_NU2'),('05_NU'),('06_NU'),('07N_NU'),('07S_NU'),
    ('08N_NU'),('08S_NU'),('10_NU1'),('11_NU2'),('13_NU2'),('15N_NU'),('15S_NU'),('17S_NU'),
    ('20_NU2'),('25_NU'),('26N_NU4'),('26N_NU5'),('26S_NU2'),('26S_NU3'),('60N_NU1'),('61_NU1'),
    ('61_NU3'),('63_NU'),('64_NU'),('71_NU'),('72_NU')
) AS c(code)
LEFT JOIN du_urban_entity du ON du.du_id = c.code
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label,
        location_type = EXCLUDED.location_type,
        location_id = EXCLUDED.location_id,
        is_active = TRUE;

-- Aggregate subjects: NOD/SOD Community Water Systems -------------------------
-- Domain-qualified short codes (…_CWS) so NOD/SOD don't collide with the
-- reservoir (NOD_Reservoirs) or future segments' aggregates.
INSERT INTO data_in_depth_subject (subject_kind, short_code, label) VALUES
    ('aggregate', 'NOD_CWS', 'North of Delta Community Water Systems'),
    ('aggregate', 'SOD_CWS', 'South of Delta Community Water Systems')
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label, is_active = TRUE;

-- Aggregate membership (aggregate -> member CWS DU) ---------------------------
INSERT INTO data_in_depth_subject_member (aggregate_id, member_id)
SELECT agg.id, mem.id
FROM (VALUES
    ('NOD_CWS','02_PU'),('NOD_CWS','02_SU'),('NOD_CWS','03_PU1'),('NOD_CWS','03_PU2'),
    ('NOD_CWS','03_SU'),('NOD_CWS','11_NU1'),('NOD_CWS','12_NU1'),('NOD_CWS','13_NU1'),
    ('NOD_CWS','16_PU'),('NOD_CWS','20_NU1'),('NOD_CWS','21_PU'),('NOD_CWS','24_NU1'),
    ('NOD_CWS','24_NU2'),('NOD_CWS','24_NU3'),('NOD_CWS','25_PU'),('NOD_CWS','26N_NU1'),
    ('NOD_CWS','26N_NU2'),('NOD_CWS','26N_NU3'),('NOD_CWS','26N_PU1'),('NOD_CWS','26N_PU2'),
    ('NOD_CWS','26N_PU3'),('NOD_CWS','26S_NU1'),('NOD_CWS','26S_PU1'),('NOD_CWS','26S_PU2'),
    ('NOD_CWS','26S_PU4'),('NOD_CWS','26S_PU5'),('NOD_CWS','26S_PU6'),('NOD_CWS','50_PU'),
    ('NOD_CWS','60N_NU2'),('NOD_CWS','AMADR'),('NOD_CWS','AMCYN'),('NOD_CWS','BNCIA'),
    ('NOD_CWS','CSPSO'),('NOD_CWS','ELDID_NU1'),('NOD_CWS','ELDID_NU2'),('NOD_CWS','ELDID_NU3'),
    ('NOD_CWS','GDPUD_NU'),('NOD_CWS','GRSVL'),('NOD_CWS','JLIND'),('NOD_CWS','MHILL_NU'),
    ('NOD_CWS','NAPA'),('NOD_CWS','NAPA2'),('NOD_CWS','PCWA3'),('NOD_CWS','PLMAS'),
    ('NOD_CWS','SUISN'),('NOD_CWS','TLMNE'),('NOD_CWS','TVAFB'),('NOD_CWS','UNION'),
    ('NOD_CWS','UPANG'),('NOD_CWS','VLLJO'),('NOD_CWS','WLDWD'),
    ('SOD_CWS','60S_NU1'),('SOD_CWS','61_NU2'),('SOD_CWS','90_PU'),('SOD_CWS','ACFC'),
    ('SOD_CWS','ANTOC'),('SOD_CWS','CCWD'),('SOD_CWS','CSB038'),('SOD_CWS','CSB103'),
    ('SOD_CWS','EBMUD'),('SOD_CWS','ESB324'),('SOD_CWS','ESB347'),('SOD_CWS','ESB355'),
    ('SOD_CWS','ESB414'),('SOD_CWS','ESB420'),('SOD_CWS','FRFLD'),('SOD_CWS','KCWA'),
    ('SOD_CWS','MWD'),('SOD_CWS','SBA029'),('SOD_CWS','SBA036'),('SOD_CWS','SBCWD'),
    ('SOD_CWS','SCVWD'),('SOD_CWS','SVWRD'),('SOD_CWS','WSB032'),
    -- +31 new (welfare-outcomes-only DUs, added 2026-08-06 - mapping supplied
    -- for the 29 newly-seeded DUs plus 62_NU/72_PU, which existed in the
    -- original 77 but were never assigned NOD/SOD until now)
    ('NOD_CWS','02_NU'),('NOD_CWS','03_PU3'),('NOD_CWS','04_NU1'),('NOD_CWS','04_NU2'),
    ('NOD_CWS','05_NU'),('NOD_CWS','06_NU'),('NOD_CWS','07N_NU'),('NOD_CWS','07S_NU'),
    ('NOD_CWS','08N_NU'),('NOD_CWS','08S_NU'),('NOD_CWS','10_NU1'),('NOD_CWS','11_NU2'),
    ('NOD_CWS','13_NU2'),('NOD_CWS','15N_NU'),('NOD_CWS','15S_NU'),('NOD_CWS','17S_NU'),
    ('NOD_CWS','20_NU2'),('NOD_CWS','25_NU'),('NOD_CWS','26N_NU4'),('NOD_CWS','26N_NU5'),
    ('NOD_CWS','26S_NU2'),('NOD_CWS','26S_NU3'),
    ('SOD_CWS','60N_NU1'),('SOD_CWS','61_NU1'),('SOD_CWS','61_NU3'),('SOD_CWS','62_NU'),
    ('SOD_CWS','63_NU'),('SOD_CWS','64_NU'),('SOD_CWS','71_NU'),('SOD_CWS','72_NU'),
    ('SOD_CWS','72_PU')
) AS m(agg_code, mem_code)
JOIN data_in_depth_subject agg ON agg.subject_kind = 'aggregate' AND agg.short_code = m.agg_code
JOIN data_in_depth_subject mem ON mem.subject_kind = 'entity'    AND mem.short_code = m.mem_code
ON CONFLICT (aggregate_id, member_id) DO NOTHING;

-- Verification --------------------------------------------------------------
\echo ''
\echo 'CWS subjects seeded (count, expect 106):'
SELECT COUNT(*) FROM data_in_depth_subject
WHERE subject_kind = 'entity' AND location_type = 'demand_unit';

\echo ''
\echo 'location_id resolution against du_urban_entity (MISSING = no label/geometry source):'
SELECT s.short_code, s.location_id,
       CASE WHEN du.du_id IS NULL THEN 'MISSING in du_urban_entity' ELSE 'ok' END AS resolves,
       s.label
FROM data_in_depth_subject s
LEFT JOIN du_urban_entity du ON du.du_id = s.location_id
WHERE s.subject_kind = 'entity' AND s.location_type = 'demand_unit'
ORDER BY resolves DESC, s.short_code;

\echo ''
\echo 'NOD/SOD CWS aggregate membership (expect NOD_CWS=73, SOD_CWS=32):'
SELECT agg.short_code AS aggregate, COUNT(*) AS members
FROM data_in_depth_subject_member l
JOIN data_in_depth_subject agg ON agg.id = l.aggregate_id
WHERE agg.short_code IN ('NOD_CWS', 'SOD_CWS')
GROUP BY agg.short_code
ORDER BY agg.short_code;

\echo ''
\echo 'CWS SUBJECTS SEEDED.'
\echo '===================='
