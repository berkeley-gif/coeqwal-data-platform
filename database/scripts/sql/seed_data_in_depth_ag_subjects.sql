-- SEED DATA_IN_DEPTH_SUBJECT (agricultural DUs)
-- =============================================
-- Seeds 132 agricultural ENTITY subjects + 2 AGGREGATE subjects
-- (NOD_Agriculture / SOD_Agriculture, with membership). Each entity is a CalSim
-- agricultural demand unit addressed by location_type='ag_demand_unit' +
-- location_id = the DU code, resolving via LOCATION_ENTITY_MAP (ag_demand_unit ->
-- du_agriculture_entity.du_id). Distinct from 'demand_unit' (urban -> du_urban_entity).
--
-- Labels come from du_agriculture_entity.agency where present, else the code.
-- NOD_Agriculture = 73 members, SOD_Agriculture = 59 members.
--
-- NOTE: 07S_PA is in this list but NOT in the du_agriculture_entity seed CSV; it
-- is seeded with label=code and will show MISSING in the resolution check.
-- 26N_NA exists in both du_agriculture_entity and du_urban_entity, but is not a
-- CWS subject, so there is no (subject_kind, short_code) collision.
--
-- Idempotent (ON CONFLICT). DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_ag_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql applied AND the ag_demand_unit
-- CHECK value present (alter_data_in_depth_subject_add_ag_demand_unit.sql on an
-- already-created DB).

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (132 agricultural DUs)'
\echo '==================================================='

INSERT INTO data_in_depth_subject (subject_kind, short_code, label, location_type, location_id)
SELECT 'entity', c.code, COALESCE(NULLIF(du.agency, ''), c.code), 'ag_demand_unit', c.code
FROM (VALUES
    ('02_NA'),('02_PA'),('02_SA'),('03_NA'),('03_PA'),('03_SA'),('04_NA'),('04_PA1'),
    ('04_PA2'),('05_NA'),('06_NA'),('06_PA'),('07N_NA'),('07N_PA'),('07S_NA'),('07S_PA'),
    ('08N_NA'),('08N_PA'),('08N_SA1'),('08N_SA2'),('08S_NA1'),('08S_NA2'),('08S_PA'),
    ('08S_SA1'),('08S_SA2'),('08S_SA3'),('09_NA'),('09_SA1'),('09_SA2'),('10_NA'),('11_NA'),
    ('11_SA1'),('11_SA2'),('11_SA3'),('11_SA4'),('12_NA'),('12_SA'),('13_NA'),('14_NA'),
    ('15N_NA1'),('15N_NA2'),('15N_SA'),('15S_NA1'),('15S_NA2'),('15S_SA'),('16_NA1'),
    ('16_NA2'),('16_PA'),('16_SA'),('17N_NA'),('17S_NA'),('17S_SA'),('18_NA'),('18_SA'),
    ('19_SA'),('20_NA1'),('20_NA2'),('20_PA'),('21_NA'),('21_PA'),('21_SA'),('22_NA'),
    ('22_SA1'),('22_SA2'),('23_NA'),('24_NA1'),('24_NA2'),('24_NA3'),('25_NA'),('25_PA1'),
    ('25_PA2'),('26N_NA'),('26S_NA'),('50_PA1'),('50_PA2'),('60N_NA1'),('60N_NA2'),
    ('60N_NA3'),('60N_NA4'),('60N_NA5'),('60S_NA1'),('60S_NA2'),('60S_PA1'),('60S_PA2'),
    ('61_NA1'),('61_NA2'),('61_NA3'),('61_NA4'),('61_NA5'),('61_NA6'),('61_PA1'),('61_PA2'),
    ('61_PA3'),('62_NA1'),('62_NA2'),('62_NA3'),('62_NA4'),('62_NA5'),('62_NA6'),('63_NA1'),
    ('63_NA2'),('63_NA3'),('63_NA4'),('64_NA1'),('64_NA2'),('64_PA1'),('64_PA2'),('64_PA3'),
    ('64_XA'),('71_NA1'),('71_NA2'),('71_PA1'),('71_PA2'),('71_PA3'),('71_PA4'),('71_PA5'),
    ('71_PA6'),('71_PA7'),('71_PA8'),('72_NA1'),('72_NA2'),('72_PA'),('72_XA1'),('72_XA2'),
    ('72_XA3'),('73_NA'),('73_PA1'),('73_PA2'),('73_PA3'),('73_XA'),('90_PA1'),('90_PA2')
) AS c(code)
LEFT JOIN du_agriculture_entity du ON du.du_id = c.code
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label,
        location_type = EXCLUDED.location_type,
        location_id = EXCLUDED.location_id,
        is_active = TRUE;

-- 2. Aggregate subjects --------------------------------------------------------
INSERT INTO data_in_depth_subject (subject_kind, short_code, label) VALUES
    ('aggregate', 'NOD_Agriculture', 'North of Delta Agriculture'),
    ('aggregate', 'SOD_Agriculture', 'South of Delta Agriculture')
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label, is_active = TRUE;

-- 3. Aggregate membership (aggregate -> member ag DUs) ------------------------
INSERT INTO data_in_depth_subject_member (aggregate_id, member_id)
SELECT agg.id, mem.id
FROM (VALUES
    ('NOD_Agriculture', '02_NA'), ('NOD_Agriculture', '02_PA'), ('NOD_Agriculture', '02_SA'), ('NOD_Agriculture', '03_NA'), ('NOD_Agriculture', '03_PA'),
    ('NOD_Agriculture', '03_SA'), ('NOD_Agriculture', '04_NA'), ('NOD_Agriculture', '04_PA1'), ('NOD_Agriculture', '04_PA2'), ('NOD_Agriculture', '05_NA'),
    ('NOD_Agriculture', '06_NA'), ('NOD_Agriculture', '06_PA'), ('NOD_Agriculture', '07N_NA'), ('NOD_Agriculture', '07N_PA'), ('NOD_Agriculture', '07S_NA'),
    ('NOD_Agriculture', '07S_PA'), ('NOD_Agriculture', '08N_NA'), ('NOD_Agriculture', '08N_PA'), ('NOD_Agriculture', '08N_SA1'), ('NOD_Agriculture', '08N_SA2'),
    ('NOD_Agriculture', '08S_NA1'), ('NOD_Agriculture', '08S_NA2'), ('NOD_Agriculture', '08S_PA'), ('NOD_Agriculture', '08S_SA1'), ('NOD_Agriculture', '08S_SA2'),
    ('NOD_Agriculture', '08S_SA3'), ('NOD_Agriculture', '09_NA'), ('NOD_Agriculture', '09_SA1'), ('NOD_Agriculture', '09_SA2'), ('NOD_Agriculture', '10_NA'),
    ('NOD_Agriculture', '11_NA'), ('NOD_Agriculture', '11_SA1'), ('NOD_Agriculture', '11_SA2'), ('NOD_Agriculture', '11_SA3'), ('NOD_Agriculture', '11_SA4'),
    ('NOD_Agriculture', '12_NA'), ('NOD_Agriculture', '12_SA'), ('NOD_Agriculture', '13_NA'), ('NOD_Agriculture', '14_NA'), ('NOD_Agriculture', '15N_NA1'),
    ('NOD_Agriculture', '15N_NA2'), ('NOD_Agriculture', '15N_SA'), ('NOD_Agriculture', '15S_NA1'), ('NOD_Agriculture', '15S_NA2'), ('NOD_Agriculture', '15S_SA'), 
    ('NOD_Agriculture', '16_NA1'), ('NOD_Agriculture', '16_NA2'), ('NOD_Agriculture', '16_PA'), ('NOD_Agriculture', '16_SA'), ('NOD_Agriculture', '17N_NA'),
    ('NOD_Agriculture', '17S_NA'), ('NOD_Agriculture', '17S_SA'), ('NOD_Agriculture', '18_NA'), ('NOD_Agriculture', '18_SA'), ('NOD_Agriculture', '19_SA'),
    ('NOD_Agriculture', '20_NA1'), ('NOD_Agriculture', '20_NA2'), ('NOD_Agriculture', '20_PA'), ('NOD_Agriculture', '21_NA'), ('NOD_Agriculture', '21_PA'),
    ('NOD_Agriculture', '21_SA'), ('NOD_Agriculture', '22_NA'), ('NOD_Agriculture', '22_SA1'), ('NOD_Agriculture', '22_SA2'), ('NOD_Agriculture', '23_NA'),
    ('NOD_Agriculture', '24_NA1'), ('NOD_Agriculture', '24_NA2'), ('NOD_Agriculture', '24_NA3'), ('NOD_Agriculture', '25_NA'), ('NOD_Agriculture', '25_PA1'),
    ('NOD_Agriculture', '25_PA2'), ('NOD_Agriculture', '26N_NA'), ('NOD_Agriculture', '26S_NA'), ('SOD_Agriculture', '50_PA1'), ('SOD_Agriculture', '50_PA2'), ('SOD_Agriculture', '60N_NA1'),
    ('SOD_Agriculture', '60N_NA2'), ('SOD_Agriculture', '60N_NA3'), ('SOD_Agriculture', '60N_NA4'), ('SOD_Agriculture', '60N_NA5'), ('SOD_Agriculture', '60S_NA1'),
    ('SOD_Agriculture', '60S_NA2'), ('SOD_Agriculture', '60S_PA1'), ('SOD_Agriculture', '60S_PA2'), ('SOD_Agriculture', '61_NA1'), ('SOD_Agriculture', '61_NA2'),
    ('SOD_Agriculture', '61_NA3'), ('SOD_Agriculture', '61_NA4'), ('SOD_Agriculture', '61_NA5'), ('SOD_Agriculture', '61_NA6'), ('SOD_Agriculture', '61_PA1'),
    ('SOD_Agriculture', '61_PA2'), ('SOD_Agriculture', '61_PA3'), ('SOD_Agriculture', '62_NA1'), ('SOD_Agriculture', '62_NA2'), ('SOD_Agriculture', '62_NA3'),
    ('SOD_Agriculture', '62_NA4'), ('SOD_Agriculture', '62_NA5'), ('SOD_Agriculture', '62_NA6'), ('SOD_Agriculture', '63_NA1'), ('SOD_Agriculture', '63_NA2'), ('SOD_Agriculture', '63_NA3'),
    ('SOD_Agriculture', '63_NA4'), ('SOD_Agriculture', '64_NA1'), ('SOD_Agriculture', '64_NA2'), ('SOD_Agriculture', '64_PA1'), ('SOD_Agriculture', '64_PA2'), ('SOD_Agriculture', '64_PA3'), ('SOD_Agriculture', '64_XA'), ('SOD_Agriculture', '71_NA1'),
    ('SOD_Agriculture', '71_NA2'), ('SOD_Agriculture', '71_PA1'), ('SOD_Agriculture', '71_PA2'), ('SOD_Agriculture', '71_PA3'), ('SOD_Agriculture', '71_PA4'),
    ('SOD_Agriculture', '71_PA5'), ('SOD_Agriculture', '71_PA6'), ('SOD_Agriculture', '71_PA7'), ('SOD_Agriculture', '71_PA8'), ('SOD_Agriculture', '72_NA1'),
    ('SOD_Agriculture', '72_NA2'), ('SOD_Agriculture', '72_PA'), ('SOD_Agriculture', '72_XA1'), ('SOD_Agriculture', '72_XA2'), ('SOD_Agriculture', '72_XA3'),
    ('SOD_Agriculture', '73_NA'), ('SOD_Agriculture', '73_PA1'), ('SOD_Agriculture', '73_PA2'), ('SOD_Agriculture', '73_PA3'), ('SOD_Agriculture', '73_XA'),
    ('SOD_Agriculture', '90_PA1'), ('SOD_Agriculture', '90_PA2')
) AS m(agg_code, mem_code)
JOIN data_in_depth_subject agg ON agg.subject_kind = 'aggregate' AND agg.short_code = m.agg_code
JOIN data_in_depth_subject mem ON mem.subject_kind = 'entity'    AND mem.short_code = m.mem_code
ON CONFLICT (aggregate_id, member_id) DO NOTHING;


-- Verification --------------------------------------------------------------
\echo ''
\echo 'Ag subjects seeded (count, expect 132):'
SELECT COUNT(*) FROM data_in_depth_subject
WHERE subject_kind = 'entity' AND location_type = 'ag_demand_unit';

\echo ''
\echo 'location_id resolution against du_agriculture_entity (MISSING expected: 07S_PA):'
SELECT s.short_code, s.location_id,
       CASE WHEN du.du_id IS NULL THEN 'MISSING in du_agriculture_entity' ELSE 'ok' END AS resolves
FROM data_in_depth_subject s
LEFT JOIN du_agriculture_entity du ON du.du_id = s.location_id
WHERE s.subject_kind = 'entity' AND s.location_type = 'ag_demand_unit'
  AND (du.du_id IS NULL)
ORDER BY s.short_code;

\echo ''
\echo 'NOD/SOD Agriculture aggregate membership (expect NOD_Agriculture=73, SOD_Agriculture=59):'
SELECT agg.short_code AS aggregate, COUNT(*) AS members
FROM data_in_depth_subject_member l
JOIN data_in_depth_subject agg ON agg.id = l.aggregate_id
WHERE agg.short_code IN ('NOD_Agriculture', 'SOD_Agriculture')
GROUP BY agg.short_code
ORDER BY agg.short_code;

\echo ''
\echo 'AG SUBJECTS SEEDED.'
\echo '==================='
