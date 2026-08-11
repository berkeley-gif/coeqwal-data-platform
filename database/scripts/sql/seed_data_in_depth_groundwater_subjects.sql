-- SEED DATA_IN_DEPTH_SUBJECT (groundwater storage WBAs)
-- =======================================================
-- Seeds 42 groundwater-storage ENTITY subjects + 2 AGGREGATE subjects
-- (NOD_GroundwaterStorage / SOD_GroundwaterStorage, with membership). Each
-- entity is a GW_STOR water-budget area addressed by location_type='wba' +
-- location_id = the wba_id, resolving via LOCATION_ENTITY_MAP (wba -> wba
-- table). 'wba' is already a valid chk_dids_location_type value - no ALTER
-- needed (unlike ag_demand_unit).
--
-- Labels come from wba.wba_name where present, else the code.
-- NOD_GroundwaterStorage = 30 members, SOD_GroundwaterStorage = 12 members.
--
-- NOTE: bare 'WBA' (no suffix) is NOT seeded - it does not appear as a column
-- in either data/raw/ground_water/GroundWater_Volumes_Annual.csv or
-- GroundWater_Levels_Annual.csv, so no extractor will ever populate it.
-- Verified: the 42 entities below match exactly (same set) in both source
-- files, and the NOD/SOD mapping covers all 42 with no gaps or extras.
--
-- Idempotent (ON CONFLICT). DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_groundwater_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql applied.

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (42 groundwater-storage WBAs)'
\echo '============================================================'

-- 1. Entity subjects -----------------------------------------------------------
INSERT INTO data_in_depth_subject (subject_kind, short_code, label, location_type, location_id)
SELECT 'entity', c.code, COALESCE(NULLIF(w.wba_name, ''), c.code), 'wba', c.code
FROM (VALUES
    ('WBA2'),('WBA3'),('WBA4'),('WBA5'),('WBA6'),('WBA7N'),('WBA7S'),('WBA8N'),('WBA8S'),
    ('WBA9'),('WBA10'),('WBA11'),('WBA12'),('WBA13'),('WBA14'),('WBA15N'),('WBA15S'),
    ('WBA16'),('WBA17N'),('WBA17S'),('WBA18'),('WBA19'),('WBA20'),('WBA21'),('WBA22'),
    ('WBA23'),('WBA24'),('WBA25'),('WBA26N'),('WBA26S'),('WBA60N'),('DETAW'),('WBA50'),
    ('WBA60S'),('WBA61'),('WBA62'),('WBA63'),('WBA64'),('WBA71'),('WBA72'),('WBA73'),('WBA90')
) AS c(code)
LEFT JOIN wba w ON w.wba_id = c.code
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label,
        location_type = EXCLUDED.location_type,
        location_id = EXCLUDED.location_id,
        is_active = TRUE;

-- 2. Aggregate subjects --------------------------------------------------------
INSERT INTO data_in_depth_subject (subject_kind, short_code, label) VALUES
    ('aggregate', 'NOD_GroundwaterStorage', 'North of Delta Groundwater Storage'),
    ('aggregate', 'SOD_GroundwaterStorage', 'South of Delta Groundwater Storage')
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label, is_active = TRUE;

-- 3. Aggregate membership (aggregate -> member WBAs) ---------------------------
INSERT INTO data_in_depth_subject_member (aggregate_id, member_id)
SELECT agg.id, mem.id
FROM (VALUES
    ('NOD_GroundwaterStorage', 'WBA2'), ('NOD_GroundwaterStorage', 'WBA3'), ('NOD_GroundwaterStorage', 'WBA4'),
    ('NOD_GroundwaterStorage', 'WBA5'), ('NOD_GroundwaterStorage', 'WBA6'), ('NOD_GroundwaterStorage', 'WBA7N'),
    ('NOD_GroundwaterStorage', 'WBA7S'), ('NOD_GroundwaterStorage', 'WBA8N'), ('NOD_GroundwaterStorage', 'WBA8S'),
    ('NOD_GroundwaterStorage', 'WBA9'), ('NOD_GroundwaterStorage', 'WBA10'), ('NOD_GroundwaterStorage', 'WBA11'),
    ('NOD_GroundwaterStorage', 'WBA12'), ('NOD_GroundwaterStorage', 'WBA13'), ('NOD_GroundwaterStorage', 'WBA14'),
    ('NOD_GroundwaterStorage', 'WBA15N'), ('NOD_GroundwaterStorage', 'WBA15S'), ('NOD_GroundwaterStorage', 'WBA16'),
    ('NOD_GroundwaterStorage', 'WBA17N'), ('NOD_GroundwaterStorage', 'WBA17S'), ('NOD_GroundwaterStorage', 'WBA18'),
    ('NOD_GroundwaterStorage', 'WBA19'), ('NOD_GroundwaterStorage', 'WBA20'), ('NOD_GroundwaterStorage', 'WBA21'),
    ('NOD_GroundwaterStorage', 'WBA22'), ('NOD_GroundwaterStorage', 'WBA23'), ('NOD_GroundwaterStorage', 'WBA24'),
    ('NOD_GroundwaterStorage', 'WBA25'), ('NOD_GroundwaterStorage', 'WBA26N'), ('NOD_GroundwaterStorage', 'WBA26S'),
    ('SOD_GroundwaterStorage', 'WBA60N'), ('SOD_GroundwaterStorage', 'DETAW'), ('SOD_GroundwaterStorage', 'WBA50'),
    ('SOD_GroundwaterStorage', 'WBA60S'), ('SOD_GroundwaterStorage', 'WBA61'), ('SOD_GroundwaterStorage', 'WBA62'),
    ('SOD_GroundwaterStorage', 'WBA63'), ('SOD_GroundwaterStorage', 'WBA64'), ('SOD_GroundwaterStorage', 'WBA71'),
    ('SOD_GroundwaterStorage', 'WBA72'), ('SOD_GroundwaterStorage', 'WBA73'), ('SOD_GroundwaterStorage', 'WBA90')
) AS m(agg_code, mem_code)
JOIN data_in_depth_subject agg ON agg.subject_kind = 'aggregate' AND agg.short_code = m.agg_code
JOIN data_in_depth_subject mem ON mem.subject_kind = 'entity'    AND mem.short_code = m.mem_code
ON CONFLICT (aggregate_id, member_id) DO NOTHING;


-- Verification --------------------------------------------------------------
\echo ''
\echo 'Groundwater-storage subjects seeded (count, expect 42):'
SELECT COUNT(*) FROM data_in_depth_subject
WHERE subject_kind = 'entity' AND location_type = 'wba';

\echo ''
\echo 'location_id resolution against wba (expect no rows):'
SELECT s.short_code, s.location_id,
       CASE WHEN w.wba_id IS NULL THEN 'MISSING in wba' ELSE 'ok' END AS resolves
FROM data_in_depth_subject s
LEFT JOIN wba w ON w.wba_id = s.location_id
WHERE s.subject_kind = 'entity' AND s.location_type = 'wba'
  AND (w.wba_id IS NULL)
ORDER BY s.short_code;

\echo ''
\echo 'NOD/SOD GroundwaterStorage aggregate membership (expect NOD_GroundwaterStorage=30, SOD_GroundwaterStorage=12):'
SELECT agg.short_code AS aggregate, COUNT(*) AS members
FROM data_in_depth_subject_member l
JOIN data_in_depth_subject agg ON agg.id = l.aggregate_id
WHERE agg.short_code IN ('NOD_GroundwaterStorage', 'SOD_GroundwaterStorage')
GROUP BY agg.short_code
ORDER BY agg.short_code;

\echo ''
\echo 'GROUNDWATER-STORAGE SUBJECTS SEEDED.'
\echo '====================================='
