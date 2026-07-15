-- SEED DATA_IN_DEPTH_SUBJECT (reservoirs + NOD/SOD aggregates)
-- ============================================================
-- Seeds the subject registry for the reservoir-storage extract:
--   * 8 reservoir ENTITY subjects (location_type='reservoir', location_id = the
--     bare short_code that exists in tier_location / reservoir_entity — NO S_
--     prefix; S_<code> is the CalSim variable, stored later as
--     data_in_depth_value.source_variable).
--   * 2 AGGREGATE subjects: NOD_Reservoirs (North of Delta) and SOD_Reservoirs
--     (South of Delta), each with 4 member reservoirs via
--     data_in_depth_subject_member. Aggregate VALUES are computed by the
--     extractor at ETL time; membership here is provenance. Short codes are
--     domain-qualified (…_Reservoirs) because NOD/SOD will recur for other
--     segments (river flows, deliveries, …) and must not collide.
--
-- Idempotent (ON CONFLICT). DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_reservoir_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql already applied.

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (reservoirs + NOD/SOD)'
\echo '==================================================='

-- 1. Reservoir entity subjects -------------------------------------------------
INSERT INTO data_in_depth_subject (subject_kind, short_code, label, location_type, location_id) VALUES
    ('entity', 'TRNTY',     'Trinity',        'reservoir', 'TRNTY'),
    ('entity', 'SHSTA',     'Shasta',         'reservoir', 'SHSTA'),
    ('entity', 'OROVL',     'Oroville',       'reservoir', 'OROVL'),
    ('entity', 'FOLSM',     'Folsom',         'reservoir', 'FOLSM'),
    ('entity', 'SLUIS_CVP', 'San Luis (CVP)', 'reservoir', 'SLUIS_CVP'),
    ('entity', 'SLUIS_SWP', 'San Luis (SWP)', 'reservoir', 'SLUIS_SWP'),
    ('entity', 'MELON',     'New Melones',    'reservoir', 'MELON'),
    ('entity', 'MLRTN',     'Millerton',      'reservoir', 'MLRTN')
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label,
        location_type = EXCLUDED.location_type,
        location_id = EXCLUDED.location_id,
        is_active = TRUE;

-- 2. Aggregate subjects --------------------------------------------------------
INSERT INTO data_in_depth_subject (subject_kind, short_code, label) VALUES
    ('aggregate', 'NOD_Reservoirs', 'North of Delta Reservoirs'),
    ('aggregate', 'SOD_Reservoirs', 'South of Delta Reservoirs')
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label, is_active = TRUE;

-- 3. Aggregate membership (aggregate -> member reservoirs) ----------------------
INSERT INTO data_in_depth_subject_member (aggregate_id, member_id)
SELECT agg.id, mem.id
FROM (VALUES
    ('NOD_Reservoirs', 'TRNTY'), ('NOD_Reservoirs', 'SHSTA'), ('NOD_Reservoirs', 'OROVL'), ('NOD_Reservoirs', 'FOLSM'),
    ('SOD_Reservoirs', 'SLUIS_CVP'), ('SOD_Reservoirs', 'SLUIS_SWP'), ('SOD_Reservoirs', 'MELON'), ('SOD_Reservoirs', 'MLRTN')
) AS m(agg_code, mem_code)
JOIN data_in_depth_subject agg ON agg.subject_kind = 'aggregate' AND agg.short_code = m.agg_code
JOIN data_in_depth_subject mem ON mem.subject_kind = 'entity'    AND mem.short_code = m.mem_code
ON CONFLICT (aggregate_id, member_id) DO NOTHING;

-- 4. Verification --------------------------------------------------------------
\echo ''
\echo 'Subjects seeded:'
SELECT subject_kind, short_code, label, location_type, location_id
FROM data_in_depth_subject
WHERE short_code IN ('TRNTY','SHSTA','OROVL','FOLSM','SLUIS_CVP','SLUIS_SWP','MELON','MLRTN','NOD_Reservoirs','SOD_Reservoirs')
ORDER BY subject_kind, short_code;

\echo ''
\echo 'Aggregate membership:'
SELECT agg.short_code AS aggregate, mem.short_code AS member
FROM data_in_depth_subject_member l
JOIN data_in_depth_subject agg ON agg.id = l.aggregate_id
JOIN data_in_depth_subject mem ON mem.id = l.member_id
ORDER BY agg.short_code, mem.short_code;

\echo ''
\echo 'Entity location_id resolution against reservoir_entity (expect all ok):'
SELECT s.short_code, s.location_id,
       CASE WHEN re.short_code IS NULL THEN 'MISSING in reservoir_entity' ELSE 'ok' END AS resolves
FROM data_in_depth_subject s
LEFT JOIN reservoir_entity re ON re.short_code = s.location_id
WHERE s.subject_kind = 'entity' AND s.location_type = 'reservoir'
ORDER BY s.short_code;

\echo ''
\echo 'SUBJECT REGISTRY SEEDED.'
\echo '========================'
