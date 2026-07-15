-- SEED DATA_IN_DEPTH_SUBJECT (river-flow channel nodes)
-- =====================================================
-- Seeds 17 river-flow ENTITY subjects for the river-flow extract. Each is a
-- CalSim channel node addressed by location_type='network_node' + location_id =
-- the bare node code (NO C_ prefix; C_<code> is the CalSim flow variable, stored
-- later as data_in_depth_value.source_variable). Resolves via LOCATION_ENTITY_MAP
-- (network_node -> network.short_code -> name), the same path ENV_FLOWS tiers use.
--
-- Labels are pulled from network.name (fallback to the code) so they match what
-- resolution returns. No aggregates defined for river flows.
--
-- Idempotent (ON CONFLICT). DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_river_flow_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql already applied.

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (17 river-flow channel nodes)'
\echo '=========================================================='

INSERT INTO data_in_depth_subject (subject_kind, short_code, label, location_type, location_id)
SELECT 'entity', c.code, COALESCE(n.name, c.code), 'network_node', c.code
FROM (VALUES
    ('AMR004'), ('FTR003'), ('FTR029'), ('MCD005'), ('MOK028'),
    ('SAC000'), ('SAC049'), ('SAC122'), ('SAC148'), ('SAC257'),
    ('SAC289'), ('SJR070'), ('SJR127'), ('STS011'), ('TRN111'),
    ('TUO003'), ('YUB002')
) AS c(code)
LEFT JOIN network n ON n.short_code = c.code
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label,
        location_type = EXCLUDED.location_type,
        location_id = EXCLUDED.location_id,
        is_active = TRUE;

-- Verification --------------------------------------------------------------
\echo ''
\echo 'River-flow subjects seeded:'
SELECT short_code, label, location_type, location_id
FROM data_in_depth_subject
WHERE subject_kind = 'entity' AND location_type = 'network_node'
  AND short_code IN ('AMR004','FTR003','FTR029','MCD005','MOK028','SAC000','SAC049',
                     'SAC122','SAC148','SAC257','SAC289','SJR070','SJR127','STS011',
                     'TRN111','TUO003','YUB002')
ORDER BY short_code;

\echo ''
\echo 'location_id resolution against network (expect all ok):'
SELECT s.short_code, s.location_id,
       CASE WHEN n.short_code IS NULL THEN 'MISSING in network' ELSE 'ok' END AS resolves
FROM data_in_depth_subject s
LEFT JOIN network n ON n.short_code = s.location_id
WHERE s.subject_kind = 'entity' AND s.location_type = 'network_node'
  AND s.short_code IN ('AMR004','FTR003','FTR029','MCD005','MOK028','SAC000','SAC049',
                       'SAC122','SAC148','SAC257','SAC289','SJR070','SJR127','STS011',
                       'TRN111','TUO003','YUB002')
ORDER BY s.short_code;

\echo ''
\echo 'RIVER-FLOW SUBJECTS SEEDED.'
\echo '==========================='
