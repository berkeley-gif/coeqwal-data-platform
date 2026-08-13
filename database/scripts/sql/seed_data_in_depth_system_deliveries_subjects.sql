-- SEED DATA_IN_DEPTH_SUBJECT (system deliveries: CVP/SWP deliveries & Delta exports)
-- ====================================================================================
-- Seeds 25 METRIC subjects (non-location) for the system_deliveries extract:
-- CVP/SWP annual delivery totals broken out by NOD/SOD/Total x AG/M&I/Refuges,
-- CVP/SWP/combined Delta export totals, and three Southern San Joaquin Valley
-- export paths. Metric subjects carry NO location_type/location_id (registry
-- CHECK requires them NULL) - same pattern as X2 (seed_data_in_depth_delta_subjects.sql)
-- and WRLCM salmon (seed_data_in_depth_salmon_subjects.sql).
--
-- UNLIKE every prior data_in_depth domain, there is NO subject_member
-- aggregation here. Each variable below is ALREADY a pre-aggregated/leaf raw
-- CalSim variable (e.g. DEL_CVP_TOTAL, DEL_CVP_TOT_N_WAMER for NOD,
-- DEL_CVP_TOT_S_WLOSS for SOD are three INDEPENDENT source columns, not a sum
-- this ETL computes) - so NOD/SOD/Total triplets are each seeded as their own
-- flat subjects.

-- Idempotent (ON CONFLICT). DML — run as your own role for audit attribution:
--   psql "$DATABASE_URL" -f database/scripts/sql/seed_data_in_depth_system_deliveries_subjects.sql
-- Prereq: create_data_in_depth_subject_table.sql already applied.

\set ON_ERROR_STOP on

\echo ''
\echo 'SEEDING data_in_depth_subject (25 system-deliveries metrics)'
\echo '============================================================='

INSERT INTO data_in_depth_subject (subject_kind, short_code, label)
SELECT 'metric', v.code, v.label
FROM (VALUES
    ('DEL_CVP_TOT_N_WAMER',     'NOD Central Valley Project deliveries (AG + M&I + Wildlife Refuges)'),
    ('DEL_CVP_TOT_S_WLOSS',     'SOD Central Valley Project deliveries (AG + M&I + Wildlife Refuges)'),
    ('DEL_CVP_TOTAL',           'Total Central Valley Project deliveries (AG + M&I + Wildlife Refuges)'),
    ('DEL_CVP_PAG_NOD',         'NOD Central Valley Project deliveries AG'),
    ('DEL_CVP_PAG_SOD',         'SOD Central Valley Project deliveries AG'),
    ('DEL_CVP_PAG_TOTAL',       'Total Central Valley Project deliveries AG'),
    ('DEL_CVP_PMI_TOTAL',       'Total Central Valley Project deliveries M&I'),
    ('DEL_CVP_PMI_N_WAMER',     'NOD Central Valley Project deliveries M&I'),
    ('DEL_CVP_PMI_S',           'SOD Central Valley Project deliveries M&I'),
    ('DEL_CVP_PRF_TOTAL',       'Central Valley Project deliveries Wildlife Refuges'),
    ('C_CVP_TOTAL_EXPORTS',     'Central Valley Project Delta Exports'),
    ('DEL_SWP_TOT_N',           'NOD State Water Project deliveries (AG + M&I)'),
    ('DEL_SWP_TOT_S',           'SOD State Water Project deliveries (AG + M&I)'),
    ('DEL_SWP_TOTAL',           'Total State Water Project deliveries (AG + M&I)'),
    ('DEL_SWP_PAG_NOD',         'NOD State Water Project deliveries AG'),
    ('DEL_SWP_PAG_S',           'SOD State Water Project deliveries AG'),
    ('DEL_SWP_PAG_TOTAL',       'Total State Water Project deliveries AG'),
    ('DEL_SWP_PMI',             'Total State Water Project deliveries M&I'),
    ('DEL_SWP_PMI_N',           'NOD State Water Project deliveries M&I'),
    ('DEL_SWP_PMI_S',           'SOD State Water Project deliveries M&I'),
    ('D_MLRTN_FRK000',          'Southern San Joaquin Valley exports (Friant Division)'),
    ('D_CAA238_CVPCV',          'Southern San Joaquin Valley exports (Cross Valley Canal)'),
    ('SWP_TA_KERNAG',           'Southern San Joaquin Valley Exports (Kern County Water Agency)'),
    ('C_CAA003_SWP',            'State Water Project Delta Exports'),
    ('C_CVPSWP_TOTAL_EXPORTS',  'total Delta Exports (CVP + SWP)')
) AS v(code, label)
ON CONFLICT (subject_kind, short_code) DO UPDATE
    SET label = EXCLUDED.label, is_active = TRUE;

-- Verification --------------------------------------------------------------
\echo ''
\echo 'System-deliveries subjects seeded (expect 25, location_type/location_id NULL for all):'
SELECT short_code, label, location_type, location_id
FROM data_in_depth_subject
WHERE subject_kind = 'metric'
  AND short_code IN (
    'DEL_CVP_TOT_N_WAMER','DEL_CVP_TOT_S_WLOSS','DEL_CVP_TOTAL','DEL_CVP_PAG_NOD','DEL_CVP_PAG_SOD',
    'DEL_CVP_PAG_TOTAL','DEL_CVP_PMI_TOTAL','DEL_CVP_PMI_N_WAMER','DEL_CVP_PMI_S','DEL_CVP_PRF_TOTAL',
    'C_CVP_TOTAL_EXPORTS','DEL_SWP_TOT_N','DEL_SWP_TOT_S','DEL_SWP_TOTAL','DEL_SWP_PAG_NOD',
    'DEL_SWP_PAG_S','DEL_SWP_PAG_TOTAL','DEL_SWP_PMI','DEL_SWP_PMI_N','DEL_SWP_PMI_S',
    'D_MLRTN_FRK000','D_CAA238_CVPCV','SWP_TA_KERNAG','C_CAA003_SWP','C_CVPSWP_TOTAL_EXPORTS'
  )
ORDER BY short_code;

\echo ''
\echo 'Row count check (expect 25):'
SELECT COUNT(*) AS n_seeded
FROM data_in_depth_subject
WHERE subject_kind = 'metric'
  AND short_code IN (
    'DEL_CVP_TOT_N_WAMER','DEL_CVP_TOT_S_WLOSS','DEL_CVP_TOTAL','DEL_CVP_PAG_NOD','DEL_CVP_PAG_SOD',
    'DEL_CVP_PAG_TOTAL','DEL_CVP_PMI_TOTAL','DEL_CVP_PMI_N_WAMER','DEL_CVP_PMI_S','DEL_CVP_PRF_TOTAL',
    'C_CVP_TOTAL_EXPORTS','DEL_SWP_TOT_N','DEL_SWP_TOT_S','DEL_SWP_TOTAL','DEL_SWP_PAG_NOD',
    'DEL_SWP_PAG_S','DEL_SWP_PAG_TOTAL','DEL_SWP_PMI','DEL_SWP_PMI_N','DEL_SWP_PMI_S',
    'D_MLRTN_FRK000','D_CAA238_CVPCV','SWP_TA_KERNAG','C_CAA003_SWP','C_CVPSWP_TOTAL_EXPORTS'
  );

\echo ''
\echo 'SYSTEM-DELIVERIES SUBJECTS SEEDED.'
\echo '===================================='
