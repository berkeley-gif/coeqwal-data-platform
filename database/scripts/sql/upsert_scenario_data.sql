-- UPSERT SCENARIO DATA FROM REPO
-- Updates scenario, theme_scenario_link, scenario_key_assumption_link,
--         scenario_key_operation_link, scenario_tag_link
--
-- Run from the repository root:
--   psql $SUPERUSER_URL -f database/scripts/sql/upsert_scenario_data.sql
--
-- Seed data is loaded via \copy from local CSVs. If seed CSVs are stale,
-- copy the latest audit exports into the seed_tables paths below:
--   cp audits/monthly_*/layer_exports/06_scenario/scenario.csv \
--      database/seed_tables/06_scenario/scenario.csv
--   (repeat for crosswalk tables)

\echo ''
\echo 'UPSERTING SCENARIO DATA (06_scenario + crosswalks)'
\echo '==================================================='

\echo ''
\echo 'Current table status:'
SELECT 'scenario' AS table_name, COUNT(*) AS record_count FROM scenario
UNION ALL
SELECT 'theme_scenario_link', COUNT(*) FROM theme_scenario_link
UNION ALL
SELECT 'scenario_key_assumption_link', COUNT(*) FROM scenario_key_assumption_link
UNION ALL
SELECT 'scenario_key_operation_link', COUNT(*) FROM scenario_key_operation_link
UNION ALL
SELECT 'scenario_tag_link', COUNT(*) FROM scenario_tag_link;

-- =============================================================================
-- STEP 1: Clear link tables first (they reference scenario)
-- =============================================================================
\echo ''
\echo 'Clearing link tables...'
TRUNCATE TABLE theme_scenario_link CASCADE;
TRUNCATE TABLE scenario_key_assumption_link CASCADE;
TRUNCATE TABLE scenario_key_operation_link CASCADE;
TRUNCATE TABLE scenario_tag_link CASCADE;
\echo 'Link tables cleared'

-- =============================================================================
-- STEP 2: Upsert scenario table via staging
-- =============================================================================
\echo ''
\echo 'Loading scenarios from repo...'

CREATE TEMP TABLE scenario_staging (
    id                   INTEGER,
    short_code           VARCHAR,
    run_name             VARCHAR,
    is_active            BOOLEAN,
    name                 VARCHAR,
    short_description    TEXT,
    baseline_scenario_id INTEGER,
    hydroclimate_id      INTEGER,
    scenario_version_id  INTEGER,
    scenario_author_id   INTEGER,
    model_source_id      INTEGER,
    created_by           INTEGER,
    updated_by           INTEGER,
    created_at           TIMESTAMP WITH TIME ZONE,
    updated_at           TIMESTAMP WITH TIME ZONE,
    long_description     TEXT
);

\copy scenario_staging (id, short_code, run_name, is_active, name, short_description, baseline_scenario_id, hydroclimate_id, scenario_version_id, scenario_author_id, model_source_id, created_by, updated_by, created_at, updated_at, long_description) FROM 'database/seed_tables/06_scenario/scenario.csv' WITH (FORMAT csv, HEADER true, NULL '');

INSERT INTO scenario (
    id, short_code, run_name, is_active, name, short_description,
    baseline_scenario_id, hydroclimate_id, scenario_version_id,
    scenario_author_id, model_source_id, created_by, updated_by,
    long_description
)
SELECT
    id, short_code, run_name, COALESCE(is_active, TRUE), name,
    short_description, baseline_scenario_id, hydroclimate_id,
    COALESCE(scenario_version_id, 1), scenario_author_id, model_source_id,
    COALESCE(created_by, 2), COALESCE(updated_by, 2),
    long_description
FROM scenario_staging
ON CONFLICT (id) DO UPDATE SET
    short_code           = EXCLUDED.short_code,
    run_name             = EXCLUDED.run_name,
    is_active            = EXCLUDED.is_active,
    name                 = EXCLUDED.name,
    short_description    = EXCLUDED.short_description,
    baseline_scenario_id = EXCLUDED.baseline_scenario_id,
    hydroclimate_id      = EXCLUDED.hydroclimate_id,
    scenario_version_id  = EXCLUDED.scenario_version_id,
    scenario_author_id   = EXCLUDED.scenario_author_id,
    model_source_id      = EXCLUDED.model_source_id,
    long_description     = EXCLUDED.long_description,
    updated_by           = EXCLUDED.updated_by;

DROP TABLE scenario_staging;

SELECT setval('scenario_id_seq', COALESCE((SELECT MAX(id) FROM scenario), 0) + 1, false);

\echo 'Scenarios upserted, sequence reset'

-- =============================================================================
-- STEP 3: Load theme_scenario_link
-- =============================================================================
\echo ''
\echo 'Loading theme_scenario_link from repo...'

\copy theme_scenario_link (theme_id, scenario_id) FROM 'database/seed_tables/08_theme/theme_scenario_link.csv' WITH (FORMAT csv, HEADER true, NULL '');

\echo 'theme_scenario_link loaded'

-- =============================================================================
-- STEP 4: Load scenario_key_assumption_link
-- =============================================================================
\echo ''
\echo 'Loading scenario_key_assumption_link from repo...'

\copy scenario_key_assumption_link (scenario_id, assumption_id) FROM 'database/seed_tables/06_scenario/scenario_key_assumption_link.csv' WITH (FORMAT csv, HEADER true, NULL '');

\echo 'scenario_key_assumption_link loaded'

-- =============================================================================
-- STEP 5: Load scenario_key_operation_link
-- =============================================================================
\echo ''
\echo 'Loading scenario_key_operation_link from repo...'

\copy scenario_key_operation_link (scenario_id, operation_id) FROM 'database/seed_tables/06_scenario/scenario_key_operation_link.csv' WITH (FORMAT csv, HEADER true, NULL '');

\echo 'scenario_key_operation_link loaded'

-- =============================================================================
-- STEP 6: Load scenario_tag_link
-- =============================================================================
\echo ''
\echo 'Loading scenario_tag_link from repo...'

\copy scenario_tag_link (scenario_id, tag_id) FROM 'database/seed_tables/06_scenario/scenario_tag_link.csv' WITH (FORMAT csv, HEADER true, NULL '');

\echo 'scenario_tag_link loaded'

-- =============================================================================
-- VERIFICATION
-- =============================================================================
\echo ''
\echo 'VERIFICATION:'
\echo '============='

\echo ''
\echo 'Scenario summary:'
SELECT id, short_code, name, is_active, baseline_scenario_id
FROM scenario
ORDER BY id;

\echo ''
\echo 'Theme assignments:'
SELECT s.short_code, t.short_code AS theme, t.name AS theme_name
FROM theme_scenario_link tsl
JOIN scenario s ON tsl.scenario_id = s.id
JOIN theme t ON tsl.theme_id = t.id
ORDER BY s.id;

\echo ''
\echo 'Assumption assignments:'
SELECT s.short_code, ad.short_code AS assumption, ac.short_code AS category
FROM scenario_key_assumption_link skal
JOIN scenario s ON skal.scenario_id = s.id
JOIN assumption_definition ad ON skal.assumption_id = ad.id
JOIN assumption_category ac ON ac.id = ad.assumption_category_id
ORDER BY s.id, ac.short_code;

\echo ''
\echo 'Operation assignments:'
SELECT s.short_code, od.short_code AS operation, oc.short_code AS category
FROM scenario_key_operation_link skol
JOIN scenario s ON skol.scenario_id = s.id
JOIN operation_definition od ON skol.operation_id = od.id
JOIN operation_category oc ON oc.id = od.operation_category_id
ORDER BY s.id, oc.short_code;

\echo ''
\echo 'Tag assignments:'
SELECT s.short_code, string_agg(t.label, ', ' ORDER BY t.label) AS tags
FROM scenario_tag_link stl
JOIN scenario s ON s.id = stl.scenario_id
JOIN scenario_tag t ON t.id = stl.tag_id
GROUP BY s.short_code
ORDER BY s.short_code;

\echo ''
\echo 'Final counts:'
SELECT
    (SELECT COUNT(*) FROM scenario) AS scenarios,
    (SELECT COUNT(*) FROM theme_scenario_link) AS theme_links,
    (SELECT COUNT(*) FROM scenario_key_assumption_link) AS assumption_links,
    (SELECT COUNT(*) FROM scenario_key_operation_link) AS operation_links,
    (SELECT COUNT(*) FROM scenario_tag_link) AS tag_links;

\echo ''
\echo 'SCENARIO DATA UPSERT COMPLETE!'
\echo '==============================='
