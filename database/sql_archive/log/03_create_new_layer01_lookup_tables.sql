-- =============================================================================
-- 03_create_new_layer01_lookup_tables.sql
-- Creates three new Layer 01 lookup tables and fixes variable_type permissions.
-- =============================================================================

\echo '============================================================================'
\echo 'MIGRATION 03: NEW LAYER 01 LOOKUP TABLES'
\echo '============================================================================'


-- =============================================================================
-- PART 1: Fix variable_type permissions
-- =============================================================================
\echo ''
\echo 'PART 1: Granting access to variable_type...'

GRANT SELECT, INSERT, UPDATE, DELETE ON variable_type TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE variable_type_id_seq TO jfantauzza;


-- =============================================================================
-- PART 2: watershed
-- =============================================================================
\echo ''
\echo 'PART 2: Creating watershed table...'

CREATE TABLE IF NOT EXISTS watershed (
    id                           SERIAL PRIMARY KEY,
    short_code                   VARCHAR UNIQUE NOT NULL,
    name                         VARCHAR NOT NULL,
    description                  TEXT,
    hydrologic_region_short_code VARCHAR,
    is_active                    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by                   INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by                   INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

ALTER TABLE watershed
    ADD CONSTRAINT watershed_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES developer(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE watershed
    ADD CONSTRAINT watershed_updated_by_fkey
    FOREIGN KEY (updated_by) REFERENCES developer(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

CREATE INDEX IF NOT EXISTS idx_watershed_hydrologic_region
    ON watershed(hydrologic_region_short_code);

CREATE TRIGGER audit_fields_watershed
    BEFORE INSERT OR UPDATE ON watershed
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

\echo '  Seeding watershed...'
INSERT INTO watershed (short_code, name, description, hydrologic_region_short_code, is_active) VALUES
    ('BEAR_RIVER',        'Bear River Watershed',               'Bear River Watershed',                          'SJR', true),
    ('SAC_RIVER',         'Sacramento River Hydrologic Region', 'Sacramento River Hydrologic Region watershed',  'SAC', true),
    ('SAN_JOAQUIN',       'San Joaquin River Hydrologic Region','San Joaquin River Hydrologic Region watershed', 'SJR', true),
    ('UPPER_AMERICAN',    'Upper American River Watershed',     'Upper American River Watershed',                'SJR', true),
    ('UPPER_FEATHER',     'Upper Feather River Watershed',      'Upper Feather River Watershed',                 'SJR', true),
    ('UPPER_MOKELUMNE',   'Upper Mokelumne River Watershed',    'Upper Mokelumne River Watershed',               'SJR', true),
    ('UPPER_STANISLAUS',  'Upper Stanislaus River',             'Upper Stanislaus River watershed',              'SJR', true),
    ('UPPER_TUOLUMNE',    'Upper Tuolumne River Watershed',     'Upper Tuolumne River Watershed',                'SJR', true),
    ('YUBA_RIVER',        'Yuba River Watershed',               'Yuba River Watershed',                          'SJR', true)
ON CONFLICT (short_code) DO NOTHING;


-- =============================================================================
-- PART 3: calsim_model_variable_type
-- Source: formerly misnamed variable_type.csv in seed_tables root.
-- =============================================================================
\echo ''
\echo 'PART 3: Creating calsim_model_variable_type table...'

CREATE TABLE IF NOT EXISTS calsim_model_variable_type (
    id          SERIAL PRIMARY KEY,
    short_code  TEXT UNIQUE NOT NULL,
    label       TEXT NOT NULL,
    description TEXT,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by  INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by  INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

ALTER TABLE calsim_model_variable_type
    ADD CONSTRAINT calsim_model_variable_type_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES developer(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE calsim_model_variable_type
    ADD CONSTRAINT calsim_model_variable_type_updated_by_fkey
    FOREIGN KEY (updated_by) REFERENCES developer(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

CREATE INDEX IF NOT EXISTS idx_calsim_model_variable_type_active
    ON calsim_model_variable_type(is_active, short_code);

CREATE TRIGGER audit_fields_calsim_model_variable_type
    BEFORE INSERT OR UPDATE ON calsim_model_variable_type
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

\echo '  Seeding calsim_model_variable_type...'
INSERT INTO calsim_model_variable_type (short_code, label, description, is_active) VALUES
    ('output',        'Output',        'Standard CalSim model output variables (flows, diversions, storage)', true),
    ('control',       'Control',       'Operational control indicators and binary flags',                     true),
    ('decision',      'Decision',      'Model decision variables and optimization targets',                   true),
    ('state',         'State',         'State variables including storage zones and bookkeeping accounts',    true),
    ('input',         'Input',         'External inputs and boundary conditions',                             true),
    ('intermediate',  'Intermediate',  'Calculated intermediate values used in model logic',                  true),
    ('aggregate',     'Aggregate',     'Variables that sum or combine multiple reservoir/system components',  true),
    ('index',         'Index',         'Index variables',                                                     true)
ON CONFLICT (short_code) DO NOTHING;


-- =============================================================================
-- PART 4: derived_variable_type
-- Source: seed_tables/derived_variable_type.csv (column renamed name→label).
-- =============================================================================
\echo ''
\echo 'PART 4: Creating derived_variable_type table...'

CREATE TABLE IF NOT EXISTS derived_variable_type (
    id          SERIAL PRIMARY KEY,
    short_code  TEXT UNIQUE NOT NULL,
    label       TEXT NOT NULL,
    description TEXT,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by  INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by  INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

ALTER TABLE derived_variable_type
    ADD CONSTRAINT derived_variable_type_created_by_fkey
    FOREIGN KEY (created_by) REFERENCES developer(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE derived_variable_type
    ADD CONSTRAINT derived_variable_type_updated_by_fkey
    FOREIGN KEY (updated_by) REFERENCES developer(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

CREATE INDEX IF NOT EXISTS idx_derived_variable_type_active
    ON derived_variable_type(is_active, short_code);

CREATE TRIGGER audit_fields_derived_variable_type
    BEFORE INSERT OR UPDATE ON derived_variable_type
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

\echo '  Seeding derived_variable_type...'
INSERT INTO derived_variable_type (short_code, label, description, is_active) VALUES
    ('sector_aggregate',        'Sector Aggregate',       'Variables that aggregate across a sector',               true),
    ('delta_variable',          'Delta',                  'Variables specific to Delta conditions and operations',  true),
    ('environmental_indicator', 'Environmental Indicator','Environmental metrics and indicators',                   true),
    ('regional_summary',        'Regional Summary',       'Variables that aggregate across a region',               true)
ON CONFLICT (short_code) DO NOTHING;


-- =============================================================================
-- PART 5: Grant permissions on new tables to jfantauzza
-- =============================================================================
\echo ''
\echo 'PART 5: Granting permissions on new tables...'

GRANT SELECT, INSERT, UPDATE, DELETE ON watershed               TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE watershed_id_seq                TO jfantauzza;

GRANT SELECT, INSERT, UPDATE, DELETE ON calsim_model_variable_type       TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE calsim_model_variable_type_id_seq        TO jfantauzza;

GRANT SELECT, INSERT, UPDATE, DELETE ON derived_variable_type   TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE derived_variable_type_id_seq    TO jfantauzza;


-- =============================================================================
-- VERIFY
-- =============================================================================
\echo ''
\echo 'VERIFY: Row counts'

SELECT 'variable_type'              AS table_name, COUNT(*) AS rows FROM variable_type
UNION ALL
SELECT 'watershed',                               COUNT(*) FROM watershed
UNION ALL
SELECT 'calsim_model_variable_type',              COUNT(*) FROM calsim_model_variable_type
UNION ALL
SELECT 'derived_variable_type',                   COUNT(*) FROM derived_variable_type
ORDER BY table_name;

\echo ''
\echo '============================================================================'
\echo 'MIGRATION 03 COMPLETE'
\echo '============================================================================'
