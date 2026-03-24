-- =============================================================================
-- CREATE SCENARIO-RELATED TABLES
-- Run this BEFORE upserting scenario data
-- Updated: March 2026 (post-migration 44 — current schema)
--
-- Creates tables across layers 05 (assumptions/operations), 06 (scenario),
-- 07 (hydroclimate), and 08 (theme).
-- =============================================================================

-- =============================================================================
-- 1. THEME TABLE (Layer 08)
-- =============================================================================
CREATE TABLE IF NOT EXISTS theme (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    is_active INTEGER NOT NULL DEFAULT 1,
    name TEXT NOT NULL,
    subtitle TEXT,
    short_title TEXT,
    simple_description TEXT,
    description TEXT,
    description_next TEXT,
    narrative JSONB,
    outcome_description TEXT,
    outcome_narrative TEXT,
    source TEXT,
    theme_version_id INTEGER NOT NULL DEFAULT 1,
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_by INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- 2. SCENARIO_AUTHOR TABLE (Layer 06)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario_author (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    email TEXT,
    organization TEXT,
    affiliation TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER
);

-- =============================================================================
-- 3. HYDROCLIMATE TABLE (Layer 07)
-- =============================================================================
CREATE TABLE IF NOT EXISTS hydroclimate (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    name TEXT,
    subtitle TEXT,
    short_title TEXT,
    simple_description TEXT,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    narrative JSONB,
    projection_year INTEGER,
    source_id INTEGER,
    notes TEXT,
    hydroclimate_version_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by INTEGER
);

-- =============================================================================
-- 4. ASSUMPTION_CATEGORY TABLE (Layer 05)
-- =============================================================================
CREATE TABLE IF NOT EXISTS assumption_category (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    label TEXT,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 2
);

-- =============================================================================
-- 5. ASSUMPTION_DEFINITION TABLE (Layer 05)
-- =============================================================================
CREATE TABLE IF NOT EXISTS assumption_definition (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR,
    short_title VARCHAR,
    assumption_category_id INTEGER NOT NULL,
    description TEXT,
    source_id INTEGER,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    notes TEXT,
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_by INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_assumption_definition_category
        FOREIGN KEY (assumption_category_id) REFERENCES assumption_category(id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- =============================================================================
-- 6. OPERATION_CATEGORY TABLE (Layer 05)
-- =============================================================================
CREATE TABLE IF NOT EXISTS operation_category (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    name TEXT,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 2
);

-- =============================================================================
-- 7. OPERATION_DEFINITION TABLE (Layer 05)
-- =============================================================================
CREATE TABLE IF NOT EXISTS operation_definition (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    name VARCHAR,
    short_title VARCHAR,
    operation_category_id INTEGER NOT NULL,
    description TEXT,
    source_id INTEGER,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    notes TEXT,
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_by INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_operation_definition_category
        FOREIGN KEY (operation_category_id) REFERENCES operation_category(id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- =============================================================================
-- 8a. SIBLING_GROUP TABLE (Layer 06 — operational configuration families)
-- =============================================================================
CREATE TABLE IF NOT EXISTS sibling_group (
    short_code VARCHAR PRIMARY KEY,
    name VARCHAR,
    short_description TEXT,
    long_description TEXT,
    baseline_group VARCHAR,
    scenario_author_id INTEGER,
    model_source_id INTEGER,
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_by INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_sibling_group_baseline
        FOREIGN KEY (baseline_group) REFERENCES sibling_group(short_code)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT fk_sibling_group_author
        FOREIGN KEY (scenario_author_id) REFERENCES scenario_author(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_sibling_group_model_source
        FOREIGN KEY (model_source_id) REFERENCES model_source(id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- =============================================================================
-- 8b. SCENARIO TABLE (Layer 06)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    run_name VARCHAR,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    hydroclimate_id INTEGER,
    sibling_group VARCHAR,
    scenario_version_id INTEGER DEFAULT 1,
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_by INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_scenario_sibling_group
        FOREIGN KEY (sibling_group) REFERENCES sibling_group(short_code)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

-- =============================================================================
-- 9. SCENARIO_TAG TABLE (Layer 06)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario_tag (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL UNIQUE,
    label VARCHAR NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 2
);

-- =============================================================================
-- 10. SCENARIO_TAG_LINK TABLE (Layer 06)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario_tag_link (
    scenario_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 2,
    PRIMARY KEY (scenario_id, tag_id),
    CONSTRAINT scenario_tag_link_scenario_id_fkey
        FOREIGN KEY (scenario_id) REFERENCES scenario(id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT scenario_tag_link_tag_id_fkey
        FOREIGN KEY (tag_id) REFERENCES scenario_tag(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- =============================================================================
-- 11. THEME_SCENARIO_LINK TABLE (Layer 08)
-- =============================================================================
CREATE TABLE IF NOT EXISTS theme_scenario_link (
    theme_id INTEGER NOT NULL,
    scenario_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 2,
    PRIMARY KEY (theme_id, scenario_id),
    CONSTRAINT theme_scenario_link_theme_id_fkey
        FOREIGN KEY (theme_id) REFERENCES theme(id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT theme_scenario_link_scenario_id_fkey
        FOREIGN KEY (scenario_id) REFERENCES scenario(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- =============================================================================
-- 12. SCENARIO_KEY_ASSUMPTION_LINK TABLE (Layer 05)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario_key_assumption_link (
    scenario_id INTEGER NOT NULL,
    assumption_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 2,
    PRIMARY KEY (scenario_id, assumption_id),
    CONSTRAINT scenario_key_assumption_link_scenario_id_fkey
        FOREIGN KEY (scenario_id) REFERENCES scenario(id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT scenario_key_assumption_link_assumption_id_fkey
        FOREIGN KEY (assumption_id) REFERENCES assumption_definition(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- =============================================================================
-- 13. SCENARIO_KEY_OPERATION_LINK TABLE (Layer 05)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario_key_operation_link (
    scenario_id INTEGER NOT NULL,
    operation_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 2,
    PRIMARY KEY (scenario_id, operation_id),
    CONSTRAINT scenario_key_operation_link_scenario_id_fkey
        FOREIGN KEY (scenario_id) REFERENCES scenario(id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT scenario_key_operation_link_operation_id_fkey
        FOREIGN KEY (operation_id) REFERENCES operation_definition(id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- =============================================================================
-- VERIFICATION
-- =============================================================================
\echo ''
\echo 'Tables created:'
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name IN (
    'theme', 'scenario', 'scenario_author', 'hydroclimate',
    'assumption_category', 'assumption_definition',
    'operation_category', 'operation_definition',
    'scenario_tag', 'scenario_tag_link',
    'theme_scenario_link', 'scenario_key_assumption_link',
    'scenario_key_operation_link'
)
ORDER BY table_name;
