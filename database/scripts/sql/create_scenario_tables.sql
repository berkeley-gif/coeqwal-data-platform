-- =============================================================================
-- CREATE SCENARIO-RELATED TABLES
-- Run this BEFORE upserting scenario data
-- Created: December 2025
-- =============================================================================

-- =============================================================================
-- 1. THEME TABLE (matches theme.csv columns)
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
    theme_version_id INTEGER NOT NULL DEFAULT 1,
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_by INTEGER
);

-- =============================================================================
-- 2. SCENARIO_AUTHOR TABLE (matches scenario_author.csv columns)
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
-- 3. HYDROCLIMATE TABLE (matches hydroclimate.csv columns)
-- =============================================================================
CREATE TABLE IF NOT EXISTS hydroclimate (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    name TEXT,
    subtitle TEXT,
    short_title TEXT,
    simple_description TEXT,
    description TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    narrative JSONB,
    projection_year TEXT,
    slr_value NUMERIC,
    slr_unit_id INTEGER,
    source_id INTEGER,
    notes TEXT,
    hydroclimate_version_id NUMERIC,
    created_by NUMERIC DEFAULT 1,
    updated_by NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Note: hydroclimate data will be loaded from S3 hydroclimate.csv

-- =============================================================================
-- 4. ASSUMPTION_DEFINITION TABLE (matches assumption_definition.csv columns)
-- =============================================================================
CREATE TABLE IF NOT EXISTS assumption_definition (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    name TEXT,
    short_title TEXT,
    subtitle TEXT,
    simple_description TEXT,
    description TEXT,
    narrative JSONB,
    category TEXT,
    source TEXT,
    source_access_date DATE,
    file TEXT,
    assumptions_version_id INTEGER DEFAULT 1,
    is_active INTEGER DEFAULT 1,
    notes TEXT,
    created_by INTEGER DEFAULT 1,
    updated_by INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- 5. OPERATION_DEFINITION TABLE (matches operation_definition.csv columns)
-- =============================================================================
CREATE TABLE IF NOT EXISTS operation_definition (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    name TEXT,
    short_title TEXT,
    subtitle TEXT,
    simple_description TEXT,
    description TEXT,
    narrative JSONB,
    category TEXT,
    is_active INTEGER DEFAULT 1,
    notes TEXT,
    operation_version_id INTEGER DEFAULT 1,
    created_by INTEGER DEFAULT 1,
    updated_by INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- 6. SCENARIO TABLE (matches scenario.csv columns)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario (
    id SERIAL PRIMARY KEY,
    scenario_id TEXT NOT NULL UNIQUE,
    short_code TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    name TEXT NOT NULL,
    subtitle TEXT,
    short_title TEXT,
    simple_description TEXT,
    description TEXT,
    narrative JSONB,
    baseline_scenario_id INTEGER,
    hydroclimate_id INTEGER,
    scenario_author_id INTEGER,
    scenario_version_id INTEGER NOT NULL DEFAULT 2,
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_by INTEGER
);

-- =============================================================================
-- 7. THEME_SCENARIO_LINK TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS theme_scenario_link (
    theme_id INTEGER NOT NULL,
    scenario_id INTEGER NOT NULL,
    PRIMARY KEY (theme_id, scenario_id)
);

-- =============================================================================
-- 8. SCENARIO_KEY_ASSUMPTION_LINK TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario_key_assumption_link (
    scenario_id INTEGER NOT NULL,
    assumption_id INTEGER NOT NULL,
    PRIMARY KEY (scenario_id, assumption_id)
);

-- =============================================================================
-- 9. SCENARIO_KEY_OPERATION_LINK TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS scenario_key_operation_link (
    scenario_id INTEGER NOT NULL,
    operation_id INTEGER NOT NULL,
    PRIMARY KEY (scenario_id, operation_id)
);

-- =============================================================================
-- VERIFICATION
-- =============================================================================
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('theme', 'scenario', 'scenario_author', 'hydroclimate', 
                   'theme_scenario_link', 'assumption_definition', 
                   'operation_definition', 'scenario_key_assumption_link', 
                   'scenario_key_operation_link')
ORDER BY table_name;

