-- =============================================================================
-- Migration 11: Add missing columns to theme (source, created_at, updated_at)
-- =============================================================================
-- theme was created without source or timestamp columns.
-- This migration adds them and populates all existing rows.
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/11_add_source_to_theme.sql
-- =============================================================================

-- ─── 1. Add wietske_medema to source lookup table ────────────────────────────

INSERT INTO source (source, description, is_active)
VALUES ('wietske_medema', 'Wietske Medema', true)
ON CONFLICT (source) DO NOTHING;

-- ─── 2. Add missing columns to theme ─────────────────────────────────────────

ALTER TABLE theme ADD COLUMN IF NOT EXISTS source     TEXT;
ALTER TABLE theme ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE theme ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();

ALTER TABLE theme
    ADD CONSTRAINT fk_theme_source
    FOREIGN KEY (source) REFERENCES source(source)
    ON UPDATE CASCADE ON DELETE RESTRICT;

-- ─── 3. Populate all current theme rows ──────────────────────────────────────

ALTER TABLE theme DISABLE TRIGGER USER;

UPDATE theme
SET source     = 'wietske_medema',
    created_at = '2024-01-01 00:00:00+00',
    updated_at = '2024-01-01 00:00:00+00',
    updated_by = 2
WHERE source IS NULL;

ALTER TABLE theme ENABLE TRIGGER USER;

-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT short_code, name, source, created_at, updated_at, updated_by FROM theme ORDER BY id;
