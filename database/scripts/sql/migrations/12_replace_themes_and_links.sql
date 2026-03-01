-- =============================================================================
-- Migration 12: Replace old themes with new 6-theme architecture
-- =============================================================================
-- Live DB still has 7 old themes (baseline, community_water, flow, gw_ag,
-- delta_flow, delta_uses, delta_export_reliability) from initial seeding.
-- This replaces them with the 6 current themes and seeds the correct
-- theme_scenario_link rows from THEME_SCENARIOS (themes.ts, Feb 2026).
--
-- New themes:
--   1  cws        Community water systems
--   2  ag_gw      Farms, groundwater & food systems
--   3  eco        Rivers, salmon & ecosystems
--   4  delta      The Delta as a living place
--   5  climate    Drought, climate risk, and resilience
--   6  governance Operations and impacts
--
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/12_replace_themes_and_links.sql
-- =============================================================================

BEGIN;

-- ─── 1. Clear old links and themes ───────────────────────────────────────────

DELETE FROM theme_scenario_link;
DELETE FROM theme;

SELECT setval('theme_id_seq', 1, false);

-- ─── 2. Insert new 6 themes ──────────────────────────────────────────────────

ALTER TABLE theme DISABLE TRIGGER USER;

INSERT INTO theme (id, short_code, is_active, name, short_title, simple_description,
                   source, theme_version_id, created_by, updated_by,
                   created_at, updated_at)
VALUES
    (1, 'cws',        1, 'Community water systems',            'Community water systems',
     'Whether people and communities can reliably access safe, affordable water for daily life, health, and essential services.',
     'wietske_medema', 1, 2, 2, '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'),
    (2, 'ag_gw',      1, 'Farms, groundwater & food systems',  'Farms & groundwater',
     'How water availability supports food production today, while sustaining groundwater and agricultural viability over time.',
     'wietske_medema', 1, 2, 2, '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'),
    (3, 'eco',        1, 'Rivers, salmon & ecosystems',        'Rivers & ecosystems',
     'Whether rivers, fish, and ecosystems receive the flows they need to remain functional and resilient.',
     'wietske_medema', 1, 2, 2, '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'),
    (4, 'delta',      1, 'The Delta as a living place',        'The Delta',
     'How water decisions affect the Delta as a place where communities, farms, and ecosystems coexist.',
     'wietske_medema', 1, 2, 2, '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'),
    (5, 'climate',    1, 'Drought, climate risk, and resilience', 'Climate resilience',
     'How the water system performs under increasing climate variability, drought risk, and extreme conditions.',
     'wietske_medema', 1, 2, 2, '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00'),
    (6, 'governance', 1, 'Operations and impacts',             'Operations & impacts',
     'How evidence, trade-offs, and equity considerations inform water-management decisions.',
     'wietske_medema', 1, 2, 2, '2024-01-01 00:00:00+00', '2024-01-01 00:00:00+00');

SELECT setval('theme_id_seq', 6);

ALTER TABLE theme ENABLE TRIGGER USER;

-- ─── 3. Insert theme_scenario_link from THEME_SCENARIOS (themes.ts) ──────────
-- Scenario integer IDs from scenario.csv:
--   s0011=1  s0020=2  s0021=3  s0023=4  s0024=5  s0025=6  s0027=7  s0029=8
--   s0026=9  s0028=10 s0030=11 s0031=12 s0032=13 s0033=14 s0039=15 s0040=16
--   s0041=17 s0042=18 s0044=19 s0045=20 s0046=21 s0065=22 s0035=23 s0036=24
--   s0037=25

INSERT INTO theme_scenario_link (theme_id, scenario_id)
VALUES
    -- cws (1): s0035, s0036, s0037
    (1, 23), (1, 24), (1, 25),
    -- ag_gw (2): s0011, s0025, s0026, s0027, s0028
    (2, 1), (2, 6), (2, 9), (2, 7), (2, 10),
    -- eco (3): s0030, s0029, s0032, s0031, s0033, s0046
    (3, 11), (3, 8), (3, 13), (3, 12), (3, 14), (3, 21),
    -- delta (4): s0040, s0041, s0042, s0039, s0044, s0045, s0028, s0065, s0030
    (4, 16), (4, 17), (4, 18), (4, 15), (4, 19), (4, 20), (4, 10), (4, 22), (4, 11),
    -- climate (5): none
    -- governance (6): s0020, s0021, s0023, s0024
    (6, 2), (6, 3), (6, 4), (6, 5);

COMMIT;

-- ─── Verify ──────────────────────────────────────────────────────────────────

SELECT id, short_code, name, source, created_by, updated_by
FROM theme ORDER BY id;

SELECT t.short_code, COUNT(tsl.scenario_id) AS scenario_count
FROM theme t
LEFT JOIN theme_scenario_link tsl ON tsl.theme_id = t.id
GROUP BY t.id, t.short_code
ORDER BY t.id;
