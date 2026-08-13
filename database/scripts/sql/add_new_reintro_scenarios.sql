-- add_new_reintro_scenarios.sql
--
-- Adds new salmon reintroduction scenarios to the database.
--
-- Created by Eric Lehmer 8/11/2026
BEGIN;

-- =============================================================================
-- STEP 1: Create Hydroclimate Sibling entries for reintroduction scenarios
-- =============================================================================

INSERT INTO scenario_hydroclimate_sibling (short_code, name, short_description, long_description, baseline_group)
SELECT 's0020-R', 'Current operations with reintroduction', 'Existing operational rules for Central Valley water allocations, as specified by DWR in 2023. Strategy also represents recent (2020) land use and allows for TUCPs [i]. This strategy serves as the baseline for comparison with other scenarios. This strategy also includes the reintroduction of up to 2,000 female winter-run Chinook salmon spawners to the McCloud River (above Shasta).', long_description, baseline_group FROM scenario_hydroclimate_sibling WHERE short_code = 's0020';
INSERT INTO scenario_hydroclimate_sibling (short_code, name, short_description, long_description, baseline_group)
SELECT 's0046-R', 'Functional environmental flows with reintroduction', 'Current operations with functional flow requirements implemented on tributaries to the Sacramento and San Joaquin River. This strategy also includes the reintroduction of up to 2,000 female winter-run Chinook salmon spawners to the McCloud River (above Shasta).', long_description, baseline_group FROM scenario_hydroclimate_sibling WHERE short_code = 's0046';
INSERT INTO scenario_hydroclimate_sibling (short_code, name, short_description, long_description, baseline_group)
SELECT 's0032-R', 'Functional environmental flows with groundwater regulations with reintroduction', 'Functional flow requirements implemented on tributaries to the Sacramento and San Joaquin River, combined with groundwater pumping limits and reduced irrigated agricultural acreage, reflecting compliance with SGMA. This strategy also includes the reintroduction of up to 2,000 female winter-run Chinook salmon spawners to the McCloud River (above Shasta).', long_description, baseline_group FROM scenario_hydroclimate_sibling WHERE short_code = 's0032';
INSERT INTO scenario_hydroclimate_sibling (short_code, name, short_description, long_description, baseline_group)
SELECT 's0031-R', 'Winter-run refuge flows with reintroduction', 'Sacramento River flow requirements and Shasta cold-water storage protection to support Sacramento River winter-run Chinook salmon life cycle needs. This strategy also includes the reintroduction of up to 2,000 female winter-run Chinook salmon spawners to the McCloud River (above Shasta).', long_description, baseline_group FROM scenario_hydroclimate_sibling WHERE short_code = 's0031';
INSERT INTO scenario_hydroclimate_sibling (short_code, name, short_description, long_description, baseline_group)
SELECT 's0033-R', 'Winter-run refuge flows with groundwater regulations with reintroduction', 'Sacramento River flow requirements and Shasta cold-water storage protection to support Sacramento River winter-run Chinook salmon life cycle needs, combined with groundwater pumping limits and reduced irrigated agricultural acreage, reflecting compliance with SGMA. This strategy also includes the reintroduction of up to 2,000 female winter-run Chinook salmon spawners to the McCloud River (above Shasta).', long_description, baseline_group FROM scenario_hydroclimate_sibling WHERE short_code = 's0033';

-- =============================================================================
-- STEP 2: Create new scenarios for reintroduction scenarios
-- =============================================================================

INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0020-R', run_name, 'FALSE', hydroclimate_id, 's0020-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0020';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0047-R', run_name, 'FALSE', hydroclimate_id, 's0020-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0047';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0056-R', run_name, 'FALSE', hydroclimate_id, 's0020-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0056';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0108-R', run_name, 'FALSE', hydroclimate_id, 's0020-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0108';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0134-R', run_name, 'FALSE', hydroclimate_id, 's0020-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0134';

INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0046-R', run_name, 'FALSE', hydroclimate_id, 's0046-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0046';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0084-R', run_name, 'FALSE', hydroclimate_id, 's0046-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0084';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0104-R', run_name, 'FALSE', hydroclimate_id, 's0046-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0104';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0130-R', run_name, 'FALSE', hydroclimate_id, 's0046-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0130';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0156-R', run_name, 'FALSE', hydroclimate_id, 's0046-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0156';

INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0032-R', run_name, 'FALSE', hydroclimate_id, 's0032-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0032';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0073-R', run_name, 'FALSE', hydroclimate_id, 's0032-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0073';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0093-R', run_name, 'FALSE', hydroclimate_id, 's0032-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0093';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0119-R', run_name, 'FALSE', hydroclimate_id, 's0032-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0119';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0145-R', run_name, 'FALSE', hydroclimate_id, 's0032-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0145';

INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0031-R', run_name, 'FALSE', hydroclimate_id, 's0031-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0031';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0072-R', run_name, 'FALSE', hydroclimate_id, 's0031-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0072';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0092-R', run_name, 'FALSE', hydroclimate_id, 's0031-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0092';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0118-R', run_name, 'FALSE', hydroclimate_id, 's0031-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0118';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0144-R', run_name, 'FALSE', hydroclimate_id, 's0031-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0144';

INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0033-R', run_name, 'FALSE', hydroclimate_id, 's0033-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0033';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0074-R', run_name, 'FALSE', hydroclimate_id, 's0033-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0074';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0094-R', run_name, 'FALSE', hydroclimate_id, 's0033-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0094';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0120-R', run_name, 'FALSE', hydroclimate_id, 's0033-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0120';
INSERT INTO scenario (short_code, run_name, is_active, hydroclimate_id, hydroclimate_sibling, scenario_version_id, scenario_author_id, model_source_id)
SELECT 's0146-R', run_name, 'FALSE', hydroclimate_id, 's0033-R', scenario_version_id, scenario_author_id, model_source_id FROM scenario WHERE short_code = 's0146';

-- =============================================================================
-- STEP 3: Create Theme Scenario Links for reintroduction scenarios
-- =============================================================================
-- NOT ENABLED
-- INSERT INTO theme_scenario_link (scenario_id, theme_id) SELECT t1.id, t2.theme_id FROM (SELECT id FROM scenario WHERE short_code = 's0020-R') as t1, (SELECT theme_id FROM theme_scenario_link, scenario WHERE theme_scenario_link.scenario_id = scenario.id AND scenario.short_code = 's0020') as t2;
-- INSERT INTO theme_scenario_link (scenario_id, theme_id) SELECT t1.id, t2.theme_id FROM (SELECT id FROM scenario WHERE short_code = 's0020-R') as t1, (SELECT theme_id FROM theme_scenario_link, scenario WHERE theme_scenario_link.scenario_id = scenario.id AND scenario.short_code = 's0020') as t2;


COMMIT;