-- Migration 43: Clean up layer 06 scenario tables
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/43_scenario_layer_cleanup.sql
--
-- scenario_author:  update record 3 (short_code, name, affiliation)
-- scenario:         resequence IDs by short_code order, move long_description to
--                   last column, fix created_by/updated_by=2, force
--                   scenario_version_id=1, populate long_description from word doc
-- dependent tables: remap scenario_id in crosswalks, tags, themes

BEGIN;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 1: UPDATE scenario_author
-- ══════════════════════════════════════════════════════════════════════

UPDATE scenario_author SET
    short_code  = 'coeqwal',
    name        = 'COEQWAL modeling team based on model files provided by USBR and DWR',
    affiliation = 'COEQWAL'
WHERE id = 3;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 2: PREPARE FOR SCENARIO TABLE RECREATION
-- ══════════════════════════════════════════════════════════════════════

DROP VIEW IF EXISTS scenario_full;

-- Drop inbound FKs (other tables → scenario)
ALTER TABLE theme_scenario_link          DROP CONSTRAINT IF EXISTS theme_scenario_link_scenario_id_fkey;
ALTER TABLE scenario_key_assumption_link DROP CONSTRAINT IF EXISTS scenario_key_assumption_link_scenario_id_fkey;
ALTER TABLE scenario_key_operation_link  DROP CONSTRAINT IF EXISTS scenario_key_operation_link_scenario_id_fkey;
ALTER TABLE scenario_tag_link            DROP CONSTRAINT IF EXISTS scenario_tag_link_scenario_id_fkey;

-- Drop outbound FKs (scenario → other tables)
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS fk_scenario_scenario_author;
ALTER TABLE scenario DROP CONSTRAINT IF EXISTS scenario_model_source_id_fkey;

-- Drop indexes
DROP INDEX IF EXISTS idx_scenario_active;
DROP INDEX IF EXISTS idx_scenario_active_version;
DROP INDEX IF EXISTS idx_scenario_baseline;
DROP INDEX IF EXISTS idx_scenario_hydroclimate;
DROP INDEX IF EXISTS idx_scenario_run_name_active;
DROP INDEX IF EXISTS idx_scenario_model_source;

-- Drop trigger
DROP TRIGGER IF EXISTS audit_fields_scenario ON scenario;

-- Build mapping: old_id → new_id (sorted by short_code)
CREATE TEMP TABLE scenario_id_map AS
SELECT id AS old_id,
       ROW_NUMBER() OVER (ORDER BY short_code)::INTEGER AS new_id
FROM scenario;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 3: REMAP scenario_id IN DEPENDENT TABLES
-- ══════════════════════════════════════════════════════════════════════

ALTER TABLE scenario_key_assumption_link DISABLE TRIGGER USER;
UPDATE scenario_key_assumption_link l
SET scenario_id = m.new_id
FROM scenario_id_map m WHERE l.scenario_id = m.old_id;
ALTER TABLE scenario_key_assumption_link ENABLE TRIGGER USER;

ALTER TABLE scenario_key_operation_link DISABLE TRIGGER USER;
UPDATE scenario_key_operation_link l
SET scenario_id = m.new_id
FROM scenario_id_map m WHERE l.scenario_id = m.old_id;
ALTER TABLE scenario_key_operation_link ENABLE TRIGGER USER;

ALTER TABLE theme_scenario_link DISABLE TRIGGER USER;
UPDATE theme_scenario_link l
SET scenario_id = m.new_id
FROM scenario_id_map m WHERE l.scenario_id = m.old_id;
ALTER TABLE theme_scenario_link ENABLE TRIGGER USER;

ALTER TABLE scenario_tag_link DISABLE TRIGGER USER;
UPDATE scenario_tag_link l
SET scenario_id = m.new_id
FROM scenario_id_map m WHERE l.scenario_id = m.old_id;
ALTER TABLE scenario_tag_link ENABLE TRIGGER USER;

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 4: RECREATE SCENARIO TABLE
-- ══════════════════════════════════════════════════════════════════════
-- Target column order (long_description moved to very end):
--   id, short_code, run_name, is_active, name, short_description,
--   baseline_scenario_id, hydroclimate_id, scenario_version_id,
--   scenario_author_id, model_source_id,
--   created_by, updated_by, created_at, updated_at,
--   long_description

ALTER SEQUENCE scenario_id_seq OWNED BY NONE;

CREATE TABLE scenario_new (
    id                    INTEGER          NOT NULL,
    short_code            VARCHAR          NOT NULL,
    run_name              VARCHAR,
    is_active             BOOLEAN          NOT NULL DEFAULT TRUE,
    name                  VARCHAR,
    short_description     TEXT,
    baseline_scenario_id  INTEGER,
    hydroclimate_id       INTEGER,
    scenario_version_id   INTEGER          DEFAULT 1,
    scenario_author_id    INTEGER,
    model_source_id       INTEGER,
    created_by            INTEGER          NOT NULL,
    updated_by            INTEGER          NOT NULL,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    long_description      TEXT
);

INSERT INTO scenario_new (
    id, short_code, run_name, is_active, name, short_description,
    baseline_scenario_id, hydroclimate_id, scenario_version_id,
    scenario_author_id, model_source_id,
    created_by, updated_by, created_at, updated_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY short_code)::INTEGER,
    short_code, run_name, is_active, name, short_description,
    baseline_scenario_id, hydroclimate_id,
    1,
    scenario_author_id, model_source_id,
    2, 2, created_at, NOW()
FROM scenario
ORDER BY short_code;

DROP TABLE scenario;
ALTER TABLE scenario_new RENAME TO scenario;

ALTER TABLE scenario ALTER COLUMN id SET DEFAULT nextval('scenario_id_seq');
ALTER SEQUENCE scenario_id_seq OWNED BY scenario.id;
SELECT setval('scenario_id_seq', 25);

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 5: POPULATE long_description FROM WORD DOC
-- ══════════════════════════════════════════════════════════════════════

UPDATE scenario SET long_description = $ld$This scenario serves as a reference and basis for select alternative scenarios. The code (WRESL and table files) and input hydrology were taken directly from DWR's 2023 Delivery Capability Report (DCR) final version (https://water.ca.gov/Library/Modeling-and-Analysis/Central-Valley-models-and-tools/CalSim-3/DCR) as made available on the DWR data portal (https://data.cnra.ca.gov/dataset/final-dcr-2023-calsim3-modelsels) and represents the current operating rules for major Central Valley systems up to and including the 2019 Biological Opinion and 2020 SWP Incidental Take Permit. A description of assumptions and changes to code that make up the operation rules in this version is provided in the 2023 DCR technical addendum (e.g. Table 2):
The hydroclimate inputs to this scenario are an adjusted version of the historical record (Oct 1921 – Sep 2021): earlier years are adjusted to reflect current climate conditions. More information on the adjustment process can be found in a DWR report here: https://data.cnra.ca.gov/dataset/finaldcr2023/resource/02429384-40a5-4167-9bd1-588ca5e213a4.

The only difference between this scenario and s0002 is that TUCP actions are allowed in this scenario (svar simulateTUCP set to 1 in the main wresl file). TUCP actions can be triggered February through May if the Sacramento Valley index and shasta storage both fall below certain thresholds. See Run\Other\TUCP_Actions.wresl for the details on TUCP action triggers.$ld$
WHERE short_code = 's0011';

UPDATE scenario SET long_description = $ld$This scenario serves as a reference and basis for alternative scenarios. The code (WRESL and table files) and input hydrology were taken directly from DWR's 2023 Delivery Capability Report (DCR) final version (https://water.ca.gov/Library/Modeling-and-Analysis/Central-Valley-models-and-tools/CalSim-3/DCR) as made available on the DWR data portal (https://data.cnra.ca.gov/dataset/final-dcr-2023-calsim3-modelsels) and represents the current operating rules for major Central Valley systems up to and including the 2019 Biological Opinion and 2020 SWP Incidental Take Permit. Unless otherwise noted, the operations and assumptions in this scenario form the basis of all other alternative scenarios. A description of assumptions and changes to code that make up the operation rules in this version is provided in the 2023 DCR technical addendum (e.g. Table 2):
The hydroclimate inputs to this scenario are an adjusted version of the historical record (Oct 1921 – Sep 2021): earlier years are adjusted to reflect current climate conditions. More information on the adjustment process can be found in a DWR report here: https://data.cnra.ca.gov/dataset/finaldcr2023/resource/02429384-40a5-4167-9bd1-588ca5e213a4. The COEQWAL team modified the agricultural demands in the SV input file to reflect a different assumed land use that was derived from the 2020 LandIQ dataset.

This scenario builds on s0011 - the only difference between s0011 and this is the agricultural land use that was used to determine irrigation demands.$ld$
WHERE short_code = 's0020';

UPDATE scenario SET long_description = $ld$This scenario serves as a reference and basis for alternative scenarios. The code (WRESL and table files) and input hydrology were taken directly from DWR's 2023 Delivery Capability Report (DCR) final version (https://water.ca.gov/Library/Modeling-and-Analysis/Central-Valley-models-and-tools/CalSim-3/DCR) as made available on the DWR data portal (https://data.cnra.ca.gov/dataset/final-dcr-2023-calsim3-modelsels) and represents the current operating rules for major Central Valley systems up to and including the 2019 Biological Opinion and 2020 SWP Incidental Take Permit. A description of assumptions and changes to code that make up the operation rules in this version is provided in the 2023 DCR technical addendum (e.g. Table 2):
The hydroclimate inputs to this scenario are an adjusted version of the historical record (Oct 1921 – Sep 2021): earlier years are adjusted to reflect current climate conditions. More information on the adjustment process can be found in a DWR report here: https://data.cnra.ca.gov/dataset/finaldcr2023/resource/02429384-40a5-4167-9bd1-588ca5e213a4. The COEQWAL team modified the agricultural demands in the SV input file to reflect a different assumed land use that was derived from the 2020 LandIQ dataset.

This scenario builds on s0020 - the only difference between s0020 and this is that TUCP actions are deactivated in this scenario (svar simulateTUCP set to 0 in the main wresl file).$ld$
WHERE short_code = 's0021';

UPDATE scenario SET long_description = $ld$This scenario serves as a reference for comparing to alternative scenarios. The code (WRESL and table files) and input hydrology were taken directly from USBR's 2024 final Long Term Operations (LTO) scenario Alt2 version 1 (Alt2v1). This scenario reflects USBR's proposed action (PA) from their consultation on long-term operations (LTO) with fisheries agencies that concluded in December 2024.
The original USBR hydroclimate inputs for this scenario were developed separately from the DWR datasets and reflected a climate centered on 2022. We modified these inputs for this scenario for COEQWAL - we replaced the 2022 median hydrology with DWR's historical adjusted dataset. Additionally we updated the crop maps used to estimate irrigation demands (we used the 2020 LandIQ dataset for this scenario). This difference in land use is the only distinction between s0023 (this scenario) and s0022.

Actions in this scenario that differ from those represented in DWR's baseline (such as s0011, s0020, s0021) include but are not limited to modified Shasta storage and CVP allocation rules to benefit winter run chinook salmon. Note that TUCP actions are NOT active in this scenario.

Technical note: s0023 was created and run before s0022 by copying and renaming the original USBR Alt2V1 study as provided by USBR (renamed but otherwise unchanged as s0017), then referencing the SV file (v0.1.2) that includes the DWR hydrology and updated 2020 LandIQ land use. Using DWR's hydrology with USBRs study required renaming precipitation variables to include the "_UHH" suffix and adding in the DEL_CVP_PSC_N variable for use with USBR logic. See the SV listing for v0.1.2 for more info.$ld$
WHERE short_code = 's0023';

UPDATE scenario SET long_description = $ld$This scenario serves as a reference and basis for alternative scenarios. The code (WRESL and table files) and input hydrology were copied from s0023 and modified to activate TUCP actions by uncommenting the conditions on the TUCP_Trigger code in TUCP_Actions.wresl (and setting the EOSeptShastEst condition to 1225 in LTO_Ops.wresl). from USBR's 2024 final Long Term Operations (LTO) scenario Alt2 version 1 (Alt2v1). The only things changed in s0023 relative to the original USBR Alt2V1 were 1) using DWR's historical adjusted hydrology and 2) updating to 2020 LandIQ land use. Therefore the operations in this scenario reflects USBR's proposed action (PA) from their consultation on long-term operations (LTO) with fisheries agencies that concluded in December 2024, but with TUCP actions allowed (which makes it comparable to USBR's Alt2V1 with TUCPs).

Actions in this scenario that differ from those represented in DWR's baseline (such as s0011, s0020, s0021) include but are not limited to modified Shasta storage and CVP allocation rules to benefit winter run chinook salmon. Note that TUCP actions ARE active in this scenario.$ld$
WHERE short_code = 's0024';

UPDATE scenario SET long_description = $ld$This scenario assigns limits to groundwater pumping to demand units in the San Joaquin valley. Groundwater pumping limits are set according to estimates of the pumping reduction needed to minimize groundwater storage decline over the full 100 year period of the baseline scenario s0011. These limits are implemented in the WRESL code on a monthly basis for each demand unit in the San Joaquin as a fraction of the applied water demand. For example, if the pumping limit for sustainable groundwater levels is estimated to be 60% of the applied water demand for demand unit X, and the applied water demand in time step Y is 10 TAF while surface water deliveries total 3 TAF, then the remaining unmet demand (10 - 3 = 7 TAF) that can be met by groundwater pumping is 60% of 10 TAF = 6 TAF. This would lead to a shortage for that demand unit in that time step of 1 TAF (10 TAF = demand, 3 TAF surface + 6 TAF groundwater = 9 TAF delivery).
This scenario is derived from s0020 - the DWR 2023 DCR baseline with 2020 updated land use. The difference between s0020 and this (s0025) scenario is the modification of groundwater pumping limits for the San Joaquin valley demand units (and associated adjustment of weighting, shortage terms, and irrigation water budget corrections).$ld$
WHERE short_code = 's0025';

UPDATE scenario SET long_description = $ld$This scenario implements a reduction in irrigated acreage (relative to the 2020 LandIQ reference level) in the San Joaquin Valley with the aim of reducing long-term groundwater decline. This scenario differs from s0020 only in the irrigation demands set for the San Joaquin Valley.$ld$
WHERE short_code = 's0026';

UPDATE scenario SET long_description = $ld$This scenario assigns limits to groundwater pumping to demand units in the Central Valley CalSim3 domain. Groundwater pumping limits are set according to estimates of the pumping reduction needed to minimize groundwater storage decline over the full 100 year period of the baseline scenario s0011. These limits are implemented in the WRESL code on a monthly basis for each demand unit in the Central Valley as a fraction of the applied water demand. For example, if the pumping limit for sustainable groundwater levels is estimated to be 60% of the applied water demand for demand unit X, and the applied water demand in time step Y is 10 TAF while surface water deliveries total 3 TAF, then the remaining unmet demand (10 - 3 = 7 TAF) that can be met by groundwater pumping is 60% of 10 TAF = 6 TAF. This would lead to a shortage for that demand unit in that time step of 1 TAF (10 TAF = demand, 3 TAF surface + 6 TAF groundwater = 9 TAF delivery).
This scenario is derived from s0020 - the DWR 2023 DCR baseline with 2020 updated land use. The difference between s0020 and this (s0027) scenario is the modification of groundwater pumping limits for the Central Valley demand units (and associated adjustment of weighting, shortage terms, and irrigation water budget corrections).$ld$
WHERE short_code = 's0027';

-- s0028: no long description provided in word doc

UPDATE scenario SET long_description = $ld$This scenario sets new minimum flow requirements on tributaries to the Sacramento and San Joaquin rivers as well as the mainstem of those rivers plus Delta outflow. This scenario (s0029) is the same as s0018 except that the land use inputs are updated to reflect the 2020 LandIQ dataset in this study.
The flow requirements were developed by the COEQWAL "eflows" team based on natural flow estimates and selected seasonal flow components that were added and scaled based on expected water availability. These flows were implemented in CalSim3 as minimum flow requirements at 17 locations throughout the Central Valley. These flow requirements are a minimum only - not a strict control on maximum flow - and frequently simulated flows will be greater than the functional requirement used as input. Where a functional flow requirement coincides with a pre-existing minimum flow requirement, the maximum of the two is used. The functional flow requirements are highly prioritized in this scenario, but may still not be fully met in some timesteps due to unavailability of water to be released from an upstream reservoir or loss of released water to groundwater (via stream seepage) that could not be accounted for.$ld$
WHERE short_code = 's0029';

UPDATE scenario SET long_description = $ld$This scenario removes minimum flow requirements on rivers and streams throughout the Central Valley and in rim watersheds where they are defined in CalSim3. Delta outflow requirements remain in place according to the stipulations of D1641 and Biological Opinions as set in the reference scenario s0020. This scenario (s0030) was created by copying the eflows/functional flows scenario s0029 and turning flow requirement switches (where they're defined) to 0 and manually setting flow requirements to 0 elsewhere. We could not find a complete listing of the minimum flow variables and locations in Calsim3 prior to setting up this run, so there is a chance some may have been missed, although the efforts to search through the WRESL code were fairly exhaustive.
This scenario was envisioned as an exploratory one meant to provide a reference point against which the functional flows/eflows (a high flow requirement scenario) and standard flow requirement scenarios could be compared.$ld$
WHERE short_code = 's0030';

-- s0031: no long description provided in word doc

UPDATE scenario SET long_description = $ld$Combines the functional flow requirements of s0029 with the reduced irrigated acreage (for improved groundwater sustainability) of s0028. The intent was to offset the high water demands of eflows/functional flows with a lower demand to mitigate extreme reservoir drawdown.$ld$
WHERE short_code = 's0032';

UPDATE scenario SET long_description = $ld$Combines the salmon flow requirements of s0031 with the reduced irrigated acreage (for improved groundwater sustainability) of s0028. The intent was to offset the high water demands of salmon flows with a lower demand to mitigate extreme reservoir drawdown.$ld$
WHERE short_code = 's0033';

UPDATE scenario SET long_description = $ld$This scenario prioritizes the allocation of surface supplies in the CVP and SWP systems to municipal and industrial (M&I) contractors sufficient to meet the health and human safety levels as defined by the Drinking Water/Community Water Systems group. CVP ag contracts (including Settlement and Exchange) are assigned a lower priority relative to standard allocation procedures to accommodate the prioritization of M&I deliveries. SWP contracts are split into ag and M&I and a higher priority allocation made to a portion of the M&I demand. All "demands" considered in allocations are set by maximum contract entitlements on an annual basis. Minimum deliveries for monthly time steps are set according to the HHS levels.$ld$
WHERE short_code = 's0035';

UPDATE scenario SET long_description = $ld$This scenario prioritizes the allocation of surface supplies in the CVP and SWP systems to municipal and industrial (M&I) contractors sufficient to meet a "functional" level of use as defined by the Drinking Water/Community Water Systems group. This functional level is set as 70% of the maximum contract entitlement. CVP ag contracts (including Settlement and Exchange) are assigned a lower priority relative to standard allocation procedures to accommodate the prioritization of these M&I deliveries. SWP contracts are split into ag and M&I and a higher priority allocation made to the first 70% of M&I contract amounts. Remaining available supply is allocated to agricultural contracts before M&I allocations are increased above the 70% level. All "demands" considered in allocations are set by maximum contract entitlements on an annual basis.$ld$
WHERE short_code = 's0036';

UPDATE scenario SET long_description = $ld$This scenario prioritizes the allocation of surface supplies in the CVP and SWP systems to municipal and industrial (M&I) contractors sufficient to meet as much of the full contract entitlements as possible. CVP ag contracts (including Settlement and Exchange) are assigned a lower priority relative to standard allocation procedures to accommodate the prioritization of these M&I deliveries. SWP contracts are split into ag and M&I and a higher priority allocation made to the first 70% of M&I contract amounts. Remaining available supply is allocated to agricultural contracts before M&I allocations are increased above the 70% level. All "demands" considered in allocations are set by maximum contract entitlements on an annual basis. Deliveries to other non-project M&I users are prioritized through higher weights (or penalties for shortages).$ld$
WHERE short_code = 's0037';

UPDATE scenario SET long_description = $ld$USBR undertook an exploratory modeling exercise in the process of developing the scenarios for what would become the 2024 LTO consultation. One set of scenarios was informed by discussions with environmental and NGO groups and included increased Delta outflows and upstream actions to conserve water for environmental and ecological purposes. These actions were formalized into what USBR labeled the "Alt3" scenario. We adapted that scenario (made available after the conclusion of the LTO consultation in December 2024) for consistency with other COEQWAL modeling configurations. This included replacing the hydrology with DWR's historical adjusted hydrology and updating agricultural demands to follow the 2020 LandIQ land use dataset.
This scenario (s0039) uses the 65% unimpaired flow requirement for the Delta as was done in the original USBR studies. Note that this requirement applies only for December - May and comes with numerous overriding and off-ramping contingencies — that is, one should not expect to see a Delta outflow result that strictly matches a 65% of unimpaired flow condition in the results.$ld$
WHERE short_code = 's0039';

UPDATE scenario SET long_description = $ld$USBR undertook an exploratory modeling exercise in the process of developing the scenarios for what would become the 2024 LTO consultation. One set of scenarios was informed by discussions with environmental and NGO groups and included increased Delta outflows and upstream actions to conserve water for environmental and ecological purposes. These actions were formalized into what USBR labeled the "Alt3" scenario. We adapted that scenario (made available after the conclusion of the LTO consultation in December 2024) for consistency with other COEQWAL modeling configurations. This included replacing the hydrology with DWR's historical adjusted hydrology and updating agricultural demands to follow the 2020 LandIQ land use dataset.
This scenario (s0040) reduces the 65% unimpaired flow requirement for the Delta (as was done in the original USBR studies and s0039) down to 35%. Note that this requirement applies only for December - May and comes with numerous overriding and off-ramping contingencies — that is, one should not expect to see a Delta outflow result that strictly matches a 35% of unimpaired flow condition in the results. In particular, where unregulated flows are available (from runoff on the valley floor during storms, for example) one would expect Delta outflows to exceed the 35% of unimpaired flow level.$ld$
WHERE short_code = 's0040';

UPDATE scenario SET long_description = $ld$USBR undertook an exploratory modeling exercise in the process of developing the scenarios for what would become the 2024 LTO consultation. One set of scenarios was informed by discussions with environmental and NGO groups and included increased Delta outflows and upstream actions to conserve water for environmental and ecological purposes. These actions were formalized into what USBR labeled the "Alt3" scenario. We adapted that scenario (made available after the conclusion of the LTO consultation in December 2024) for consistency with other COEQWAL modeling configurations. This included replacing the hydrology with DWR's historical adjusted hydrology and updating agricultural demands to follow the 2020 LandIQ land use dataset.
This scenario (s0041) reduces the 65% unimpaired flow requirement for the Delta (as was done in the original USBR studies and s0039) down to 45%. Note that this requirement applies only for December - May and comes with numerous overriding and off-ramping contingencies — that is, one should not expect to see a Delta outflow result that strictly matches a 45% of unimpaired flow condition in the results. In particular, where unregulated flows are available (from runoff on the valley floor during storms, for example) one would expect Delta outflows to exceed the 45% of unimpaired flow level.$ld$
WHERE short_code = 's0041';

UPDATE scenario SET long_description = $ld$USBR undertook an exploratory modeling exercise in the process of developing the scenarios for what would become the 2024 LTO consultation. One set of scenarios was informed by discussions with environmental and NGO groups and included increased Delta outflows and upstream actions to conserve water for environmental and ecological purposes. These actions were formalized into what USBR labeled the "Alt3" scenario. We adapted that scenario (made available after the conclusion of the LTO consultation in December 2024) for consistency with other COEQWAL modeling configurations. This included replacing the hydrology with DWR's historical adjusted hydrology and updating agricultural demands to follow the 2020 LandIQ land use dataset.
This scenario (s0042) reduces the 65% unimpaired flow requirement for the Delta (as was done in the original USBR studies and s0039) down to 55%. Note that this requirement applies only for December - May and comes with numerous overriding and off-ramping contingencies — that is, one should not expect to see a Delta outflow result that strictly matches a 55% of unimpaired flow condition in the results. In particular, where unregulated flows are available (from runoff on the valley floor during storms, for example) one would expect Delta outflows to exceed the 55% of unimpaired flow level.$ld$
WHERE short_code = 's0042';

UPDATE scenario SET long_description = $ld$CVP allocations are reduced in the spring to increase target carryover storage in Shasta reservoir by 20% relative to the baseline (s0020). Because of uncertainty in water supply when allocations are set in CalSim3 (i.e. CalSim does not "know" how much water there will be), the carryover condition is not guaranteed to be met. However, Shasta reservoir storage should be increased in many years. The modified allocations, Shasta storage, and deliveries may affect hydrology and reservoir conditions in other parts of the system not targeted by actions in this scenario. This is a consequence of the interconnections in the Central Valley water management and hydrologic system as reflected in a complex but incomplete model. Scenario was developed by adapting USBR's Alt3 allocation logic to the base s0020 scenario.$ld$
WHERE short_code = 's0044';

UPDATE scenario SET long_description = $ld$In recent years, many parties have called for the removal or loosening of requirements for certain X2 conditions to be met in August-October ("fall X2") as originally set by the 2008-2009 biological opinions, arguing that the actions have not had the intended benefit for Delta ecology/fish. Furthermore, the most recent revised USBR proposed operation (updating the version released December 2024) removes this fall X2 requirement. This scenario implements a version of this action - removing the requirement for X2 conditions to be met in late summer and fall. Other Delta flow and salinity requirements are still in place and may control regardless of whether the fall X2 requirement is in effect.$ld$
WHERE short_code = 's0045';

UPDATE scenario SET long_description = $ld$Same as s0029 (functional flows scenario) except that the downstream Sacramento and San Joaquin River flow requirements are not activated, nor is a Delta outflow functional flow requirement. Existing Delta outflow requirements (i.e. from D1641 and other salinity-driven flows) are still in place.$ld$
WHERE short_code = 's0046';

UPDATE scenario SET long_description = $ld$Used DWR's 2025 climate adaptation scenario CCA6 (available under "Downloads" here https://cap-recon.azurewebsites.net/) as the base for this scenario. The sea level rise, land use, and hydrology were adapted to be comparable to the s0020 conditions used in other COEQWAL scenarios (historical adjusted hydro, no SLR, 2020 LandIQ land use). This scenario does include Voluntary Agreement (or "Healthy Rivers and Landscapes", HRL) actions, which previous DCP scenarios did not. These actions include additional flows and some rice acreage fallowing in the Sacramento Valley.$ld$
WHERE short_code = 's0065';

-- ══════════════════════════════════════════════════════════════════════
-- PHASE 6: REBUILD CONSTRAINTS, INDEXES, TRIGGER, VIEW, GRANTS
-- ══════════════════════════════════════════════════════════════════════

-- PK + UNIQUE
ALTER TABLE scenario ADD CONSTRAINT scenario_pkey PRIMARY KEY (id);
ALTER TABLE scenario ADD CONSTRAINT scenario_short_code_key UNIQUE (short_code);

-- Outbound FKs
ALTER TABLE scenario ADD CONSTRAINT fk_scenario_scenario_author
    FOREIGN KEY (scenario_author_id) REFERENCES scenario_author(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE scenario ADD CONSTRAINT scenario_model_source_id_fkey
    FOREIGN KEY (model_source_id) REFERENCES model_source(id)
    ON DELETE RESTRICT ON UPDATE CASCADE;

-- Indexes
CREATE INDEX idx_scenario_active          ON scenario(is_active);
CREATE INDEX idx_scenario_active_version  ON scenario(is_active, scenario_version_id);
CREATE INDEX idx_scenario_baseline        ON scenario(baseline_scenario_id);
CREATE INDEX idx_scenario_hydroclimate    ON scenario(hydroclimate_id);
CREATE INDEX idx_scenario_run_name_active ON scenario(run_name, is_active);
CREATE INDEX idx_scenario_model_source    ON scenario(model_source_id);

-- Audit trigger
CREATE TRIGGER audit_fields_scenario
    BEFORE INSERT OR UPDATE ON scenario
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

-- Inbound FKs
ALTER TABLE theme_scenario_link ADD CONSTRAINT theme_scenario_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_assumption_link ADD CONSTRAINT scenario_key_assumption_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_key_operation_link ADD CONSTRAINT scenario_key_operation_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE scenario_tag_link ADD CONSTRAINT scenario_tag_link_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenario(id)
    ON DELETE CASCADE ON UPDATE CASCADE;

-- Recreate scenario_full view
CREATE VIEW scenario_full AS
SELECT
    s.id,
    s.short_code,
    s.run_name,
    s.name,
    s.is_active,
    sa.short_code                                       AS author,
    hc.short_code                                       AS hydroclimate,
    MAX(CASE WHEN oc.short_code = 'biops'               THEN od.short_code END) AS biops,
    MAX(CASE WHEN oc.short_code = 'tucp'                THEN od.short_code END) AS tucp,
    MAX(CASE WHEN oc.short_code = 'gw_restrictions'     THEN od.short_code END) AS gw_restrictions,
    MAX(CASE WHEN oc.short_code = 'infrastructure'      THEN od.short_code END) AS infrastructure,
    MAX(CASE WHEN oc.short_code = 'flow'                THEN od.short_code END) AS flow,
    MAX(CASE WHEN oc.short_code = 'delta_outflow'       THEN od.short_code END) AS delta_outflow,
    MAX(CASE WHEN oc.short_code = 'comm_delivery'       THEN od.short_code END) AS comm_delivery,
    MAX(CASE WHEN oc.short_code = 'regulatory_salinity' THEN od.short_code END) AS regulatory_salinity,
    MAX(CASE WHEN oc.short_code = 'carryover'           THEN od.short_code END) AS carryover,
    MAX(CASE WHEN ac.short_code = 'land_use'            THEN ad.short_code END) AS land_use,
    MAX(CASE WHEN ac.short_code = 'gw_model'            THEN ad.short_code END) AS gw_model
FROM scenario s
LEFT JOIN scenario_author                sa   ON sa.id  = s.scenario_author_id
LEFT JOIN hydroclimate                   hc   ON hc.id  = s.hydroclimate_id
LEFT JOIN scenario_key_operation_link    skol ON skol.scenario_id = s.id
LEFT JOIN operation_definition           od   ON od.id  = skol.operation_id
LEFT JOIN operation_category             oc   ON oc.id  = od.operation_category_id
LEFT JOIN scenario_key_assumption_link   skal ON skal.scenario_id = s.id
LEFT JOIN assumption_definition          ad   ON ad.id  = skal.assumption_id
LEFT JOIN assumption_category            ac   ON ac.id  = ad.assumption_category_id
WHERE s.is_active = TRUE
GROUP BY
    s.id, s.short_code, s.run_name, s.name, s.is_active,
    sa.short_code, hc.short_code
ORDER BY s.id;

COMMENT ON VIEW scenario_full IS
    'Wide view of ACTIVE scenario configurations (is_active = TRUE only). '
    'Pivots operation and assumption links into named columns per category.';

-- Grants
GRANT SELECT, INSERT, UPDATE, DELETE ON scenario TO jfantauzza;
GRANT SELECT ON scenario_full TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE scenario_id_seq TO jfantauzza;

-- Clean up temp table
DROP TABLE scenario_id_map;

COMMIT;

-- ══════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ══════════════════════════════════════════════════════════════════════

\echo ''
\echo 'scenario_author record 3:'
SELECT id, short_code, name, affiliation FROM scenario_author WHERE id = 3;

\echo ''
\echo 'scenario column order:'
SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'scenario';

\echo ''
\echo 'scenario data (should be sequential IDs in short_code order):'
SELECT id, short_code, is_active, scenario_version_id, created_by, updated_by,
       CASE WHEN long_description IS NOT NULL THEN 'YES' ELSE 'no' END AS has_long_desc
FROM scenario ORDER BY id;

\echo ''
\echo 'scenario_full view check:'
SELECT count(*) AS active_scenarios FROM scenario_full;

\echo ''
\echo 'crosswalk integrity check (should match scenario short_codes):'
SELECT 'operations' AS link_type, s.short_code, count(*) AS links
FROM scenario_key_operation_link l
JOIN scenario s ON s.id = l.scenario_id
GROUP BY s.short_code ORDER BY s.short_code
LIMIT 10;

\echo ''
\echo 'tag integrity check:'
SELECT s.short_code, string_agg(t.label, ', ' ORDER BY t.label) AS tags
FROM scenario_tag_link stl
JOIN scenario s ON s.id = stl.scenario_id
JOIN scenario_tag t ON t.id = stl.tag_id
GROUP BY s.short_code ORDER BY s.short_code;

\echo ''
\echo '43 SCENARIO LAYER CLEANUP COMPLETE'
\echo '===================================='
