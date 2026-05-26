-- =============================================================================
-- MIGRATION 48: Update sibling_group titles/descriptions, fix scenario_author attribution
-- =============================================================================
-- Run as: psql $SUPERUSER_URL -f <this_file>
-- =============================================================================

BEGIN;

-- Disable triggers so we control attribution
ALTER TABLE sibling_group DISABLE TRIGGER audit_fields_sibling_group;
ALTER TABLE scenario_author DISABLE TRIGGER audit_fields_scenario_author;

-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. Fix scenario_author updated_by
-- ═══════════════════════════════════════════════════════════════════════════════

UPDATE scenario_author SET updated_by = 2 WHERE updated_by = 1;

-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. Update sibling_group names and descriptions
-- ═══════════════════════════════════════════════════════════════════════════════

UPDATE sibling_group SET name = 'Current operations',
    short_description = 'Existing operational rules for Central Valley water allocations, as specified by DWR in 2023. Strategy also represents recent (2020) land use and allows for TUCPs. This strategy serves as the baseline for comparison with other scenarios.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0020';

UPDATE sibling_group SET name = 'Current operations without TUCPs',
    short_description = 'Existing operational rules for Central Valley water allocations, as specified by DWR in 2023, with recent (2020) land use but without TUCPs.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0021';

UPDATE sibling_group SET name = 'Current operations with historical land use',
    short_description = 'Existing operational rules for Central Valley water allocations, as specified by DWR in 2023, with historical (2004-2013) land use and TUCPs.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0011';

UPDATE sibling_group SET name = 'Current USBR operations',
    short_description = 'Existing operational rules for Central Valley water allocations, as specified by USBR (Alt2V1) in 2024. Strategy represents the latest federal biological opinions, recent (2020) land use, and TUCPs.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0024';

UPDATE sibling_group SET name = 'Current USBR operations without TUCPs',
    short_description = 'Existing operational rules for Central Valley water allocations, as specified by USBR (Alt2V1) in 2024. Strategy represents the latest federal biological opinions, recent (2020) land use, but without TUCPs.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0023';

UPDATE sibling_group SET name = 'Groundwater pumping limits in the San Joaquin Valley',
    short_description = 'Groundwater pumping limits applied to farms in the San Joaquin Valley region, reflecting compliance with SGMA requirements.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0025';

UPDATE sibling_group SET name = 'Groundwater pumping limits and reduced crop acreage in the San Joaquin Valley',
    short_description = 'Groundwater pumping limits applied to farms in the San Joaquin Valley, with projected reductions in irrigated agricultural land use, reflecting compliance with SGMA requirements.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0026';

UPDATE sibling_group SET name = 'Groundwater pumping limits in the Central Valley',
    short_description = 'Groundwater pumping limits applied throughout the Sacramento and San Joaquin Valley, reflecting compliance with SGMA requirements.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0027';

UPDATE sibling_group SET name = 'Groundwater pumping limits and reduced crop acreage in the Central Valley',
    short_description = 'Groundwater pumping limits applied to farms in the Sacramento and San Joaquin Valley, with projected reductions in irrigated agricultural land use, reflecting compliance with SGMA requirements.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0028';

UPDATE sibling_group SET name = 'No flow requirements',
    short_description = 'Current operations without minimum flow requirements on Central Valley rivers.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0030';

UPDATE sibling_group SET name = 'Functional environmental flows',
    short_description = 'Current operations with functional flow requirements implemented on tributaries to the Sacramento and San Joaquin River.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0046';

UPDATE sibling_group SET name = 'Functional environmental flows with groundwater regulations',
    short_description = 'Functional flow requirements implemented on tributaries to the Sacramento and San Joaquin River, combined with groundwater pumping limits and reduced irrigated agricultural acreage, reflecting compliance with SGMA.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0032';

UPDATE sibling_group SET name = 'Salmon-friendly flows',
    short_description = 'Sacramento River flow requirements and Shasta cold-water storage protection to support Sacramento River winter run Chinook salmon life cycle needs.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0031';

UPDATE sibling_group SET name = 'Salmon-friendly flows with groundwater regulations',
    short_description = 'Sacramento River flow requirements and Shasta cold-water storage protection to support Sacramento River winter run Chinook salmon life cycle needs, combined with groundwater pumping limits and reduced irrigated agricultural acreage, reflecting compliance with SGMA.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0033';

UPDATE sibling_group SET name = 'Prioritizing human health delivery levels to community water systems',
    short_description = 'Water deliveries to satisfy human health and safety needs of community water systems are prioritized.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0035';

UPDATE sibling_group SET name = 'Prioritizing functional delivery levels to community water systems',
    short_description = 'Water deliveries to satisfy functional needs of community water systems are prioritized.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0036';

UPDATE sibling_group SET name = 'Prioritizing full demands of community water systems',
    short_description = 'Water deliveries to satisfy full contract or demand levels of community water systems are prioritized.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0037';

UPDATE sibling_group SET name = 'Reduce delta outflows (35% unimpaired flow)',
    short_description = 'USBR Alternative 3 for the Long-Term Operation (LTO) of the Central Valley Project. Strategy reduces Delta outflow requirements (from approximately 45% to 35% of unimpaired volume).',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0040';

UPDATE sibling_group SET name = 'Maintain Delta outflows (45% of unimpaired flows)',
    short_description = 'USBR Alternative 3 for the Long-Term Operation (LTO) of the Central Valley Project. Strategy maintains Delta outflow requirements near current volumes.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0041';

UPDATE sibling_group SET name = 'Increase Delta outflows (55% of unimpaired flows)',
    short_description = 'USBR Alternative 3 for the Long-Term Operation (LTO) of the Central Valley Project. Strategy increases Delta outflow requirements (from approximately 45% to 55% of unimpaired volume).',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0042';

UPDATE sibling_group SET name = 'Increase Delta outflows (65% of unimpaired flow)',
    short_description = 'USBR Alternative 3 for the Long-Term Operation (LTO) of the Central Valley Project. Strategy increases Delta outflow requirements (from approximately 45% to 65% of unimpaired volume).',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0039';

UPDATE sibling_group SET name = 'Increase Shasta carry-over storage',
    short_description = 'Increases the year-to-year storage carry-over target by 20%.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0044';

UPDATE sibling_group SET name = 'Relax Delta salinity standards',
    short_description = 'Removes the fall (X2) salinity standard in the Delta, based on current USBR operations.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0045';

UPDATE sibling_group SET name = 'Delta Conveyance Project',
    short_description = 'DWR''s 2025 Delta Conveyance Project scenario, with current land use.',
    updated_by = 2, updated_at = NOW()
WHERE short_code = 's0065';

-- Re-enable triggers
ALTER TABLE sibling_group ENABLE TRIGGER audit_fields_sibling_group;
ALTER TABLE scenario_author ENABLE TRIGGER audit_fields_scenario_author;

-- ═══════════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════════

SELECT short_code, name, LEFT(short_description, 60) AS description_preview
FROM sibling_group
ORDER BY short_code;

SELECT 'scenario_author attribution' AS check,
       COUNT(*) FILTER (WHERE updated_by = 1) AS bad_updated_by
FROM scenario_author;

COMMIT;
