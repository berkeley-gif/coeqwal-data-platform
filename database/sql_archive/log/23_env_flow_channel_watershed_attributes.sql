-- Migration 23: Environmental flow channel and watershed attribute updates
--
-- Changes:
--   1. watershed table  - split SAC_RIVER into SAC_UPPER + SAC_LOWER, add 3 new watersheds,
--      add unimp_sv_variable column.
--   2. channel_entity table  - add 5 new attribute columns (watershed_short_code,
--      unimp_sv_variable, has_mif, has_eflows, channel_class) and populate for all
--      60 channels that appear in the CalSim DV CSV.
--   3. channel_variable table  - add 20 regulatory MIF variables (C_*_MIF) and
--      1 missing flow variable (C_ISF001_OMR027).
--
-- Author: jfantauzza
-- Date: 2026-02-28
-- ============================================================================


-- ============================================================================
-- PART 1: Extend watershed table
-- ============================================================================

\echo 'PART 1: Extending watershed table...'

ALTER TABLE watershed
    ADD COLUMN IF NOT EXISTS unimp_sv_variable VARCHAR;

COMMENT ON COLUMN watershed.unimp_sv_variable IS
    'CalSim SV input variable name for watershed-level unimpaired natural flow '
    '(e.g. UNIMP_FOLS). NULL when no SV UNIMP reference exists for this watershed.';

UPDATE watershed
SET
    short_code          = 'SAC_UPPER',
    name                = 'Sacramento River  - Upper (above Bend Bridge)',
    description         = 'Sacramento River mainstem above Bend Bridge near Red Bluff; '
                          'Shasta Reservoir inflow is the unimpaired natural flow reference',
    unimp_sv_variable   = 'UNIMP_SHAS'
WHERE short_code = 'SAC_RIVER';

INSERT INTO watershed (short_code, name, description, hydrologic_region_short_code, unimp_sv_variable, is_active)
VALUES (
    'SAC_LOWER',
    'Sacramento River  - Lower (at and below Bend Bridge)',
    'Sacramento River mainstem at Bend Bridge (rm 257) and downstream to the Delta; '
    'Sacramento River at Bend Bridge gauge is the unimpaired reference (UNIMP_SRBB)',
    'SAC', 'UNIMP_SRBB', true
);

INSERT INTO watershed (short_code, name, description, hydrologic_region_short_code, unimp_sv_variable, is_active)
VALUES
    ('UPPER_MERCED',
     'Upper Merced River Watershed',
     'Merced River drainage above Lake McClure (Exchequer Reservoir)',
     'SJR', 'UNIMP_ME', true),

    ('TRINITY_RIVER',
     'Trinity River Watershed',
     'Trinity River drainage above Lewiston Reservoir; source of diversion to Sacramento basin via Clear Creek Tunnel',
     'NC', 'UNIMP_TRIN', true),

    ('CLEAR_CREEK',
     'Clear Creek / Whiskeytown Watershed',
     'Clear Creek drainage above Whiskeytown Reservoir; receives Trinity diversion water, tributary to Sacramento River below Shasta',
     'SAC', 'UNIMP_WH', true)
ON CONFLICT (short_code) DO NOTHING;

UPDATE watershed SET unimp_sv_variable = 'UNIMP_FOLS'  WHERE short_code = 'UPPER_AMERICAN';
UPDATE watershed SET unimp_sv_variable = 'UNIMP_OROV'  WHERE short_code = 'UPPER_FEATHER';
UPDATE watershed SET unimp_sv_variable = 'UNIMP_YUBA'  WHERE short_code = 'YUBA_RIVER';
UPDATE watershed SET unimp_sv_variable = 'UNIMP_ST'    WHERE short_code = 'UPPER_STANISLAUS';
UPDATE watershed SET unimp_sv_variable = 'UNIMP_TU'    WHERE short_code = 'UPPER_TUOLUMNE';
UPDATE watershed SET unimp_sv_variable = 'UNIMP_SJ'    WHERE short_code = 'SAN_JOAQUIN';

\echo '  watershed: updated SAC_RIVER to SAC_UPPER, added SAC_LOWER + 3 new watersheds, unimp_sv_variable populated'

SELECT short_code, name, unimp_sv_variable FROM watershed ORDER BY short_code;


-- ============================================================================
-- PART 2: Extend channel_entity table
-- ============================================================================

\echo 'PART 2: Extending channel_entity table...'

ALTER TABLE channel_entity
    ADD COLUMN IF NOT EXISTS watershed_short_code VARCHAR REFERENCES watershed(short_code),
    ADD COLUMN IF NOT EXISTS unimp_sv_variable     VARCHAR,
    ADD COLUMN IF NOT EXISTS has_mif               BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_eflows            BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS channel_class         VARCHAR CHECK (channel_class IN ('stream','canal','reservoir_release'));

COMMENT ON COLUMN channel_entity.watershed_short_code IS
    'Watershed this channel drains from (FK to watershed.short_code). NULL for canals and delta tidal channels.';
COMMENT ON COLUMN channel_entity.unimp_sv_variable IS
    'CalSim SV unimpaired flow variable used as natural baseline for % unimpaired metric. '
    'Usually inherited from watershed but may differ (e.g. SAC_UPPER vs SAC_LOWER on same river).';
COMMENT ON COLUMN channel_entity.has_mif IS
    'True if a C_*_MIF companion variable exists in the CalSim DV output for this channel.';
COMMENT ON COLUMN channel_entity.has_eflows IS
    'True if an EFLOWS_* functional flow target variable exists in the CalSim SV input.';
COMMENT ON COLUMN channel_entity.channel_class IS
    'Physical channel type: stream (natural watercourse), canal (constructed conveyance), '
    'reservoir_release (regulated outflow from dam/reservoir).';

CREATE INDEX IF NOT EXISTS idx_channel_entity_watershed
    ON channel_entity(watershed_short_code);

CREATE INDEX IF NOT EXISTS idx_channel_entity_has_mif
    ON channel_entity(has_mif) WHERE has_mif = TRUE;


DO $$
DECLARE
    ch RECORD;
    data RECORD;
BEGIN
    FOR data IN (
        SELECT * FROM (VALUES
            ('C_AMR004',  'UPPER_AMERICAN',   'UNIMP_FOLS', true,  true,  'stream'),
            ('C_CAA003',  NULL,               NULL,         false, false, 'canal'),
            ('C_CHW017',  NULL,               NULL,         false, false, 'canal'),
            ('C_CLV004',  NULL,               NULL,         false, false, 'canal'),
            ('C_DMC000',  NULL,               NULL,         false, false, 'canal'),
            ('C_DMC003',  NULL,               NULL,         false, false, 'canal'),
            ('C_EBP016',  'SAN_JOAQUIN',      'UNIMP_SJ',   false, false, 'stream'),
            ('C_FOLSM',   'UPPER_AMERICAN',   'UNIMP_FOLS', false, false, 'reservoir_release'),
            ('C_FTR003',  'UPPER_FEATHER',    'UNIMP_OROV', true,  true,  'stream'),
            ('C_FTR012',  'UPPER_FEATHER',    'UNIMP_OROV', false, false, 'stream'),
            ('C_FTR028',  'UPPER_FEATHER',    'UNIMP_OROV', false, false, 'stream'),
            ('C_FTR029',  'UPPER_FEATHER',    'UNIMP_OROV', true,  true,  'stream'),
            ('C_FTR059',  'UPPER_FEATHER',    'UNIMP_OROV', true,  false, 'stream'),
            ('C_FTR068',  'UPPER_FEATHER',    'UNIMP_OROV', false, false, 'stream'),
            ('C_ISF001_OMR027', NULL,         NULL,         false, false, 'stream'),
            ('C_KSWCK',   'SAC_UPPER',        'UNIMP_SHAS', true,  false, 'stream'),
            ('C_LWSTN',   'TRINITY_RIVER',    'UNIMP_TRIN', false, false, 'reservoir_release'),
            ('C_MCD005',  'UPPER_MERCED',     'UNIMP_ME',   true,  true,  'stream'),
            ('C_MCD021',  'UPPER_MERCED',     'UNIMP_ME',   false, false, 'stream'),
            ('C_MELON',   'UPPER_STANISLAUS', 'UNIMP_ST',   false, false, 'reservoir_release'),
            ('C_MOK019',  'UPPER_MOKELUMNE',  NULL,         false, false, 'stream'),
            ('C_MOK028',  'UPPER_MOKELUMNE',  NULL,         true,  true,  'stream'),
            ('C_NTOMA',   'UPPER_AMERICAN',   'UNIMP_FOLS', true,  false, 'stream'),
            ('C_OMR014',  'SAN_JOAQUIN',      'UNIMP_SJ',   false, false, 'stream'),
            ('C_OROVL',   'UPPER_FEATHER',    'UNIMP_OROV', false, false, 'reservoir_release'),
            ('C_SAC000',  'SAC_LOWER',        'UNIMP_SRBB', false, true,  'stream'),
            ('C_SAC007',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC029B', 'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC041',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC048',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC049',  'SAC_LOWER',        'UNIMP_SRBB', true,  true,  'stream'),
            ('C_SAC083',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC085',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC120',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC122',  'SAC_LOWER',        'UNIMP_SRBB', true,  true,  'stream'),
            ('C_SAC148',  'SAC_LOWER',        'UNIMP_SRBB', true,  true,  'stream'),
            ('C_SAC201',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC240',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_SAC257',  'SAC_LOWER',        'UNIMP_SRBB', true,  true,  'stream'),
            ('C_SAC289',  'SAC_UPPER',        'UNIMP_SHAS', true,  true,  'stream'),
            ('C_SHSTA',   'SAC_UPPER',        'UNIMP_SHAS', false, false, 'reservoir_release'),
            ('C_SJR013',  'SAN_JOAQUIN',      'UNIMP_SJ',   false, false, 'stream'),
            ('C_SJR070',  'SAN_JOAQUIN',      'UNIMP_SJ',   true,  true,  'stream'),
            ('C_SJR115',  'SAN_JOAQUIN',      'UNIMP_SJ',   false, false, 'stream'),
            ('C_SJR127',  'SAN_JOAQUIN',      'UNIMP_SJ',   true,  true,  'stream'),
            ('C_SJR180',  'SAN_JOAQUIN',      'UNIMP_SJ',   false, false, 'stream'),
            ('C_SJR225',  'SAN_JOAQUIN',      'UNIMP_SJ',   false, false, 'stream'),
            ('C_SSL001',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_STN026',  'UPPER_STANISLAUS', 'UNIMP_ST',   false, false, 'stream'),
            ('C_STS004',  'UPPER_STANISLAUS', 'UNIMP_ST',   false, false, 'stream'),
            ('C_STS011',  'UPPER_STANISLAUS', 'UNIMP_ST',   true,  true,  'stream'),
            ('C_STS017',  'UPPER_STANISLAUS', 'UNIMP_ST',   false, false, 'stream'),
            ('C_STS059',  'UPPER_STANISLAUS', 'UNIMP_ST',   true,  false, 'stream'),
            ('C_TRN111',  'TRINITY_RIVER',    'UNIMP_TRIN', true,  true,  'stream'),
            ('C_TRNTY',   'TRINITY_RIVER',    'UNIMP_TRIN', false, false, 'reservoir_release'),
            ('C_TUO003',  'UPPER_TUOLUMNE',   'UNIMP_TU',   true,  true,  'stream'),
            ('C_WKYTN',   'CLEAR_CREEK',      'UNIMP_WH',   false, false, 'reservoir_release'),
            ('C_YBP020',  'SAC_LOWER',        'UNIMP_SRBB', false, false, 'stream'),
            ('C_YUB002',  'YUBA_RIVER',       'UNIMP_YUBA', true,  true,  'stream')
        ) AS t(network_arc_id, watershed_short_code, unimp_sv_variable, has_mif, has_eflows, channel_class)
    ) LOOP
        UPDATE channel_entity
        SET
            watershed_short_code = data.watershed_short_code,
            unimp_sv_variable    = data.unimp_sv_variable,
            has_mif              = data.has_mif,
            has_eflows           = data.has_eflows,
            channel_class        = data.channel_class
        WHERE network_arc_id = data.network_arc_id;
    END LOOP;
END $$;

\echo '  channel_entity: added 5 columns, populated for 60 DV channels'

SELECT
    COUNT(*) FILTER (WHERE watershed_short_code IS NOT NULL) AS with_watershed,
    COUNT(*) FILTER (WHERE has_mif = true)  AS with_mif,
    COUNT(*) FILTER (WHERE has_eflows = true) AS with_eflows
FROM channel_entity;


-- ============================================================================
-- PART 3: Add MIF variables to channel_variable
-- ============================================================================

\echo 'PART 3: Adding MIF and missing channel variables...'

INSERT INTO channel_variable
    (calsim_id, name, description,
     channel_entity_id, variable_type, unit_id, temporal_scale_id,
     variable_version_id, is_regulatory, regulatory_authority,
     is_aggregate, source_ids, created_by, updated_by)
SELECT
    t.calsim_id,
    t.name,
    'Model-computed binding minimum instream flow (D-1641, BiOps, VAMP, eflows combined). '
        'CalSim DV output. Part C = FLOW-MIN-INSTREAM.',
    ce.id,
    'flow', 2, 3, 1, true, 'CalSim-III',
    false, '{1,3,4}', 1, 1
FROM (VALUES
    ('C_AMR004_MIF',  'American River at I-80 Bridge  - MIF',                 'C_AMR004'),
    ('C_FTR003_MIF',  'Feather River  - MIF',                                 'C_FTR003'),
    ('C_FTR029_MIF',  'Feather River at Yuba City  - MIF',                    'C_FTR029'),
    ('C_FTR059_MIF',  'Feather River at Thermalito Afterbay  - MIF',          'C_FTR059'),
    ('C_KSWCK_MIF',   'Keswick Dam  - MIF',                                   'C_KSWCK'),
    ('C_MCD005_MIF',  'Merced River at Stevinson  - MIF',                     'C_MCD005'),
    ('C_MOK028_MIF',  'Mokelumne River  - MIF',                               'C_MOK028'),
    ('C_NTOMA_MIF',   'American River at Lake Natoma  - MIF',                 'C_NTOMA'),
    ('C_SAC049_MIF',  'Sacramento River at Freeport  - MIF',                  'C_SAC049'),
    ('C_SAC122_MIF',  'Sacramento River at Tisdale Weir  - MIF',              'C_SAC122'),
    ('C_SAC148_MIF',  'Sacramento River at Colusa Weir  - MIF',               'C_SAC148'),
    ('C_SAC257_MIF',  'Sacramento River at Bend Bridge  - MIF',               'C_SAC257'),
    ('C_SAC289_MIF',  'Sacramento River at South Bonnieville  - MIF',         'C_SAC289'),
    ('C_SJR070_MIF',  'San Joaquin River at Vernalis  - MIF',                 'C_SJR070'),
    ('C_SJR127_MIF',  'San Joaquin River at Salt Slough confluence  - MIF',   'C_SJR127'),
    ('C_STS011_MIF',  'Stanislaus River  - MIF',                              'C_STS011'),
    ('C_STS059_MIF',  'Stanislaus River (upper, Goodwin Dam area)  - MIF',    'C_STS059'),
    ('C_TRN111_MIF',  'Trinity River at Lewiston  - MIF',                     'C_TRN111'),
    ('C_TUO003_MIF',  'Tuolumne River  - MIF',                                'C_TUO003'),
    ('C_YUB002_MIF',  'Yuba River at Marysville  - MIF',                      'C_YUB002')
) AS t(calsim_id, name, parent_arc_id)
JOIN channel_entity ce ON ce.network_arc_id = t.parent_arc_id
WHERE NOT EXISTS (
    SELECT 1 FROM channel_variable WHERE calsim_id = t.calsim_id
);

INSERT INTO channel_variable
    (calsim_id, name, description, channel_entity_id,
     variable_type, unit_id, temporal_scale_id, variable_version_id,
     is_regulatory, is_aggregate, source_ids, created_by, updated_by)
SELECT
    'C_ISF001_OMR027',
    'Interior SF Bay / Old-Middle River',
    'Delta interior flow variable  - Old River and Middle River confluence. CalSim DV CHANNEL variable.',
    ce.id, 'flow', 2, 3, 1, false, false, '{1,3,4}', 1, 1
FROM channel_entity ce
WHERE ce.network_arc_id = 'C_ISF001_OMR027'
  AND NOT EXISTS (SELECT 1 FROM channel_variable WHERE calsim_id = 'C_ISF001_OMR027');

\echo '  channel_variable: added 20 MIF variables + C_ISF001_OMR027'

SELECT COUNT(*) AS total_mif_variables
FROM channel_variable
WHERE calsim_id LIKE 'C_%_MIF';


-- ============================================================================
-- Final verification
-- ============================================================================

\echo 'Verification summary:'
SELECT 'watershed rows'        AS entity, COUNT(*) FROM watershed
UNION ALL
SELECT 'channel_entity rows'   AS entity, COUNT(*) FROM channel_entity
UNION ALL
SELECT 'channel_variable rows' AS entity, COUNT(*) FROM channel_variable
UNION ALL
SELECT 'MIF variables'         AS entity, COUNT(*) FROM channel_variable WHERE is_regulatory = true
UNION ALL
SELECT 'channels with watershed' AS entity, COUNT(*) FROM channel_entity WHERE watershed_short_code IS NOT NULL
UNION ALL
SELECT 'channels with MIF flag'  AS entity, COUNT(*) FROM channel_entity WHERE has_mif = true;
