-- CREATE CHANNEL ENTITY AND VARIABLE TABLES
-- Part of: ENTITY LAYER (03) / VARIABLE LAYER (04)
-- Tables: channel_entity, channel_variable
--
-- channel_entity: CalSim-III arc-level channel reaches, canals, and reservoir outlets.
--   Includes env-flow attribute columns added in migration 23 (watershed_short_code,
--   unimp_sv_variable, has_mif, has_eflows, channel_class) so the table is ready
--   for the full env-flow ETL pipeline without further ALTER TABLE.
--
-- channel_variable: CalSim DV/SV variable definitions keyed to channel entities.
--   Includes is_regulatory and regulatory_authority for MIF variables.
--
-- Prerequisites:
--   - watershed table must exist (migration 23 Part 1 already ran)
--
-- Run with: psql $SUPERUSER_URL -f 01_create_channel_entity_variable_tables.sql

\echo ''
\echo '============================================='
\echo 'CREATING CHANNEL ENTITY AND VARIABLE TABLES'
\echo '============================================='


-- ============================================
-- 1. CHANNEL_ENTITY TABLE
-- ============================================
\echo ''
\echo 'Creating channel_entity table...'

CREATE TABLE IF NOT EXISTS channel_entity (
    id                   SERIAL PRIMARY KEY,

    -- Network linkage
    network_arc_id       VARCHAR(30) NOT NULL UNIQUE,   -- CalSim arc ID, e.g. C_SAC049
    short_code           VARCHAR(100),
    name                 VARCHAR(200),
    description          TEXT,
    subtype              VARCHAR(50),                    -- Stream, Canal, Reservoir Release, etc.

    -- Classification
    entity_type_id       INTEGER NOT NULL DEFAULT 1,
    schematic_type_id    INTEGER,
    hydrologic_region_id VARCHAR(10),                    -- SAC, SJR, DELTA, etc. (string codes)
    boundary_condition   VARCHAR(50),
    from_node            VARCHAR(30),
    to_node              VARCHAR(30),
    length_m             NUMERIC(14, 4),

    -- Display flags
    has_tiers            BOOLEAN DEFAULT FALSE,
    is_main              BOOLEAN DEFAULT FALSE,
    has_gis_data         INTEGER DEFAULT 1,

    -- Versioning & provenance
    entity_version_id    INTEGER NOT NULL DEFAULT 1,
    source_ids           TEXT,

    -- Environmental flow attributes (added via migration 23)
    watershed_short_code VARCHAR(30) REFERENCES watershed(short_code),
    unimp_sv_variable    VARCHAR(30),
    has_mif              BOOLEAN NOT NULL DEFAULT FALSE,
    has_eflows           BOOLEAN NOT NULL DEFAULT FALSE,
    channel_class        VARCHAR(30) CHECK (
                             channel_class IN ('stream', 'canal', 'reservoir_release')
                         ),

    -- Audit
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by           INTEGER NOT NULL DEFAULT 1,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by           INTEGER NOT NULL DEFAULT 1
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_channel_entity_network_arc   ON channel_entity(network_arc_id);
CREATE INDEX IF NOT EXISTS idx_channel_entity_watershed      ON channel_entity(watershed_short_code);
CREATE INDEX IF NOT EXISTS idx_channel_entity_has_mif        ON channel_entity(has_mif) WHERE has_mif = TRUE;
CREATE INDEX IF NOT EXISTS idx_channel_entity_has_eflows     ON channel_entity(has_eflows) WHERE has_eflows = TRUE;
CREATE INDEX IF NOT EXISTS idx_channel_entity_channel_class  ON channel_entity(channel_class);

-- Comments
COMMENT ON TABLE  channel_entity IS
    'CalSim-III arc-level channel reaches, constructed canals, and reservoir outlet arcs. '
    'Covers all ~670 arcs in the network; the 60 env-flow DV channels have watershed/MIF/eflows attributes populated.';
COMMENT ON COLUMN channel_entity.network_arc_id      IS 'CalSim-III arc identifier (Part B), e.g. C_SAC049.';
COMMENT ON COLUMN channel_entity.watershed_short_code IS
    'Watershed this channel drains from (FK → watershed.short_code). NULL for canals and delta tidal channels.';
COMMENT ON COLUMN channel_entity.unimp_sv_variable   IS
    'CalSim SV unimpaired flow variable used as natural baseline for % unimpaired metric. '
    'Usually inherited from watershed but may differ (e.g. SAC_UPPER vs SAC_LOWER on same river).';
COMMENT ON COLUMN channel_entity.has_mif             IS 'True if a C_*_MIF companion variable exists in the CalSim DV output for this channel.';
COMMENT ON COLUMN channel_entity.has_eflows          IS 'True if an EFLOWS_* functional flow target variable exists in the CalSim SV input.';
COMMENT ON COLUMN channel_entity.channel_class       IS
    'Physical channel type: stream (natural watercourse), canal (constructed conveyance), '
    'reservoir_release (regulated outflow from dam/reservoir).';

\echo '✅ channel_entity table created'


-- ============================================
-- 2. CHANNEL_VARIABLE TABLE
-- ============================================
\echo ''
\echo 'Creating channel_variable table...'

CREATE TABLE IF NOT EXISTS channel_variable (
    id                      INTEGER PRIMARY KEY,        -- explicit id from seed CSV

    -- CalSim variable identity
    calsim_id               VARCHAR(40) NOT NULL UNIQUE, -- e.g. C_SAC049, C_SAC049_MIF
    name                    VARCHAR(200),
    description             TEXT,

    -- Entity linkage
    channel_entity_id       INTEGER REFERENCES channel_entity(id),

    -- Classification
    variable_type           VARCHAR(50),                -- flow, diversion, return_flow, etc.
    unit_id                 INTEGER,                    -- FK → unit lookup (2 = CFS)
    temporal_scale_id       INTEGER,                    -- FK → temporal_scale (3 = monthly)
    variable_version_id     INTEGER DEFAULT 1,

    -- Regulatory flags (migration 23)
    is_regulatory           BOOLEAN NOT NULL DEFAULT FALSE,
    regulatory_authority    VARCHAR(100),

    -- Aggregation
    is_aggregate            BOOLEAN NOT NULL DEFAULT FALSE,
    aggregated_variable_ids TEXT,
    variable_id             UUID,

    -- Provenance
    source_ids              TEXT,
    created_by              INTEGER NOT NULL DEFAULT 1,
    updated_by              INTEGER DEFAULT 1,

    -- Audit
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_channel_variable_calsim_id      ON channel_variable(calsim_id);
CREATE INDEX IF NOT EXISTS idx_channel_variable_entity         ON channel_variable(channel_entity_id);
CREATE INDEX IF NOT EXISTS idx_channel_variable_is_regulatory  ON channel_variable(is_regulatory) WHERE is_regulatory = TRUE;
CREATE INDEX IF NOT EXISTS idx_channel_variable_type           ON channel_variable(variable_type);

-- Comments
COMMENT ON TABLE  channel_variable IS
    'CalSim-III variable definitions for channel/arc measurements. ~1352 rows including '
    '20 regulatory MIF variables (C_*_MIF, is_regulatory=true).';
COMMENT ON COLUMN channel_variable.calsim_id          IS 'CalSim-III variable name (Part B), e.g. C_SAC049, C_SAC049_MIF.';
COMMENT ON COLUMN channel_variable.is_regulatory      IS 'TRUE for binding regulatory minimum instream flow (MIF) variables.';
COMMENT ON COLUMN channel_variable.regulatory_authority IS '"CalSim-III" for MIF variables (D-1641, BiOps, VAMP, eflows combined).';

\echo '✅ channel_variable table created'


-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo 'Tables created:'
SELECT
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns c2
     WHERE c2.table_name = t.table_name
       AND c2.table_schema = 'public') AS column_count
FROM information_schema.tables t
WHERE table_name IN ('channel_entity', 'channel_variable')
  AND table_schema = 'public'
ORDER BY table_name;

\echo ''
\echo '✅ channel_entity and channel_variable tables ready for data load'
