-- CREATE DATA_IN_DEPTH_SUBJECT (+ _MEMBER) TABLES
-- ================================================
-- Registry of everything a data_in_depth series can be "about" - the flexible
-- "related entity" for the trend-report extracts. Three kinds:
--
--   entity     - a physical CalSim node/entity, addressed by the same
--                (location_type, location_id) polymorphic pair as tier_location,
--                resolved via etl/common/tier_location_entities.py. Lines up with
--                a tier_location row by join when one exists, but is NOT owned by
--                the tier staging/sync.
--   aggregate  - a synthetic rollup (e.g. NOD "North of Delta", sac_valley).
--                Its constituents are recorded in data_in_depth_subject_member;
--                the aggregate VALUE is computed by the extractor at ETL time
--                (no aggregation logic lives in the DB).
--   metric     - a non-location series (e.g. X2, WYT_SAC). No location.
--
-- data_in_depth data tables reference this by a single FK:
--   data_in_depth_subject_id INTEGER REFERENCES data_in_depth_subject(id)
--
-- Geometry is intentionally omitted for now. entity subjects resolve geometry
-- through the registry; if aggregates/metrics ever need a stored polygon, add a
-- nullable geometry triad later (geom geometry(Geometry,4326), geom_wkt text,
-- srid int) per database/topic_docs/geometry.md - a non-destructive ALTER.
--
-- Run from repo root as superuser:
--   psql "$SUPERUSER_URL" -f database/scripts/sql/create_data_in_depth_subject_table.sql

\set ON_ERROR_STOP on

\echo ''
\echo 'CREATING DATA_IN_DEPTH_SUBJECT TABLES'
\echo '====================================='

DROP TABLE IF EXISTS data_in_depth_subject_member CASCADE;
DROP TABLE IF EXISTS data_in_depth_subject CASCADE;

-- ---------------------------------------------------------------------------
-- 1. data_in_depth_subject
-- ---------------------------------------------------------------------------
CREATE TABLE data_in_depth_subject (
    id            SERIAL PRIMARY KEY,
    subject_kind  VARCHAR     NOT NULL,
    short_code    VARCHAR     NOT NULL,
    label         VARCHAR,
    description   TEXT,

    -- Polymorphic pointer, ONLY for subject_kind = 'entity' (same convention
    -- as tier_location; resolved via LOCATION_ENTITY_MAP). NULL otherwise.
    location_type VARCHAR,
    location_id   VARCHAR,

    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by    INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by    INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),

    CONSTRAINT data_in_depth_subject_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT data_in_depth_subject_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer (id) ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT data_in_depth_subject_unique UNIQUE (subject_kind, short_code),

    CONSTRAINT chk_dids_kind
        CHECK (subject_kind IN ('entity', 'aggregate', 'metric')),
    -- entity subjects must carry a location; non-entity subjects must not.
    CONSTRAINT chk_dids_entity_location
        CHECK (subject_kind <> 'entity' OR (location_type IS NOT NULL AND location_id IS NOT NULL)),
    CONSTRAINT chk_dids_nonentity_location
        CHECK (subject_kind = 'entity' OR (location_type IS NULL AND location_id IS NULL)),
    -- location_type, when present, matches the tier_location / registry enum.
    CONSTRAINT chk_dids_location_type
        CHECK (location_type IS NULL OR location_type IN
               ('network_node', 'wba', 'reservoir', 'compliance_station', 'region', 'demand_unit'))
);

\echo 'data_in_depth_subject created.'

CREATE INDEX idx_dids_kind     ON data_in_depth_subject (subject_kind);
CREATE INDEX idx_dids_location ON data_in_depth_subject (location_type, location_id);
CREATE INDEX idx_dids_active   ON data_in_depth_subject (is_active) WHERE is_active = TRUE;

SELECT apply_audit_trigger_to_table('data_in_depth_subject');

-- ---------------------------------------------------------------------------
-- 2. data_in_depth_subject_member  (aggregate -> constituent subjects)
-- ---------------------------------------------------------------------------
CREATE TABLE data_in_depth_subject_member (
    id            SERIAL PRIMARY KEY,
    aggregate_id  INTEGER     NOT NULL,   -- data_in_depth_subject with subject_kind='aggregate'
    member_id     INTEGER     NOT NULL,   -- constituent data_in_depth_subject

    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by    INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by    INTEGER     NOT NULL DEFAULT coeqwal_current_operator(),

    CONSTRAINT dids_member_aggregate_fkey
        FOREIGN KEY (aggregate_id) REFERENCES data_in_depth_subject (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT dids_member_member_fkey
        FOREIGN KEY (member_id) REFERENCES data_in_depth_subject (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT dids_member_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES developer (id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT dids_member_updated_by_fkey
        FOREIGN KEY (updated_by) REFERENCES developer (id) ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT dids_member_unique UNIQUE (aggregate_id, member_id),
    CONSTRAINT chk_dids_member_not_self CHECK (aggregate_id <> member_id)
);

\echo 'data_in_depth_subject_member created.'

CREATE INDEX idx_dids_member_aggregate ON data_in_depth_subject_member (aggregate_id);
CREATE INDEX idx_dids_member_member    ON data_in_depth_subject_member (member_id);

SELECT apply_audit_trigger_to_table('data_in_depth_subject_member');

-- ---------------------------------------------------------------------------
-- 3. Register in domain_family_map (entity family) + grants
-- ---------------------------------------------------------------------------
\echo 'Registering in domain_family_map (entity family)...'
INSERT INTO domain_family_map (schema_name, table_name, version_family_id, note)
SELECT 'public', t.table_name, vf.id, t.note
FROM version_family vf
CROSS JOIN (VALUES
    ('data_in_depth_subject',        'Subject registry for data_in_depth extracts (entity/aggregate/metric)'),
    ('data_in_depth_subject_member', 'Aggregate -> member links for data_in_depth subjects')
) AS t(table_name, note)
WHERE vf.short_code = 'entity'
ON CONFLICT (schema_name, table_name) DO UPDATE
    SET version_family_id = EXCLUDED.version_family_id, note = EXCLUDED.note;

\echo 'Granting to coeqwal_developer...'
GRANT SELECT, INSERT, UPDATE, DELETE ON data_in_depth_subject        TO coeqwal_developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON data_in_depth_subject_member TO coeqwal_developer;
GRANT USAGE, SELECT ON SEQUENCE data_in_depth_subject_id_seq        TO coeqwal_developer;
GRANT USAGE, SELECT ON SEQUENCE data_in_depth_subject_member_id_seq TO coeqwal_developer;

-- ---------------------------------------------------------------------------
-- 4. Comments
-- ---------------------------------------------------------------------------
COMMENT ON TABLE data_in_depth_subject IS
    'Registry of subjects a data_in_depth series can be about. subject_kind: '
    'entity (physical node via location_type/location_id, resolved through '
    'LOCATION_ENTITY_MAP; joins to tier_location on that pair when present), '
    'aggregate (rollup; members in data_in_depth_subject_member; value computed '
    'at ETL time), metric (non-location series e.g. X2, WYT_SAC).';
COMMENT ON COLUMN data_in_depth_subject.location_type IS
    'Set only for subject_kind=entity. Same enum as tier_location; no FK (polymorphic).';
COMMENT ON COLUMN data_in_depth_subject.location_id IS
    'Set only for subject_kind=entity. Resolved via LOCATION_ENTITY_MAP; validate at load time.';
COMMENT ON TABLE data_in_depth_subject_member IS
    'Aggregate subject -> constituent subjects. Provenance only; the aggregate '
    'value is computed by the extractor at ETL time, not from these rows in SQL.';

-- ---------------------------------------------------------------------------
-- 5. Verification
-- ---------------------------------------------------------------------------
\echo ''
\echo 'VERIFICATION:'
\echo '============='
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) AS cols
FROM (VALUES ('data_in_depth_subject'), ('data_in_depth_subject_member')) AS t(table_name);

\echo ''
\echo 'Constraints on data_in_depth_subject:'
SELECT conname, contype FROM pg_constraint
WHERE conrelid = 'data_in_depth_subject'::regclass ORDER BY contype, conname;

\echo ''
\echo 'domain_family_map registration:'
SELECT table_name, version_family_id, note FROM domain_family_map
WHERE table_name IN ('data_in_depth_subject', 'data_in_depth_subject_member');

\echo ''
\echo 'DATA_IN_DEPTH_SUBJECT TABLES CREATED.'
\echo '====================================='
