-- CREATE TIER_LOCATION TABLE
-- ====================================
-- Narrow catalog of which locations contribute to each tier outcome.
-- Each row says: "for this tier_short_code, this location_id (of this
-- location_type) is one of the locations that produces the per-scenario
-- tier result." Display names, geometry, and other attributes are
-- resolved by joining `location_id` to the entity tables documented in
-- `etl/common/tier_location_entities.py` (the join registry mirrors the
-- query patterns the public API uses).
--
-- The tier teams' staging CSVs in `etl/tier_data/staging/` are the
-- source of truth for membership. Reconcile with:
--   python etl/tier_data/sync_tier_locations_from_staging.py
-- which inserts new rows, marks dropped rows is_active = FALSE, and
-- preserves history. There is no seed CSV for tier_location.
--
-- Run from the repository root as superuser (FKs and the SERIAL
-- sequence need elevated privileges):
--   psql "$SUPERUSER_URL" -f database/scripts/sql/create_tier_location_table.sql

\echo ''
\echo 'CREATING TIER_LOCATION TABLE'
\echo '============================'

DROP TABLE IF EXISTS tier_location CASCADE;

\echo ''
\echo 'Creating tier_location table...'

CREATE TABLE tier_location (
    id              SERIAL PRIMARY KEY,
    tier_short_code VARCHAR NOT NULL,
    location_type   VARCHAR NOT NULL,
    location_id     VARCHAR NOT NULL,
    display_order   INTEGER NOT NULL DEFAULT 1,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by      INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by      INTEGER NOT NULL DEFAULT coeqwal_current_operator(),

    UNIQUE (tier_short_code, location_id),

    CHECK (location_type IN ('network_node', 'wba', 'reservoir', 'compliance_station', 'region', 'demand_unit'))
);

\echo 'tier_location table created'

\echo ''
\echo 'Adding foreign key constraints...'

ALTER TABLE tier_location
ADD CONSTRAINT tier_location_tier_short_code_fkey
FOREIGN KEY (tier_short_code)
REFERENCES tier_definition(short_code)
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE tier_location
ADD CONSTRAINT tier_location_created_by_fkey
FOREIGN KEY (created_by)
REFERENCES developer(id)
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE tier_location
ADD CONSTRAINT tier_location_updated_by_fkey
FOREIGN KEY (updated_by)
REFERENCES developer(id)
ON DELETE RESTRICT ON UPDATE CASCADE;

\echo 'Foreign key constraints added'

\echo ''
\echo 'Creating indexes...'

CREATE INDEX idx_tier_location_tier   ON tier_location(tier_short_code);
CREATE INDEX idx_tier_location_type   ON tier_location(location_type);
CREATE INDEX idx_tier_location_active ON tier_location(is_active);

\echo 'Indexes created'

\echo ''
\echo 'TIER_LOCATION TABLE CREATED.'
\echo 'Next: python etl/tier_data/sync_tier_locations_from_staging.py'
\echo '============================'
