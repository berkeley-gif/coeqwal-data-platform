-- LOAD SPATIAL TIER VISUALIZATION TABLES
-- ========================================
-- Loads reservoir polygons, WBA polygons, and compliance stations
-- for tier map visualization

\echo ''
\echo '🗺️  LOADING SPATIAL TIER VISUALIZATION TABLES'
\echo '=============================================='

-- ============================================================================
-- 1. CREATE AND LOAD RESERVOIRS TABLE
-- ============================================================================

\echo ''
\echo '🏔️  Creating reservoirs table...'

DROP TABLE IF EXISTS reservoirs CASCADE;

CREATE TABLE reservoirs (
    id SERIAL PRIMARY KEY,
    calsim_short_code VARCHAR NOT NULL UNIQUE,
    reservoir_name VARCHAR NOT NULL,
    geom_wkt TEXT NOT NULL,
    srid INTEGER DEFAULT 4326,
    geom GEOMETRY,
    area_sqkm NUMERIC,
    elevation_m NUMERIC,
    gnis_id VARCHAR,
    nhd_permanent_id VARCHAR,
    data_source VARCHAR DEFAULT 'NHD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

\echo '✅ Reservoirs table created'

ALTER TABLE reservoirs ADD CONSTRAINT reservoirs_created_by_fkey FOREIGN KEY (created_by) REFERENCES developer(id);
ALTER TABLE reservoirs ADD CONSTRAINT reservoirs_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES developer(id);

CREATE UNIQUE INDEX idx_reservoirs_calsim_code ON reservoirs(calsim_short_code);
CREATE INDEX idx_reservoirs_geom ON reservoirs USING GIST(geom);

\echo '🔗 Reservoirs FK and indexes created'

\echo '📥 Loading reservoirs from S3...'

SELECT aws_s3.table_import_from_s3(
    'reservoirs',
    'calsim_short_code, reservoir_name, geom_wkt, srid, area_sqkm, elevation_m, gnis_id, nhd_permanent_id, data_source',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '03_GIS/reservoirs.csv',
    'us-west-2'
);

UPDATE reservoirs SET geom = ST_GeomFromText(geom_wkt, srid);

\echo '✅ Reservoirs loaded and geometries converted'

-- ============================================================================
-- 2. CREATE AND LOAD WBA (AQUIFER) TABLE
-- ============================================================================

\echo ''
\echo '💧 Creating wba table...'

DROP TABLE IF EXISTS wba CASCADE;

CREATE TABLE wba (
    id SERIAL PRIMARY KEY,
    wba_id VARCHAR NOT NULL UNIQUE,
    wba_name VARCHAR NOT NULL,
    geom_wkt TEXT NOT NULL,
    srid INTEGER DEFAULT 4326,
    geom GEOMETRY,
    area_acres NUMERIC,
    hydrologic_region VARCHAR,
    comments TEXT,
    data_source VARCHAR DEFAULT 'CalSim_Geopackage',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

\echo '✅ WBA table created'

ALTER TABLE wba ADD CONSTRAINT wba_created_by_fkey FOREIGN KEY (created_by) REFERENCES developer(id);
ALTER TABLE wba ADD CONSTRAINT wba_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES developer(id);

CREATE UNIQUE INDEX idx_wba_id ON wba(wba_id);
CREATE INDEX idx_wba_region ON wba(hydrologic_region);
CREATE INDEX idx_wba_geom ON wba USING GIST(geom);

\echo '🔗 WBA FK and indexes created'

\echo '📥 Loading WBA from S3...'

SELECT aws_s3.table_import_from_s3(
    'wba',
    'wba_id, wba_name, geom_wkt, srid, area_acres, hydrologic_region, comments, data_source',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '03_GIS/wba.csv',
    'us-west-2'
);

UPDATE wba SET geom = ST_GeomFromText(geom_wkt, srid);

\echo '✅ WBA loaded and geometries converted'

-- ============================================================================
-- 3. CREATE AND LOAD COMPLIANCE STATIONS TABLE
-- ============================================================================

\echo ''
\echo '📍 Creating compliance_stations table...'

DROP TABLE IF EXISTS compliance_stations CASCADE;

CREATE TABLE compliance_stations (
    id SERIAL PRIMARY KEY,
    station_code VARCHAR NOT NULL UNIQUE,
    station_name VARCHAR NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    srid INTEGER DEFAULT 4326,
    geom_wkt TEXT NOT NULL,
    geom GEOMETRY,
    tier_use VARCHAR,
    data_source VARCHAR,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

\echo '✅ Compliance stations table created'

ALTER TABLE compliance_stations ADD CONSTRAINT compliance_stations_created_by_fkey FOREIGN KEY (created_by) REFERENCES developer(id);
ALTER TABLE compliance_stations ADD CONSTRAINT compliance_stations_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES developer(id);

CREATE UNIQUE INDEX idx_compliance_code ON compliance_stations(station_code);
CREATE INDEX idx_compliance_tier ON compliance_stations(tier_use);
CREATE INDEX idx_compliance_geom ON compliance_stations USING GIST(geom);

\echo '🔗 Compliance stations FK and indexes created'

\echo '📥 Loading compliance stations from S3...'

SELECT aws_s3.table_import_from_s3(
    'compliance_stations',
    'station_code, station_name, latitude, longitude, srid, tier_use, geom_wkt, data_source, notes',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '03_GIS/compliance_stations.csv',
    'us-west-2'
);

UPDATE compliance_stations SET geom = ST_GeomFromText(geom_wkt, srid);

\echo '✅ Compliance stations loaded and geometries converted'

-- ============================================================================
-- VERIFICATION
-- ============================================================================

\echo ''
\echo '🔍 VERIFYING SPATIAL TABLES'
\echo '==========================='

\echo ''
\echo '📊 Reservoirs:'
SELECT 
    calsim_short_code,
    reservoir_name,
    ROUND(area_sqkm::numeric, 2) as area_sqkm,
    elevation_m,
    GeometryType(geom) as geom_type
FROM reservoirs
ORDER BY calsim_short_code;

\echo ''
\echo '📊 WBA (Aquifers):'
SELECT 
    COUNT(*) as total_wbas,
    COUNT(geom) as with_geometry,
    SUM(area_acres) as total_acres
FROM wba;

\echo ''
\echo '📊 WBA by Region:'
SELECT 
    hydrologic_region,
    COUNT(*) as wba_count,
    ROUND(SUM(area_acres)::numeric, 0) as total_acres
FROM wba
GROUP BY hydrologic_region
ORDER BY wba_count DESC;

\echo ''
\echo '📊 Compliance Stations:'
SELECT 
    station_code,
    station_name,
    latitude,
    longitude,
    tier_use,
    GeometryType(geom) as geom_type
FROM compliance_stations;

\echo ''
\echo '🎉 ALL SPATIAL TABLES SUCCESSFULLY LOADED!'
\echo '=========================================='
\echo 'Summary:'
\echo '• 7 reservoir polygons for RES_STOR tier visualization'
\echo '• 42 WBA polygons for GW_STOR tier visualization'
\echo '• 2 compliance stations for FW_DELTA_USES tier'
\echo '• All geometries converted to PostGIS format'
\echo '• Spatial indexes (GIST) created for fast queries'
\echo ''
\echo 'Ready for tier_location_result table creation!'

