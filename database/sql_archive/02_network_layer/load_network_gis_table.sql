-- LOAD NETWORK_GIS TABLE FROM S3
-- ================================
-- Loads network_gis.csv with spatial data (WKT geometry)
-- Depends on: network table (for network_id FK via short_code lookup)
-- Note: Replaces old network_gis table with new structure

\echo ''
\echo '🗺️  LOADING NETWORK_GIS TABLE FROM S3'
\echo '======================================'

\echo ''
\echo '🗑️  Dropping old network_gis table...'
DROP TABLE IF EXISTS network_gis CASCADE;
\echo '✅ Old network_gis dropped'

\echo ''
\echo '🏗️  Creating network_gis table...'

CREATE TABLE network_gis (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR NOT NULL,
    network_id INTEGER NOT NULL,
    precision_level VARCHAR NOT NULL DEFAULT 'precise',
    geom_wkt TEXT NOT NULL,
    srid INTEGER DEFAULT 4326,
    geom GEOMETRY,
    estimated_accuracy_meters NUMERIC,
    source_id INTEGER NOT NULL DEFAULT 4,
    network_version_id INTEGER NOT NULL DEFAULT 12,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT coeqwal_current_operator(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT coeqwal_current_operator()
);

\echo '✅ network_gis table created'

\echo '🔗 Adding foreign key constraints...'

ALTER TABLE network_gis 
ADD CONSTRAINT network_gis_network_id_fkey 
FOREIGN KEY (network_id) 
REFERENCES network(id) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE network_gis 
ADD CONSTRAINT network_gis_source_id_fkey 
FOREIGN KEY (source_id) 
REFERENCES source(id) 
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE network_gis 
ADD CONSTRAINT network_gis_network_version_id_fkey 
FOREIGN KEY (network_version_id) 
REFERENCES version(id) 
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE network_gis 
ADD CONSTRAINT network_gis_created_by_fkey 
FOREIGN KEY (created_by) 
REFERENCES developer(id) 
ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE network_gis 
ADD CONSTRAINT network_gis_updated_by_fkey 
FOREIGN KEY (updated_by) 
REFERENCES developer(id) 
ON DELETE RESTRICT ON UPDATE CASCADE;

\echo '✅ Foreign key constraints added'

\echo '📊 Creating indexes...'

CREATE UNIQUE INDEX idx_network_gis_network_id 
    ON network_gis(network_id);

CREATE INDEX idx_network_gis_short_code 
    ON network_gis(short_code);

CREATE INDEX idx_network_gis_precision 
    ON network_gis(precision_level);

CREATE INDEX idx_network_gis_version 
    ON network_gis(network_version_id);


\echo '✅ Indexes created'

\echo ''
\echo '🔄 Creating staging table for network_id lookup...'

DROP TABLE IF EXISTS network_gis_staging;

CREATE TEMP TABLE network_gis_staging (
    short_code VARCHAR,
    network_id INTEGER,
    precision_level VARCHAR,
    geom_wkt TEXT,
    srid INTEGER,
    estimated_accuracy_meters NUMERIC,
    source_id INTEGER,
    network_version_id INTEGER
);

\echo '📥 Loading network_gis.csv from S3 into staging...'

SELECT aws_s3.table_import_from_s3(
    'network_gis_staging',
    'short_code, network_id, precision_level, geom_wkt, srid, estimated_accuracy_meters, source_id, network_version_id',
    '(format csv, header true)',
    'coeqwal-seeds-dev',
    '02_network/network_gis.csv',
    'us-west-2'
);

\echo '✅ Data loaded into staging'

\echo ''
\echo '🔗 Looking up network_id from network table via short_code...'

UPDATE network_gis_staging ngs
SET network_id = n.id
FROM network n
WHERE ngs.short_code = n.short_code;

\echo ''
\echo '🔍 Verifying network_id lookups...'

SELECT 
    COUNT(*) as total_records,
    COUNT(network_id) as successful_lookups,
    COUNT(*) - COUNT(network_id) as failed_lookups
FROM network_gis_staging;

\echo ''
\echo '✅ Inserting into network_gis table...'

INSERT INTO network_gis (
    short_code, network_id, precision_level, geom_wkt, srid,
    estimated_accuracy_meters, source_id, network_version_id
)
SELECT 
    short_code, network_id, precision_level, geom_wkt, srid,
    estimated_accuracy_meters, source_id, network_version_id
FROM network_gis_staging
WHERE network_id IS NOT NULL;

DROP TABLE network_gis_staging;

\echo '✅ network_gis data loaded'

\echo ''
\echo '🗺️  Converting WKT to PostGIS geometry...'

UPDATE network_gis
SET geom = ST_GeomFromText(geom_wkt, srid);

\echo '✅ Geometry conversion complete'

\echo '📊 Creating spatial index...'

CREATE INDEX idx_network_gis_geom 
    ON network_gis USING GIST(geom);

\echo '✅ Spatial index created'

\echo ''
\echo '🔍 VERIFYING NETWORK_GIS DATA'
\echo '============================='

\echo ''
\echo '📊 Total GIS records:'
SELECT COUNT(*) as total_gis_records FROM network_gis;

\echo ''
\echo '📊 Geometry breakdown:'
SELECT 
    GeometryType(geom) as geometry_type,
    COUNT(*) as count
FROM network_gis
GROUP BY GeometryType(geom)
ORDER BY count DESC;

\echo ''
\echo '📊 Precision levels:'
SELECT 
    precision_level,
    COUNT(*) as count
FROM network_gis
GROUP BY precision_level;

\echo ''
\echo '📊 Join verification (GIS + Network):'
SELECT 
    COUNT(DISTINCT ng.network_id) as unique_network_elements_with_gis,
    (SELECT COUNT(*) FROM network WHERE has_gis = true) as network_records_marked_has_gis
FROM network_gis ng;

\echo ''
\echo '📊 Sample spatial records:'
SELECT 
    ng.short_code,
    n.entity_type_id,
    ng.precision_level,
    GeometryType(ng.geom) as geom_type,
    ST_AsText(ST_Centroid(ng.geom)) as centroid,
    ROUND(ST_Length(ng.geom::geography)::numeric, 2) as length_m
FROM network_gis ng
JOIN network n ON ng.network_id = n.id
WHERE n.entity_type_id = 1
LIMIT 3;

\echo ''
\echo '🎉 NETWORK_GIS TABLE SUCCESSFULLY LOADED!'
\echo '========================================='
\echo 'Spatial data with PostGIS geometry now available!'
\echo '• WKT geometries converted to PostGIS format'
\echo '• Spatial index created for fast queries'
\echo '• Ready for mapping and spatial analysis'

