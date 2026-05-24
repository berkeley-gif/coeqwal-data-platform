-- 57_du_urban_gw_sw_boolean.sql
--
-- Convert du_urban_entity.gw and .sw from VARCHAR(5) ('0'/'1'/empty) to
-- BOOLEAN NULL. Ag and refuge entity tables already use BOOLEAN.
--
-- Safe to re-run: checks information_schema before altering.
--
-- After applying, reload seed from:
--   database/seed_tables/04_calsim_data/du_urban_entity.csv
-- (gw/sw values are true/false/empty)

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'du_urban_entity'
          AND column_name = 'gw'
          AND data_type = 'character varying'
    ) THEN
        ALTER TABLE du_urban_entity
            ALTER COLUMN gw TYPE BOOLEAN
            USING (
                CASE
                    WHEN gw IS NULL OR TRIM(gw) = '' THEN NULL
                    WHEN TRIM(gw) IN ('1', 'true', 'TRUE', 't', 'yes') THEN TRUE
                    WHEN TRIM(gw) IN ('0', 'false', 'FALSE', 'f', 'no') THEN FALSE
                    ELSE NULL
                END
            ),
            ALTER COLUMN sw TYPE BOOLEAN
            USING (
                CASE
                    WHEN sw IS NULL OR TRIM(sw) = '' THEN NULL
                    WHEN TRIM(sw) IN ('1', 'true', 'TRUE', 't', 'yes') THEN TRUE
                    WHEN TRIM(sw) IN ('0', 'false', 'FALSE', 'f', 'no') THEN FALSE
                    ELSE NULL
                END
            );
    END IF;
END $$;

COMMENT ON COLUMN du_urban_entity.gw IS
    'TRUE when the demand unit has groundwater-supplied systems (CalSim Table 3-7).';
COMMENT ON COLUMN du_urban_entity.sw IS
    'TRUE when the demand unit has surface-water-supplied systems (CalSim Table 3-7).';

\echo ''
\echo 'du_urban_entity gw/sw column types:'
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'du_urban_entity'
  AND column_name IN ('gw', 'sw')
ORDER BY column_name;

COMMIT;
