-- =============================================================================
-- 57_install_coeqwal_developer_role.sql
--
-- Installs the `coeqwal_developer` group role, backfills GRANTs on existing
-- objects in the public schema, and sets ALTER DEFAULT PRIVILEGES so all
-- future tables/sequences/functions created by `postgres` (via $SUPERUSER_URL)
-- auto-grant to the group.
--
-- After this runs, the per-developer onboarding path becomes:
--   1. SELECT register_developer('alice', 'alice@example.com', ...);
--   2. GRANT coeqwal_developer TO alice;
-- and Alice gets RW on everything that exists, plus everything created later.
--
-- Idempotent: safe to re-run. Must be run as superuser ($SUPERUSER_URL).
-- =============================================================================

\echo '============================================================================'
\echo 'INSTALLING coeqwal_developer GROUP ROLE'
\echo '============================================================================'

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Create the group role (NOLOGIN: cannot connect directly, only via membership)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coeqwal_developer') THEN
        CREATE ROLE coeqwal_developer NOLOGIN;
        RAISE NOTICE 'Created role coeqwal_developer';
    ELSE
        RAISE NOTICE 'Role coeqwal_developer already exists (skipping CREATE)';
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 2. Schema access (required for any object access)
-- -----------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO coeqwal_developer;

-- -----------------------------------------------------------------------------
-- 3. Backfill grants on objects that already exist
-- -----------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA public TO coeqwal_developer;
GRANT USAGE,  SELECT, UPDATE         ON ALL SEQUENCES IN SCHEMA public TO coeqwal_developer;
GRANT EXECUTE                        ON ALL FUNCTIONS IN SCHEMA public TO coeqwal_developer;

-- -----------------------------------------------------------------------------
-- 4. Default privileges for objects created BY postgres GOING FORWARD
--    (anything created via $SUPERUSER_URL inherits these grants automatically)
-- -----------------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO coeqwal_developer;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO coeqwal_developer;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO coeqwal_developer;

-- -----------------------------------------------------------------------------
-- 5. Add the existing developer to the group
--    (Future devs onboard via register_developer + GRANT coeqwal_developer TO ...)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jfantauzza') THEN
        EXECUTE 'GRANT coeqwal_developer TO jfantauzza';
        RAISE NOTICE 'Granted coeqwal_developer to jfantauzza';
    ELSE
        RAISE NOTICE 'Role jfantauzza does not exist, skipping grant';
    END IF;
END $$;

COMMIT;

\echo ''
\echo '============================================================================'
\echo 'VERIFICATION'
\echo '============================================================================'

\echo ''
\echo 'Role exists and is NOLOGIN:'
SELECT rolname, rolsuper, rolinherit, rolcanlogin
FROM pg_roles
WHERE rolname = 'coeqwal_developer';

\echo ''
\echo 'Group members:'
SELECT m.rolname AS member, r.rolname AS group_role
FROM pg_auth_members am
JOIN pg_roles m ON am.member = m.oid
JOIN pg_roles r ON am.roleid = r.oid
WHERE r.rolname = 'coeqwal_developer';

\echo ''
\echo 'Default privileges set for postgres in public schema:'
SELECT defaclrole::regrole         AS owner,
       defaclnamespace::regnamespace AS schema,
       CASE defaclobjtype
           WHEN 'r' THEN 'table'
           WHEN 'S' THEN 'sequence'
           WHEN 'f' THEN 'function'
           WHEN 'T' THEN 'type'
           ELSE defaclobjtype::text
       END                         AS obj_type,
       defaclacl                   AS privileges
FROM pg_default_acl
WHERE defaclrole = 'postgres'::regrole
  AND defaclnamespace = 'public'::regnamespace
ORDER BY obj_type;

\echo ''
\echo 'Done. From a $DATABASE_URL session, this should now succeed:'
\echo '  SELECT COUNT(*) FROM tier_location;'
