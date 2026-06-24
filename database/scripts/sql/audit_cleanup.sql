-- ============================================================================
-- audit_cleanup.sql
--
-- Audit-and-corrections code paired with the monthly schema audit. It gathers
-- a backlog of schema corrections into one pass with a dry-run.
-- ---------------------------------------------------------------------------
-- WHAT THIS IS
--
-- None of these corrections are required for the database to keep
-- running. This script cleans up long-running known issues.
--
-- It corrects schema drift accumulated during the last year: stale
-- artifacts, duplicate indexes and FKs, drifted ID sequences,
-- missing audit triggers and FKs, and hardcoded bootstrap defaults.
--  None of it changes the data, the API contract, or
-- the ETL.
--
-- Skipping the script costs nothing other than carrying the drift
-- forward. Applying it tightens the schema.
--
-- ENVIRONMENT: Run on Cloud9 as `$SUPERUSER_URL`
--
-- ---------------------------------------------------------------------------
-- TO RUN:
--
-- Logs go to `audits/audit_cleanup/`, alongside the monthly audit snapshots
-- and the verification reports. Create the directory if it does not exist yet:
--
--        mkdir -p audits/audit_cleanup
--
--   1. Dry-run first
--
--        cd ~/environment/coeqwal-backend
--        git pull
--        psql $SUPERUSER_URL -v dry_run=true \
--          -f database/scripts/sql/audit_cleanup.sql \
--          | tee audits/audit_cleanup/audit_dry_run_$(date +%Y%m%d_%H%M%S).log
--
--   2. Review the per-section log and the aggregate summary at the bottom
--      of the output.
--
--   3. Apply for real. Same command, no flag.
--
--        psql $SUPERUSER_URL -f database/scripts/sql/audit_cleanup.sql \
--          | tee audits/audit_cleanup/audit_apply_$(date +%Y%m%d_%H%M%S).log
--
--      To apply one tier/section at a time, pass `-v tier=N`. See TIERED EXECUTION
--      below for the four tiers and their sections.
--
--        psql $SUPERUSER_URL -v tier=1 \
--          -f database/scripts/sql/audit_cleanup.sql \
--          | tee audits/audit_cleanup/audit_apply_tier1_$(date +%Y%m%d_%H%M%S).log
--
--   4. Re-run any time. Every section is idempotent. The single signal for
--      "nothing changed" is `rows_affected = 0` on every row of the per-
--      section log. The `status` column will read `skipped` for single-
--      action sections or `ok` for looping sections, but the count is the
--      signal in both cases.
--
-- ---------------------------------------------------------------------------
-- RUNNING BY TIER
--
-- Pass `-v tier=N` to limit a run to one tier, or a comma list like
-- `-v tier=1,2` to run several. Running with no `-v tier` flag at all (the
-- normal case) runs every section. `-v tier=all` is the same thing, just
-- explicit.
--
-- Each section runs as its own DO block. Sections 0 (preflight) and 99
-- (report) always run, regardless of tier.
--
--   Tier 1  trivially safe, fully reversible, no data dependency
--     § 1   drop stale `scenario_backup` table
--     § 5   resync 5 known-drifted ID sequences
--     § 7   drop 41 redundant indexes from exact-duplicate pairs
--     § 8   drop 38 left-prefix duplicate indexes
--
--   Tier 2  low risk, single-action, contained scope
--     § 2   drop 2 duplicate FKs on `network_entity_type`
--     § 3   rename `ag_du_delivery_monthly_id_seq` to `ag_du_demand_monthly_id_seq`
--     § 4   attach `audit_fields_tier_location` trigger to `tier_location`
--     § 13  backfill the NULL row in `hydroclimate.created_by`
--
--   Tier 3  intrusive but contained, changes future insert behavior
--     § 6   promote 5 manual-id Layer 03 tables to sequence-backed `id`
--     § 15  drop 23 lookup and version-ID defaults (no need anymore)
--
--   Tier 4  data-dependent, may partial-fail based on data state
--     § 9   add 94 `created_by` / `updated_by` FKs referencing `developer.id`
--     § 10  fix `is_active integer` to `boolean` on 3 tables
--     § 11  fix `hydroclimate` numeric audit and version columns to integer
--     § 12  fix `has_gis_data integer` to `boolean` on 2 tables (reservoir_entity, channel_entity)
--     § 14  add `hydroclimate.created_by` / `.updated_by` FKs (depends on § 11, § 13)
--   The dry-run pre-checks in § 9 and § 14 list any specific (table, column)
--   pairs whose FK add would fail on the current data (orphan values or
--   wrong column type), so you see exactly what apply will skip.
--
-- Worked example, cautious path:
--
--   1. Dry-run everything to see the full picture
--        psql ... -v dry_run=true -f audit_cleanup.sql | tee ...
--   2. Apply tier 1 (trivially safe)
--        psql ... -v tier=1 -f audit_cleanup.sql | tee ...
--   3. Apply tier 2 once tier 1 looks good
--        psql ... -v tier=2 -f audit_cleanup.sql | tee ...
--   4. Continue with tiers 3 and 4 at your own pace.
--
-- Finer control inside a tier: comment out any DO $$ ... END $$; block you
-- do not want to run. Each section is bracketed by `=` separator comments,
-- easy to skip.
--
-- If you pass a tier value that is not `all` or a comma list of 1-4 (e.g.
-- `-v tier=5` or `-v tier=foo`), no DDL runs. The preflight row at the top
-- of the report names the bad value and the valid options. Correct the
-- flag and re-run.
--
-- ---------------------------------------------------------------------------
-- CONTINUE-ON-FAILURE
--
-- Each section is a DO block with its own EXCEPTION handler. If a section
-- fails, its own work rolls back, a row is logged with status `failed` and the
-- SQLERRM, and the next section runs. Section 99 prints the full log at the end.
--
-- A failure partway through leaves every earlier section already applied. The
-- per-section log (and the `audits/audit_cleanup/` tee file) is the record of
-- exactly what landed.
-- ============================================================================

\set ON_ERROR_STOP off

-- Dry-run flag. Default `false` (apply for real). Pass `-v dry_run=true` to
-- preview without changing anything:
--     psql -v dry_run=true -f audit_cleanup.sql
\if :{?dry_run}
\else
\set dry_run false
\endif

SELECT set_config(
    'audit.dry_run',
    CASE WHEN :'dry_run' = 'true' THEN 'true' ELSE 'false' END,
    false
);

-- Tier flag. Default `all` (run every section). Pass `-v tier=N` to limit the
-- run to one risk tier, or a comma list like `-v tier=1,2` to run a subset.
-- Valid tiers: 1, 2, 3, 4, all. See header docs for what falls in each tier.
\if :{?tier}
\else
\set tier 'all'
\endif

SELECT set_config('audit.tier', :'tier', false);

-- Banner to show the mode before any DDL runs
\echo
\echo '============================================================'
\if :{?dry_run}
\if :dry_run
\echo 'audit_cleanup.sql  MODE: DRY-RUN  (no changes will be made)'
\else
\echo 'audit_cleanup.sql  MODE: APPLY    (changes will be made)'
\endif
\else
\echo 'audit_cleanup.sql  MODE: APPLY    (changes will be made)'
\echo '(to preview safely, re-run with: -v dry_run=true)'
\endif
\echo 'TIER:' :tier
\echo '============================================================'
\echo

-- ---------------------------------------------------------------------------
-- Setup: per-session log table
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS pg_temp.migration_log;
CREATE TEMP TABLE migration_log (
    step_id       integer,
    step_name     text,
    status        text,
    rows_affected integer DEFAULT 0,
    duration_ms   numeric DEFAULT 0,
    details       text,
    ran_at        timestamptz DEFAULT now()
);

-- Helper: Returns true when the section should run, false when it should skip itself. 
-- Falls back to "run" for tier='all' or empty, and falls back to "skip" for any value that 
-- does not parse.
CREATE OR REPLACE FUNCTION pg_temp.tier_selected(section_tier integer)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    tier_setting text := current_setting('audit.tier', true);
    requested integer[];
BEGIN
    IF tier_setting IS NULL OR tier_setting = '' OR tier_setting = 'all' THEN
        RETURN true;
    END IF;
    BEGIN
        requested := string_to_array(tier_setting, ',')::integer[];
    EXCEPTION WHEN OTHERS THEN
        RETURN false;
    END;
    RETURN section_tier = ANY(requested);
END;
$$;

-- ============================================================================
-- Section 0: pre-flight environment check
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    missing text[] := ARRAY[]::text[];
    tier_setting text := current_setting('audit.tier', true);
    tier_part text;
    tier_value_int integer;
    tier_invalid boolean := false;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'developer' AND relkind = 'r') THEN
        missing := array_append(missing, 'table:developer');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'version' AND relkind = 'r') THEN
        missing := array_append(missing, 'table:version');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'network' AND relkind = 'r') THEN
        missing := array_append(missing, 'table:network');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'scenario' AND relkind = 'r') THEN
        missing := array_append(missing, 'table:scenario');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'coeqwal_current_operator') THEN
        missing := array_append(missing, 'function:coeqwal_current_operator');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'set_audit_fields') THEN
        missing := array_append(missing, 'function:set_audit_fields');
    END IF;

    IF array_length(missing, 1) IS NULL THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (0, 'preflight', 'ok', 'all canaries present',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
    ELSE
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (0, 'preflight', 'warning',
                'canaries missing, later sections will likely fail or skip: '
                || array_to_string(missing, ', '),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
    END IF;

    -- Tier flags. Accept 'all', empty, or a comma list of integers
    -- in {1,2,3,4}. Any other value yields a warning row and no sections run
    -- (the per-section tier_selected() guard returns false on parse failure).
    IF tier_setting IS NOT NULL AND tier_setting <> '' AND tier_setting <> 'all' THEN
        BEGIN
            FOREACH tier_part IN ARRAY string_to_array(tier_setting, ',')
            LOOP
                tier_value_int := tier_part::integer;
                IF tier_value_int < 1 OR tier_value_int > 4 THEN
                    tier_invalid := true;
                    EXIT;
                END IF;
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            tier_invalid := true;
        END;
        IF tier_invalid THEN
            INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
            VALUES (0, 'preflight-tier', 'warning',
                    format('invalid tier value %L, expected ''all'' or comma list of 1,2,3,4. No sections will run, every section will report skipped.', tier_setting),
                    0);
        END IF;
    END IF;
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (0, 'preflight', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 1: drop scenario_backup
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 1;
    target_exists boolean;
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (1, 'drop obsolete scenario_backup table', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'scenario_backup' AND relkind = 'r')
    INTO target_exists;

    IF NOT target_exists THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (1, 'drop obsolete scenario_backup table', 'skipped', 'table already absent',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF is_dry THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (1, 'drop obsolete scenario_backup table', 'would_drop', 'would drop table scenario_backup',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    -- scenario_backup: stale 75-row snapshot from the column-reorder migration
    -- No PK, no FKs, not referenced by any code in the repo.
    DROP TABLE scenario_backup;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (1, 'drop obsolete scenario_backup table', 'ok', 1, 'dropped',
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (1, 'drop obsolete scenario_backup table', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 2: drop duplicate FKs on network_entity_type
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 2;
    dropped integer := 0;
    skipped integer := 0;
    failed_details text := '';
    dup_name text;
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (2, 'drop duplicate FKs on network_entity_type', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOREACH dup_name IN ARRAY ARRAY[
        -- network_entity_type.created_by has two FKs to developer.id, one
        -- intentionally named fk_network_entity_type_created_by (kept) and
        -- one Postgres-auto-named _fkey (dropped here). Same shape for
        -- updated_by.
        'network_entity_type_created_by_fkey',
        'network_entity_type_updated_by_fkey'
    ]
    LOOP
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE t.relname = 'network_entity_type'
                  AND c.conname = dup_name
            ) THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            IF is_dry THEN
                dropped := dropped + 1;
                CONTINUE;
            END IF;

            EXECUTE format('ALTER TABLE network_entity_type DROP CONSTRAINT %I', dup_name);
            dropped := dropped + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details || dup_name || ':' || SQLERRM || ' | ';
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (2, 'drop duplicate FKs on network_entity_type',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_drop'
                 ELSE 'ok' END,
            dropped,
            format('dropped=%s skipped=%s%s', dropped, skipped,
                   CASE WHEN failed_details <> '' THEN ' | failures: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (2, 'drop duplicate FKs on network_entity_type', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 3: rename sequence ag_du_delivery_monthly_id_seq
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 2;
    old_exists boolean;
    new_exists boolean;
    target_exists boolean;
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (3, 'rename ag_du_delivery_monthly_id_seq to match renamed table', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ag_du_delivery_monthly_id_seq' AND relkind = 'S')
    INTO old_exists;
    SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ag_du_demand_monthly_id_seq' AND relkind = 'S')
    INTO new_exists;
    SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ag_du_demand_monthly' AND relkind = 'r')
    INTO target_exists;

    IF NOT target_exists THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (3, 'rename ag_du_delivery_monthly_id_seq to match renamed table', 'skipped',
                'target table ag_du_demand_monthly missing',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF NOT old_exists AND new_exists THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (3, 'rename ag_du_delivery_monthly_id_seq to match renamed table', 'skipped',
                'rename already applied',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF NOT old_exists AND NOT new_exists THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (3, 'rename ag_du_delivery_monthly_id_seq to match renamed table', 'skipped',
                'neither old nor new sequence exists',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF is_dry THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (3, 'rename ag_du_delivery_monthly_id_seq to match renamed table', 'would_rename',
                'would rename ag_du_delivery_monthly_id_seq to ag_du_demand_monthly_id_seq, re-point column default, and OWNED BY',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    -- ag_du_demand_monthly: the table was renamed from ag_du_delivery_monthly
    -- (it holds demand statistics, not delivery: surface-water and groundwater
    -- deliveries live in ag_du_sw_delivery_monthly and ag_du_gw_pumping_monthly).
    -- The rename was applied to the table but not to its backing sequence.
    --
    -- This is a cosmetic name mismatch, not a functional break. The column
    -- default stores the sequence reference by OID (nextval(...::regclass)), so
    -- it kept resolving to the same sequence after the table rename. Inserts
    -- (including every ETL/run_all.py load) auto-assigned ids normally. No row
    -- was lost or silently skipped. This fix only realigns the names:
    --   1. rename the sequence to match the table
    --   2. re-point the column default so it reads the new name too
    --   3. re-anchor OWNED BY so a future DROP TABLE drops the sequence too
    ALTER SEQUENCE ag_du_delivery_monthly_id_seq RENAME TO ag_du_demand_monthly_id_seq;
    ALTER TABLE ag_du_demand_monthly ALTER COLUMN id SET DEFAULT nextval('ag_du_demand_monthly_id_seq');
    ALTER SEQUENCE ag_du_demand_monthly_id_seq OWNED BY ag_du_demand_monthly.id;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (3, 'rename ag_du_delivery_monthly_id_seq to match renamed table', 'ok', 1,
            'sequence renamed, column default re-pointed, OWNED BY set',
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (3, 'rename ag_du_delivery_monthly_id_seq to match renamed table', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 4: attach set_audit_fields trigger to tier_location
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 2;
    target_rel oid;
    trigger_exists boolean;
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (4, 'attach set_audit_fields trigger to tier_location', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    SELECT oid INTO target_rel FROM pg_class WHERE relname = 'tier_location' AND relkind = 'r';
    IF target_rel IS NULL THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (4, 'attach set_audit_fields trigger to tier_location', 'skipped',
                'tier_location table missing',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = target_rel
          AND NOT tgisinternal
          AND tgname LIKE '%audit_fields%'
    ) INTO trigger_exists;

    IF trigger_exists THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (4, 'attach set_audit_fields trigger to tier_location', 'skipped',
                'audit-fields trigger already attached',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF is_dry THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (4, 'attach set_audit_fields trigger to tier_location', 'would_attach',
                'would create trigger audit_fields_tier_location BEFORE INSERT OR UPDATE',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    -- tier_location: has the four audit columns and DDL-level defaults
    -- (NOW(), coeqwal_current_operator()), but the audit trigger was never
    -- attached.
    CREATE TRIGGER audit_fields_tier_location
    BEFORE INSERT OR UPDATE ON tier_location
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (4, 'attach set_audit_fields trigger to tier_location', 'ok', 1,
            'trigger attached',
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (4, 'attach set_audit_fields trigger to tier_location', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Background for Sections 5 and 15: integer ids vs short codes
--
-- Reference tables were originally given stable integer ids, loaded from seed
-- CSVs with explicit values (developer.id = 2, source.id = 4, etc.). Later
-- layers keyed on the natural short codes the data already had
-- (scenario.short_code, tier_definition.short_code, du_id) instead, leaving
-- the earlier integer-id batch behind.
--
-- Sections 5 and 15 clean up the leftovers. Section 5 fixes the sequence drift
-- from explicit-id loads. Section 15 drops the hardcoded id defaults (like
-- source_id = 4) wired into other tables.
-- ============================================================================

-- ============================================================================
-- Section 5: resync 5 known-drifted ID sequences
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 1;
    pair record;
    new_value bigint;
    resynced integer := 0;
    skipped integer := 0;
    failed_details text := '';
    details_out text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (5, 'resync 5 drifted sequences', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOR pair IN
        SELECT * FROM (VALUES
            -- All five drifted because the CSV loader inserted explicit
            -- `id` values, which bypasses the sequence's last_value
            -- advancement. The fix is one setval(MAX(id)) per sequence.
            -- Row counts are from the May 2026 audit snapshot.
            ('developer_id_seq',            'developer'),            -- 6 rows
            ('version_family_id_seq',       'version_family'),       -- 14 rows
            ('version_id_seq',              'version'),              -- 14 rows
            ('cws_aggregate_entity_id_seq', 'cws_aggregate_entity'), -- 6 rows
            ('du_urban_group_id_seq',       'du_urban_group')        -- 11 rows
        ) AS s(seq_name, target_table)
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = pair.seq_name AND relkind = 'S') THEN
                skipped := skipped + 1;
                details_out := details_out || pair.seq_name || ':seq-missing | ';
                CONTINUE;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = pair.target_table AND relkind = 'r') THEN
                skipped := skipped + 1;
                details_out := details_out || pair.seq_name || ':target-table-missing | ';
                CONTINUE;
            END IF;

            EXECUTE format('SELECT COALESCE(MAX(id), 0) FROM %I', pair.target_table) INTO new_value;

            IF is_dry THEN
                resynced := resynced + 1;
                details_out := details_out || format('%s:would_setval_%s | ', pair.seq_name, new_value);
                CONTINUE;
            END IF;

            EXECUTE format('SELECT setval(%L, %s)', pair.seq_name, GREATEST(new_value, 1));
            resynced := resynced + 1;
            details_out := details_out || format('%s:%s | ', pair.seq_name, new_value);
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details || pair.seq_name || ':' || SQLERRM || ' | ';
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (5, 'resync 5 drifted sequences',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_resync'
                 ELSE 'ok' END,
            resynced,
            format('resynced=%s skipped=%s | %s%s',
                   resynced, skipped, details_out,
                   CASE WHEN failed_details <> '' THEN ' failures: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (5, 'resync 5 drifted sequences', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 6: promote 5 manual-id Layer 03 tables to sequence-backed id
--
-- These five tables were defined as `id integer NOT NULL` with no
-- sequence default, so every INSERT has to supply the id by hand. That
-- works for the seed CSV load but is fragile for any new insert.
--
-- The rest of the schema follows the standard SERIAL-equivalent pattern:
-- `id integer NOT NULL DEFAULT nextval('<table>_id_seq')`. This section
-- retrofits that pattern onto the five outliers.
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 3;
    target_table text;
    seq_name text;
    current_default text;
    new_value bigint;
    promoted integer := 0;
    skipped integer := 0;
    failed_details text := '';
    details_out text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (6, 'promote 5 manual-id tables to sequence-backed id', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOREACH target_table IN ARRAY ARRAY[
        -- All five were built with `id integer` and a hand-assigned id at
        -- load time (from a CSV column). They never carried a
        -- nextval() default, so further inserts have to keep guessing ids.
        -- Create a sequence, set the column default to nextval(seq), then
        -- setval(MAX(id)) so the next INSERT lands at MAX+1.
        -- Row counts and id ranges are from the May 2026 audit snapshot.
        'reservoir_entity',         -- 92 rows, ids 1..92
        'reservoir_group',          -- 4 rows, ids 1..4
        'reservoir_group_member',   -- 24 rows, ids 1..24
        'du_urban_group',           -- 11 rows (orphan sequence already exists,
                                    -- so Section 5 setvals it harmlessly. This
                                    -- section wires it to the column default)
        'mi_contractor_group'       -- 6 rows, ids 1..6
    ]
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = target_table AND relkind = 'r') THEN
                skipped := skipped + 1;
                details_out := details_out || target_table || ':table-missing | ';
                CONTINUE;
            END IF;

            seq_name := target_table || '_id_seq';

            SELECT pg_get_expr(d.adbin, d.adrelid)
            INTO current_default
            FROM pg_attribute a
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            JOIN pg_class c ON c.oid = a.attrelid
            WHERE c.relname = target_table
              AND a.attname = 'id'
              AND NOT a.attisdropped;

            IF current_default IS NOT NULL AND current_default LIKE 'nextval%' THEN
                skipped := skipped + 1;
                details_out := details_out || target_table || ':already-sequenced | ';
                CONTINUE;
            END IF;

            EXECUTE format('SELECT COALESCE(MAX(id), 0) FROM %I', target_table) INTO new_value;

            IF is_dry THEN
                promoted := promoted + 1;
                details_out := details_out || format('%s:would_promote_seq_max_%s | ', target_table, new_value);
                CONTINUE;
            END IF;

            EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I', seq_name);
            EXECUTE format('ALTER TABLE %I ALTER COLUMN id SET DEFAULT nextval(%L)', target_table, seq_name);
            EXECUTE format('ALTER SEQUENCE %I OWNED BY %I.id', seq_name, target_table);
            EXECUTE format('SELECT setval(%L, %s)', seq_name, GREATEST(new_value, 1));

            promoted := promoted + 1;
            details_out := details_out || format('%s:promoted_setval_%s | ', target_table, new_value);
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details || target_table || ':' || SQLERRM || ' | ';
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (6, 'promote 5 manual-id tables to sequence-backed id',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_promote'
                 ELSE 'ok' END,
            promoted,
            format('promoted=%s skipped=%s | %s%s',
                   promoted, skipped, details_out,
                   CASE WHEN failed_details <> '' THEN ' failures: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (6, 'promote 5 manual-id tables to sequence-backed id', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 7: drop redundant indexes from exact-duplicate pairs
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 1;
    pair record;
    dropped integer := 0;
    skipped integer := 0;
    failed_details text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (7, 'drop 41 redundant indexes from exact-duplicate pairs', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOR pair IN
        SELECT * FROM (VALUES
            -- target_table                          drop_name                                            keep_name (canonical, stays in place)
            ('ag_aggregate_monthly',              'idx_ag_agg_monthly_scenario',                       'idx_ag_aggregate_monthly_combined'),
            ('ag_aggregate_period_summary',      'idx_ag_agg_period_scenario',                        'uq_ag_aggregate_period_summary'),
            ('ag_du_demand_monthly',             'idx_ag_du_delivery_scenario_du',                    'idx_ag_du_demand_monthly_combined'),
            ('ag_du_period_summary',             'idx_ag_du_period_scenario_du',                      'uq_ag_du_period_summary'),
            ('ag_du_shortage_monthly',           'idx_ag_du_shortage_scenario_du',                    'idx_ag_du_shortage_monthly_combined'),
            ('channel_entity',                   'idx_channel_entity_network_arc',                    'channel_entity_network_arc_id_key'),
            ('channel_variable',                 'idx_channel_variable_calsim_id',                    'channel_variable_calsim_id_key'),
            ('compliance_station',               'idx_compliance_code',                               'compliance_station_station_code_key'),
            ('cws_aggregate_entity',             'idx_cws_aggregate_entity_short_code',               'cws_aggregate_entity_short_code_key'),
            ('cws_aggregate_monthly',            'idx_cws_agg_monthly_aggregate',                     'idx_cws_aggregate_monthly_entity'),
            ('cws_aggregate_monthly',            'idx_cws_agg_monthly_combined',                      'idx_cws_aggregate_monthly_combined'),
            ('cws_aggregate_monthly',            'idx_cws_agg_monthly_scenario',                      'idx_cws_aggregate_monthly_scenario'),
            ('cws_aggregate_period_summary',     'idx_cws_agg_period_aggregate',                      'idx_cws_aggregate_period_entity'),
            ('cws_aggregate_period_summary',     'idx_cws_agg_period_scenario',                       'idx_cws_aggregate_period_scenario'),
            ('du_delivery_monthly',              'idx_du_delivery_scenario_du',                       'idx_du_delivery_monthly_combined'),
            ('du_period_summary',                'idx_du_period_scenario_du',                         'uq_du_period_summary'),
            ('du_shortage_monthly',              'idx_du_shortage_scenario_du',                       'idx_du_shortage_monthly_combined'),
            ('du_urban_entity',                  'idx_du_urban_entity_du_id',                         'du_urban_entity_du_id_key'),
            ('du_urban_group',                   'idx_du_urban_group_short_code',                     'du_urban_group_short_code_key'),
            ('du_urban_variable',                'idx_du_urban_variable_du_id',                       'uq_du_urban_variable_du_id'),
            ('env_flow_channel_period_summary',  'idx_env_flow_period_summary_arc_scenario',          'uq_env_flow_period_summary'),
            ('mi_contractor',                    'idx_mi_contractor_short_code',                      'mi_contractor_short_code_key'),
            ('mi_contractor_delivery_arc',       'idx_mi_contractor_delivery_arc_arc',                'uq_delivery_arc'),
            ('mi_contractor_period_summary',     'idx_mi_period_scenario_contractor',                 'uq_mi_period_summary'),
            ('mi_delivery_monthly',              'idx_mi_delivery_scenario_contractor',               'idx_mi_delivery_monthly_combined'),
            ('mi_shortage_monthly',              'idx_mi_shortage_scenario_contractor',               'idx_mi_shortage_monthly_combined'),
            ('network_entity_type',              'idx_network_entity_type_short_code',                'network_entity_type_short_code_key'),
            ('network_type',                     'network_type_short_code_entity_key',                'network_type_short_code_network_entity_type_id_key'),
            ('refuge_du_period_summary',         'idx_refuge_period_summary_scenario_du',             'uq_refuge_period_summary'),
            ('reservoir',                        'idx_reservoir_calsim_code',                         'reservoir_calsim_short_code_key'),
            ('reservoir_entity',                 'idx_reservoir_entity_short_code',                   'reservoir_entity_short_code_key'),
            ('reservoir_group',                  'idx_reservoir_group_short_code',                    'reservoir_group_short_code_key'),
            ('reservoir_monthly_percentile',     'idx_reservoir_percentile_combined',                 'idx_reservoir_percentile_scenario_entity'),
            ('reservoir_monthly_percentile',     'idx_reservoir_percentile_reservoir',                'idx_reservoir_percentile_entity'),
            ('reservoir_period_summary',         'idx_period_summary_dead_prob',                      'idx_period_summary_dead_pool_prob'),
            ('reservoir_spill_monthly',          'idx_reservoir_spill_scenario',                      'idx_spill_monthly_combined'),
            ('reservoir_storage_monthly',        'idx_reservoir_storage_scenario',                    'idx_storage_monthly_combined'),
            ('theme_scenario_link',              'idx_theme_scenario_reverse',                        'theme_scenario_link_pkey'),
            ('tier_location_result',             'idx_tier_location_unique',                          'tier_location_result_scenario_short_code_tier_short_code_lo_key'),
            ('tier_location_result',             'tier_location_result_unique',                       'tier_location_result_scenario_short_code_tier_short_code_lo_key'),
            ('wba',                              'idx_wba_id',                                        'wba_wba_id_key')
        ) AS s(target_table, drop_name, keep_name)
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = pair.drop_name) THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            IF is_dry THEN
                dropped := dropped + 1;
                CONTINUE;
            END IF;

            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', pair.target_table, pair.drop_name);
            EXECUTE format('DROP INDEX IF EXISTS %I', pair.drop_name);
            dropped := dropped + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details ||
                format('%s.%s (keep %s):%s | ', pair.target_table, pair.drop_name, pair.keep_name, SQLERRM);
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (7, 'drop 41 redundant indexes from exact-duplicate pairs',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_drop'
                 ELSE 'ok' END,
            dropped,
            format('dropped=%s skipped=%s%s', dropped, skipped,
                   CASE WHEN failed_details <> '' THEN ' | failures: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (7, 'drop 41 redundant indexes from exact-duplicate pairs', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 8: drop 38 left-prefix duplicate indexes
--
-- Each row is a single-column index whose column is the leading column of
-- an existing composite. Queries that need only the leading column can use
-- the composite. The `covered_by` column documents which composite (or
-- unique constraint) makes each drop safe.
--
-- The theme row is the opposite: the unique constraint
-- `theme_short_code_key` is the canonical one to keep, so we drop the
-- non-unique filter index `idx_theme_short_code_active`.
--
-- `covered_by` always names an index that survives the full run. Where a table
-- also had an exact-duplicate pair cleaned up in Section 7, this column points
-- at the twin that Section 7 keeps (the `*_combined` index), not the twin it
-- drops, so the documented cover is still present after both sections run.
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 1;
    pair record;
    dropped integer := 0;
    skipped integer := 0;
    failed_details text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (8, 'drop 38 left-prefix duplicate indexes', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOR pair IN
        SELECT * FROM (VALUES
            -- target_table                          drop_name                                       covered_by (kept)
            ('ag_aggregate_monthly',              'idx_ag_aggregate_monthly_scenario',              'idx_ag_aggregate_monthly_combined'),
            ('ag_aggregate_period_summary',      'idx_ag_aggregate_period_summary_scenario',       'uq_ag_aggregate_period_summary'),
            ('ag_du_demand_monthly',             'idx_ag_du_demand_monthly_scenario',              'idx_ag_du_demand_monthly_combined'),
            ('ag_du_gw_pumping_monthly',         'idx_ag_du_gw_pumping_monthly_scenario',          'idx_ag_du_gw_pumping_monthly_combined'),
            ('ag_du_period_summary',             'idx_ag_du_period_summary_scenario',              'uq_ag_du_period_summary'),
            ('ag_du_shortage_monthly',           'idx_ag_du_shortage_monthly_scenario',            'idx_ag_du_shortage_monthly_combined'),
            ('ag_du_sw_delivery_monthly',        'idx_ag_du_sw_delivery_monthly_scenario',         'idx_ag_du_sw_delivery_monthly_combined'),
            ('cws_aggregate_monthly',            'idx_cws_aggregate_monthly_scenario',             'idx_cws_aggregate_monthly_combined'),
            ('cws_aggregate_period_summary',     'idx_cws_aggregate_period_scenario',              'uq_cws_aggregate_period'),
            ('delta_monthly',                    'idx_delta_monthly_scenario',                     'uq_delta_monthly'),
            ('delta_period_summary',             'idx_delta_summary_scenario',                     'uq_delta_period_summary'),
            ('du_delivery_monthly',              'idx_du_delivery_monthly_scenario',               'idx_du_delivery_monthly_combined'),
            ('du_period_summary',                'idx_du_period_summary_scenario',                 'uq_du_period_summary'),
            ('du_shortage_monthly',              'idx_du_shortage_monthly_scenario',               'idx_du_shortage_monthly_combined'),
            ('du_urban_delivery_arc',            'idx_du_urban_delivery_arc_du_id',                'uq_du_urban_delivery_arc'),
            ('du_urban_group_member',            'idx_du_urban_group_member_group',                'uq_du_urban_group_member'),
            ('env_flow_channel_monthly',         'idx_env_flow_monthly_arc',                       'idx_env_flow_monthly_arc_scenario'),
            ('env_flow_channel_period_summary',  'idx_env_flow_period_summary_arc',                'uq_env_flow_period_summary'),
            ('env_flow_channel_seasonal',        'idx_env_flow_seasonal_arc',                      'idx_env_flow_seasonal_arc_scenario'),
            ('mi_contractor_group_member',       'idx_mi_contractor_group_member_group',           'uq_mi_contractor_group_member'),
            ('mi_contractor_period_summary',     'idx_mi_period_summary_scenario',                 'uq_mi_period_summary'),
            ('mi_delivery_monthly',              'idx_mi_delivery_monthly_scenario',               'idx_mi_delivery_monthly_combined'),
            ('mi_shortage_monthly',              'idx_mi_shortage_monthly_scenario',               'idx_mi_shortage_monthly_combined'),
            ('network_arc',                      'idx_network_arc_from_node',                      'idx_network_arc_connectivity'),
            ('refuge_du_delivery_monthly',       'idx_refuge_delivery_monthly_scenario',           'idx_refuge_delivery_monthly_scenario_du'),
            ('refuge_du_period_summary',         'idx_refuge_period_summary_scenario',             'uq_refuge_period_summary'),
            ('refuge_du_shortage_monthly',       'idx_refuge_shortage_monthly_scenario',           'idx_refuge_shortage_monthly_scenario_du'),
            ('reservoir_group_member',           'idx_reservoir_group_member_group',               'uq_reservoir_group_member'),
            ('reservoir_monthly_percentile',     'idx_reservoir_percentile_scenario',              'idx_reservoir_percentile_scenario_entity'),
            ('reservoir_period_summary',         'idx_period_summary_scenario',                    'uq_period_summary'),
            ('reservoir_spill_monthly',          'idx_spill_monthly_scenario',                     'idx_spill_monthly_combined'),
            ('reservoir_storage_monthly',        'idx_storage_monthly_scenario',                   'idx_storage_monthly_combined'),
            ('scenario',                         'idx_scenario_active',                            'idx_scenario_active_version'),
            ('sensitivity_climate',              'idx_sensitivity_climate_sibling',                'sensitivity_climate_sibling_group_module_entity_id_metric_n_key'),
            ('sensitivity_operational',          'idx_sensitivity_operational_hydro',              'sensitivity_operational_hydroclimate_id_module_entity_id_me_key'),
            ('theme',                            'idx_theme_short_code_active',                    'theme_short_code_key'),
            ('tier_location_result',             'idx_tier_location_scenario',                     'idx_tier_location_combined'),
            ('tier_result',                      'idx_tier_result_scenario',                       'idx_tier_result_scenario_tier')
        ) AS s(target_table, drop_name, covered_by)
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = pair.drop_name) THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            IF is_dry THEN
                dropped := dropped + 1;
                CONTINUE;
            END IF;

            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', pair.target_table, pair.drop_name);
            EXECUTE format('DROP INDEX IF EXISTS %I', pair.drop_name);
            dropped := dropped + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details ||
                format('%s.%s (covered_by %s):%s | ', pair.target_table, pair.drop_name, pair.covered_by, SQLERRM);
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (8, 'drop 38 left-prefix duplicate indexes',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_drop'
                 ELSE 'ok' END,
            dropped,
            format('dropped=%s skipped=%s%s', dropped, skipped,
                   CASE WHEN failed_details <> '' THEN ' | failures: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (8, 'drop 38 left-prefix duplicate indexes', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 9: add created_by / updated_by FKs referencing developer.id
--
-- Every audited table carries the same four audit columns (created_at,
-- created_by, updated_at, updated_by), but developer.id FK enforcement is
-- inconsistent. The junction/link tables and a few lookups got their FKs at
-- creation. The entity, definition, category, and scenario tables did not.
-- It is an artifact of tables built at different times, not a meaningful
-- split.
--
-- Each new foreign key is named fk_<table>_<column> (for example
-- fk_network_entity_type_created_by) (the same style the schema uses).
-- Deleting or changing a developer row does not cascade to these columns
-- (ON DELETE / ON UPDATE NO ACTION), matching the other audit FKs.
--
-- Before adding any foreign key, two checks run (in both dry-run and apply).
-- If either fails, the section reports `would_fail` and says why:
--   type     the column has to be an integer type. If it is still numeric, adding the
--            key would fail, so we skip that column.
--   orphans  every value in the column that is not NULL has to match a real
--            developer.id. If any do not, Postgres would reject the key, so we
--            count the strays first and skip instead of erroring.
--
-- Two tables are left out on purpose: scenario_backup (Section 1 drops it) and
-- hydroclimate (its columns need a numeric-to-integer fix first, done in
-- Section 14).
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 4;
    target_table text;
    col_name text;
    fk_name text;
    col_type text;
    orphan_count integer;
    added integer := 0;
    skipped integer := 0;
    would_fail integer := 0;
    failed_details text := '';
    fail_reasons text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (9, 'add 94 developer FKs', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOREACH target_table IN ARRAY ARRAY[
        -- Layer 03 entity registries (one row per real-world thing)
        'ag_aggregate_entity',
        'cws_aggregate_entity',
        'du_agriculture_entity',
        'du_urban_delivery_arc',
        'du_urban_entity',
        'du_urban_group',
        'du_urban_group_member',
        'du_urban_variable',
        'mi_contractor',
        'mi_contractor_delivery_arc',
        'mi_contractor_group',
        'mi_contractor_group_member',
        'reservoir_entity',
        'reservoir_group',
        'reservoir_group_member',
        -- Layer 05 assumptions and operations
        'assumption_category',
        'assumption_definition',
        'operation_category',
        'operation_definition',
        -- Layer 06 scenarios
        'scenario',
        'scenario_author',
        'scenario_hydroclimate_sibling',
        -- Layer 08 themes
        'theme',
        -- Layer 11 ag aggregate and ag DU result tables
        'ag_aggregate_monthly',
        'ag_aggregate_period_summary',
        'ag_du_demand_monthly',
        'ag_du_gw_pumping_monthly',
        'ag_du_period_summary',
        'ag_du_shortage_monthly',
        'ag_du_sw_delivery_monthly',
        -- Layer 11 cws aggregate result tables
        'cws_aggregate_monthly',
        'cws_aggregate_period_summary',
        -- Layer 11 delta result tables
        'delta_monthly',
        'delta_period_summary',
        -- Layer 11 DU result tables
        'du_delivery_monthly',
        'du_period_summary',
        'du_shortage_monthly',
        -- Layer 11 MI result tables
        'mi_contractor_period_summary',
        'mi_delivery_monthly',
        'mi_shortage_monthly',
        -- Layer 11 refuge DU result tables
        'refuge_du_delivery_monthly',
        'refuge_du_period_summary',
        'refuge_du_shortage_monthly',
        -- Layer 11 reservoir result tables
        'reservoir_monthly_percentile',
        'reservoir_period_summary',
        'reservoir_spill_monthly',
        'reservoir_storage_monthly'
    ]
    LOOP
        FOREACH col_name IN ARRAY ARRAY['created_by', 'updated_by']
        LOOP
            fk_name := 'fk_' || target_table || '_' || col_name;
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = target_table AND relkind = 'r') THEN
                    skipped := skipped + 1;
                    CONTINUE;
                END IF;

                SELECT t.typname INTO col_type
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_class c ON c.oid = a.attrelid
                WHERE c.relname = target_table
                  AND a.attname = col_name
                  AND NOT a.attisdropped;

                IF col_type IS NULL THEN
                    skipped := skipped + 1;
                    CONTINUE;
                END IF;

                IF EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    WHERE t.relname = target_table
                      AND c.conname = fk_name
                ) THEN
                    skipped := skipped + 1;
                    CONTINUE;
                END IF;
                IF EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                    WHERE t.relname = target_table
                      AND a.attname = col_name
                      AND c.contype = 'f'
                ) THEN
                    skipped := skipped + 1;
                    CONTINUE;
                END IF;

                IF col_type NOT IN ('int2', 'int4', 'int8') THEN
                    would_fail := would_fail + 1;
                    fail_reasons := fail_reasons ||
                        format('%s.%s:type-is-%s | ', target_table, col_name, col_type);
                    CONTINUE;
                END IF;

                EXECUTE format(
                    'SELECT COUNT(*) FROM %I t WHERE t.%I IS NOT NULL '
                    'AND NOT EXISTS (SELECT 1 FROM developer d WHERE d.id = t.%I)',
                    target_table, col_name, col_name
                ) INTO orphan_count;
                IF orphan_count > 0 THEN
                    would_fail := would_fail + 1;
                    fail_reasons := fail_reasons ||
                        format('%s.%s:%s-orphan-values | ', target_table, col_name, orphan_count);
                    CONTINUE;
                END IF;

                IF is_dry THEN
                    added := added + 1;
                    CONTINUE;
                END IF;

                EXECUTE format(
                    'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES developer (id) ON DELETE NO ACTION ON UPDATE NO ACTION',
                    target_table, fk_name, col_name
                );
                added := added + 1;
            EXCEPTION WHEN OTHERS THEN
                failed_details := failed_details ||
                    target_table || '.' || col_name || ':' || SQLERRM || ' | ';
            END;
        END LOOP;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (9, 'add 94 developer FKs',
            CASE
                WHEN failed_details <> '' THEN 'partial'
                WHEN would_fail > 0 AND is_dry THEN 'would_partial'
                WHEN would_fail > 0 THEN 'partial'
                WHEN is_dry THEN 'would_add'
                ELSE 'ok'
            END,
            added,
            format('added=%s skipped=%s would_fail=%s%s%s',
                   added, skipped, would_fail,
                   CASE WHEN fail_reasons <> '' THEN ' | infeasible: ' || fail_reasons ELSE '' END,
                   CASE WHEN failed_details <> '' THEN ' | failures: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (9, 'add 94 developer FKs', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Impact audit for Sections 10, 11, 12 (column-type changes)
--
-- Type changes these sections make:
--   Section 10  is_active  integer to boolean  on hydroclimate, scenario_author, theme
--   Section 11  hydroclimate_version_id, created_by, updated_by  numeric to integer  on hydroclimate
--   Section 12  has_gis_data  integer to boolean (default false)  on reservoir_entity, channel_entity
--
-- All workspace repos were grepped (re-checked 2026-06-05) for code reading
-- these columns. Nothing in-repo breaks:
--   api    every is_active filter uses `= TRUE`, never `= 1`, so changing the
--          column to boolean makes no difference to it. None of the three
--          converted tables (hydroclimate, scenario_author, theme) are
--          filtered on is_active.
--          has_gis_data is only read (never compared) from du_agriculture_entity,
--          which Section 12 does not touch (it converts reservoir_entity and
--          channel_entity).
--   etl    no INSERT/UPDATE against these seed-loaded reference tables.
--   front  is_active and has_gis_data only come from already-boolean columns,
--          via truthy filters, not integer compares.
--
-- ============================================================================

-- ============================================================================
-- Section 10: fix is_active integer to boolean
--
-- Tables: hydroclimate, scenario_author, theme.
-- Pre-validation: refuse the ALTER if any row has is_active NOT IN (0, 1).
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 4;
    target_table text;
    current_type text;
    bad_count integer;
    fixed integer := 0;
    skipped integer := 0;
    failed_details text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (10, 'fix is_active integer to boolean', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOREACH target_table IN ARRAY ARRAY[
        -- Three tables defined is_active as integer (0/1) rather than the
        -- boolean used everywhere else in the schema (scenario, network,
        -- network_type, network_subtype, etc.). Convert to boolean and set
        -- the default to true to match the convention.
        'hydroclimate',
        'scenario_author',
        'theme'
    ]
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = target_table AND relkind = 'r') THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            SELECT t.typname INTO current_type
            FROM pg_attribute a
            JOIN pg_type t ON t.oid = a.atttypid
            JOIN pg_class c ON c.oid = a.attrelid
            WHERE c.relname = target_table
              AND a.attname = 'is_active'
              AND NOT a.attisdropped;

            IF current_type IS NULL THEN
                skipped := skipped + 1;
                failed_details := failed_details || target_table || ':is_active-column-missing | ';
                CONTINUE;
            END IF;
            IF current_type IN ('bool', 'boolean') THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            EXECUTE format('SELECT COUNT(*) FROM %I WHERE is_active IS NOT NULL AND is_active NOT IN (0, 1)', target_table)
            INTO bad_count;
            IF bad_count > 0 THEN
                failed_details := failed_details || format('%s:%s-out-of-range-rows | ', target_table, bad_count);
                CONTINUE;
            END IF;

            IF is_dry THEN
                fixed := fixed + 1;
                CONTINUE;
            END IF;

            EXECUTE format(
                'ALTER TABLE %I '
                'ALTER COLUMN is_active DROP DEFAULT, '
                'ALTER COLUMN is_active TYPE boolean USING (is_active <> 0), '
                'ALTER COLUMN is_active SET DEFAULT true',
                target_table
            );
            fixed := fixed + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details || target_table || ':' || SQLERRM || ' | ';
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (10, 'fix is_active integer to boolean',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_alter'
                 ELSE 'ok' END,
            fixed,
            format('fixed=%s skipped=%s%s', fixed, skipped,
                   CASE WHEN failed_details <> '' THEN ' | issues: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (10, 'fix is_active integer to boolean', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 11: fix hydroclimate numeric audit and version columns to integer
--
-- Columns: hydroclimate_version_id, created_by, updated_by.
-- Pre-validation: refuse the ALTER if any row has a fractional value.
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 4;
    col_name text;
    current_type text;
    bad_count integer;
    fixed integer := 0;
    skipped integer := 0;
    failed_details text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (11, 'fix hydroclimate numeric columns to integer', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'hydroclimate' AND relkind = 'r') THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (11, 'fix hydroclimate numeric columns to integer', 'skipped',
                'hydroclimate table missing',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOREACH col_name IN ARRAY ARRAY[
        -- hydroclimate is the only table where these three columns are
        -- numeric instead of integer. The other 92 tables with the same
        -- audit shape are integer. Fix here so Section 14 can add the
        -- developer.id FKs to created_by and updated_by.
        'hydroclimate_version_id',
        'created_by',
        'updated_by'
    ]
    LOOP
        BEGIN
            SELECT t.typname INTO current_type
            FROM pg_attribute a
            JOIN pg_type t ON t.oid = a.atttypid
            JOIN pg_class c ON c.oid = a.attrelid
            WHERE c.relname = 'hydroclimate'
              AND a.attname = col_name
              AND NOT a.attisdropped;

            IF current_type IS NULL THEN
                skipped := skipped + 1;
                failed_details := failed_details || col_name || ':column-missing | ';
                CONTINUE;
            END IF;
            IF current_type IN ('int4', 'integer') THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            EXECUTE format(
                'SELECT COUNT(*) FROM hydroclimate WHERE %I IS NOT NULL AND %I <> floor(%I)',
                col_name, col_name, col_name
            ) INTO bad_count;
            IF bad_count > 0 THEN
                failed_details := failed_details || format('%s:%s-fractional-rows | ', col_name, bad_count);
                CONTINUE;
            END IF;

            IF is_dry THEN
                fixed := fixed + 1;
                CONTINUE;
            END IF;

            EXECUTE format(
                'ALTER TABLE hydroclimate ALTER COLUMN %I TYPE integer USING (%I::integer)',
                col_name, col_name
            );
            fixed := fixed + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details || col_name || ':' || SQLERRM || ' | ';
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (11, 'fix hydroclimate numeric columns to integer',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_alter'
                 ELSE 'ok' END,
            fixed,
            format('fixed=%s skipped=%s%s', fixed, skipped,
                   CASE WHEN failed_details <> '' THEN ' | issues: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (11, 'fix hydroclimate numeric columns to integer', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 12: fix has_gis_data integer to boolean (2 tables)
--
-- Tables: reservoir_entity, channel_entity.
-- Same legacy integer 0/1 drift as the is_active columns in Section 10.
-- Convert to boolean with DEFAULT false: a freshly inserted entity has no GIS
-- sibling yet (the geometry row is added separately), so false is the honest
-- starting point.
-- Pre-validation: refuse the ALTER if any row has has_gis_data NOT IN (0, 1).
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 4;
    target_table text;
    current_type text;
    bad_count integer;
    fixed integer := 0;
    skipped integer := 0;
    failed_details text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (12, 'fix has_gis_data integer to boolean', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOREACH target_table IN ARRAY ARRAY[
        'reservoir_entity',
        'channel_entity'
    ]
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = target_table AND relkind = 'r') THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            SELECT t.typname INTO current_type
            FROM pg_attribute a
            JOIN pg_type t ON t.oid = a.atttypid
            JOIN pg_class c ON c.oid = a.attrelid
            WHERE c.relname = target_table
              AND a.attname = 'has_gis_data'
              AND NOT a.attisdropped;

            IF current_type IS NULL THEN
                skipped := skipped + 1;
                failed_details := failed_details || target_table || ':has_gis_data-column-missing | ';
                CONTINUE;
            END IF;
            IF current_type IN ('bool', 'boolean') THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            EXECUTE format('SELECT COUNT(*) FROM %I WHERE has_gis_data IS NOT NULL AND has_gis_data NOT IN (0, 1)', target_table)
            INTO bad_count;
            IF bad_count > 0 THEN
                failed_details := failed_details || format('%s:%s-out-of-range-rows | ', target_table, bad_count);
                CONTINUE;
            END IF;

            IF is_dry THEN
                fixed := fixed + 1;
                CONTINUE;
            END IF;

            EXECUTE format(
                'ALTER TABLE %I '
                'ALTER COLUMN has_gis_data DROP DEFAULT, '
                'ALTER COLUMN has_gis_data TYPE boolean USING (has_gis_data <> 0), '
                'ALTER COLUMN has_gis_data SET DEFAULT false',
                target_table
            );
            fixed := fixed + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details || target_table || ':' || SQLERRM || ' | ';
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (12, 'fix has_gis_data integer to boolean',
            CASE WHEN failed_details <> '' THEN 'partial'
                 WHEN is_dry THEN 'would_alter'
                 ELSE 'ok' END,
            fixed,
            format('fixed=%s skipped=%s%s', fixed, skipped,
                   CASE WHEN failed_details <> '' THEN ' | issues: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (12, 'fix has_gis_data integer to boolean', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 13: backfill the 1 NULL row in hydroclimate.created_by
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 2;
    null_count integer;
    fallback_id integer;
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (13, 'backfill hydroclimate.created_by NULLs', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'hydroclimate' AND relkind = 'r') THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (13, 'backfill hydroclimate.created_by NULLs', 'skipped',
                'hydroclimate table missing',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    SELECT COUNT(*) INTO null_count FROM hydroclimate WHERE created_by IS NULL;
    IF null_count = 0 THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (13, 'backfill hydroclimate.created_by NULLs', 'skipped',
                'no NULL rows',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF is_dry THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
        VALUES (13, 'backfill hydroclimate.created_by NULLs', 'would_update', null_count,
                format('would update %s row(s) using coeqwal_current_operator()', null_count),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    BEGIN
        SELECT coeqwal_current_operator() INTO fallback_id;
    EXCEPTION WHEN OTHERS THEN
        fallback_id := 1;
    END;

    -- hydroclimate.created_by has one NULL row (the rest are integer audit
    -- ids). Backfill with the current developer.id (from
    -- coeqwal_current_operator()) so Section 14 can add the FK without a
    -- NULL-row exemption.
    UPDATE hydroclimate SET created_by = fallback_id WHERE created_by IS NULL;
    GET DIAGNOSTICS null_count = ROW_COUNT;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (13, 'backfill hydroclimate.created_by NULLs', 'ok', null_count,
            format('updated %s row(s) to developer id %s', null_count, fallback_id),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (13, 'backfill hydroclimate.created_by NULLs', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 14: add hydroclimate created_by / updated_by FKs
--
-- Depends on Section 11 (numeric to integer type fix) and Section 13
-- (NULL backfill). If Section 11 did not run or failed, the column is still
-- numeric and this section skips. Same constraint naming and rules as
-- Section 9.
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 4;
    col_name text;
    current_type text;
    fk_name text;
    orphan_count integer;
    added integer := 0;
    skipped integer := 0;
    would_fail integer := 0;
    pending integer := 0;
    pending_reasons text := '';
    failed_details text := '';
    fail_reasons text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (14, 'add hydroclimate developer FKs', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'hydroclimate' AND relkind = 'r') THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (14, 'add hydroclimate developer FKs', 'skipped',
                'hydroclimate table missing',
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOREACH col_name IN ARRAY ARRAY['created_by', 'updated_by']
    LOOP
        fk_name := 'fk_hydroclimate_' || col_name;
        BEGIN
            SELECT t.typname INTO current_type
            FROM pg_attribute a
            JOIN pg_type t ON t.oid = a.atttypid
            JOIN pg_class c ON c.oid = a.attrelid
            WHERE c.relname = 'hydroclimate'
              AND a.attname = col_name
              AND NOT a.attisdropped;

            IF current_type IS NULL THEN
                skipped := skipped + 1;
                fail_reasons := fail_reasons || col_name || ':column-missing | ';
                CONTINUE;
            END IF;
            IF current_type NOT IN ('int2', 'int4', 'int8') THEN
                IF is_dry THEN
                    -- Section 11 converts this column to integer earlier in the
                    -- same apply run. A dry-run changes nothing, so the column is
                    -- still numeric at this point. That is expected.
                    pending := pending + 1;
                    pending_reasons := pending_reasons ||
                        format('%s:pending-section-11-conversion | ', col_name);
                ELSE
                    -- Apply run: a still-numeric column means Section 11 did not
                    -- convert it (it skipped or failed). That is a real blocker
                    -- for the FK add, so flag it for attention.
                    would_fail := would_fail + 1;
                    fail_reasons := fail_reasons ||
                        format('%s:still-%s-section-11-did-not-convert | ', col_name, current_type);
                END IF;
                CONTINUE;
            END IF;
            IF EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE t.relname = 'hydroclimate'
                  AND c.conname = fk_name
            ) THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;
            IF EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                WHERE t.relname = 'hydroclimate'
                  AND a.attname = col_name
                  AND c.contype = 'f'
            ) THEN
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            EXECUTE format(
                'SELECT COUNT(*) FROM hydroclimate t WHERE t.%I IS NOT NULL '
                'AND NOT EXISTS (SELECT 1 FROM developer d WHERE d.id = t.%I)',
                col_name, col_name
            ) INTO orphan_count;
            IF orphan_count > 0 THEN
                would_fail := would_fail + 1;
                fail_reasons := fail_reasons ||
                    format('%s:%s-orphan-values | ', col_name, orphan_count);
                CONTINUE;
            END IF;

            IF is_dry THEN
                added := added + 1;
                CONTINUE;
            END IF;

            EXECUTE format(
                'ALTER TABLE hydroclimate ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES developer (id) ON DELETE NO ACTION ON UPDATE NO ACTION',
                fk_name, col_name
            );
            added := added + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details || col_name || ':' || SQLERRM || ' | ';
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (14, 'add hydroclimate developer FKs',
            CASE
                WHEN failed_details <> '' THEN 'partial'
                WHEN would_fail > 0 AND is_dry THEN 'would_partial'
                WHEN would_fail > 0 THEN 'partial'
                WHEN is_dry THEN 'would_add'
                ELSE 'ok'
            END,
            added,
            format('added=%s skipped=%s pending=%s would_fail=%s%s%s%s',
                   added, skipped, pending, would_fail,
                   CASE WHEN pending_reasons <> '' THEN ' | pending: ' || pending_reasons ELSE '' END,
                   CASE WHEN fail_reasons <> '' THEN ' | infeasible: ' || fail_reasons ELSE '' END,
                   CASE WHEN failed_details <> '' THEN ' | failures: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (14, 'add hydroclimate developer FKs', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 15: drop magic-number lookup and version-ID defaults
--
-- A lot of these were set during the original bootstrap so inserts would work
-- without specifying the IDs. Others seemed like good ideas at the time.
-- ============================================================================
DO $$
DECLARE
    t0 timestamptz := clock_timestamp();
    is_dry boolean := COALESCE(NULLIF(current_setting('audit.dry_run', true), ''), 'true') <> 'false';
    _tier constant integer := 3;
    rec record;
    current_default text;
    dropped integer := 0;
    skipped integer := 0;
    drifted integer := 0;
    failed_details text := '';
BEGIN
    IF NOT pg_temp.tier_selected(_tier) THEN
        INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
        VALUES (15, 'drop magic-number lookup and version-ID defaults', 'skipped',
                format('tier %s not selected', _tier),
                EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
        RETURN;
    END IF;

    FOR rec IN
        SELECT target_table, column_name, expected_default
        FROM (VALUES
            -- network entity-type lookup chain: source_id default 4
            ('network_arc',          'source_id',           '4'),
            ('network_gis',          'source_id',           '4'),
            ('network_node',         'source_id',           '4'),
            ('network_subtype',      'source_id',           '4'),
            ('network_type',         'source_id',           '4'),
            -- network entity-type lookup chain: model_source_id default 1
            ('network_arc',          'model_source_id',     '1'),
            ('network_node',         'model_source_id',     '1'),
            ('network_subtype',      'model_source_id',     '1'),
            ('network_type',         'model_source_id',     '1'),
            -- network versioning: network_version_id default 12
            ('network',              'network_version_id',  '12'),
            ('network_arc',          'network_version_id',  '12'),
            ('network_gis',          'network_version_id',  '12'),
            ('network_node',         'network_version_id',  '12'),
            -- tier versioning: tier_version_id default 8
            ('tier_definition',      'tier_version_id',     '8'),
            ('tier_location_result', 'tier_version_id',     '8'),
            ('tier_result',          'tier_version_id',     '8'),
            -- channel and reservoir entity-type and version defaults
            ('channel_entity',       'entity_type_id',      '1'),
            ('reservoir_entity',     'entity_type_id',      '1'),
            ('channel_entity',       'entity_version_id',   '1'),
            ('reservoir_entity',     'entity_version_id',   '1'),
            -- scenario, theme, channel_variable: per-table version defaults
            ('scenario',             'scenario_version_id', '1'),
            ('theme',                'theme_version_id',    '1'),
            ('channel_variable',     'variable_version_id', '1')
        ) AS t(target_table, column_name, expected_default)
    LOOP
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = rec.target_table AND relkind = 'r') THEN
                skipped := skipped + 1;
                failed_details := failed_details || format('%s.%s:table-missing | ', rec.target_table, rec.column_name);
                CONTINUE;
            END IF;

            -- Read the current default expression for the column. Returns
            -- NULL when the column exists but has no default. Returns no
            -- row when the column itself is missing.
            SELECT pg_get_expr(d.adbin, d.adrelid)
            INTO current_default
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE c.relname = rec.target_table
              AND c.relkind = 'r'
              AND a.attname = rec.column_name
              AND NOT a.attisdropped;

            IF NOT FOUND THEN
                skipped := skipped + 1;
                failed_details := failed_details || format('%s.%s:column-missing | ', rec.target_table, rec.column_name);
                CONTINUE;
            END IF;

            IF current_default IS NULL THEN
                -- Default already dropped. Idempotent skip.
                skipped := skipped + 1;
                CONTINUE;
            END IF;

            IF current_default <> rec.expected_default THEN
                -- Default has drifted from the bootstrap value. Leave it
                -- alone and surface a warning so the developer can decide
                drifted := drifted + 1;
                failed_details := failed_details ||
                    format('%s.%s:drifted(expected %s, found %s) | ',
                           rec.target_table, rec.column_name,
                           rec.expected_default, current_default);
                CONTINUE;
            END IF;

            IF is_dry THEN
                dropped := dropped + 1;
                CONTINUE;
            END IF;

            EXECUTE format('ALTER TABLE %I ALTER COLUMN %I DROP DEFAULT',
                           rec.target_table, rec.column_name);
            dropped := dropped + 1;
        EXCEPTION WHEN OTHERS THEN
            failed_details := failed_details ||
                format('%s.%s:%s | ', rec.target_table, rec.column_name, SQLERRM);
        END;
    END LOOP;

    INSERT INTO pg_temp.migration_log (step_id, step_name, status, rows_affected, details, duration_ms)
    VALUES (15, 'drop magic-number lookup and version-ID defaults',
            CASE WHEN drifted > 0 OR failed_details <> '' THEN 'warning'
                 WHEN is_dry THEN 'would_drop'
                 ELSE 'ok' END,
            dropped,
            format('dropped=%s skipped=%s drifted=%s%s',
                   dropped, skipped, drifted,
                   CASE WHEN failed_details <> '' THEN ' | issues: ' || failed_details ELSE '' END),
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
EXCEPTION WHEN OTHERS THEN
    INSERT INTO pg_temp.migration_log (step_id, step_name, status, details, duration_ms)
    VALUES (15, 'drop magic-number lookup and version-ID defaults', 'failed', SQLERRM,
            EXTRACT(MILLISECOND FROM clock_timestamp() - t0));
END $$;

-- ============================================================================
-- Section 99: report
-- ============================================================================
\echo
\echo '==================== AUDIT CLEANUP REPORT ===================='
\echo

SELECT step_id,
       step_name,
       status,
       rows_affected,
       round(duration_ms, 1) AS ms,
       details
FROM pg_temp.migration_log
ORDER BY step_id;

\echo
\echo '---------------- aggregate summary ----------------'

SELECT
    COUNT(*) FILTER (WHERE status = 'ok')                            AS ok,
    COUNT(*) FILTER (WHERE status LIKE 'would\_%' ESCAPE '\'
                       AND status <> 'would_partial')                AS would_do,
    COUNT(*) FILTER (WHERE status = 'would_partial')                 AS would_partial,
    COUNT(*) FILTER (WHERE status = 'skipped')                       AS skipped,
    COUNT(*) FILTER (WHERE status = 'partial')                       AS partial,
    COUNT(*) FILTER (WHERE status = 'failed')                        AS failed,
    COUNT(*) FILTER (WHERE status = 'warning')                       AS warnings,
    COUNT(*)                                                         AS total_sections
FROM pg_temp.migration_log;

\echo
\echo '---------------- attention required (if any) ----------------'

SELECT step_id, step_name, status, details
FROM pg_temp.migration_log
WHERE status IN ('failed', 'warning', 'partial', 'would_partial')
ORDER BY step_id;

-- One-line verdict. `run_clean` is true when no row in migration_log has
-- a status that needs attention (failed, partial, warning, would_partial).
SELECT
    CASE WHEN COUNT(*) FILTER (
                WHERE status IN ('failed', 'partial', 'warning', 'would_partial')
             ) = 0
         THEN 'true' ELSE 'false' END AS run_clean
FROM pg_temp.migration_log \gset

\echo
\if :run_clean
\if :dry_run
\echo 'RESULT: READY TO APPLY  (no warnings, no infeasible items)'
\else
\echo 'RESULT: SUCCESS  (all sections finished cleanly)'
\endif
\else
\if :dry_run
\echo 'RESULT: NEEDS ATTENTION before apply  (see attention required table above)'
\else
\echo 'RESULT: NEEDS ATTENTION  (see attention required table above)'
\endif
\endif

\echo
\if :dry_run
\echo 'Done. Dry-run complete. To apply for real, re-run without the flag:'
\echo '    psql $SUPERUSER_URL \\'
\echo '      -f database/scripts/sql/audit_cleanup.sql \\'
\echo '      | tee audits/audit_cleanup/audit_apply_$(date +%Y%m%d_%H%M%S).log'
\else
\echo 'Done. Apply complete.'
\echo 'Ask Jill to hand-edit the ERD to get you to a good state. From there you'
\echo 'can make minor edits yourself. Running the monthly audit'
\echo '(database/audit/run_monthly_audit.py) regenerates the schema snapshot.'
\endif
\echo
