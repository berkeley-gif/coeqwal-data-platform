-- MIGRATION 54: Create sensitivity analysis tables
--
-- Two tables for cross-scenario sensitivity analysis:
--   sensitivity_climate      — how each metric changes across hydroclimate levels
--                              (historical vs cc50 vs cc95) within a sibling group
--   sensitivity_operational  — how each metric varies across different operational
--                              configurations within a single hydroclimate level
--
-- Rows span water_month 1–12 (monthly) and 0 (annual/period-of-record).

BEGIN;

-- ──────────────────────────────────────────────────────────────────────
-- 1. Climate sensitivity
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sensitivity_climate (
    id                  SERIAL PRIMARY KEY,
    sibling_group       VARCHAR(20)  NOT NULL,
    module              VARCHAR(30)  NOT NULL,
    entity_id           VARCHAR(120) NOT NULL,
    metric_name         VARCHAR(60)  NOT NULL,
    water_month         SMALLINT     NOT NULL CHECK (water_month BETWEEN 0 AND 12),
    unit                VARCHAR(20),

    hist_value          DOUBLE PRECISION,
    cc50_value          DOUBLE PRECISION,
    cc95_value          DOUBLE PRECISION,
    cc50_abs_change     DOUBLE PRECISION,
    cc95_abs_change     DOUBLE PRECISION,
    cc50_pct_change     DOUBLE PRECISION,
    cc95_pct_change     DOUBLE PRECISION,

    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (sibling_group, module, entity_id, metric_name, water_month)
);

CREATE INDEX IF NOT EXISTS idx_sensitivity_climate_module_month
    ON sensitivity_climate (module, water_month);
CREATE INDEX IF NOT EXISTS idx_sensitivity_climate_sibling
    ON sensitivity_climate (sibling_group);

-- ──────────────────────────────────────────────────────────────────────
-- 2. Operational sensitivity
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sensitivity_operational (
    id                  SERIAL PRIMARY KEY,
    hydroclimate_id     INTEGER      NOT NULL REFERENCES hydroclimate(id),
    module              VARCHAR(30)  NOT NULL,
    entity_id           VARCHAR(120) NOT NULL,
    metric_name         VARCHAR(60)  NOT NULL,
    water_month         SMALLINT     NOT NULL CHECK (water_month BETWEEN 0 AND 12),
    unit                VARCHAR(20),

    scenario_count      INTEGER      NOT NULL,
    min_value           DOUBLE PRECISION,
    max_value           DOUBLE PRECISION,
    mean_value          DOUBLE PRECISION,
    std_value           DOUBLE PRECISION,
    range_value         DOUBLE PRECISION,
    pct_range           DOUBLE PRECISION,

    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (hydroclimate_id, module, entity_id, metric_name, water_month)
);

CREATE INDEX IF NOT EXISTS idx_sensitivity_operational_module_month
    ON sensitivity_operational (module, water_month);
CREATE INDEX IF NOT EXISTS idx_sensitivity_operational_hydro
    ON sensitivity_operational (hydroclimate_id);

-- ──────────────────────────────────────────────────────────────────────
-- Verification
-- ──────────────────────────────────────────────────────────────────────
SELECT 'sensitivity_climate'    AS "table", COUNT(*) AS rows FROM sensitivity_climate
UNION ALL
SELECT 'sensitivity_operational', COUNT(*) FROM sensitivity_operational;

COMMIT;
