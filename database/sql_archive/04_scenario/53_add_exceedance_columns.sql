-- Migration 53: Add shortage exceedance columns to refuge and env_flow period summary tables.
-- du_period_summary already has shortage_exc_p* columns (from a prior migration).

BEGIN;

ALTER TABLE refuge_du_period_summary
    ADD COLUMN IF NOT EXISTS shortage_exc_p5  NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p10 NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p25 NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p50 NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p75 NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p90 NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p95 NUMERIC;

ALTER TABLE env_flow_channel_period_summary
    ADD COLUMN IF NOT EXISTS flow_exc_p5_cfs  NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p10_cfs NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p25_cfs NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p50_cfs NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p75_cfs NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p90_cfs NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p95_cfs NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p5_taf  NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p10_taf NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p25_taf NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p50_taf NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p75_taf NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p90_taf NUMERIC,
    ADD COLUMN IF NOT EXISTS flow_exc_p95_taf NUMERIC;

COMMIT;
