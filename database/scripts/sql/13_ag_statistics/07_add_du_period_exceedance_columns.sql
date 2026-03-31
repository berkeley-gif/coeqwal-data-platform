-- Add exceedance percentile columns to ag_du_period_summary for
-- sw_delivery, gw_pumping, and shortage.

ALTER TABLE ag_du_period_summary
    ADD COLUMN IF NOT EXISTS sw_delivery_exc_p5  NUMERIC,
    ADD COLUMN IF NOT EXISTS sw_delivery_exc_p10 NUMERIC,
    ADD COLUMN IF NOT EXISTS sw_delivery_exc_p25 NUMERIC,
    ADD COLUMN IF NOT EXISTS sw_delivery_exc_p50 NUMERIC,
    ADD COLUMN IF NOT EXISTS sw_delivery_exc_p75 NUMERIC,
    ADD COLUMN IF NOT EXISTS sw_delivery_exc_p90 NUMERIC,
    ADD COLUMN IF NOT EXISTS sw_delivery_exc_p95 NUMERIC,
    ADD COLUMN IF NOT EXISTS gw_pumping_exc_p5   NUMERIC,
    ADD COLUMN IF NOT EXISTS gw_pumping_exc_p10  NUMERIC,
    ADD COLUMN IF NOT EXISTS gw_pumping_exc_p25  NUMERIC,
    ADD COLUMN IF NOT EXISTS gw_pumping_exc_p50  NUMERIC,
    ADD COLUMN IF NOT EXISTS gw_pumping_exc_p75  NUMERIC,
    ADD COLUMN IF NOT EXISTS gw_pumping_exc_p90  NUMERIC,
    ADD COLUMN IF NOT EXISTS gw_pumping_exc_p95  NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p5     NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p10    NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p25    NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p50    NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p75    NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p90    NUMERIC,
    ADD COLUMN IF NOT EXISTS shortage_exc_p95    NUMERIC;
