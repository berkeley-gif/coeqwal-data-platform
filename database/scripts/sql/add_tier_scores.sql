-- add_tier_scores.sql
--
-- Add weighted_score, normalized_score fields to tier_result that stores the 
-- scores used in website plots.
--
-- Created by Eric Lehmer 7/9/2026
BEGIN;

ALTER TABLE tier_result ADD COLUMN IF NOT EXISTS weighted_score NUMERIC(5,3);
ALTER TABLE tier_result ADD COLUMN IF NOT EXISTS normalized_score NUMERIC(5,3);

COMMIT;