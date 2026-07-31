-- add_tier_scores.sql
--
-- Rename weighted_score to average_score in tier_result 
--
-- Created by Eric Lehmer 7/31/2026
BEGIN;

ALTER TABLE tier_result RENAME COLUMN weighted_score TO average_score;

COMMIT;