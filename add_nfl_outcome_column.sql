-- =============================================================================
-- ADD NFL OUTCOME SCORE TO DYNASTY_PROSPECTS
-- Captures actual NFL career performance for historical prospects.
-- Run this in Supabase SQL Editor.
-- =============================================================================

-- NFL outcome score (0-100) based on actual fantasy production, games started, etc.
-- Only populated for drafted prospects with NFL career data.
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS nfl_outcome_score DECIMAL(5,2);

-- NFL seasons played (for weighting career vs prospect grade)
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS nfl_seasons_played INTEGER DEFAULT 0;

-- Best single-season fantasy PPG (the "peak" signal)
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS nfl_peak_ppg DECIMAL(6,2);

-- Index for efficient queries
CREATE INDEX IF NOT EXISTS idx_prospects_nfl_outcome ON dynasty_prospects(nfl_outcome_score DESC NULLS LAST);

-- =============================================================================
-- VERIFY
-- =============================================================================
-- SELECT name, position, draft_year, overall_grade, nfl_outcome_score, nfl_seasons_played
-- FROM dynasty_prospects
-- WHERE nfl_outcome_score IS NOT NULL
-- ORDER BY nfl_outcome_score DESC
-- LIMIT 20;
