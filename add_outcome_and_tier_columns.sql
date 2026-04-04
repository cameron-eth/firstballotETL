-- =============================================================================
-- ADD OUTCOME RANGES + TIER_NUMERIC COLUMNS
-- Run this in the Supabase SQL Editor before re-running the model pipelines.
-- =============================================================================

-- dynasty_prospects: prospect outcome ceiling/floor labels
-- Populated by prospect_grading.py and rerank_prospects.py
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS outcome_ceiling TEXT;
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS outcome_floor   TEXT;

-- dynasty_player_tiers: numeric tier for filtering/sorting (0=Generational … 8=Cut Candidate)
-- Populated by firstballotmodel exporter (tiering.py)
ALTER TABLE dynasty_player_tiers ADD COLUMN IF NOT EXISTS tier_numeric INTEGER;

-- =============================================================================
-- INDEXES for efficient tier/outcome filtering in the frontend
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_prospects_outcome_ceiling
    ON dynasty_prospects(outcome_ceiling);

CREATE INDEX IF NOT EXISTS idx_prospects_outcome_floor
    ON dynasty_prospects(outcome_floor);

CREATE INDEX IF NOT EXISTS idx_player_tiers_tier_numeric
    ON dynasty_player_tiers(tier_numeric);

-- =============================================================================
-- VERIFY (uncomment to check after running)
-- =============================================================================
-- SELECT name, position, draft_year, overall_grade, outcome_ceiling, outcome_floor
-- FROM dynasty_prospects
-- ORDER BY overall_grade DESC NULLS LAST
-- LIMIT 20;

-- SELECT player_name, position, total_score, tier, tier_numeric
-- FROM dynasty_player_tiers
-- ORDER BY total_score DESC NULLS LAST
-- LIMIT 20;
