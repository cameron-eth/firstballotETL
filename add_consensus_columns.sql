-- =============================================================================
-- Add external consensus input columns to dynasty_prospects
-- Run this in Supabase SQL Editor for existing environments.
-- =============================================================================

ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_rank INTEGER;
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_position_rank INTEGER;
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_avg_rank DECIMAL(6,2);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_rank_stddev DECIMAL(6,2);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_best_rank INTEGER;
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_worst_rank INTEGER;
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_source VARCHAR(100);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS consensus_updated_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_prospects_consensus_rank ON dynasty_prospects(consensus_rank);
