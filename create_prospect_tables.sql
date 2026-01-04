-- =============================================================================
-- PROSPECT DATA FOUNDATION
-- Run this in Supabase SQL Editor
-- =============================================================================

-- =============================================================================
-- 1. HISTORICAL PROSPECTS TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS historical_prospects (
    id SERIAL PRIMARY KEY,
    draft_year INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(10) NOT NULL,
    college VARCHAR(255),
    nfl_team VARCHAR(100),
    draft_round INTEGER,
    draft_pick INTEGER,
    height DECIMAL(5,2),
    weight DECIMAL(5,2),
    hs_rank INTEGER,
    hs_stars INTEGER,
    hs_rating DECIMAL(6,4),
    hs_school VARCHAR(255),
    hs_city VARCHAR(100),
    hs_state VARCHAR(10),
    pre_draft_rank INTEGER,
    pre_draft_position_rank INTEGER,
    pre_draft_grade DECIMAL(5,2),
    draft_round_percentile DECIMAL(6,2),
    draft_pick_percentile DECIMAL(6,2),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(name, draft_year, position)
);

CREATE INDEX IF NOT EXISTS idx_hist_draft_year ON historical_prospects(draft_year);
CREATE INDEX IF NOT EXISTS idx_hist_position ON historical_prospects(position);
CREATE INDEX IF NOT EXISTS idx_hist_hs_stars ON historical_prospects(hs_stars);
CREATE INDEX IF NOT EXISTS idx_hist_draft_round ON historical_prospects(draft_round);

-- =============================================================================
-- 2. DYNASTY PROSPECTS TABLE
-- =============================================================================

-- Drop and recreate to ensure clean state
DROP TABLE IF EXISTS dynasty_prospects;

CREATE TABLE dynasty_prospects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(10) NOT NULL,
    school VARCHAR(255),
    espn_id BIGINT,
    cfbd_id BIGINT,
    sleeper_id VARCHAR(50),
    draft_year INTEGER NOT NULL DEFAULT 2026,
    draft_round_projection INTEGER,
    height DECIMAL(5,2),
    weight DECIMAL(5,2),
    class VARCHAR(20),
    hs_rank INTEGER,
    hs_stars INTEGER,
    hs_rating DECIMAL(6,4),
    hs_school VARCHAR(255),
    hs_state VARCHAR(10),
    rank INTEGER,
    tier VARCHAR(20),
    tier_numeric INTEGER,
    valuation DECIMAL(10,2),
    position_multiplier DECIMAL(5,2),
    overall_grade DECIMAL(5,2),
    hs_recruiting_score DECIMAL(5,2),
    college_production_score DECIMAL(5,2),
    draft_projection_score DECIMAL(5,2),
    physical_measurables_score DECIMAL(5,2),
    expert_consensus_score DECIMAL(5,2),
    grade_tier VARCHAR(20),
    historical_percentile DECIMAL(5,2),
    nfl_comparisons TEXT,
    college_stats JSONB,
    college_games INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(name, draft_year, position)
);

CREATE INDEX idx_prospects_rank ON dynasty_prospects(rank);
CREATE INDEX idx_prospects_position ON dynasty_prospects(position);
CREATE INDEX idx_prospects_draft_year ON dynasty_prospects(draft_year);
CREATE INDEX idx_prospects_tier ON dynasty_prospects(tier_numeric);
CREATE INDEX idx_prospects_grade ON dynasty_prospects(overall_grade DESC);

-- =============================================================================
-- 3. ARCHIVE SCHEMA
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS archive;
