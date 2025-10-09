-- ============================================================================
-- NFL NGS Data Tables - Schema for Supabase/PostgreSQL
-- ============================================================================

-- ============================================================================
-- Table 1: NGS Passing Stats (Quarterbacks)
-- ============================================================================
CREATE TABLE IF NOT EXISTS nfl_ngs_passing_stats (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Season & Game Info
    season INTEGER NOT NULL,
    season_type VARCHAR(10) NOT NULL, -- 'REG', 'POST'
    week INTEGER NOT NULL,
    
    -- Player Info
    player_gsis_id VARCHAR(20) NOT NULL,
    player_display_name VARCHAR(100),
    player_first_name VARCHAR(50),
    player_last_name VARCHAR(50),
    player_position VARCHAR(5),
    player_jersey_number INTEGER,
    player_short_name VARCHAR(50),
    team_abbr VARCHAR(5),
    
    -- Basic Passing Stats
    attempts INTEGER DEFAULT 0,
    completions INTEGER DEFAULT 0,
    pass_yards DECIMAL(10, 2) DEFAULT 0,
    pass_touchdowns INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0,
    completion_percentage DECIMAL(5, 2) DEFAULT 0,
    passer_rating DECIMAL(6, 2) DEFAULT 0,
    
    -- NGS Advanced Metrics
    avg_time_to_throw DECIMAL(4, 2) DEFAULT 0,
    avg_completed_air_yards DECIMAL(6, 2) DEFAULT 0,
    avg_intended_air_yards DECIMAL(6, 2) DEFAULT 0,
    avg_air_yards_differential DECIMAL(6, 2) DEFAULT 0,
    aggressiveness DECIMAL(5, 2) DEFAULT 0,
    max_completed_air_distance DECIMAL(6, 2) DEFAULT 0,
    avg_air_yards_to_sticks DECIMAL(6, 2) DEFAULT 0,
    expected_completion_percentage DECIMAL(5, 2) DEFAULT 0,
    completion_percentage_above_expectation DECIMAL(6, 2) DEFAULT 0,
    avg_air_distance DECIMAL(6, 2) DEFAULT 0,
    max_air_distance DECIMAL(6, 2) DEFAULT 0,
    
    -- Fantasy Scoring
    fantasy_points DECIMAL(10, 2) DEFAULT 0,
    fantasy_ppg DECIMAL(10, 2) DEFAULT 0,
    fantasy_points_per_attempt DECIMAL(6, 3) DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(player_gsis_id, season, season_type, week)
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_passing_player ON nfl_ngs_passing_stats(player_gsis_id);
CREATE INDEX IF NOT EXISTS idx_passing_season ON nfl_ngs_passing_stats(season);
CREATE INDEX IF NOT EXISTS idx_passing_week ON nfl_ngs_passing_stats(season, week);
CREATE INDEX IF NOT EXISTS idx_passing_team ON nfl_ngs_passing_stats(team_abbr);
CREATE INDEX IF NOT EXISTS idx_passing_position ON nfl_ngs_passing_stats(player_position);
CREATE INDEX IF NOT EXISTS idx_passing_fantasy ON nfl_ngs_passing_stats(fantasy_points DESC);


-- ============================================================================
-- Table 2: NGS Rushing Stats (All Positions)
-- ============================================================================
CREATE TABLE IF NOT EXISTS nfl_ngs_rushing_stats (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Season & Game Info
    season INTEGER NOT NULL,
    season_type VARCHAR(10) NOT NULL, -- 'REG', 'POST'
    week INTEGER NOT NULL,
    
    -- Player Info
    player_gsis_id VARCHAR(20) NOT NULL,
    player_display_name VARCHAR(100),
    player_first_name VARCHAR(50),
    player_last_name VARCHAR(50),
    player_position VARCHAR(5), -- 'QB', 'RB', 'WR', 'TE'
    player_jersey_number INTEGER,
    player_short_name VARCHAR(50),
    team_abbr VARCHAR(5),
    
    -- Basic Rushing Stats
    rush_attempts INTEGER DEFAULT 0,
    rush_yards DECIMAL(10, 2) DEFAULT 0,
    avg_rush_yards DECIMAL(5, 2) DEFAULT 0,
    rush_touchdowns INTEGER DEFAULT 0,
    
    -- NGS Advanced Metrics
    efficiency DECIMAL(6, 2) DEFAULT 0,
    percent_attempts_gte_eight_defenders DECIMAL(5, 2) DEFAULT 0,
    avg_time_to_los DECIMAL(4, 2) DEFAULT 0, -- Time to line of scrimmage
    expected_rush_yards DECIMAL(10, 2) DEFAULT 0,
    rush_yards_over_expected DECIMAL(10, 2) DEFAULT 0,
    rush_yards_over_expected_per_att DECIMAL(6, 2) DEFAULT 0,
    rush_pct_over_expected DECIMAL(6, 2) DEFAULT 0,
    
    -- Fantasy Scoring
    fantasy_points DECIMAL(10, 2) DEFAULT 0,
    fantasy_ppg DECIMAL(10, 2) DEFAULT 0,
    fantasy_points_per_rush DECIMAL(6, 3) DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(player_gsis_id, season, season_type, week)
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_rushing_player ON nfl_ngs_rushing_stats(player_gsis_id);
CREATE INDEX IF NOT EXISTS idx_rushing_season ON nfl_ngs_rushing_stats(season);
CREATE INDEX IF NOT EXISTS idx_rushing_week ON nfl_ngs_rushing_stats(season, week);
CREATE INDEX IF NOT EXISTS idx_rushing_team ON nfl_ngs_rushing_stats(team_abbr);
CREATE INDEX IF NOT EXISTS idx_rushing_position ON nfl_ngs_rushing_stats(player_position);
CREATE INDEX IF NOT EXISTS idx_rushing_fantasy ON nfl_ngs_rushing_stats(fantasy_points DESC);


-- ============================================================================
-- Table 3: NGS Receiving Stats (WR, TE, RB)
-- ============================================================================
CREATE TABLE IF NOT EXISTS nfl_ngs_receiving_stats (
    -- Primary Key
    id BIGSERIAL PRIMARY KEY,
    
    -- Season & Game Info
    season INTEGER NOT NULL,
    season_type VARCHAR(10) NOT NULL, -- 'REG', 'POST'
    week INTEGER NOT NULL,
    
    -- Player Info
    player_gsis_id VARCHAR(20) NOT NULL,
    player_display_name VARCHAR(100),
    player_first_name VARCHAR(50),
    player_last_name VARCHAR(50),
    player_position VARCHAR(5), -- 'WR', 'TE', 'RB'
    player_jersey_number INTEGER,
    player_short_name VARCHAR(50),
    team_abbr VARCHAR(5),
    
    -- Basic Receiving Stats
    targets INTEGER DEFAULT 0,
    receptions INTEGER DEFAULT 0,
    yards DECIMAL(10, 2) DEFAULT 0, -- Receiving yards
    rec_touchdowns INTEGER DEFAULT 0,
    catch_percentage DECIMAL(5, 2) DEFAULT 0,
    
    -- NGS Advanced Metrics
    avg_cushion DECIMAL(5, 2) DEFAULT 0,
    avg_separation DECIMAL(5, 2) DEFAULT 0,
    avg_intended_air_yards DECIMAL(6, 2) DEFAULT 0,
    percent_share_of_intended_air_yards DECIMAL(6, 2) DEFAULT 0,
    avg_yac DECIMAL(6, 2) DEFAULT 0, -- Yards after catch
    avg_expected_yac DECIMAL(6, 2) DEFAULT 0,
    avg_yac_above_expectation DECIMAL(6, 2) DEFAULT 0,
    
    -- Fantasy Scoring (PPR)
    fantasy_points DECIMAL(10, 2) DEFAULT 0,
    fantasy_ppg DECIMAL(10, 2) DEFAULT 0,
    fantasy_points_per_reception DECIMAL(6, 3) DEFAULT 0,
    fantasy_points_per_target DECIMAL(6, 3) DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(player_gsis_id, season, season_type, week)
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_receiving_player ON nfl_ngs_receiving_stats(player_gsis_id);
CREATE INDEX IF NOT EXISTS idx_receiving_season ON nfl_ngs_receiving_stats(season);
CREATE INDEX IF NOT EXISTS idx_receiving_week ON nfl_ngs_receiving_stats(season, week);
CREATE INDEX IF NOT EXISTS idx_receiving_team ON nfl_ngs_receiving_stats(team_abbr);
CREATE INDEX IF NOT EXISTS idx_receiving_position ON nfl_ngs_receiving_stats(player_position);
CREATE INDEX IF NOT EXISTS idx_receiving_fantasy ON nfl_ngs_receiving_stats(fantasy_points DESC);


-- ============================================================================
-- Optional: Combined View for All Player Stats
-- ============================================================================
CREATE OR REPLACE VIEW nfl_player_combined_stats AS
SELECT 
    COALESCE(p.player_gsis_id, ru.player_gsis_id, re.player_gsis_id) as player_gsis_id,
    COALESCE(p.player_display_name, ru.player_display_name, re.player_display_name) as player_name,
    COALESCE(p.player_position, ru.player_position, re.player_position) as position,
    COALESCE(p.team_abbr, ru.team_abbr, re.team_abbr) as team,
    COALESCE(p.season, ru.season, re.season) as season,
    COALESCE(p.week, ru.week, re.week) as week,
    
    -- Passing
    COALESCE(p.pass_yards, 0) as pass_yards,
    COALESCE(p.pass_touchdowns, 0) as pass_touchdowns,
    COALESCE(p.fantasy_points, 0) as passing_fantasy_points,
    
    -- Rushing
    COALESCE(ru.rush_yards, 0) as rush_yards,
    COALESCE(ru.rush_touchdowns, 0) as rush_touchdowns,
    COALESCE(ru.fantasy_points, 0) as rushing_fantasy_points,
    
    -- Receiving
    COALESCE(re.receptions, 0) as receptions,
    COALESCE(re.yards, 0) as receiving_yards,
    COALESCE(re.rec_touchdowns, 0) as receiving_touchdowns,
    COALESCE(re.fantasy_points, 0) as receiving_fantasy_points,
    
    -- Total Fantasy
    COALESCE(p.fantasy_points, 0) + 
    COALESCE(ru.fantasy_points, 0) + 
    COALESCE(re.fantasy_points, 0) as total_fantasy_points
    
FROM nfl_ngs_passing_stats p
FULL OUTER JOIN nfl_ngs_rushing_stats ru 
    ON p.player_gsis_id = ru.player_gsis_id 
    AND p.season = ru.season 
    AND p.week = ru.week
FULL OUTER JOIN nfl_ngs_receiving_stats re 
    ON COALESCE(p.player_gsis_id, ru.player_gsis_id) = re.player_gsis_id 
    AND COALESCE(p.season, ru.season) = re.season 
    AND COALESCE(p.week, ru.week) = re.week;


-- ============================================================================
-- Helper Functions & Triggers
-- ============================================================================

-- Auto-update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to all tables
CREATE TRIGGER update_passing_updated_at BEFORE UPDATE ON nfl_ngs_passing_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rushing_updated_at BEFORE UPDATE ON nfl_ngs_rushing_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_receiving_updated_at BEFORE UPDATE ON nfl_ngs_receiving_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ============================================================================
-- Useful Query Examples
-- ============================================================================

-- Get top fantasy performers across all positions for 2025
-- SELECT * FROM nfl_player_combined_stats 
-- WHERE season = 2025 
-- ORDER BY total_fantasy_points DESC 
-- LIMIT 50;

-- Get QB stats with NGS metrics
-- SELECT player_display_name, team_abbr, pass_yards, pass_touchdowns, 
--        completion_percentage_above_expectation, fantasy_points
-- FROM nfl_ngs_passing_stats
-- WHERE season = 2025 AND player_position = 'QB'
-- ORDER BY fantasy_points DESC;

-- Get RB efficiency leaders
-- SELECT player_display_name, team_abbr, rush_attempts, rush_yards,
--        efficiency, rush_yards_over_expected_per_att, fantasy_points
-- FROM nfl_ngs_rushing_stats
-- WHERE season = 2025 AND player_position = 'RB' AND rush_attempts >= 50
-- ORDER BY efficiency DESC;

-- Get WR separation leaders
-- SELECT player_display_name, team_abbr, receptions, yards,
--        avg_separation, avg_cushion, fantasy_points
-- FROM nfl_ngs_receiving_stats
-- WHERE season = 2025 AND player_position = 'WR'
-- ORDER BY avg_separation DESC;

