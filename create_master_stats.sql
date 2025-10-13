-- ============================================================================
-- NFL Data Pipeline - Database Schema
-- ============================================================================
-- Creates:
--   1. nfl_player_stats table (weekly fantasy stats from nflreadpy)
--   2. master_player_stats view (aggregated fantasy + NGS metrics)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: nfl_player_stats
-- Source: nflreadpy (complete weekly stats with all stat types combined)
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS nfl_player_stats CASCADE;

CREATE TABLE nfl_player_stats (
    -- Player identifiers
    player_id TEXT NOT NULL,
    player_gsis_id TEXT,
    player_display_name TEXT NOT NULL,
    player_name TEXT,
    position TEXT,
    position_group TEXT,
    
    -- Team and game info
    team TEXT,
    opponent_team TEXT,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    season_type TEXT NOT NULL,
    
    -- Passing stats
    completions INTEGER,
    attempts INTEGER,
    passing_yards NUMERIC,
    passing_tds INTEGER,
    passing_interceptions INTEGER,
    passing_2pt_conversions INTEGER,
    
    -- Rushing stats
    carries INTEGER,
    rushing_yards NUMERIC,
    rushing_tds INTEGER,
    rushing_2pt_conversions INTEGER,
    
    -- Receiving stats
    receptions INTEGER,
    targets INTEGER,
    receiving_yards NUMERIC,
    receiving_tds INTEGER,
    receiving_2pt_conversions INTEGER,
    
    -- Advanced metrics
    target_share NUMERIC,
    air_yards_share NUMERIC,
    
    -- Fantasy points (pre-calculated)
    fantasy_points NUMERIC,
    fantasy_points_ppr NUMERIC NOT NULL,
    
    -- Metadata
    headshot_url TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (player_id, season, season_type, week)
);

CREATE INDEX idx_player_stats_gsis_id ON nfl_player_stats(player_gsis_id);
CREATE INDEX idx_player_stats_season ON nfl_player_stats(season);
CREATE INDEX idx_player_stats_player_season ON nfl_player_stats(player_id, season);
CREATE INDEX idx_player_stats_name ON nfl_player_stats(player_display_name);

COMMENT ON TABLE nfl_player_stats IS 
'Complete weekly player stats from nflreadpy. Includes all stat types (pass + rush + rec) with pre-calculated fantasy points.';

-- ----------------------------------------------------------------------------
-- Materialized View: master_player_stats
-- Combines fantasy PPG from nfl_player_stats with NGS advanced metrics
-- ----------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS master_player_stats CASCADE;

CREATE MATERIALIZED VIEW master_player_stats AS
WITH 
-- Step 1: Calculate fantasy PPG from nfl_player_stats (complete data)
fantasy_agg AS (
  SELECT 
    player_gsis_id,
    player_display_name,
    position,
    team as team_abbr,
    season,
    'REG' as season_type,  -- Only regular season for now
    COUNT(DISTINCT week) as games_played,
    SUM(fantasy_points_ppr) as total_fantasy_points,
    ROUND((SUM(fantasy_points_ppr) / NULLIF(COUNT(DISTINCT week), 0))::numeric, 2) as fantasy_ppg
  FROM nfl_player_stats
  WHERE season_type = 'REG'  -- Only regular season stats
  GROUP BY player_gsis_id, player_display_name, position, team, season
),

-- Step 2: Aggregate NGS passing metrics (QBs)
ngs_passing_agg AS (
  SELECT
    player_gsis_id,
    season,
    season_type,
    AVG(avg_time_to_throw) as avg_time_to_throw,
    AVG(avg_completed_air_yards) as avg_completed_air_yards,
    AVG(avg_intended_air_yards) as avg_intended_air_yards,
    AVG(avg_air_yards_differential) as avg_air_yards_differential,
    AVG(aggressiveness) as aggressiveness,
    AVG(max_completed_air_distance) as max_completed_air_distance,
    AVG(avg_air_yards_to_sticks) as avg_air_yards_to_sticks,
    AVG(passer_rating) as avg_passer_rating,
    AVG(completion_percentage) as avg_completion_percentage,
    AVG(expected_completion_percentage) as avg_expected_completion_percentage,
    AVG(completion_percentage_above_expectation) as avg_cpoe
  FROM nfl_ngs_passing_stats
  WHERE season_type = 'REG'
  GROUP BY player_gsis_id, season, season_type
),

-- Step 3: Aggregate NGS rushing metrics (RBs)
ngs_rushing_agg AS (
  SELECT
    player_gsis_id,
    season,
    season_type,
    AVG(efficiency) as avg_rush_efficiency,
    AVG(percent_attempts_gte_eight_defenders) as avg_rush_8plus_defenders_pct,
    AVG(avg_time_to_los) as avg_time_to_los,
    AVG(rush_yards_over_expected_per_att) as avg_rush_yards_over_expected,
    AVG(avg_rush_yards) as avg_rush_yards_per_attempt
  FROM nfl_ngs_rushing_stats
  WHERE season_type = 'REG'
  GROUP BY player_gsis_id, season, season_type
),

-- Step 4: Aggregate NGS receiving metrics (WRs, TEs, some RBs)
ngs_receiving_agg AS (
  SELECT
    player_gsis_id,
    season,
    season_type,
    AVG(avg_cushion) as avg_cushion,
    AVG(avg_separation) as avg_separation,
    AVG(avg_intended_air_yards) as avg_rec_intended_air_yards,
    AVG(percent_share_of_intended_air_yards) as avg_air_yards_share,
    AVG(avg_yac) as avg_yac,
    AVG(avg_expected_yac) as avg_expected_yac,
    AVG(avg_yac_above_expectation) as avg_yac_above_expectation,
    AVG(catch_percentage) as avg_catch_percentage
  FROM nfl_ngs_receiving_stats
  WHERE season_type = 'REG'
  GROUP BY player_gsis_id, season, season_type
)

-- Final: Join fantasy stats with NGS advanced metrics
SELECT 
  f.season,
  f.season_type,
  f.player_gsis_id,
  f.player_display_name,
  f.position,
  f.team_abbr,
  f.games_played,
  f.total_fantasy_points,
  f.fantasy_ppg,
  
  -- NGS Passing metrics (for QBs)
  p.avg_time_to_throw,
  p.avg_completed_air_yards,
  p.avg_intended_air_yards,
  p.avg_air_yards_differential,
  p.aggressiveness,
  p.max_completed_air_distance,
  p.avg_air_yards_to_sticks,
  p.avg_passer_rating,
  p.avg_completion_percentage,
  p.avg_expected_completion_percentage,
  p.avg_cpoe,
  
  -- NGS Rushing metrics (for RBs)
  r.avg_rush_efficiency,
  r.avg_rush_8plus_defenders_pct,
  r.avg_time_to_los,
  r.avg_rush_yards_over_expected,
  r.avg_rush_yards_per_attempt,
  
  -- NGS Receiving metrics (for WRs, TEs, RBs)
  rec.avg_cushion,
  rec.avg_separation,
  rec.avg_rec_intended_air_yards,
  rec.avg_air_yards_share,
  rec.avg_yac,
  rec.avg_expected_yac,
  rec.avg_yac_above_expectation,
  rec.avg_catch_percentage

FROM fantasy_agg f
LEFT JOIN ngs_passing_agg p 
  ON f.player_gsis_id = p.player_gsis_id 
  AND f.season = p.season 
  AND f.season_type = p.season_type
LEFT JOIN ngs_rushing_agg r 
  ON f.player_gsis_id = r.player_gsis_id 
  AND f.season = r.season 
  AND f.season_type = r.season_type
LEFT JOIN ngs_receiving_agg rec 
  ON f.player_gsis_id = rec.player_gsis_id 
  AND f.season = rec.season 
  AND f.season_type = rec.season_type;

-- Create indexes for fast lookups
CREATE INDEX idx_master_player_stats_gsis_id ON master_player_stats(player_gsis_id);
CREATE INDEX idx_master_player_stats_season ON master_player_stats(season);
CREATE INDEX idx_master_player_stats_ppg ON master_player_stats(fantasy_ppg DESC NULLS LAST);
CREATE INDEX idx_master_player_stats_position ON master_player_stats(position);

-- Create function to refresh the materialized view (for automated pipeline)
CREATE OR REPLACE FUNCTION refresh_master_stats()
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW master_player_stats;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Comment
COMMENT ON MATERIALIZED VIEW master_player_stats IS 
'Aggregated player stats combining fantasy PPG (from nflreadpy) with NGS advanced metrics.
Refreshed after ETL pipeline runs. Source: nfl_player_stats + NGS stats tables.';

