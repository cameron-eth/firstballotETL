-- =============================================================================
-- FirstBallotFF — Canonical Schema Migration
-- Run in Supabase SQL editor. All steps are idempotent (safe to re-run).
--
-- Creates:
--   players                   — identity hub (IDs + name + position only)
--   player_bio                — headshot, college, hometown, jersey, team_color
--   player_combine            — physical measurables + combine results
--   player_draft              — draft capital + pre-draft context
--   player_season_predictions — ML predictions + breakout/bust flags
--
-- Adds player_uuid FK (nullable) to all existing tables.
-- Backfills all new tables from existing scattered data.
-- =============================================================================


-- =============================================================================
-- STEP 1: players — lean identity hub
-- =============================================================================
CREATE TABLE IF NOT EXISTS players (
  id            UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  gsis_id       TEXT    UNIQUE,          -- NFL GSIS e.g. "00-0031280"
  espn_id       INTEGER,                 -- ESPN athlete ID (not globally unique: prospects repeat across draft years)
  ktc_player_id INTEGER UNIQUE,          -- KeepTradeCut integer ID
  sleeper_id    TEXT    UNIQUE,          -- Sleeper platform ID
  cfbd_id       INTEGER,                 -- College Football Data API
  name          TEXT    NOT NULL,
  first_name    TEXT,
  last_name     TEXT,
  position      TEXT,
  is_prospect   BOOLEAN NOT NULL DEFAULT false,
  draft_year    INTEGER,                 -- NULL for active NFL players
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- gsis_id is hard-unique (one NFL ID per player)
CREATE INDEX IF NOT EXISTS idx_players_gsis_id       ON players(gsis_id);
-- espn_id unique among NFL players; prospects can repeat across draft years
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_espn_id_nfl
  ON players(espn_id) WHERE is_prospect = false AND espn_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_espn_id_prospect
  ON players(espn_id, draft_year) WHERE is_prospect = true AND espn_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_players_ktc_player_id ON players(ktc_player_id);
CREATE INDEX IF NOT EXISTS idx_players_cfbd_id       ON players(cfbd_id);
CREATE INDEX IF NOT EXISTS idx_players_name          ON players(name);
CREATE INDEX IF NOT EXISTS idx_players_position      ON players(position);


-- =============================================================================
-- STEP 2: player_bio — one row per player (headshot, college, social bio)
-- =============================================================================
CREATE TABLE IF NOT EXISTS player_bio (
  player_uuid UUID PRIMARY KEY REFERENCES players(id) ON DELETE CASCADE,
  headshot_url TEXT,
  college      TEXT,
  hometown     TEXT,
  team_color   TEXT,
  jersey       TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- =============================================================================
-- STEP 3: player_combine — physical measurables + NFL combine results
-- One row per player (combine data is static / career-level)
-- =============================================================================
CREATE TABLE IF NOT EXISTS player_combine (
  player_uuid UUID PRIMARY KEY REFERENCES players(id) ON DELETE CASCADE,
  height      NUMERIC,       -- inches
  weight      NUMERIC,       -- lbs
  forty       NUMERIC,       -- 40-yard dash (seconds)
  vertical    NUMERIC,       -- vertical jump (inches)
  broad_jump  NUMERIC,       -- broad jump (inches)
  three_cone  NUMERIC,       -- 3-cone drill (seconds)
  shuttle     NUMERIC,       -- shuttle run (seconds)
  bench       INTEGER,       -- bench press reps
  hs_rank     INTEGER,       -- national HS recruiting rank
  hs_stars    INTEGER,       -- HS recruiting stars (1-5)
  hs_rating   NUMERIC,       -- HS recruiting rating (0-1)
  hs_school   TEXT,
  hs_city     TEXT,
  hs_state    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_player_combine_forty  ON player_combine(forty);
CREATE INDEX IF NOT EXISTS idx_player_combine_height ON player_combine(height);


-- =============================================================================
-- STEP 4: player_draft — draft capital + pre-draft context
-- One row per player (draft is a one-time event)
-- =============================================================================
CREATE TABLE IF NOT EXISTS player_draft (
  player_uuid       UUID PRIMARY KEY REFERENCES players(id) ON DELETE CASCADE,
  draft_year        INTEGER,
  draft_round       INTEGER,
  draft_pick        INTEGER,       -- pick within round
  draft_overall     INTEGER,       -- overall pick number
  draft_team        TEXT,
  pre_draft_rank    INTEGER,
  pre_draft_pos_rank INTEGER,
  pre_draft_grade   NUMERIC,
  draft_round_pct   NUMERIC,       -- percentile within round
  draft_pick_pct    NUMERIC,       -- overall pick percentile
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_player_draft_year  ON player_draft(draft_year);
CREATE INDEX IF NOT EXISTS idx_player_draft_round ON player_draft(draft_round);


-- =============================================================================
-- STEP 5: player_season_predictions — ML model predictions per player per season
-- Replaces the predictions portion of master_player_dataset
-- =============================================================================
CREATE TABLE IF NOT EXISTS player_season_predictions (
  player_uuid          UUID REFERENCES players(id) ON DELETE CASCADE,
  season               INTEGER NOT NULL,
  predicted_fantasy_ppg NUMERIC,
  prediction_error     NUMERIC,
  is_breakout          BOOLEAN,
  is_bust              BOOLEAN,
  position_tier        TEXT,
  tier                 TEXT,
  prospect_tier        TEXT,
  age_group            TEXT,
  experience_level     TEXT,
  performance_category TEXT,
  prediction_accuracy  TEXT,
  car_av               NUMERIC,   -- career approximate value
  w_av                 NUMERIC,   -- weighted approximate value
  seasons_started      INTEGER,
  allpro               INTEGER,
  probowls             INTEGER,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (player_uuid, season)
);

CREATE INDEX IF NOT EXISTS idx_psp_season    ON player_season_predictions(season);
CREATE INDEX IF NOT EXISTS idx_psp_breakout  ON player_season_predictions(is_breakout);
CREATE INDEX IF NOT EXISTS idx_psp_bust      ON player_season_predictions(is_bust);


-- =============================================================================
-- STEP 6: Backfill players from dynasty_player_tiers (NFL players)
-- =============================================================================
INSERT INTO players (gsis_id, espn_id, name, position, is_prospect)
SELECT
  t.player_id,
  -- espn_id not stored in dynasty_player_tiers; will be enriched later via ETL
  NULL::INTEGER,
  t.player_name,
  t.position,
  false
FROM dynasty_player_tiers t
WHERE t.player_id IS NOT NULL
ON CONFLICT (gsis_id) DO UPDATE SET
  name       = EXCLUDED.name,
  position   = EXCLUDED.position,
  updated_at = now();


-- =============================================================================
-- STEP 7: Backfill players from dynasty_prospects
-- espn_id IS the stable cross-reference for prospects
-- =============================================================================

-- 7a: Prospects WITH espn_id — unique per (espn_id, draft_year)
INSERT INTO players (espn_id, cfbd_id, name, first_name, last_name,
                     position, is_prospect, draft_year)
SELECT
  dp.espn_id::INTEGER,
  dp.cfbd_id::INTEGER,
  dp.name,
  dp.first_name,
  dp.last_name,
  dp.position,
  true,
  dp.draft_year
FROM dynasty_prospects dp
WHERE dp.espn_id IS NOT NULL
ON CONFLICT (espn_id, draft_year) WHERE is_prospect = true DO UPDATE SET
  cfbd_id    = COALESCE(EXCLUDED.cfbd_id,    players.cfbd_id),
  name       = EXCLUDED.name,
  first_name = COALESCE(EXCLUDED.first_name, players.first_name),
  last_name  = COALESCE(EXCLUDED.last_name,  players.last_name),
  updated_at = now();

-- 7b: Prospects WITHOUT espn_id — deduplicate on name+position+draft_year
INSERT INTO players (cfbd_id, name, first_name, last_name,
                     position, is_prospect, draft_year)
SELECT
  dp.cfbd_id::INTEGER,
  dp.name,
  dp.first_name,
  dp.last_name,
  dp.position,
  true,
  dp.draft_year
FROM dynasty_prospects dp
WHERE dp.espn_id IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM players p
    WHERE p.name      = dp.name
      AND p.position  = dp.position
      AND p.draft_year = dp.draft_year
      AND p.is_prospect = true
  );


-- =============================================================================
-- STEP 8: Add player_uuid FK column to all existing tables (nullable for now)
-- =============================================================================
ALTER TABLE dynasty_player_tiers     ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE dynasty_prospects        ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE ktc_player_values        ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE master_player_stats      ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE nfl_player_stats         ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE nfl_player_combined_stats ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE nfl_ngs_passing_stats    ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE nfl_ngs_rushing_stats    ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE nfl_ngs_receiving_stats  ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE historical_prospects     ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);
ALTER TABLE master_player_dataset    ADD COLUMN IF NOT EXISTS player_uuid UUID REFERENCES players(id);


-- =============================================================================
-- STEP 9: Backfill player_uuid on existing tables
-- =============================================================================

-- dynasty_player_tiers → via gsis_id
UPDATE dynasty_player_tiers t
SET player_uuid = p.id
FROM players p
WHERE p.gsis_id = t.player_id
  AND t.player_uuid IS NULL;

-- dynasty_prospects → via espn_id + draft_year
UPDATE dynasty_prospects t
SET player_uuid = p.id
FROM players p
WHERE p.espn_id   = t.espn_id::INTEGER
  AND p.draft_year = t.draft_year
  AND p.is_prospect = true
  AND t.espn_id IS NOT NULL
  AND t.player_uuid IS NULL;

-- dynasty_prospects without espn_id → via name+position+draft_year
UPDATE dynasty_prospects t
SET player_uuid = p.id
FROM players p
WHERE p.name      = t.name
  AND p.position  = t.position
  AND p.draft_year = t.draft_year
  AND p.is_prospect = true
  AND t.espn_id IS NULL
  AND t.player_uuid IS NULL;

-- master_player_stats → via gsis_id
UPDATE master_player_stats t
SET player_uuid = p.id
FROM players p
WHERE p.gsis_id = t.player_gsis_id
  AND t.player_uuid IS NULL;

-- nfl_player_stats → via gsis_id
UPDATE nfl_player_stats t
SET player_uuid = p.id
FROM players p
WHERE p.gsis_id = t.player_gsis_id
  AND t.player_uuid IS NULL;

-- nfl_player_combined_stats → via gsis_id
UPDATE nfl_player_combined_stats t
SET player_uuid = p.id
FROM players p
WHERE p.gsis_id = t.player_gsis_id
  AND t.player_uuid IS NULL;

-- NGS tables → via gsis_id
UPDATE nfl_ngs_passing_stats t
SET player_uuid = p.id
FROM players p
WHERE p.gsis_id = t.player_gsis_id
  AND t.player_uuid IS NULL;

UPDATE nfl_ngs_rushing_stats t
SET player_uuid = p.id
FROM players p
WHERE p.gsis_id = t.player_gsis_id
  AND t.player_uuid IS NULL;

UPDATE nfl_ngs_receiving_stats t
SET player_uuid = p.id
FROM players p
WHERE p.gsis_id = t.player_gsis_id
  AND t.player_uuid IS NULL;

-- historical_prospects → via name+position+draft_year
-- (no espn_id on this table — name match is the best we have)
UPDATE historical_prospects t
SET player_uuid = p.id
FROM players p
WHERE p.name      = t.name
  AND p.position  = t.position
  AND p.draft_year = t.draft_year
  AND p.is_prospect = true
  AND t.player_uuid IS NULL;

-- master_player_dataset → via player_name_std (normalized full name) matched to players.name
-- Note: master_player_dataset uses abbreviated names (A.Abdullah) but player_name_std is full
UPDATE master_player_dataset t
SET player_uuid = p.id
FROM players p
WHERE LOWER(p.name) = LOWER(t.player_name_std)
  AND p.position = t.position
  AND t.player_uuid IS NULL;

-- ktc_player_values → via ktc_player_id
-- (players.ktc_player_id populated by Phase 2 Python fuzzy-link script)
UPDATE ktc_player_values t
SET player_uuid = p.id
FROM players p
WHERE p.ktc_player_id = t.ktc_player_id
  AND t.player_uuid IS NULL;


-- =============================================================================
-- STEP 10: Backfill player_bio
-- Priority: dynasty_player_tiers headshot > nfl_player_stats > master_player_stats
--           > dynasty_prospects headshot
--           college from dynasty_prospects > historical_prospects
-- =============================================================================
INSERT INTO player_bio (player_uuid, headshot_url, college, hometown, team_color, jersey)
SELECT
  p.id,
  COALESCE(
    dt.headshot_url,
    -- nfl_player_stats: most recent non-null headshot for this gsis_id
    (SELECT ns.headshot_url
     FROM nfl_player_stats ns
     WHERE ns.player_gsis_id = p.gsis_id
       AND ns.headshot_url IS NOT NULL
     ORDER BY ns.season DESC, ns.week DESC
     LIMIT 1),
    ms.headshot_url,
    dp.headshot_url
  ) AS headshot_url,
  COALESCE(dp.school, hp.college) AS college,
  dp.hometown,
  dp.team_color,
  dp.jersey
FROM players p
LEFT JOIN dynasty_player_tiers dt ON dt.player_id = p.gsis_id
LEFT JOIN master_player_stats  ms ON ms.player_gsis_id = p.gsis_id
LEFT JOIN dynasty_prospects    dp ON dp.player_uuid = p.id
LEFT JOIN historical_prospects hp ON hp.player_uuid = p.id
ON CONFLICT (player_uuid) DO UPDATE SET
  headshot_url = COALESCE(EXCLUDED.headshot_url, player_bio.headshot_url),
  college      = COALESCE(EXCLUDED.college,      player_bio.college),
  hometown     = COALESCE(EXCLUDED.hometown,     player_bio.hometown),
  team_color   = COALESCE(EXCLUDED.team_color,   player_bio.team_color),
  jersey       = COALESCE(EXCLUDED.jersey,       player_bio.jersey),
  updated_at   = now();


-- =============================================================================
-- STEP 11: Backfill player_combine
-- Priority for combine metrics:
--   forty/vertical/broad_jump/three_cone/shuttle → dynasty_prospects.college_stats JSON first
--   height/weight                                → dynasty_prospects first, historical_prospects fallback
--   hs_* recruiting                              → dynasty_prospects first, historical_prospects fallback
--   bench                                        → dynasty_prospects.college_stats JSON only
-- =============================================================================
INSERT INTO player_combine (
  player_uuid,
  height, weight,
  forty, vertical, broad_jump, three_cone, shuttle, bench,
  hs_rank, hs_stars, hs_rating, hs_school, hs_city, hs_state
)
SELECT
  p.id,
  -- height: dynasty_prospects > historical_prospects
  COALESCE(dp.height, hp.height::NUMERIC),
  -- weight: dynasty_prospects > historical_prospects
  COALESCE(dp.weight, hp.weight::NUMERIC),
  -- combine metrics: JSON blob in college_stats (prospects) first
  COALESCE(
    NULLIF((dp.college_stats->>'forty_time')::NUMERIC, 0),
    NULLIF((dp.college_stats->>'40yd')::NUMERIC, 0)
  ),
  NULLIF((dp.college_stats->>'vertical')::NUMERIC, 0),
  NULLIF((dp.college_stats->>'broad_jump')::NUMERIC, 0),
  COALESCE(
    NULLIF((dp.college_stats->>'three_cone')::NUMERIC, 0),
    NULLIF((dp.college_stats->>'cone')::NUMERIC, 0)
  ),
  NULLIF((dp.college_stats->>'shuttle')::NUMERIC, 0),
  NULLIF((dp.college_stats->>'bench')::INTEGER, 0),
  -- HS recruiting: dynasty_prospects > historical_prospects
  COALESCE(dp.hs_rank,   hp.hs_rank),
  COALESCE(dp.hs_stars,  hp.hs_stars),
  COALESCE(dp.hs_rating, hp.hs_rating),
  COALESCE(dp.hs_school, hp.hs_school),
  hp.hs_city,
  COALESCE(dp.hs_state,  hp.hs_state)
FROM players p
LEFT JOIN dynasty_prospects    dp ON dp.player_uuid = p.id
LEFT JOIN historical_prospects hp ON hp.player_uuid = p.id
WHERE (
  dp.height IS NOT NULL OR dp.weight IS NOT NULL
  OR dp.college_stats IS NOT NULL
  OR hp.height IS NOT NULL
  OR dp.hs_rank IS NOT NULL OR hp.hs_rank IS NOT NULL
)
ON CONFLICT (player_uuid) DO UPDATE SET
  height     = COALESCE(EXCLUDED.height,     player_combine.height),
  weight     = COALESCE(EXCLUDED.weight,     player_combine.weight),
  forty      = COALESCE(EXCLUDED.forty,      player_combine.forty),
  vertical   = COALESCE(EXCLUDED.vertical,   player_combine.vertical),
  broad_jump = COALESCE(EXCLUDED.broad_jump, player_combine.broad_jump),
  three_cone = COALESCE(EXCLUDED.three_cone, player_combine.three_cone),
  shuttle    = COALESCE(EXCLUDED.shuttle,    player_combine.shuttle),
  bench      = COALESCE(EXCLUDED.bench,      player_combine.bench),
  hs_rank    = COALESCE(EXCLUDED.hs_rank,    player_combine.hs_rank),
  hs_stars   = COALESCE(EXCLUDED.hs_stars,   player_combine.hs_stars),
  hs_rating  = COALESCE(EXCLUDED.hs_rating,  player_combine.hs_rating),
  hs_school  = COALESCE(EXCLUDED.hs_school,  player_combine.hs_school),
  hs_city    = COALESCE(EXCLUDED.hs_city,    player_combine.hs_city),
  hs_state   = COALESCE(EXCLUDED.hs_state,   player_combine.hs_state),
  updated_at = now();


-- =============================================================================
-- STEP 12: Backfill player_draft
-- Sources: historical_prospects (most complete for drafted players)
--          dynasty_prospects.college_stats->>'draft_overall_pick' for recent classes
-- =============================================================================
INSERT INTO player_draft (
  player_uuid, draft_year, draft_round, draft_pick, draft_overall,
  draft_team, pre_draft_rank, pre_draft_pos_rank, pre_draft_grade,
  draft_round_pct, draft_pick_pct
)
SELECT
  p.id,
  COALESCE(hp.draft_year, dp.draft_year),
  hp.draft_round,
  hp.draft_pick,
  COALESCE(
    hp.draft_round * 32 + hp.draft_pick - 32,   -- approximate overall
    (dp.college_stats->>'draft_overall_pick')::INTEGER
  ),
  hp.nfl_team,
  hp.pre_draft_rank,
  hp.pre_draft_position_rank,
  hp.pre_draft_grade,
  hp.draft_round_percentile,
  hp.draft_pick_percentile
FROM players p
LEFT JOIN historical_prospects hp ON hp.player_uuid = p.id
LEFT JOIN dynasty_prospects    dp ON dp.player_uuid = p.id
WHERE hp.draft_round IS NOT NULL
   OR (dp.college_stats->>'draft_overall_pick') IS NOT NULL
ON CONFLICT (player_uuid) DO UPDATE SET
  draft_year       = COALESCE(EXCLUDED.draft_year,        player_draft.draft_year),
  draft_round      = COALESCE(EXCLUDED.draft_round,       player_draft.draft_round),
  draft_pick       = COALESCE(EXCLUDED.draft_pick,        player_draft.draft_pick),
  draft_overall    = COALESCE(EXCLUDED.draft_overall,     player_draft.draft_overall),
  draft_team       = COALESCE(EXCLUDED.draft_team,        player_draft.draft_team),
  pre_draft_rank   = COALESCE(EXCLUDED.pre_draft_rank,    player_draft.pre_draft_rank),
  pre_draft_grade  = COALESCE(EXCLUDED.pre_draft_grade,   player_draft.pre_draft_grade),
  draft_round_pct  = COALESCE(EXCLUDED.draft_round_pct,   player_draft.draft_round_pct),
  draft_pick_pct   = COALESCE(EXCLUDED.draft_pick_pct,    player_draft.draft_pick_pct),
  updated_at       = now();


-- =============================================================================
-- STEP 13: Backfill player_season_predictions from master_player_dataset
-- Deduplicate: master_player_dataset has multiple rows per player per season
-- (different model runs). Take the row with the lowest prediction_error.
-- =============================================================================
INSERT INTO player_season_predictions (
  player_uuid, season, predicted_fantasy_ppg, prediction_error,
  is_breakout, is_bust, position_tier, tier, prospect_tier,
  age_group, experience_level, performance_category, prediction_accuracy,
  car_av, w_av, seasons_started, allpro, probowls
)
SELECT DISTINCT ON (md.player_uuid, md.season)
  md.player_uuid,
  md.season,
  md.predicted_fantasy_ppg,
  md.prediction_error,
  md.is_breakout,
  md.is_bust,
  md.position_tier,
  md.tier,
  md.prospect_tier,
  md.age_group,
  md.experience_level,
  md.performance_category,
  md.prediction_accuracy,
  md.car_av,
  md.w_av,
  md.seasons_started,
  md.allpro,
  md.probowls
FROM master_player_dataset md
WHERE md.player_uuid IS NOT NULL
ORDER BY md.player_uuid, md.season, ABS(md.prediction_error) ASC NULLS LAST
ON CONFLICT (player_uuid, season) DO UPDATE SET
  predicted_fantasy_ppg = COALESCE(EXCLUDED.predicted_fantasy_ppg, player_season_predictions.predicted_fantasy_ppg),
  prediction_error      = COALESCE(EXCLUDED.prediction_error,      player_season_predictions.prediction_error),
  is_breakout           = COALESCE(EXCLUDED.is_breakout,           player_season_predictions.is_breakout),
  is_bust               = COALESCE(EXCLUDED.is_bust,               player_season_predictions.is_bust),
  updated_at            = now();


-- =============================================================================
-- VERIFICATION — uncomment and run after migration to confirm coverage
-- =============================================================================
-- SELECT 'players'                        AS tbl, COUNT(*) AS rows FROM players
-- UNION ALL
-- SELECT 'player_bio',                    COUNT(*) FROM player_bio
-- UNION ALL
-- SELECT 'player_combine',                COUNT(*) FROM player_combine
-- UNION ALL
-- SELECT 'player_draft',                  COUNT(*) FROM player_draft
-- UNION ALL
-- SELECT 'player_season_predictions',     COUNT(*) FROM player_season_predictions
-- UNION ALL
-- SELECT 'dynasty_player_tiers w/ uuid',  COUNT(*) FROM dynasty_player_tiers  WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'dynasty_player_tiers no uuid',  COUNT(*) FROM dynasty_player_tiers  WHERE player_uuid IS NULL
-- UNION ALL
-- SELECT 'dynasty_prospects w/ uuid',     COUNT(*) FROM dynasty_prospects     WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'dynasty_prospects no uuid',     COUNT(*) FROM dynasty_prospects     WHERE player_uuid IS NULL
-- UNION ALL
-- SELECT 'ktc_player_values w/ uuid',     COUNT(*) FROM ktc_player_values     WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'master_player_stats w/ uuid',   COUNT(*) FROM master_player_stats   WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'nfl_player_stats w/ uuid',      COUNT(*) FROM nfl_player_stats      WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'nfl_ngs_passing w/ uuid',       COUNT(*) FROM nfl_ngs_passing_stats WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'nfl_ngs_rushing w/ uuid',       COUNT(*) FROM nfl_ngs_rushing_stats WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'nfl_ngs_receiving w/ uuid',     COUNT(*) FROM nfl_ngs_receiving_stats WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'historical_prospects w/ uuid',  COUNT(*) FROM historical_prospects  WHERE player_uuid IS NOT NULL
-- UNION ALL
-- SELECT 'master_player_dataset w/ uuid', COUNT(*) FROM master_player_dataset WHERE player_uuid IS NOT NULL;
