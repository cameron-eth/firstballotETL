-- Run in Supabase SQL Editor (once).
-- Adds espn_id for trade calculator / rankings headshots; aligns with ESPN NFL player pages.

ALTER TABLE dynasty_player_tiers
  ADD COLUMN IF NOT EXISTS espn_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_dynasty_player_tiers_espn_id
  ON dynasty_player_tiers(espn_id)
  WHERE espn_id IS NOT NULL;

-- NFL headshot CDN (matches app: headshots/nfl/players/full/{id}.png)
UPDATE dynasty_player_tiers
SET
  espn_id = 3929630,
  headshot_url = 'https://a.espncdn.com/i/headshots/nfl/players/full/3929630.png'
WHERE player_name = 'Saquon Barkley';

UPDATE dynasty_player_tiers
SET
  espn_id = 3139477,
  headshot_url = 'https://a.espncdn.com/i/headshots/nfl/players/full/3139477.png'
WHERE player_name = 'Patrick Mahomes';

UPDATE dynasty_player_tiers
SET
  espn_id = 3117251,
  headshot_url = 'https://a.espncdn.com/i/headshots/nfl/players/full/3117251.png'
WHERE player_name = 'Christian McCaffrey';
