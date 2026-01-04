-- =============================================================================
-- ADD ENRICHMENT COLUMNS TO DYNASTY_PROSPECTS
-- Run this in Supabase SQL Editor to add new columns for enriched data
-- =============================================================================

-- Add first_name and last_name columns
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);

-- Add hometown column
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS hometown VARCHAR(255);

-- Add jersey number column
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS jersey INTEGER;

-- Add headshot_url column (generated from espn_id)
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS headshot_url TEXT;

-- Add team colors for UI customization
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS team_color VARCHAR(7);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS team_color_secondary VARCHAR(7);

-- Create indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_prospects_cfbd_id ON dynasty_prospects(cfbd_id);
CREATE INDEX IF NOT EXISTS idx_prospects_espn_id ON dynasty_prospects(espn_id);

-- =============================================================================
-- UPDATE EXISTING DATA: Generate headshot URLs from espn_id
-- =============================================================================
UPDATE dynasty_prospects 
SET headshot_url = 'https://a.espncdn.com/combiner/i?img=/i/headshots/college-football/players/full/' || espn_id || '.png&w=350&h=254'
WHERE espn_id IS NOT NULL AND headshot_url IS NULL;

-- =============================================================================
-- HELPFUL QUERIES
-- =============================================================================

-- Check enrichment status
-- SELECT 
--     name,
--     position,
--     CASE WHEN cfbd_id IS NOT NULL THEN '✓' ELSE '✗' END as has_cfbd,
--     CASE WHEN espn_id IS NOT NULL THEN '✓' ELSE '✗' END as has_espn,
--     CASE WHEN height IS NOT NULL THEN '✓' ELSE '✗' END as has_height,
--     CASE WHEN weight IS NOT NULL THEN '✓' ELSE '✗' END as has_weight,
--     CASE WHEN headshot_url IS NOT NULL THEN '✓' ELSE '✗' END as has_headshot
-- FROM dynasty_prospects
-- WHERE draft_year = 2026
-- ORDER BY rank
-- LIMIT 50;

-- Count enrichment coverage
-- SELECT 
--     COUNT(*) as total,
--     COUNT(cfbd_id) as with_cfbd,
--     COUNT(espn_id) as with_espn,
--     COUNT(height) as with_height,
--     COUNT(weight) as with_weight,
--     COUNT(headshot_url) as with_headshot
-- FROM dynasty_prospects
-- WHERE draft_year = 2026;

