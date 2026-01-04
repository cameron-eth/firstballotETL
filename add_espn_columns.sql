-- =============================================================================
-- ADD ESPN DATA COLUMNS TO DYNASTY_PROSPECTS
-- Run this in Supabase SQL Editor
-- =============================================================================

-- Add class/experience column (Freshman, Sophomore, Junior, Senior)
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS class VARCHAR(20);

-- Add first_name and last_name if not exists
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);

-- Add hometown/birthplace
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS hometown VARCHAR(255);

-- Add jersey number
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS jersey INTEGER;

-- Add headshot URL
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS headshot_url TEXT;

-- Add team color for UI customization
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS team_color VARCHAR(10);

-- Add college stats JSON (career stats from ESPN)
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS college_stats JSONB;

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_prospects_espn_id ON dynasty_prospects(espn_id);
CREATE INDEX IF NOT EXISTS idx_prospects_class ON dynasty_prospects(class);

-- =============================================================================
-- UPDATE EXISTING RECORDS: Generate headshot URLs from ESPN IDs
-- =============================================================================
UPDATE dynasty_prospects 
SET headshot_url = 'https://a.espncdn.com/i/headshots/college-football/players/full/' || espn_id || '.png'
WHERE espn_id IS NOT NULL AND headshot_url IS NULL;

-- =============================================================================
-- VERIFY CHANGES
-- =============================================================================
-- SELECT 
--     name,
--     position,
--     class,
--     height,
--     weight,
--     hometown,
--     jersey,
--     CASE WHEN headshot_url IS NOT NULL THEN '✓' ELSE '✗' END as has_headshot,
--     CASE WHEN college_stats IS NOT NULL THEN '✓' ELSE '✗' END as has_stats
-- FROM dynasty_prospects
-- WHERE draft_year = 2026
-- ORDER BY rank
-- LIMIT 20;

