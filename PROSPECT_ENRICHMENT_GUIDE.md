# Prospect Data Enrichment Guide 🎓📊

This guide explains how to use the `enrich_prospect_data.py` script to enhance dynasty prospect records with data from the College Football Data API (CFBD) and ESPN.

## Overview

The enrichment pipeline fetches additional player data:
- **CFBD**: Player ID, height, weight, hometown, jersey number
- **ESPN**: Athlete ID (enables headshot URLs)

## Data Sources

### College Football Data API (CFBD)
- **Endpoint**: `GET /player/search`
- **Fields**: `id`, `firstName`, `lastName`, `height`, `weight`, `jersey`, `hometown`, `team`, `teamColor`
- **Rate Limit**: ~1000 requests/hour
- **Auth**: Bearer token (API key required)

### ESPN Search API
- **Endpoint**: `GET /apis/common/v3/search`
- **Fields**: `id` (athlete ID), `displayName`
- **Rate Limit**: No auth required, reasonable rate limits
- **Headshot URL Pattern**: 
  ```
  https://a.espncdn.com/combiner/i?img=/i/headshots/college-football/players/full/{espn_id}.png&w=350&h=254
  ```

## Prerequisites

1. **CFBD API Key**: Sign up at [collegefootballdata.com](https://collegefootballdata.com)
2. **Environment Variables**:
   ```bash
   export CFBD_API_KEY=your-api-key
   export SUPABASE_URL=your-supabase-url
   export SUPABASE_SERVICE_KEY=your-service-key
   ```

## Quick Start

### Basic Usage

```bash
cd firstballotETL
python enrich_prospect_data.py
```

This will:
- Fetch prospects from `dynasty_prospects` table
- Search CFBD for player data (height, weight, hometown, cfbd_id)
- Search ESPN for athlete IDs (for headshots)
- Update database with enriched data

### Command Line Options

```bash
python enrich_prospect_data.py [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--no-cfbd` | Skip CFBD data fetching |
| `--no-espn` | Skip ESPN ID fetching |
| `--limit N` | Only process first N prospects |
| `--all` | Process all prospects (not just missing data) |
| `--api-key KEY` | Override CFBD API key |

## Usage Examples

### 1. Full Enrichment (Default)

```bash
python enrich_prospect_data.py
```

Fetches all missing data from both CFBD and ESPN.

### 2. CFBD Only (Heights/Weights)

```bash
python enrich_prospect_data.py --no-espn
```

Only fetches physical measurements from CFBD.

### 3. Test Run (Limited)

```bash
python enrich_prospect_data.py --limit 5
```

Process only 5 prospects for testing.

### 4. Force Re-Enrichment

```bash
python enrich_prospect_data.py --all
```

Re-fetch data for all prospects, even if data already exists.

## Database Schema

Before running, ensure your `dynasty_prospects` table has these columns:

```sql
-- Run add_enrichment_columns.sql in Supabase
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS hometown VARCHAR(255);
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS jersey INTEGER;
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS headshot_url TEXT;
ALTER TABLE dynasty_prospects ADD COLUMN IF NOT EXISTS cfbd_id BIGINT;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_prospects_cfbd_id ON dynasty_prospects(cfbd_id);
CREATE INDEX IF NOT EXISTS idx_prospects_espn_id ON dynasty_prospects(espn_id);
```

## Output Fields

After enrichment, prospects will have:

| Field | Source | Description |
|-------|--------|-------------|
| `cfbd_id` | CFBD | College Football Data player ID |
| `espn_id` | ESPN | ESPN athlete ID (for headshots) |
| `first_name` | CFBD | First name |
| `last_name` | CFBD | Last name |
| `height` | CFBD | Height in inches |
| `weight` | CFBD | Weight in pounds |
| `hometown` | CFBD | Player's hometown |
| `jersey` | CFBD | Jersey number |
| `headshot_url` | Generated | ESPN headshot URL |

## Headshot URL Generation

Once a prospect has an `espn_id`, the API automatically generates headshot URLs:

```typescript
// Auto-generated in API
const getHeadshotUrl = (espnId: number) => 
  `https://a.espncdn.com/combiner/i?img=/i/headshots/college-football/players/full/${espnId}.png&w=350&h=254`
```

The ProspectCard component displays headshots when available:

```tsx
{prospect.headshot_url ? (
  <img src={prospect.headshot_url} alt={prospect.name} />
) : (
  <span>{initials}</span>
)}
```

## CFBD API Response Example

```json
{
  "id": "4362887",
  "team": "Notre Dame",
  "name": "Jeremiyah Love",
  "firstName": "Jeremiyah",
  "lastName": "Love",
  "weight": 205,
  "height": 70,
  "jersey": 4,
  "position": "RB",
  "hometown": "St. Louis, MO",
  "teamColor": "0C2340",
  "teamColorSecondary": "C99700"
}
```

## Troubleshooting

### Issue: "CFBD_API_KEY not set"

**Solution**: Get a free API key from [collegefootballdata.com](https://collegefootballdata.com)

```bash
export CFBD_API_KEY=your-key
```

### Issue: "Player not found in CFBD"

**Possible causes**:
- Name spelling mismatch
- Player transferred schools
- Freshman not yet in database

**Solution**: Run with `--limit 1` to debug specific players

### Issue: "ESPN ID not found"

**Possible causes**:
- Player doesn't have an ESPN page
- Name spelling varies between sources
- Recently transferred players

**Solution**: These prospects will show initials instead of headshots

### Issue: "Rate limited"

**Solution**: The script has built-in delays (150ms between requests). If still rate limited:
```python
self.request_delay = 0.3  # Increase to 300ms
```

## Pipeline Integration

### Recommended Order

1. **Update rankings** first (brings in new prospects):
   ```bash
   python update_ff_rankings.py
   ```

2. **Run college ranking pipeline** (calculates tiers):
   ```bash
   python college_ranking_pipeline.py
   ```

3. **Enrich prospect data** (adds headshots & measurements):
   ```bash
   python enrich_prospect_data.py
   ```

### Automation

```bash
# Full prospect update pipeline
cd firstballotETL
python update_ff_rankings.py
python college_ranking_pipeline.py
python enrich_prospect_data.py
```

## Monitoring Coverage

Query your database to check enrichment status:

```sql
SELECT 
    COUNT(*) as total,
    COUNT(cfbd_id) as with_cfbd,
    COUNT(espn_id) as with_espn,
    COUNT(height) as with_height,
    COUNT(weight) as with_weight,
    COUNT(headshot_url) as with_headshot
FROM dynasty_prospects
WHERE draft_year = 2026;
```

## Best Practices

1. **Run incrementally**: Default behavior only enriches missing data
2. **Test first**: Use `--limit 5` to verify API connections
3. **Schedule weekly**: Run after major roster changes
4. **Monitor errors**: Check console output for failed lookups

---

**Questions?** Check the main [README.md](README.md) for general setup.

