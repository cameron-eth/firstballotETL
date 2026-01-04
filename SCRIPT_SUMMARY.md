# Fantasy Rankings Update Script - Summary

## What Was Created

A Python script (`update_ff_rankings.py`) that automatically updates your dynasty fantasy football rankings table using nflreadpy's fantasy rankings data.

## Files Created/Modified

1. **`update_ff_rankings.py`** - Main script (262 lines)
   - Fetches rankings from nflreadpy
   - Transforms data to match your database schema
   - Uploads to Supabase `dynasty_sf_top_150` table

2. **`RANKINGS_UPDATE_GUIDE.md`** - Comprehensive usage guide
   - Installation instructions
   - Command-line options
   - Usage examples
   - Troubleshooting tips
   - Best practices

3. **`README.md`** - Updated with fantasy rankings section
   - Added Fantasy Rankings section to data available
   - Added update_ff_rankings.py usage examples

## Key Features

### ✅ Smart Data Fetching
- Uses `nflreadpy.load_ff_rankings()` to get expert consensus rankings
- Automatically filters for dynasty-op (superflex) rankings
- Falls back to dynasty-overall if superflex not available
- Handles multiple ranking types (draft, week, all)

### ✅ Data Quality
- Removes duplicate players automatically
- Filters to latest rankings date
- Validates required columns
- Handles missing data gracefully

### ✅ Database Integration
- Connects to Supabase using existing config
- Option to clear existing records before upload
- Batch uploading with progress tracking
- Error handling and retry logic

### ✅ Flexibility
- Configurable top-N players (default: 400)
- Dry-run mode for testing
- CSV backups automatically saved
- Multiple ranking type support

## Verified Test Results

**Test Run (Dry Run):**
```bash
python update_ff_rankings.py --dry-run --top-n 30
```

**Results:**
- ✅ Fetched 5,171 ranking records from nflreadpy
- ✅ Filtered to dynasty-op (superflex) rankings
- ✅ Latest data from 2025-10-24
- ✅ No duplicates in output
- ✅ Clean top 30 rankings with proper superflex distribution:
  - 14 QBs (superflex heavy, as expected)
  - 10 WRs
  - 5 RBs
  - 1 TE

**Top 10 Players (Verified):**
1. Josh Allen (QB, BUF)
2. Lamar Jackson (QB, BAL)
3. Jayden Daniels (QB, WAS)
4. Jalen Hurts (QB, PHI)
5. Ja'Marr Chase (WR, CIN)
6. Justin Jefferson (WR, MIN)
7. Justin Herbert (QB, LAC)
8. Drake Maye (QB, NE)
9. Puka Nacua (WR, LAR)
10. Joe Burrow (QB, CIN)

## Usage Commands

### Quick Start (Production)
```bash
cd firstballotETL
python update_ff_rankings.py
```

### Common Options
```bash
# Test without uploading
python update_ff_rankings.py --dry-run

# Get weekly rankings instead
python update_ff_rankings.py --type week

# Limit to top 150 players
python update_ff_rankings.py --top-n 150

# Don't clear existing records
python update_ff_rankings.py --no-clear
```

## Database Schema

The script populates the `dynasty_sf_top_150` table with:

| Column | Type | Description |
|--------|------|-------------|
| PLAYER NAME | TEXT | Full player name |
| RK | INTEGER | Rank (1-N) |
| POS | TEXT | Position (QB, RB, WR, TE) |
| TEAM | TEXT | Team abbreviation |

## Integration with Your App

Your Next.js app already has the API endpoint to fetch these rankings:

```typescript
// app/api/rankings/route.ts
const { data } = await supabaseServer
  .from('dynasty_sf_top_150')
  .select('*')
  .order('RK', { ascending: true });
```

Used by:
- `/api/rankings` - Main rankings endpoint
- `/api/draft-analysis` - Draft grading
- `/api/trade-market` - Trade evaluations
- `/league-buddy` - Team analysis

## Scheduling Recommendations

### Option 1: Manual Updates
Run whenever rankings need updating (weekly during season).

### Option 2: Cron Job
```bash
# Daily at 3 AM
0 3 * * * cd /path/to/firstballotETL && python update_ff_rankings.py
```

### Option 3: GitHub Actions
Create `.github/workflows/update-rankings.yml`:
```yaml
name: Update Fantasy Rankings
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:
jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: python update_ff_rankings.py
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
```

## Data Source

Rankings come from nflreadpy's fantasy football rankings dataset, which aggregates:
- Expert consensus rankings (ECR)
- Multiple fantasy platforms
- Regular updates throughout the season
- Historical data for trend analysis

## Benefits

1. **Automated Updates** - No manual data entry
2. **Expert Consensus** - Aggregated from multiple sources
3. **Always Current** - Latest rankings on demand
4. **Flexible** - Works with different ranking types
5. **Reliable** - Error handling and validation
6. **Tested** - Verified working with dry runs

## Next Steps

1. **Test with your database:**
   ```bash
   python update_ff_rankings.py --dry-run
   ```

2. **Run production update:**
   ```bash
   python update_ff_rankings.py
   ```

3. **Verify in your app:**
   - Visit your app's rankings page
   - Check draft analysis tool
   - Test trade evaluations

4. **Set up automation:**
   - Choose scheduling method (manual, cron, or GitHub Actions)
   - Configure to run weekly during season

## Support

- **Documentation:** See `RANKINGS_UPDATE_GUIDE.md`
- **Configuration:** Uses existing `config.py` and `.env`
- **Troubleshooting:** Check guide for common issues

---

**Status:** ✅ Complete and tested  
**Created:** October 25, 2025  
**Ready for:** Production use

