# Fantasy Rankings Update Guide 🏆

This guide explains how to use the `update_ff_rankings.py` script to update your dynasty fantasy football rankings using nflreadpy.

## Overview

The script fetches fantasy football rankings from nflreadpy's `load_ff_rankings()` function and uploads them to your Supabase `dynasty_sf_top_150` table. This provides up-to-date expert consensus rankings for your fantasy app.

## Prerequisites

1. Python 3.8+ installed
2. Required packages installed:
   ```bash
   pip install pandas nflreadpy supabase python-dotenv
   ```
3. Environment variables set in `.env` file:
   ```env
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_KEY=your-service-role-key
   ```

## Quick Start

### Basic Usage (Default)

Update rankings with draft rankings (most common use case):

```bash
python update_ff_rankings.py
```

This will:
- Fetch the latest draft rankings from nflreadpy
- Clear existing records in `dynasty_sf_top_150` table
- Upload top 400 players to the database
- Save a CSV backup

### Command Line Options

```bash
python update_ff_rankings.py [OPTIONS]
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--type {draft,week,all}` | Type of rankings to fetch | `draft` |
| `--top-n N` | Number of top players to include | `400` |
| `--no-clear` | Don't clear existing rankings before upload | False (clears by default) |
| `--dry-run` | Preview data without uploading | False |

## Usage Examples

### 1. Standard Dynasty Rankings Update

```bash
python update_ff_rankings.py
```

Fetches draft rankings and updates the database with top 400 players.

### 2. Weekly Rankings (Redraft)

```bash
python update_ff_rankings.py --type week
```

Fetches current week rankings instead of dynasty/draft rankings.

### 3. Preview Without Uploading (Dry Run)

```bash
python update_ff_rankings.py --dry-run
```

Perfect for testing! Shows you what would be uploaded without actually touching the database.

### 4. Limit to Top 150 Only

```bash
python update_ff_rankings.py --top-n 150
```

Only keeps the top 150 ranked players (matches table name).

### 5. Append Without Clearing

```bash
python update_ff_rankings.py --no-clear
```

Adds new rankings without deleting existing records (useful for historical tracking).

### 6. All Historical Rankings

```bash
python update_ff_rankings.py --type all
```

Fetches all available historical rankings data.

## Output

### Console Output

The script provides detailed progress information:
- ✅ Fetch status with record count
- 🔄 Data transformation details
- 📊 Position breakdown (QB, RB, WR, TE)
- 📤 Upload progress with batch tracking
- 💾 CSV backup location

### CSV Backup

Every run creates a CSV backup file:
```
ff_rankings_draft_YYYYMMDD.csv
```

This provides a local backup and allows you to review rankings before/after updates.

### Database Table

Updates the `dynasty_sf_top_150` table with columns:
- `PLAYER NAME` - Full player name
- `RK` - Rank (1-N)
- `POS` - Position (QB, RB, WR, TE)
- `TEAM` - Team abbreviation

## Data Source: nflreadpy

The rankings come from nflreadpy's fantasy football rankings dataset, which aggregates expert consensus rankings from multiple fantasy football analysts and platforms.

### Ranking Types

**1. Draft Rankings (`--type draft`)**
- Best for: Dynasty leagues, startup drafts
- Updated: Regularly throughout the season
- Focus: Long-term player value

**2. Weekly Rankings (`--type week`)**
- Best for: Redraft leagues, weekly start/sit decisions  
- Updated: Weekly during the season
- Focus: Short-term performance projections

**3. All Rankings (`--type all`)**
- Best for: Historical analysis, trend tracking
- Updated: Includes all historical data
- Focus: Complete dataset for research

## Scheduling Updates

### Manual Updates

Run the script manually whenever you want fresh rankings:
```bash
cd firstballotETL
python update_ff_rankings.py
```

### Automated Updates (Cron)

Set up a cron job for automatic daily updates:

```bash
# Edit crontab
crontab -e

# Add this line for daily updates at 3 AM
0 3 * * * cd /path/to/firstballotETL && python update_ff_rankings.py
```

### GitHub Actions (Recommended)

Add to `.github/workflows/update-rankings.yml`:

```yaml
name: Update Fantasy Rankings

on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:      # Manual trigger

jobs:
  update-rankings:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      
      - name: Update rankings
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
        run: |
          cd firstballotETL
          python update_ff_rankings.py
```

## Troubleshooting

### Issue: "Supabase client not configured"

**Solution:** Make sure your `.env` file has:
```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-key
```

### Issue: "No data fetched"

**Solution:** 
- Check your internet connection
- nflreadpy may be temporarily unavailable
- Try using `--dry-run` to see if data is being fetched

### Issue: "Upload failed"

**Solution:**
- Check that `dynasty_sf_top_150` table exists in Supabase
- Verify your service role key has write permissions
- Check table schema matches expected columns

### Issue: "Column mismatch"

**Solution:** The script auto-maps common column names. If you get errors:
1. Run with `--dry-run` to see the data structure
2. Check the nflreadpy data format
3. Update the `column_mapping` dict in `transform_rankings_for_dynasty()`

## Best Practices

1. **Always test first**: Run with `--dry-run` before production updates
2. **Keep backups**: The CSV backups are automatically saved
3. **Monitor updates**: Check console output for errors
4. **Update regularly**: Run at least weekly during the season
5. **Version control**: Track changes to the CSV backups

## Integration with Your App

After updating rankings, your Next.js app can fetch them:

```typescript
// app/api/rankings/route.ts
const { data } = await supabaseServer
  .from('dynasty_sf_top_150')
  .select('*')
  .order('RK', { ascending: true });
```

The rankings are immediately available to:
- Draft analysis tools
- Trade evaluations
- Player comparisons
- Team rankings

## Questions?

- Check the main [README.md](README.md) for general setup
- Review [nflreadpy documentation](https://pypi.org/project/nflreadpy/)
- Open an issue for bugs or feature requests

---

**Happy ranking! 🏈📊**

