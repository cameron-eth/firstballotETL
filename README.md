# First Ballot ETL 🏈

**Automated NFL Next Gen Stats data pipeline** with fantasy scoring, Supabase integration, and GitHub Actions.

[![Update NFL Stats](https://github.com/cameron-eth/firstballotETL/actions/workflows/update-nfl-stats.yml/badge.svg)](https://github.com/cameron-eth/firstballotETL/actions/workflows/update-nfl-stats.yml)

## 🎯 What This Does

Fetches NFL **Next Gen Stats (NGS)** data and automatically:
- Calculates **fantasy scoring** (PPR)
- Uploads to **Supabase** database
- Saves **CSV backups**
- Runs **daily via GitHub Actions**

## 📊 Data Available

### Passing Stats (QBs)
- Basic: yards, TDs, INTs, completions
- NGS: time to throw, CPOE, air yards, aggressiveness
- Fantasy: points, PPG, points per attempt
- **32 columns** | ~192 records per season

### Rushing Stats (All Positions)
- Basic: attempts, yards, TDs, YPC
- NGS: efficiency, yards over expected, stacked box %
- Fantasy: points, PPG, points per rush
- **25 columns** | ~204 records per season

### Receiving Stats (WR/TE/RB)
- Basic: targets, receptions, yards, TDs, catch%
- NGS: separation, cushion, YAC above expected
- Fantasy: points, PPG, points per reception
- **27 columns** | ~497 records per season

### Fantasy Rankings (Dynasty)
- Player rankings from expert consensus (nflreadpy)
- Dynasty superflex top 150+ rankings
- Updated regularly for draft/trade analysis
- **dynasty_sf_top_150 table**

**Total: ~893 player records per season with fantasy + NGS metrics**

---

## ⚡ Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/cameron-eth/firstballotETL.git
cd firstballotETL
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Set Environment Variables
```bash
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_KEY="your-service-key"
```

### 4. Run Pipeline
```bash
# Fetch 2025 data
python impl.py --mode ngs --ngs-types passing rushing receiving --year 2025

# Or fetch specific types
python impl.py --mode ngs --ngs-types passing --year 2025
```

---

## 🤖 GitHub Actions Setup

### Enable Automated Updates

1. **Add Repository Secrets**
   - Go to your repo → Settings → Secrets → Actions
   - Add `SUPABASE_URL`
   - Add `SUPABASE_SERVICE_KEY`

2. **Runs Automatically**
   - Daily at 6 AM UTC (2 AM EST)
   - Manual trigger available in Actions tab

3. **Manual Trigger**
   - Go to Actions tab
   - Select "Update NFL Stats"
   - Click "Run workflow"
   - Choose year and stat types

---

## 🗄️ Database Schema

### Three Main Tables

```sql
-- Passing stats (QBs)
nfl_ngs_passing_stats

-- Rushing stats (all positions) 
nfl_ngs_rushing_stats

-- Receiving stats (WR/TE/RB)
nfl_ngs_receiving_stats
```

### Combined View

```sql
-- All player stats in one view
nfl_player_combined_stats
```

**Setup:** Run `create_tables.sql` in your Supabase SQL Editor

---

## 📖 Documentation

- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Quick setup guide
- **[DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)** - Complete schema reference
- **[SETUP_DATABASE.md](SETUP_DATABASE.md)** - Detailed database setup
- **[create_tables.sql](create_tables.sql)** - SQL schema

---

## 🔍 Example Queries

### Top Fantasy QBs
```sql
SELECT player_display_name, team_abbr, 
       fantasy_points, fantasy_ppg, passer_rating
FROM nfl_ngs_passing_stats
WHERE season = 2025
ORDER BY fantasy_points DESC
LIMIT 10;
```

### Most Efficient RBs
```sql
SELECT player_display_name, team_abbr,
       rush_yards, fantasy_points, efficiency
FROM nfl_ngs_rushing_stats
WHERE season = 2025 AND player_position = 'RB'
ORDER BY efficiency DESC;
```

### WRs with Best Separation
```sql
SELECT player_display_name, team_abbr,
       yards, rec_touchdowns, avg_separation
FROM nfl_ngs_receiving_stats
WHERE season = 2025 AND player_position = 'WR'
ORDER BY avg_separation DESC;
```

---

## 🛠️ Configuration

### config.toml
```toml
[database]
enable_database = true    # Master switch for uploads

[data]
save_to_csv = true       # Keep CSV backups
save_to_database = true  # Upload to Supabase
start_year = 2020
end_year = 2025
```

---

## 📦 Output

### Local Files (data_output/)
- `ngs_passing_YYYY.csv` - QB stats (32 columns)
- `ngs_rushing_YYYY.csv` - Rushing stats (25 columns)
- `ngs_receiving_YYYY.csv` - Receiving stats (27 columns)

### Supabase Tables
- Indexed and optimized for queries
- Automatic upsert on conflict
- Queryable via API

---

## 🚀 Usage Examples

### Fetch Current Season Stats
```bash
python impl.py --mode ngs --year 2025
```

### Fetch Multiple Years
```bash
python impl.py --mode ngs --years 2023 2024 2025
```

### Fetch Specific Stats
```bash
python impl.py --mode ngs --ngs-types passing --year 2025
```

### Update Fantasy Rankings
```bash
# Update dynasty rankings (draft rankings)
python update_ff_rankings.py

# Fetch weekly rankings instead
python update_ff_rankings.py --type week

# Dry run to preview data without uploading
python update_ff_rankings.py --dry-run

# Keep top 200 players only
python update_ff_rankings.py --top-n 200
```

### Build Historical Database
```bash
for year in 2020 2021 2022 2023 2024 2025; do
  python impl.py --mode ngs --year $year
  sleep 5
done
```

---

## 📊 Data Sample

**Top 5 Fantasy QBs (2025)**
| Player | Team | FPts | PPG | Rating |
|--------|------|------|-----|--------|
| Matthew Stafford | LAR | 100.1 | 16.7 | 107.3 |
| Jared Goff | DET | 91.5 | 15.2 | 120.7 |
| Baker Mayfield | TB | 89.3 | 14.9 | 104.4 |
| Dak Prescott | DAL | 88.2 | 14.7 | 101.1 |
| Josh Allen | BUF | 80.7 | 13.4 | 108.4 |

---

## 🔐 Security

- Never commit `.env` file (in `.gitignore`)
- Use GitHub Secrets for Actions
- Use service_role key for pipeline
- Enable RLS on Supabase tables for production

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 📝 License

MIT License - feel free to use for your fantasy football projects!

---

## 🏆 Built For

**First Ballot Fantasy** - Advanced NFL analytics and fantasy insights

---

## ⭐ Features

- ✅ Automated daily updates via GitHub Actions
- ✅ Fantasy scoring (PPR) calculated automatically
- ✅ NGS advanced metrics included
- ✅ Position-separated tables
- ✅ CSV backups always saved
- ✅ Supabase integration with upsert
- ✅ Historical data support (2020-2025)
- ✅ Manual trigger via GitHub Actions
- ✅ Optimized database schema with indexes
- ✅ Comprehensive documentation

---

**Questions?** Check the documentation files or open an issue!
