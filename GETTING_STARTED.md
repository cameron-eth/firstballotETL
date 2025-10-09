# Getting Started - NFL Data Pipeline with Supabase

Complete setup guide to get your NFL stats pipeline running with Supabase in **5 minutes**.

## ✅ What You Have

- ✅ Python pipeline that fetches NFL NGS data
- ✅ Fantasy scoring calculations (PPR)
- ✅ Position-separated comprehensive tables
- ✅ CSV file exports
- ✅ **Supabase database integration** (NEW!)
- ✅ SQL schema with 3 tables + combined view
- ✅ Auto-upsert for duplicate handling

---

## 🚀 Quick Setup (2 Ways)

### **Option A: CSV Only** (No Database - Simplest)

```bash
# Just run the pipeline - data saves to CSV
python impl.py --mode ngs --ngs-types passing rushing receiving --year 2025

# Check output
ls data_output/
```

Done! Your CSV files are in `data_output/`

---

### **Option B: Supabase Integration** (Recommended)

#### **Step 1: Set Environment Variables (30 seconds)**

```bash
# Export credentials (replace with your actual values)
export SUPABASE_URL="https://xxxxxx.supabase.co"
export SUPABASE_SERVICE_KEY="eyJhbGc..."
```

**Where to get these:**
- Go to Supabase Dashboard → Your Project → Settings → API
- Copy **Project URL** and **Service Role Key**

#### **Step 2: Create Tables (1 minute)**

1. Open Supabase SQL Editor
2. Copy all of `create_tables.sql`
3. Paste and execute

#### **Step 3: Run Pipeline (2-3 minutes)**

```bash
python impl.py --mode ngs --ngs-types passing rushing receiving --year 2025
```

#### **Step 4: Verify Upload**

```sql
-- In Supabase SQL Editor
SELECT COUNT(*) FROM nfl_ngs_passing_stats WHERE season = 2025;
SELECT COUNT(*) FROM nfl_ngs_rushing_stats WHERE season = 2025;
SELECT COUNT(*) FROM nfl_ngs_receiving_stats WHERE season = 2025;
```

**Expected counts for 2025:**
- Passing: ~192 records (QBs)
- Rushing: ~204 records (all positions)
- Receiving: ~497 records (WR/TE/RB)

Done! 🎉

---

## 📊 What You Can Do Now

### 1. Query Data via SQL

```sql
-- Top fantasy QBs
SELECT player_display_name, team_abbr, fantasy_points, fantasy_ppg, 
       completion_percentage_above_expectation as cpoe
FROM nfl_ngs_passing_stats
WHERE season = 2025
ORDER BY fantasy_points DESC
LIMIT 15;

-- Top RBs by efficiency
SELECT player_display_name, team_abbr, rush_yards, fantasy_points,
       efficiency, rush_yards_over_expected_per_att
FROM nfl_ngs_rushing_stats
WHERE season = 2025 AND player_position = 'RB'
ORDER BY efficiency DESC;
```

### 2. Use in Next.js App

```typescript
// app/api/stats/route.ts
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_KEY!
)

export async function GET() {
  const { data, error } = await supabase
    .from('nfl_ngs_passing_stats')
    .select('*')
    .eq('season', 2025)
    .order('fantasy_points', { ascending: false })
    .limit(15)
  
  return Response.json({ data })
}
```

### 3. Build Historical Database

```bash
# Fetch multiple years
for year in 2020 2021 2022 2023 2024 2025; do
  python impl.py --mode ngs --year $year
  echo "Completed $year"
  sleep 5
done
```

---

## 📁 File Structure

```
Pipeline/
├── impl.py                   # Main pipeline script
├── config.py                 # Configuration manager
├── config.toml               # Settings (years, output, etc.)
├── utils.py                  # Data fetching & upload functions
├── requirements.txt          # Python dependencies
│
├── create_tables.sql         # Database schema (run in Supabase)
├── DATABASE_SCHEMA.md        # Table documentation
├── SETUP_DATABASE.md         # Detailed database setup
├── GETTING_STARTED.md        # This file
│
├── env.example               # Environment template
├── .env                      # Your credentials (create this)
├── .gitignore                # Excludes .env and data files
│
└── data_output/              # CSV exports
    ├── ngs_passing_2025.csv
    ├── ngs_rushing_2025.csv
    └── ngs_receiving_2025.csv
```

---

## 🎯 Usage Examples

### Fetch Current Season
```bash
python impl.py --mode ngs --year 2025
```

### Fetch Specific Stat Type
```bash
python impl.py --mode ngs --ngs-types passing --year 2025
```

### Fetch Multiple Years
```bash
python impl.py --mode ngs --years 2023 2024 2025
```

### Disable Database Upload (CSV Only)
```bash
# Option 1: Don't set environment variables
unset SUPABASE_URL
unset SUPABASE_SERVICE_KEY

# Option 2: Edit config.toml
save_to_database = false
```

---

## ⚙️ Configuration

### config.toml
```toml
[database]
enable_database = true        # Master switch for DB uploads

[data]
save_to_csv = true           # Keep CSV backups
save_to_database = true      # Upload to Supabase
start_year = 2020
end_year = 2025
```

---

## 🔍 Output Format

### Console Output
```
QUARTERBACK STATS - Fantasy & NGS Metrics
Player                Team  Att   Yds  TD  INT  FPts  PPG  FP/Att  TTT  CPOE  Rating
Matthew Stafford      LAR   183  1503  11    2 100.1 16.7   0.547 2.65   2.6   107.3
Jared Goff            DET   145  1187  12    2  91.5 15.2   0.631 2.91   7.9   120.7
```

### CSV Files
- Clean, formatted data
- All 25-32 columns
- Ready for Excel/analysis

### Supabase Tables
- Optimized indexes
- Upsert on conflict
- Queryable via API

---

## 🛠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| "Supabase client not configured" | Set environment variables or check `.env` file |
| "relation does not exist" | Run `create_tables.sql` in Supabase |
| Data not uploading | Check console for errors, verify credentials |
| Duplicate key errors | Normal! Pipeline uses upsert to update records |
| Missing data | Check if year has available data (2025 NGS available) |

---

## 📚 Documentation

- **[README.md](README.md)** - Overview and usage
- **[DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)** - Complete schema reference
- **[SETUP_DATABASE.md](SETUP_DATABASE.md)** - Detailed database setup
- **[create_tables.sql](create_tables.sql)** - SQL schema

---

## 💡 Pro Tips

1. **Always keep CSV backups** - Set `save_to_csv = true`
2. **Use service_role key** for pipeline (write access)
3. **Enable RLS on tables** for production apps
4. **Run pipeline weekly** to keep data fresh
5. **Query combined view** for full player stats across all categories

---

## 🎉 Next Steps

1. ✅ Run your first fetch with database upload
2. ✅ Query data in Supabase SQL Editor
3. ✅ Integrate into your Next.js app
4. ✅ Build a fantasy dashboard
5. ✅ Set up automated updates (cron)

**Need help?** Check the other MD files or review the code comments!

