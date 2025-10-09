# Database Setup Guide

Complete guide to connect your NFL data pipeline to Supabase.

## 🚀 Quick Start (5 minutes)

### Step 1: Get Supabase Credentials

1. Go to [Supabase Dashboard](https://supabase.com/dashboard)
2. Select your project
3. Go to **Settings** → **API**
4. Copy:
   - **Project URL** (looks like: `https://xxxxx.supabase.co`)
   - **Service Role Key** (secret key, starts with `eyJ...`)

### Step 2: Set Environment Variables

```bash
# In Pipeline directory
cd Pipeline

# Set environment variables (macOS/Linux)
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_KEY="your-service-role-key-here"

# Or create .env file
cp env.example .env
# Edit .env and add your credentials
```

### Step 3: Create Database Tables

```bash
# Run the SQL script in Supabase SQL Editor
# Copy contents of create_tables.sql and execute
```

### Step 4: Run Pipeline with Database Upload

```bash
# Fetch 2025 NGS data and upload to Supabase
python impl.py --mode ngs --ngs-types passing rushing receiving --year 2025
```

Done! Your data is now in Supabase 🎉

---

## 📝 Detailed Setup

### A. Create .env File

```bash
cd Pipeline
nano .env
```

Add:
```bash
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_SERVICE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### B. Configure Pipeline Settings

Edit `config.toml` if needed:

```toml
[database]
enable_database = true  # Set to false to disable uploads

[data]
save_to_csv = true      # Keep CSV backups
save_to_database = true # Upload to Supabase
```

### C. Verify Connection

```bash
# Test connection (will show warning if not configured)
python impl.py --mode ngs --ngs-types passing --year 2024
```

---

## 🔄 Data Flow

```
1. Pipeline fetches NFL data
         ↓
2. Adds fantasy scoring
         ↓
3. Saves to CSV (backup)
         ↓
4. Uploads to Supabase (upsert)
         ↓
5. Data queryable via API
```

---

## 📊 What Gets Uploaded

### Table: nfl_ngs_passing_stats
- **192 records** for 2025 (QBs)
- **32 columns** including fantasy points + NGS metrics

### Table: nfl_ngs_rushing_stats  
- **204 records** for 2025 (all positions)
- **25 columns** including fantasy points + efficiency

### Table: nfl_ngs_receiving_stats
- **497 records** for 2025 (WR/TE/RB)
- **27 columns** including fantasy points + separation

---

## 🔍 Verify Data Upload

### Query in Supabase SQL Editor:

```sql
-- Check record counts
SELECT 'passing' as table, COUNT(*) as records 
FROM nfl_ngs_passing_stats WHERE season = 2025
UNION ALL
SELECT 'rushing', COUNT(*) 
FROM nfl_ngs_rushing_stats WHERE season = 2025
UNION ALL
SELECT 'receiving', COUNT(*) 
FROM nfl_ngs_receiving_stats WHERE season = 2025;

-- View top QBs
SELECT player_display_name, team_abbr, 
       fantasy_points, fantasy_ppg, passer_rating
FROM nfl_ngs_passing_stats
WHERE season = 2025
ORDER BY fantasy_points DESC
LIMIT 10;
```

---

## ⚙️ Configuration Options

### Disable Database Uploads (CSV only):

**Option 1:** Edit `config.toml`
```toml
[data]
save_to_database = false
```

**Option 2:** Don't set environment variables (pipeline will skip upload)

### Upload Only (No CSV):

```toml
[data]
save_to_csv = false
save_to_database = true
```

---

## 🔐 Security Best Practices

1. **Never commit .env file** - Already in `.gitignore`
2. **Use service_role key** for pipeline (has write access)
3. **Use anon key** for frontend (read-only via RLS)
4. **Enable RLS** on tables for production:

```sql
ALTER TABLE nfl_ngs_passing_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE nfl_ngs_rushing_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE nfl_ngs_receiving_stats ENABLE ROW LEVEL SECURITY;

-- Allow public reads
CREATE POLICY "Public read" ON nfl_ngs_passing_stats 
  FOR SELECT USING (true);
  
-- Restrict writes to service role
CREATE POLICY "Service writes" ON nfl_ngs_passing_stats 
  FOR ALL USING (auth.role() = 'service_role');
```

---

## 🛠️ Troubleshooting

### Error: "Supabase client not configured"
**Solution:** Set environment variables or check `.env` file

### Error: "duplicate key value violates unique constraint"
**Solution:** This is expected! Pipeline uses UPSERT to update existing records

### Error: "relation does not exist"
**Solution:** Run `create_tables.sql` in Supabase SQL Editor

### Data not appearing in Supabase
**Solution:** 
1. Check console output for upload confirmation
2. Verify credentials are correct
3. Check Supabase logs for errors

---

## 📈 Next Steps

### 1. Query Data via API

```typescript
// In your Next.js app
const { data, error } = await supabase
  .from('nfl_ngs_passing_stats')
  .select('*')
  .eq('season', 2025)
  .order('fantasy_points', { ascending: false })
  .limit(15);
```

### 2. Set Up Automated Updates

```bash
# Add to crontab (daily at 6 AM)
0 6 * * * cd /path/to/Pipeline && python impl.py --mode all --year 2025
```

### 3. Build Dashboard

Use the combined view for full player stats:

```sql
SELECT * FROM nfl_player_combined_stats 
WHERE season = 2025 
ORDER BY total_fantasy_points DESC;
```

---

## 📋 Checklist

- [ ] Created Supabase project
- [ ] Got API credentials (URL + service key)
- [ ] Set environment variables
- [ ] Ran `create_tables.sql`
- [ ] Tested pipeline upload
- [ ] Verified data in Supabase
- [ ] (Optional) Enabled RLS
- [ ] (Optional) Set up automated runs

---

## 💡 Tips

- **Upsert behavior**: Duplicate records are automatically updated (not errors)
- **Batch uploads**: Data uploads in chunks of 1000 records
- **CSV backup**: Always saved locally even when uploading to database
- **Multiple years**: Run pipeline for each year to build historical database

```bash
# Build full historical database
for year in 2020 2021 2022 2023 2024 2025; do
  python impl.py --mode ngs --ngs-types passing rushing receiving --year $year
  sleep 5
done
```

