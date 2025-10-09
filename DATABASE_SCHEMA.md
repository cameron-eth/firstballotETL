# NFL NGS Database Schema

Complete PostgreSQL/Supabase schema for storing NFL Next Gen Stats with fantasy scoring.

## 📊 Table Structure

### **3 Main Tables**

1. **nfl_ngs_passing_stats** - QB passing metrics (32 columns)
2. **nfl_ngs_rushing_stats** - All position rushing metrics (25 columns)
3. **nfl_ngs_receiving_stats** - WR/TE/RB receiving metrics (27 columns)

### **1 Combined View**

- **nfl_player_combined_stats** - Unified view joining all three tables

---

## 🗃️ Table Schemas

### Table 1: nfl_ngs_passing_stats

**Purpose:** Store QB passing stats and advanced NGS metrics

**Key Columns:**
- **Basic Stats:** attempts, completions, pass_yards, pass_touchdowns, interceptions, passer_rating
- **NGS Metrics:** 
  - `avg_time_to_throw` - Time from snap to throw
  - `completion_percentage_above_expectation` (CPOE) - Key efficiency metric
  - `avg_intended_air_yards` - Average depth of target
  - `aggressiveness` - Percentage of tight window throws
- **Fantasy:** fantasy_points, fantasy_ppg, fantasy_points_per_attempt

**Positions:** QB (primary)

---

### Table 2: nfl_ngs_rushing_stats

**Purpose:** Store rushing stats for all positions with NGS tracking data

**Key Columns:**
- **Basic Stats:** rush_attempts, rush_yards, avg_rush_yards, rush_touchdowns
- **NGS Metrics:**
  - `efficiency` - Yards gained vs expected (key metric)
  - `rush_yards_over_expected_per_att` - RYOE per attempt
  - `percent_attempts_gte_eight_defenders` - Stacked box %
  - `avg_time_to_los` - Speed to line of scrimmage
- **Fantasy:** fantasy_points, fantasy_ppg, fantasy_points_per_rush

**Positions:** RB (primary), QB, WR, TE

---

### Table 3: nfl_ngs_receiving_stats

**Purpose:** Store receiving stats with NGS separation/route metrics

**Key Columns:**
- **Basic Stats:** targets, receptions, yards, rec_touchdowns, catch_percentage
- **NGS Metrics:**
  - `avg_separation` - Distance from nearest defender at catch
  - `avg_cushion` - Defender cushion at snap
  - `avg_yac_above_expectation` - YAC over expected
  - `percent_share_of_intended_air_yards` - Air yard market share
- **Fantasy:** fantasy_points, fantasy_ppg, fantasy_points_per_reception, fantasy_points_per_target

**Positions:** WR (primary), TE, RB

---

## 🚀 Setup Instructions

### 1. Create Tables in Supabase

```bash
# Copy SQL to Supabase SQL Editor
cat create_tables.sql

# Or run directly if you have psql
psql $DATABASE_URL -f create_tables.sql
```

### 2. Set Row Level Security (RLS) - Optional

```sql
-- Enable RLS on all tables
ALTER TABLE nfl_ngs_passing_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE nfl_ngs_rushing_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE nfl_ngs_receiving_stats ENABLE ROW LEVEL SECURITY;

-- Allow public read access
CREATE POLICY "Public read access" ON nfl_ngs_passing_stats FOR SELECT USING (true);
CREATE POLICY "Public read access" ON nfl_ngs_rushing_stats FOR SELECT USING (true);
CREATE POLICY "Public read access" ON nfl_ngs_receiving_stats FOR SELECT USING (true);

-- Restrict write access (service role only)
CREATE POLICY "Service role only" ON nfl_ngs_passing_stats FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role only" ON nfl_ngs_rushing_stats FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role only" ON nfl_ngs_receiving_stats FOR ALL USING (auth.role() = 'service_role');
```

---

## 📥 Loading Data

### Update Pipeline to Use Supabase

Uncomment the Supabase code in `impl.py` and `utils.py`:

```python
# In utils.py - use upload_to_supabase instead of save_dataframe
upload_to_supabase(
    df=df,
    table_name='nfl_ngs_passing_stats',
    supabase_client=supabase,
    batch_size=1000,
    verbose=True
)
```

---

## 🔍 Example Queries

### Top Fantasy QBs (2025)

```sql
SELECT 
    player_display_name,
    team_abbr,
    pass_yards,
    pass_touchdowns,
    fantasy_points,
    fantasy_ppg,
    completion_percentage_above_expectation as cpoe,
    passer_rating
FROM nfl_ngs_passing_stats
WHERE season = 2025 
    AND player_position = 'QB'
    AND attempts >= 50
ORDER BY fantasy_points DESC
LIMIT 15;
```

### Efficient RBs (High Fantasy Points Per Touch)

```sql
SELECT 
    player_display_name,
    team_abbr,
    rush_attempts,
    rush_yards,
    rush_touchdowns,
    fantasy_points,
    fantasy_points_per_rush,
    efficiency,
    rush_yards_over_expected_per_att as ryoe
FROM nfl_ngs_rushing_stats
WHERE season = 2025 
    AND player_position = 'RB'
    AND rush_attempts >= 30
ORDER BY fantasy_points_per_rush DESC
LIMIT 15;
```

### WRs with Best Separation

```sql
SELECT 
    player_display_name,
    team_abbr,
    targets,
    receptions,
    yards,
    rec_touchdowns,
    fantasy_points,
    avg_separation,
    avg_cushion,
    catch_percentage
FROM nfl_ngs_receiving_stats
WHERE season = 2025 
    AND player_position = 'WR'
    AND targets >= 20
ORDER BY avg_separation DESC
LIMIT 15;
```

### Combined View - Top Overall Fantasy Performers

```sql
SELECT 
    player_name,
    position,
    team,
    pass_yards,
    rush_yards,
    receiving_yards,
    total_fantasy_points,
    week
FROM nfl_player_combined_stats
WHERE season = 2025
ORDER BY total_fantasy_points DESC
LIMIT 50;
```

### Week-by-Week QB Performance

```sql
SELECT 
    week,
    player_display_name,
    team_abbr,
    pass_yards,
    pass_touchdowns,
    fantasy_points,
    completion_percentage_above_expectation as cpoe
FROM nfl_ngs_passing_stats
WHERE season = 2025 
    AND player_gsis_id = '00-0033873' -- Patrick Mahomes
ORDER BY week;
```

### Position Group Averages

```sql
SELECT 
    player_position,
    COUNT(*) as player_count,
    AVG(fantasy_points) as avg_fantasy_points,
    AVG(fantasy_ppg) as avg_ppg,
    AVG(avg_separation) as avg_separation,
    AVG(catch_percentage) as avg_catch_pct
FROM nfl_ngs_receiving_stats
WHERE season = 2025 
    AND targets >= 10
GROUP BY player_position
ORDER BY avg_fantasy_points DESC;
```

---

## 📋 Column Reference

### Common Columns (All Tables)

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `season` | INTEGER | NFL season year |
| `season_type` | VARCHAR(10) | 'REG' or 'POST' |
| `week` | INTEGER | Week number (0 = preseason) |
| `player_gsis_id` | VARCHAR(20) | NFL GSIS player ID |
| `player_display_name` | VARCHAR(100) | Full player name |
| `player_position` | VARCHAR(5) | Position (QB/RB/WR/TE) |
| `team_abbr` | VARCHAR(5) | Team abbreviation |

### Fantasy Scoring Formula

**Passing:**
```
fantasy_points = (pass_yards * 0.04) + (pass_touchdowns * 4) + (interceptions * -2)
```

**Rushing:**
```
fantasy_points = (rush_yards * 0.1) + (rush_touchdowns * 6)
```

**Receiving (PPR):**
```
fantasy_points = (receptions * 1) + (yards * 0.1) + (rec_touchdowns * 6)
```

---

## 🎯 Key NGS Metrics Explained

### Passing
- **CPOE** (Completion % Above Expectation) - Measures QB accuracy vs expected
- **Time to Throw** - Speed of decision making
- **Aggressiveness** - % of tight window throws

### Rushing
- **Efficiency** - Yards gained relative to expected based on blocking
- **RYOE** (Rush Yards Over Expected) - Actual vs expected yards
- **8+ Defenders %** - How often facing stacked box

### Receiving
- **Separation** - Distance from defender at catch (higher = better)
- **Cushion** - Space given at snap (varies by route)
- **YAC Above Expected** - After-catch ability

---

## 📊 Indexes

All tables include indexes on:
- `player_gsis_id` - Fast player lookups
- `season`, `week` - Fast date range queries
- `team_abbr` - Fast team queries
- `player_position` - Fast position filtering
- `fantasy_points` - Fast leaderboard queries

---

## 🔄 Auto-Updates

Tables include:
- **Unique constraints** on (player_gsis_id, season, season_type, week)
- **Auto-updating timestamps** via triggers
- **Default values of 0** for stats (no NULLs)

This ensures clean upsert operations and consistent data.

