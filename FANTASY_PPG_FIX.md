# Fantasy PPG Calculation Fix

## Problem Summary
The `fantasy_ppg` (fantasy points per game) calculation in `utils.py` was fundamentally incorrect, causing player fantasy point averages to be dramatically underreported in the League Buddy feature.

## Root Cause

### OLD (BROKEN) Logic
```python
df['fantasy_ppg'] = df['fantasy_points'] / df.groupby('player_gsis_id')['week'].transform('count')
```

This divides **each individual week's fantasy points** by the **total number of weeks the player appeared**.

**Example:** Player with 3 weeks:
- Week 1: 26 pts → PPG = 26 / 3 = **8.67** ❌
- Week 2: 13 pts → PPG = 13 / 3 = **4.33** ❌
- Week 3: 37 pts → PPG = 37 / 3 = **12.33** ❌

Each row showed a different (incorrect) PPG value based on that week's points divided by total games.

### NEW (FIXED) Logic
```python
df['fantasy_ppg'] = df.groupby('player_gsis_id')['fantasy_points'].transform('mean')
```

This calculates the **average of all fantasy points** for each player across all weeks.

**Example:** Same player:
- Week 1: 26 pts → PPG = (26+13+37) / 3 = **25.33** ✓
- Week 2: 13 pts → PPG = (26+13+37) / 3 = **25.33** ✓
- Week 3: 37 pts → PPG = (26+13+37) / 3 = **25.33** ✓

All rows correctly show the same PPG value: the true season average.

## Impact
This fix affects all three position types in the ETL pipeline:
- **Passing stats** (QBs)
- **Rushing stats** (RBs)
- **Receiving stats** (WRs/TEs)

## Scoring Rules (PPR)
- **Passing**: 0.1 per yard (1 pt per 10 yards), 6 per TD, -2 per INT
- **Rushing**: 0.1 per yard (1 pt per 10 yards), 6 per TD
- **Receiving**: 1 per reception, 0.1 per yard (1 pt per 10 yards), 6 per TD

## Next Steps
After this fix, you need to:
1. Re-run the ETL pipeline to regenerate all NGS stats with correct `fantasy_ppg` values
2. The corrected data will automatically flow to the League Buddy feature
3. Player projections will now show accurate fantasy points per game

## Files Modified
- `firstballotETL/utils.py` - Lines 232, 242, 253

