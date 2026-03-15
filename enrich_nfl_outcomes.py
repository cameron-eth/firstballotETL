#!/usr/bin/env python3
"""
NFL Career Outcome Enrichment

Matches dynasty_prospects with master_player_stats to calculate an
NFL outcome score for historical (drafted) prospects.

This bridges the gap between "prospect grade" and "actual NFL value" —
e.g., Malik Willis was a 3rd-round pick (low prospect grade) but
became a starting QB (high NFL outcome).

Usage:
    python enrich_nfl_outcomes.py
    python enrich_nfl_outcomes.py --dry-run
"""

import sys
import argparse
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))
from config import config


def normalize_name(name: str) -> str:
    """Normalize name for fuzzy matching between prospect and NFL tables."""
    s = (name or '').lower().strip()
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", '', s)
    s = s.replace("'", "").replace("'", "").replace("-", " ")
    return re.sub(r'\s+', ' ', s).strip()


def calculate_nfl_outcome_score(
    peak_ppg: float,
    career_ppg: float,
    total_games: int,
    seasons: int,
    position: str,
) -> float:
    """
    Calculate NFL outcome score (0-100) from career stats.

    Components:
    - Peak fantasy PPG (40%): best single-season output
    - Career fantasy PPG (25%): sustained production
    - Games played (20%): durability/starter status
    - Seasons in league (15%): longevity

    Position-aware thresholds:
    - QB: 20+ ppg = elite, 15 = solid, 10 = backup
    - RB: 16+ ppg = elite, 12 = solid, 8 = backup
    - WR: 16+ ppg = elite, 12 = solid, 7 = backup
    - TE: 12+ ppg = elite, 8 = solid, 5 = backup
    """
    import numpy as np

    pos = position.upper()
    # Position-specific PPG thresholds [low, elite]
    ppg_range = {
        'QB': (5, 22),
        'RB': (4, 18),
        'WR': (4, 18),
        'TE': (3, 14),
    }.get(pos, (4, 18))

    peak_score = float(np.interp(peak_ppg, [ppg_range[0], ppg_range[1]], [20, 100]))
    career_score = float(np.interp(career_ppg, [ppg_range[0], ppg_range[1] * 0.8], [15, 95]))
    games_score = float(np.interp(total_games, [0, 16, 48, 100, 180], [0, 25, 55, 80, 100]))
    seasons_score = float(np.interp(seasons, [0, 1, 3, 5, 8], [0, 20, 50, 75, 100]))

    score = (
        peak_score * 0.40 +
        career_score * 0.25 +
        games_score * 0.20 +
        seasons_score * 0.15
    )

    return round(min(100.0, max(0.0, score)), 1)


def main():
    parser = argparse.ArgumentParser(description='Enrich dynasty prospects with NFL career outcomes')
    parser.add_argument('--dry-run', action='store_true', help='Show changes without updating')
    args = parser.parse_args()

    print("=" * 80)
    print("NFL CAREER OUTCOME ENRICHMENT")
    print("=" * 80)

    supabase = config.get_supabase_client()
    if not supabase:
        print("❌ Failed to get Supabase client")
        return False

    # Fetch all historical prospects (drafted classes)
    current_year = datetime.now().year
    print("\n📊 Fetching historical prospects...")
    prospects_result = supabase.table('dynasty_prospects') \
        .select('id, name, position, draft_year, rank, overall_grade, sleeper_id') \
        .lt('draft_year', current_year) \
        .order('draft_year', desc=True) \
        .execute()

    if not prospects_result.data:
        print("❌ No historical prospects found")
        return False

    prospects = prospects_result.data
    print(f"   Found {len(prospects)} historical prospects")

    # Fetch NFL stats from master_player_stats
    # Get all seasons for each player to calculate career aggregates
    print("\n📊 Fetching NFL career stats...")
    skill_positions = ['QB', 'RB', 'WR', 'TE']
    stats_result = supabase.table('master_player_stats') \
        .select('player_display_name, position, season, fantasy_ppg, games_played') \
        .in_('position', skill_positions) \
        .gte('games_played', 1) \
        .execute()

    if not stats_result.data:
        print("❌ No NFL stats found in master_player_stats")
        return False

    nfl_rows = stats_result.data
    print(f"   Found {len(nfl_rows)} NFL player-season records")

    # Aggregate NFL stats by player
    nfl_careers: Dict[str, Dict] = {}
    for row in nfl_rows:
        name = normalize_name(row.get('player_display_name', ''))
        if not name:
            continue

        if name not in nfl_careers:
            nfl_careers[name] = {
                'position': row.get('position', ''),
                'seasons': [],
                'total_games': 0,
                'peak_ppg': 0.0,
                'career_ppg_sum': 0.0,
                'career_ppg_count': 0,
            }

        career = nfl_careers[name]
        ppg = float(row.get('fantasy_ppg') or 0)
        games = int(row.get('games_played') or 0)
        season = row.get('season')

        if season not in career['seasons']:
            career['seasons'].append(season)
        career['total_games'] += games
        career['peak_ppg'] = max(career['peak_ppg'], ppg)
        if ppg > 0:
            career['career_ppg_sum'] += ppg
            career['career_ppg_count'] += 1

    print(f"   Aggregated careers for {len(nfl_careers)} NFL players")

    # Match prospects to NFL careers and calculate outcome scores
    print("\n🔄 Matching and scoring...")
    matched = 0
    updates = []

    for prospect in prospects:
        p_name = normalize_name(prospect.get('name', ''))
        position = prospect.get('position', '')

        career = nfl_careers.get(p_name)
        if not career:
            continue

        # Position sanity check (allow some flexibility)
        if career['position'] and position and career['position'] != position:
            # Allow QB↔QB, WR/TE flexibility, etc.
            pass  # Still match — player may have changed position designation

        peak_ppg = career['peak_ppg']
        career_ppg = (career['career_ppg_sum'] / career['career_ppg_count']
                      if career['career_ppg_count'] > 0 else 0)
        total_games = career['total_games']
        seasons = len(career['seasons'])

        nfl_score = calculate_nfl_outcome_score(
            peak_ppg=peak_ppg,
            career_ppg=career_ppg,
            total_games=total_games,
            seasons=seasons,
            position=position,
        )

        updates.append({
            'id': prospect['id'],
            'name': prospect['name'],
            'position': position,
            'draft_year': prospect.get('draft_year'),
            'old_grade': prospect.get('overall_grade'),
            'nfl_outcome_score': nfl_score,
            'nfl_seasons_played': seasons,
            'nfl_peak_ppg': round(peak_ppg, 2),
        })
        matched += 1

    print(f"   Matched {matched}/{len(prospects)} prospects to NFL careers")

    # Show top matches
    updates.sort(key=lambda x: x['nfl_outcome_score'], reverse=True)
    print(f"\n   Top 15 NFL Outcomes:")
    for u in updates[:15]:
        print(f"      {u['name']:25} ({u['position']}, {u['draft_year']}) "
              f"NFL: {u['nfl_outcome_score']:5.1f}  Peak PPG: {u['nfl_peak_ppg']:5.1f}  "
              f"Seasons: {u['nfl_seasons_played']}  Prospect: {u['old_grade']}")

    # Show "biggest surprises" — high NFL outcome but low prospect grade
    print(f"\n   Biggest NFL Surprises (high NFL, low prospect grade):")
    surprises = sorted(updates, key=lambda x: (x['nfl_outcome_score'] or 0) - (x['old_grade'] or 0), reverse=True)
    for u in surprises[:10]:
        diff = (u['nfl_outcome_score'] or 0) - (u['old_grade'] or 0)
        if diff > 5:
            print(f"      {u['name']:25} ({u['position']}, {u['draft_year']}) "
                  f"Prospect: {u['old_grade']:5.1f} → NFL: {u['nfl_outcome_score']:5.1f}  "
                  f"(+{diff:.1f})")

    # Persist
    if not args.dry_run:
        print(f"\n💾 Updating {len(updates)} prospects...")
        errors = 0
        for u in updates:
            try:
                supabase.table('dynasty_prospects').update({
                    'nfl_outcome_score': u['nfl_outcome_score'],
                    'nfl_seasons_played': u['nfl_seasons_played'],
                    'nfl_peak_ppg': u['nfl_peak_ppg'],
                }).eq('id', u['id']).execute()
            except Exception as e:
                errors += 1
                print(f"   ❌ Error updating {u['name']}: {e}")

        print(f"\n✅ Updated {len(updates) - errors} prospects ({errors} errors)")
    else:
        print(f"\n🔍 DRY RUN: Would update {len(updates)} prospects")

    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
