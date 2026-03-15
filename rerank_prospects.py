#!/usr/bin/env python3
"""
Rerank Prospects Historically by Position

After combine data is updated (height/weight), this script:
1. Recalculates grades for prospects using updated combine data
2. Reranks prospects by overall_grade within draft_year and position
3. Updates the rank field in the database

Usage:
    python rerank_prospects.py --position TE
    python rerank_prospects.py --position TE --draft-year 2025
    python rerank_prospects.py --position TE --historical
    python rerank_prospects.py --position TE --dry-run
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import config
from prospect_grading import (
    calculate_prospect_grade,
    score_hs_recruiting,
    score_college_production,
    score_draft_projection,
    score_physical_measurables,
    score_expert_consensus,
    score_age_factor,
    get_grade_tier,
    GRADE_WEIGHTS,
    get_grade_weights,
    apply_star_effect,
    STAR_EFFECT_PROSPECTS,
    normalize_player_name,
)


def estimate_draft_round_from_rank(rank: int, draft_year: int) -> tuple:
    """
    Estimate draft round from prospect rank for upcoming classes.
    Returns (projected_round, projected_pick)
    """
    if rank <= 5:
        return (1, rank * 2)  # Top 5 -> picks 2-10
    elif rank <= 12:
        return (1, 10 + (rank - 5) * 2)  # 6-12 -> picks 12-24
    elif rank <= 24:
        return (2, 32 + (rank - 12))  # 13-24 -> round 2
    elif rank <= 36:
        return (3, 64 + (rank - 24))  # 25-36 -> round 3
    elif rank <= 50:
        return (4, None)
    elif rank <= 75:
        return (5, None)
    else:
        return (6, None)


def get_tier_from_grade(grade: float) -> tuple:
    """
    Determine tier and tier_numeric from overall grade.
    Returns (tier_string, tier_numeric)
    """
    # Aligned with evidence-weighted tier thresholds.
    if grade >= 90:
        return ("Tier 1", 1)
    elif grade >= 85:
        return ("Tier 2", 2)
    elif grade >= 78:
        return ("Tier 3", 3)
    elif grade >= 70:
        return ("Tier 4", 4)
    elif grade >= 60:
        return ("Tier 5", 5)
    else:
        return ("Tier 6", 6)


def recalculate_grade(prospect: dict) -> dict:
    """
    Recalculate grade for a prospect using updated combine data.
    Returns dict of grade fields to update.
    """
    position = prospect.get('position', 'TE')
    current_rank = prospect.get('rank') or 50
    draft_year = prospect.get('draft_year') or 2026
    
    # HS recruiting data
    hs_stars = prospect.get('hs_stars')
    hs_rank = prospect.get('hs_rank')
    hs_rating = prospect.get('hs_rating')
    
    # College stats
    college_stats = prospect.get('college_stats') or {}
    if isinstance(college_stats, str):
        import json
        try:
            college_stats = json.loads(college_stats)
        except:
            college_stats = {}
    college_games = prospect.get('college_games') or 0
    class_year = prospect.get('class')
    age_at_draft = None
    if isinstance(college_stats, dict):
        age_at_draft = college_stats.get('age_at_draft') or college_stats.get('age')
    
    # Physical measurables (updated from combine in college_stats when available)
    height = prospect.get('height')
    weight = prospect.get('weight')
    forty_time = (
        college_stats.get('forty_time')
        or college_stats.get('forty')
        or college_stats.get('40yd')
        or prospect.get('forty_time')
    )
    vertical = college_stats.get('vertical')
    broad_jump = college_stats.get('broad_jump') or college_stats.get('broad')
    bench = college_stats.get('bench')
    three_cone = college_stats.get('three_cone') or college_stats.get('3cone')
    shuttle = college_stats.get('shuttle')
    
    # For historical prospects with actual draft data, use that.
    # For current draft-year players without finalized draft capital, do NOT penalize.
    # For other classes without draft capital, estimate from current rank WITH a discount.
    draft_round = prospect.get('draft_round_projection')
    draft_pick = college_stats.get('draft_overall_pick') if isinstance(college_stats, dict) else None

    current_year = datetime.now().year
    if not draft_round:
        if int(draft_year) < current_year:
            # Already-drafted class with missing capital:
            # estimate from rank, but apply a conservative haircut.
            er, ep = estimate_draft_round_from_rank(current_rank, draft_year)
            draft_score = max(35.0, score_draft_projection(er, ep) - 8.0)
            draft_round, draft_pick = er, ep
        elif int(draft_year) == current_year:
            # Current draft class: use full rank-estimated capital, no compression.
            draft_round, draft_pick = estimate_draft_round_from_rank(current_rank, draft_year)
            draft_score = score_draft_projection(draft_round, draft_pick)
        else:
            # Future classes: estimate from rank, but apply uncertainty discount.
            draft_round, draft_pick = estimate_draft_round_from_rank(current_rank, draft_year)
            raw_draft_score = score_draft_projection(draft_round, draft_pick)
            # Compress toward neutral (60) — top picks still score well, but not 95.
            draft_score = 60.0 + (raw_draft_score - 60.0) * 0.55
    else:
        draft_score = score_draft_projection(draft_round, draft_pick)
    
    # Calculate component scores
    hs_score = score_hs_recruiting(hs_stars, hs_rank, hs_rating)
    production_score = score_college_production(position, college_stats, college_games)
    physical_score = score_physical_measurables(
        position,
        height,
        weight,
        forty_time=forty_time,
        vertical=vertical,
        broad_jump=broad_jump,
        bench=bench,
        three_cone=three_cone,
        shuttle=shuttle,
        draft_year=draft_year,
    )
    consensus_score = score_expert_consensus(current_rank)
    age_score = score_age_factor(class_year, age_at_draft)
    weights = get_grade_weights(draft_year, draft_round, draft_pick)
    
    # Calculate weighted overall grade and normalize by total configured weight
    weighted_total = (
        hs_score * weights['hs_recruiting'] +
        production_score * weights['college_production'] +
        draft_score * weights['draft_projection'] +
        physical_score * weights['physical_measurables'] +
        consensus_score * weights['expert_consensus'] +
        age_score * weights['age_factor']
    )
    total_weight = sum(weights.values()) or 1.0
    overall = weighted_total / total_weight

    # ── Historical drafted classes: stretch curve ──
    has_real_capital = bool(
        college_stats.get('draft_overall_pick') if isinstance(college_stats, dict) else False
    )
    if int(draft_year) < current_year and has_real_capital:
        overall = 60.0 + (overall - 60.0) * 1.25
        overall = min(overall, 100.0)

    # ── Future classes: strong completeness penalty + hard ceiling ──
    if int(draft_year) > current_year:
        has_hs = bool(hs_stars or hs_rank or hs_rating)
        has_production = bool(college_stats and college_games and college_games > 0)
        has_combine = bool(forty_time or vertical or broad_jump or bench or three_cone or shuttle)

        evidence = float(sum([has_hs, has_production, has_combine]))  # 0-3
        confidence = evidence / 3.0

        prior = 64.0
        blend = 0.60 + 0.40 * confidence
        overall = prior + (overall - prior) * blend

    overall, _ = apply_star_effect(prospect.get('name'), overall, draft_year, rank=current_rank)

    overall = round(overall, 2)
    
    # Determine tier
    tier, tier_numeric = get_tier_from_grade(overall)
    grade_tier = get_grade_tier(overall)
    
    return {
        'overall_grade': overall,
        'tier': tier,
        'tier_numeric': tier_numeric,
        'grade_tier': grade_tier,
        'hs_recruiting_score': round(hs_score, 2),
        'college_production_score': round(production_score, 2),
        'draft_projection_score': round(draft_score, 2),
        'physical_measurables_score': round(physical_score, 2),
        'expert_consensus_score': round(consensus_score, 2),
        'draft_year': draft_year,
        'updated_at': datetime.now().isoformat()
    }


def rerank_prospects(
    position: str,
    draft_year: Optional[int] = None,
    historical: bool = False,
    dry_run: bool = False
) -> bool:
    """
    Rerank prospects by position after recalculating grades.
    
    Args:
        position: Position to rerank (TE, WR, RB, QB)
        draft_year: Specific draft year to rerank (None = all years)
        historical: If True, only rerank historical prospects (draft_year < current year)
        dry_run: If True, don't update database, just show what would change
    """
    print("=" * 80)
    print(f"RERANKING {position} PROSPECTS")
    if draft_year:
        print(f"  Draft Year: {draft_year}")
    if historical:
        print(f"  Historical Only: Yes")
    if dry_run:
        print(f"  DRY RUN MODE: No database updates")
    print("=" * 80)
    
    supabase = config.get_supabase_client()
    if not supabase:
        print("❌ Failed to get Supabase client")
        return False
    
    # Build query
    query = supabase.table('dynasty_prospects').select('*')
    query = query.eq('position', position.upper())
    
    if draft_year:
        query = query.eq('draft_year', draft_year)
    elif historical:
        current_year = datetime.now().year
        query = query.lt('draft_year', current_year)
    
    query = query.order('draft_year', desc=True).order('rank')
    
    # Fetch prospects
    print("\n📊 Fetching prospects...")
    result = query.execute()
    
    if not result.data:
        print(f"❌ No {position} prospects found")
        return False
    
    prospects = result.data
    print(f"   Found {len(prospects)} {position} prospects")
    
    # Group by draft year
    by_year = {}
    for p in prospects:
        year = p.get('draft_year') or 'Unknown'
        if year not in by_year:
            by_year[year] = []
        by_year[year].append(p)
    
    print("\n   Prospects by year:")
    for year in sorted(by_year.keys(), reverse=True):
        print(f"      {year}: {len(by_year[year])} prospects")
    
    # Recalculate grades and rerank
    print("\n🔄 Recalculating grades and reranking...")
    all_updates = []
    rank_changes = []
    
    for year, year_prospects in sorted(by_year.items(), reverse=True):
        print(f"\n   Processing {year} class ({len(year_prospects)} {position}s)...")
        
        # Recalculate grades for all prospects in this year
        graded_prospects = []
        for prospect in year_prospects:
            prospect_id = prospect.get('id')
            name = prospect.get('name', 'Unknown')
            old_rank = prospect.get('rank')
            
            try:
                # Recalculate grade
                grades = recalculate_grade(prospect)
                
                # Store with original data for ranking
                graded_prospects.append({
                    'id': prospect_id,
                    'name': name,
                    'old_rank': old_rank,
                    'overall_grade': grades['overall_grade'],
                    'grades': grades,
                    'prospect': prospect
                })
            except Exception as e:
                print(f"      ❌ Error grading {name}: {e}")

        # Sort by overall_grade (descending) to determine new ranks
        graded_prospects.sort(key=lambda x: x['overall_grade'], reverse=True)
        
        # Assign new ranks
        for new_rank, item in enumerate(graded_prospects, 1):
            old_rank = item['old_rank']
            name = item['name']
            grade = item['overall_grade']
            
            # Prepare update with new rank
            update = {
                'id': item['id'],
                'rank': new_rank,
                **item['grades']  # Include all grade fields
            }
            all_updates.append(update)
            
            # Track rank changes
            if old_rank is not None and old_rank != new_rank:
                rank_changes.append({
                    'name': name,
                    'year': year,
                    'old_rank': old_rank,
                    'new_rank': new_rank,
                    'grade': grade,
                    'change': new_rank - old_rank
                })
            elif old_rank is None:
                rank_changes.append({
                    'name': name,
                    'year': year,
                    'old_rank': 'N/A',
                    'new_rank': new_rank,
                    'grade': grade,
                    'change': 0
                })
        
        # Show top 5 for this year
        print(f"      Top 5 {position}s for {year}:")
        for idx, item in enumerate(graded_prospects[:5], 1):
            old_r = item['old_rank']
            change_str = ""
            if old_r is not None and old_r != idx:
                change = idx - old_r
                change_str = f" ({change:+d})" if change != 0 else ""
            old_rank_str = f"#{old_r}" if old_r is not None else "unranked"
            print(f"         {idx:2}. {item['name']:25} Grade: {item['overall_grade']:5.1f} "
                  f"(was {old_rank_str}{change_str})")
    
    # Summary of rank changes
    print("\n" + "=" * 80)
    print("RANK CHANGES SUMMARY")
    print("=" * 80)
    
    if rank_changes:
        # Sort by absolute change (biggest movers first)
        rank_changes.sort(key=lambda x: abs(x['change']), reverse=True)
        
        print(f"\n   Total prospects with rank changes: {len(rank_changes)}")
        print(f"\n   Biggest Movers:")
        for change in rank_changes[:10]:
            direction = "↑" if change['change'] < 0 else "↓"
            print(f"      {direction} {change['name']:25} {change['year']}: "
                  f"#{change['old_rank']} → #{change['new_rank']} "
                  f"({change['change']:+d}) Grade: {change['grade']:.1f}")
    else:
        print("\n   ✓ No rank changes (all ranks already correct)")
    
    # Update database (one at a time to use .update().eq())
    if not dry_run:
        print("\n💾 Updating database...")
        total_updated = 0
        errors = 0
        
        for update in all_updates:
            prospect_id = update.pop('id')
            try:
                supabase.table('dynasty_prospects').update(update).eq('id', prospect_id).execute()
                total_updated += 1
                if total_updated % 20 == 0:
                    print(f"   ✓ Updated {total_updated}/{len(all_updates)} prospects")
            except Exception as e:
                errors += 1
                print(f"   ❌ Error updating id={prospect_id}: {e}")
        
        print(f"\n✅ Successfully updated {total_updated} prospects ({errors} errors)")
    else:
        print(f"\n🔍 DRY RUN: Would update {len(all_updates)} prospects")
        print("   Run without --dry-run to apply changes")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Rerank prospects historically by position after combine data updates'
    )
    parser.add_argument(
        '--position',
        '-p',
        type=str,
        choices=['TE', 'WR', 'RB', 'QB'],
        required=True,
        help='Position to rerank'
    )
    parser.add_argument(
        '--draft-year',
        '-y',
        type=int,
        help='Specific draft year to rerank (default: all years)'
    )
    parser.add_argument(
        '--historical',
        action='store_true',
        help='Only rerank historical prospects (draft_year < current year)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would change without updating database'
    )
    
    args = parser.parse_args()
    
    try:
        success = rerank_prospects(
            position=args.position,
            draft_year=args.draft_year,
            historical=args.historical,
            dry_run=args.dry_run
        )
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
