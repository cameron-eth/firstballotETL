#!/usr/bin/env python3
"""
Grade All Prospects in dynasty_prospects Table

Calculates and updates grades for all prospects, including historical classes.
Uses the grading system from prospect_grading.py with adjustments for:
- Historical prospects: Uses actual draft position if available
- Current prospects: Uses projected draft position based on rank
"""

import sys
import json
from pathlib import Path
from datetime import datetime

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


def grade_prospect(prospect: dict) -> dict:
    """
    Calculate grades for a single prospect.
    Returns dict of grade fields to update.
    """
    position = prospect.get('position', 'WR')
    rank = prospect.get('rank') or 50
    draft_year = prospect.get('draft_year') or 2026
    
    # HS recruiting data
    hs_stars = prospect.get('hs_stars')
    hs_rank = prospect.get('hs_rank')
    hs_rating = prospect.get('hs_rating')
    
    # College stats
    college_stats = prospect.get('college_stats') or {}
    if isinstance(college_stats, str):
        try:
            college_stats = json.loads(college_stats)
        except Exception:
            college_stats = {}
    if not isinstance(college_stats, dict):
        college_stats = {}
    college_games = prospect.get('college_games') or 0
    class_year = prospect.get('class')
    age_at_draft = None
    if isinstance(college_stats, dict):
        age_at_draft = college_stats.get('age_at_draft') or college_stats.get('age')
    
    # Physical measurables + combine metrics from college_stats when available
    height = prospect.get('height')
    weight = prospect.get('weight')
    forty_time = (
        college_stats.get('forty_time')
        or college_stats.get('forty')
        or college_stats.get('40yd')
    )
    vertical = college_stats.get('vertical')
    broad_jump = college_stats.get('broad_jump') or college_stats.get('broad')
    bench = college_stats.get('bench')
    three_cone = college_stats.get('three_cone') or college_stats.get('3cone')
    shuttle = college_stats.get('shuttle')
    
    # For historical prospects with actual draft data, use that.
    # For current draft-year players without finalized draft capital, do NOT penalize.
    # For other classes without draft capital, estimate from rank WITH a discount.
    draft_round = prospect.get('draft_round_projection')
    draft_pick = college_stats.get('draft_overall_pick') if isinstance(college_stats, dict) else None

    current_year = datetime.now().year
    if not draft_round:
        if int(draft_year) < current_year:
            # Already-drafted class with missing capital:
            # estimate from rank, but apply a conservative haircut.
            er, ep = estimate_draft_round_from_rank(rank, draft_year)
            draft_score = max(35.0, score_draft_projection(er, ep) - 8.0)
            draft_round, draft_pick = er, ep
        elif int(draft_year) == current_year:
            # Neutral-not-penalized score for current class awaiting draft outcomes
            draft_score = 72.0
        else:
            # Future classes: estimate from rank, but apply uncertainty discount.
            # A rank-based projection is NOT the same as verified draft capital.
            draft_round, draft_pick = estimate_draft_round_from_rank(rank, draft_year)
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
    consensus_score = score_expert_consensus(rank)
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
    # Verified draft capital should create clear separation between elite picks
    # and mid/late rounders. A #2 pick (Saquon) should grade ~88, not ~83.
    has_real_capital = bool(
        college_stats.get('draft_overall_pick') if isinstance(college_stats, dict) else False
    )
    if int(draft_year) < current_year and has_real_capital:
        overall = 60.0 + (overall - 60.0) * 1.25
        overall = min(overall, 96.0)

    # ── Future classes: confidence-based evidence regression ──
    # Instead of a hard ceiling, we regress toward a conservative prior based
    # on how much verified evidence exists (Bayesian "prove-it" mechanism).
    # More data → grade stays near raw.  Less data → pulled toward neutral.
    if int(draft_year) > current_year:
        has_hs = bool(hs_stars or hs_rank or hs_rating)
        has_production = bool(college_stats and college_games and college_games > 0)
        has_combine = bool(forty_time or vertical or broad_jump or bench or three_cone or shuttle)

        evidence = float(sum([has_hs, has_production, has_combine]))  # 0-3
        confidence = evidence / 3.0

        prior = 64.0
        blend = 0.60 + 0.40 * confidence
        overall = prior + (overall - prior) * blend

    overall, _ = apply_star_effect(prospect.get('name'), overall, draft_year, rank=rank)

    # ── NFL career outcome blend for historical prospects ──
    # When nfl_outcome_score is available, blend it into the grade so
    # late-round starters (Malik Willis) get credit for NFL success.
    # Blend weight increases with seasons played: 1 season = 20%, 3+ = 40%.
    nfl_outcome = prospect.get('nfl_outcome_score')
    nfl_seasons = prospect.get('nfl_seasons_played') or 0
    if nfl_outcome is not None and int(draft_year) < current_year:
        nfl_outcome = float(nfl_outcome)
        # Weight increases with NFL track record
        if nfl_seasons >= 3:
            nfl_weight = 0.40
        elif nfl_seasons >= 2:
            nfl_weight = 0.30
        else:
            nfl_weight = 0.20
        overall = overall * (1.0 - nfl_weight) + nfl_outcome * nfl_weight

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


def main():
    """Main function to grade all prospects."""
    print("=" * 80)
    print("GRADING ALL PROSPECTS IN DYNASTY_PROSPECTS")
    print("=" * 80)
    
    supabase = config.get_supabase_client()
    if not supabase:
        print("❌ Failed to get Supabase client")
        return False
    
    # Fetch all prospects
    print("\n📊 Fetching prospects...")
    result = supabase.table('dynasty_prospects').select('*').order('draft_year', desc=True).order('rank').execute()
    
    if not result.data:
        print("❌ No prospects found")
        return False
    
    prospects = result.data
    print(f"   Found {len(prospects)} total prospects")
    
    # Group by year for reporting
    by_year = {}
    for p in prospects:
        year = p.get('draft_year') or 'Unknown'
        if year not in by_year:
            by_year[year] = []
        by_year[year].append(p)
    
    print("\n   Prospects by year:")
    for year in sorted(by_year.keys(), reverse=True):
        print(f"      {year}: {len(by_year[year])} prospects")
    
    # Grade each prospect (in-memory first so we can apply class percentile calibration)
    print("\n🔄 Grading prospects...")
    graded_rows = []
    errors = 0

    for i, prospect in enumerate(prospects):
        name = prospect.get('name', 'Unknown')
        draft_year = prospect.get('draft_year', 'N/A')
        try:
            grades = grade_prospect(prospect)
            graded_rows.append({
                'prospect': prospect,
                'grades': grades,
            })
            if (i + 1) % 50 == 0:
                print(f"   Prepared {i + 1}/{len(prospects)}...")
        except Exception as e:
            errors += 1
            print(f"   ❌ Error grading {name} ({draft_year}): {e}")

    # Persist updates
    updated = 0
    for item in graded_rows:
        prospect_id = item['prospect'].get('id')
        try:
            supabase.table('dynasty_prospects').update(item['grades']).eq('id', prospect_id).execute()
            updated += 1
        except Exception as e:
            errors += 1
            name = item['prospect'].get('name', 'Unknown')
            draft_year = item['prospect'].get('draft_year', 'N/A')
            print(f"   ❌ Error updating {name} ({draft_year}): {e}")
    
    # Summary
    print("\n" + "=" * 80)
    print("GRADING COMPLETE")
    print("=" * 80)
    print(f"   ✓ Updated: {updated}")
    print(f"   ✗ Errors: {errors}")
    
    # Show sample of graded prospects by year
    print("\n📊 Sample grades by year:")
    for year in sorted(by_year.keys(), reverse=True)[:3]:
        print(f"\n   {year} Class (top 5):")
        
        # Re-fetch to show updated grades
        sample = supabase.table('dynasty_prospects')\
            .select('name, position, rank, overall_grade, tier, grade_tier')\
            .eq('draft_year', year)\
            .order('rank')\
            .limit(5)\
            .execute()
        
        if sample.data:
            for p in sample.data:
                print(f"      {p.get('rank', 'N/A'):3}. {p.get('name', 'Unknown'):20} "
                      f"({p.get('position', '??')}) - Grade: {p.get('overall_grade', 'N/A')} "
                      f"[{p.get('tier', 'N/A')}]")
    
    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

