#!/usr/bin/env python3
"""
Grade All Prospects in dynasty_prospects Table

Calculates and updates grades for all prospects, including historical classes.
Uses the grading system from prospect_grading.py with adjustments for:
- Historical prospects: Uses actual draft position if available
- Current prospects: Uses projected draft position based on rank
"""

import sys
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
    get_grade_tier,
    GRADE_WEIGHTS
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
    if grade >= 85:
        return ("Tier 1", 1)
    elif grade >= 75:
        return ("Tier 2", 2)
    elif grade >= 65:
        return ("Tier 3", 3)
    elif grade >= 55:
        return ("Tier 4", 4)
    elif grade >= 45:
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
    college_games = prospect.get('college_games') or 0
    
    # Physical measurables
    height = prospect.get('height')
    weight = prospect.get('weight')
    
    # For historical prospects with actual draft data, use that
    # For future prospects, estimate from rank
    draft_round = prospect.get('draft_round_projection')
    draft_pick = None
    
    if not draft_round:
        # Estimate from rank
        draft_round, draft_pick = estimate_draft_round_from_rank(rank, draft_year)
    
    # Calculate component scores
    hs_score = score_hs_recruiting(hs_stars, hs_rank, hs_rating)
    production_score = score_college_production(position, college_stats, college_games)
    draft_score = score_draft_projection(draft_round, draft_pick)
    physical_score = score_physical_measurables(position, height, weight)
    consensus_score = score_expert_consensus(rank)
    
    # Calculate weighted overall grade
    overall = (
        hs_score * GRADE_WEIGHTS['hs_recruiting'] +
        production_score * GRADE_WEIGHTS['college_production'] +
        draft_score * GRADE_WEIGHTS['draft_projection'] +
        physical_score * GRADE_WEIGHTS['physical_measurables'] +
        consensus_score * GRADE_WEIGHTS['expert_consensus']
    )
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
    
    # Grade each prospect
    print("\n🔄 Grading prospects...")
    updated = 0
    errors = 0
    
    for i, prospect in enumerate(prospects):
        prospect_id = prospect.get('id')
        name = prospect.get('name', 'Unknown')
        draft_year = prospect.get('draft_year', 'N/A')
        
        try:
            # Calculate grades
            grades = grade_prospect(prospect)
            
            # Update database
            supabase.table('dynasty_prospects').update(grades).eq('id', prospect_id).execute()
            updated += 1
            
            # Progress indicator
            if (i + 1) % 50 == 0:
                print(f"   Processed {i + 1}/{len(prospects)}...")
                
        except Exception as e:
            errors += 1
            print(f"   ❌ Error grading {name} ({draft_year}): {e}")
    
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

