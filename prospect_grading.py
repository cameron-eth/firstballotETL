#!/usr/bin/env python3
"""
Prospect Grading System
Calculates overall prospect grades based on historical baselines.

Gold Standard Profile:
- 4-5 star HS recruit (weighted ~10%)
- High college production (weighted ~30%)
- 1st round draft capital projection (weighted ~25%)
- Elite measurables (weighted ~15%)
- Strong expert consensus (weighted ~20%)
"""

import os
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple
import pandas as pd
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))


# ==============================================================================
# GRADE WEIGHTS - Gold Standard Configuration
# ==============================================================================

GRADE_WEIGHTS = {
    'hs_recruiting': 0.10,      # HS stars/ranking (not too high per user)
    'college_production': 0.30, # College stats vs peers
    'draft_projection': 0.25,   # Projected draft round/pick
    'physical_measurables': 0.15, # Height/weight/speed for position
    'expert_consensus': 0.20,   # Current dynasty ranking position
}

# ==============================================================================
# POSITION-SPECIFIC IDEAL MEASURABLES
# ==============================================================================

IDEAL_MEASURABLES = {
    'QB': {
        'height': (74, 78),    # 6'2" - 6'6" ideal
        'weight': (215, 240),
        'height_weight': 0.6,  # Height more important for QB
    },
    'RB': {
        'height': (69, 73),    # 5'9" - 6'1" ideal
        'weight': (205, 230),
        'height_weight': 0.4,  # Weight more important for RB
    },
    'WR': {
        'height': (71, 76),    # 5'11" - 6'4" ideal
        'weight': (185, 220),
        'height_weight': 0.5,  # Balanced
    },
    'TE': {
        'height': (75, 79),    # 6'3" - 6'7" ideal
        'weight': (240, 270),
        'height_weight': 0.5,  # Balanced
    },
}

# ==============================================================================
# HS RECRUITING SCORING
# ==============================================================================

def score_hs_recruiting(
    stars: Optional[int],
    national_rank: Optional[int],
    rating: Optional[float]
) -> float:
    """
    Score HS recruiting profile (0-100).
    
    Gold standard: 5-star, top-10 national rank, 0.99+ rating
    
    Weight distribution:
    - Stars: 50%
    - National rank: 30%
    - Composite rating: 20%
    """
    score = 50.0  # Default for unknown
    
    # Stars component (50% of HS score)
    if stars:
        star_scores = {5: 100, 4: 80, 3: 55, 2: 30, 1: 10}
        stars_score = star_scores.get(int(stars), 50)
    else:
        stars_score = 50
    
    # National rank component (30% of HS score)
    if national_rank:
        if national_rank <= 5:
            rank_score = 100
        elif national_rank <= 10:
            rank_score = 95
        elif national_rank <= 25:
            rank_score = 90
        elif national_rank <= 50:
            rank_score = 85
        elif national_rank <= 100:
            rank_score = 75
        elif national_rank <= 200:
            rank_score = 65
        elif national_rank <= 300:
            rank_score = 55
        else:
            rank_score = 40
    else:
        rank_score = 50
    
    # Rating component (20% of HS score)
    if rating:
        if rating >= 0.9980:
            rating_score = 100
        elif rating >= 0.9900:
            rating_score = 90
        elif rating >= 0.9500:
            rating_score = 75
        elif rating >= 0.9000:
            rating_score = 60
        elif rating >= 0.8500:
            rating_score = 45
        else:
            rating_score = 30
    else:
        rating_score = 50
    
    # Combine components
    score = (stars_score * 0.50) + (rank_score * 0.30) + (rating_score * 0.20)
    
    return round(score, 1)


# ==============================================================================
# COLLEGE PRODUCTION SCORING
# ==============================================================================

def score_college_production(
    position: str,
    stats: Optional[Dict],
    games: int = 0
) -> float:
    """
    Score college production (0-100).
    
    Gold standard varies by position:
    - QB: High passing yards/game, TD/INT ratio, completion %
    - RB: High rushing yards/game, YPC, receiving ability
    - WR: High receiving yards/game, catches, TDs
    - TE: Receiving production + blocking grade
    """
    if not stats:
        return 50.0  # Default for unknown
    
    score = 50.0
    
    if position == 'QB':
        # Evaluate QB production
        pass_yds = stats.get('pass_yds', 0) or 0
        pass_tds = stats.get('pass_tds', 0) or 0
        pass_int = stats.get('pass_int', 0) or 0
        
        # Yards per game (assuming ~40 games for 3-year starter)
        ypg = pass_yds / max(games, 30) if games else pass_yds / 30
        
        if ypg >= 300:
            score = 95
        elif ypg >= 250:
            score = 85
        elif ypg >= 200:
            score = 70
        elif ypg >= 150:
            score = 55
        else:
            score = 40
        
        # TD/INT ratio bonus
        if pass_int > 0 and pass_tds / pass_int >= 3.0:
            score += 5
        elif pass_int > 0 and pass_tds / pass_int >= 2.0:
            score += 2
    
    elif position == 'RB':
        rush_yds = stats.get('rush_yds', 0) or 0
        rush_tds = stats.get('rush_tds', 0) or 0
        rec_yds = stats.get('rec_yds', 0) or 0
        
        total_yds = rush_yds + rec_yds
        
        if total_yds >= 4000:
            score = 95
        elif total_yds >= 3000:
            score = 85
        elif total_yds >= 2000:
            score = 70
        elif total_yds >= 1000:
            score = 55
        else:
            score = 40
        
        # Receiving bonus for dual-threat
        if rec_yds >= 500:
            score += 5
    
    elif position in ['WR', 'TE']:
        rec_yds = stats.get('rec_yds', 0) or 0
        rec_tds = stats.get('rec_tds', 0) or 0
        receptions = stats.get('rec', 0) or 0
        
        if rec_yds >= 3000:
            score = 95
        elif rec_yds >= 2000:
            score = 85
        elif rec_yds >= 1500:
            score = 70
        elif rec_yds >= 800:
            score = 55
        else:
            score = 40
        
        # TD bonus
        if rec_tds >= 20:
            score += 5
        elif rec_tds >= 10:
            score += 2
    
    return min(100, max(0, round(score, 1)))


# ==============================================================================
# DRAFT PROJECTION SCORING
# ==============================================================================

def score_draft_projection(
    projected_round: Optional[int],
    projected_pick: Optional[int]
) -> float:
    """
    Score draft capital projection (0-100).
    
    Gold standard: Round 1, top 10 pick
    """
    if not projected_round:
        return 50.0  # Default for unknown
    
    # Base score by round
    round_scores = {
        1: 90,
        2: 75,
        3: 60,
        4: 45,
        5: 35,
        6: 25,
        7: 15,
    }
    score = round_scores.get(projected_round, 10)
    
    # Bonus for top of round
    if projected_pick:
        if projected_round == 1:
            if projected_pick <= 5:
                score = 100
            elif projected_pick <= 10:
                score = 97
            elif projected_pick <= 16:
                score = 94
            elif projected_pick <= 24:
                score = 91
        elif projected_round == 2:
            if projected_pick <= 40:
                score = 80
            elif projected_pick <= 50:
                score = 77
    
    return round(score, 1)


# ==============================================================================
# PHYSICAL MEASURABLES SCORING
# ==============================================================================

def score_physical_measurables(
    position: str,
    height: Optional[float],
    weight: Optional[float],
    forty_time: Optional[float] = None
) -> float:
    """
    Score physical measurables (0-100).
    
    Gold standard: Ideal height/weight for position, elite speed
    """
    if not height and not weight:
        return 50.0  # Default for unknown
    
    ideals = IDEAL_MEASURABLES.get(position.upper(), IDEAL_MEASURABLES['WR'])
    
    # Height score
    height_score = 50.0
    if height:
        h_min, h_max = ideals['height']
        if h_min <= height <= h_max:
            height_score = 90  # In ideal range
        elif height < h_min:
            diff = h_min - height
            height_score = max(30, 90 - (diff * 10))  # -10 per inch below
        else:
            diff = height - h_max
            height_score = max(50, 90 - (diff * 5))  # -5 per inch above (less penalty)
    
    # Weight score
    weight_score = 50.0
    if weight:
        w_min, w_max = ideals['weight']
        if w_min <= weight <= w_max:
            weight_score = 90  # In ideal range
        elif weight < w_min:
            diff = w_min - weight
            weight_score = max(30, 90 - (diff * 0.5))  # -0.5 per lb below
        else:
            diff = weight - w_max
            weight_score = max(40, 90 - (diff * 0.3))  # -0.3 per lb above
    
    # Combine with position-specific weighting
    h_weight = ideals.get('height_weight', 0.5)
    score = (height_score * h_weight) + (weight_score * (1 - h_weight))
    
    # Forty time bonus (if available)
    if forty_time:
        if position in ['RB', 'WR']:
            if forty_time <= 4.35:
                score += 10
            elif forty_time <= 4.45:
                score += 5
            elif forty_time >= 4.60:
                score -= 10
    
    return min(100, max(0, round(score, 1)))


# ==============================================================================
# EXPERT CONSENSUS SCORING
# ==============================================================================

def score_expert_consensus(rank: int, total_prospects: int = 50) -> float:
    """
    Score expert consensus ranking (0-100).
    
    Gold standard: Top 5 dynasty prospect ranking
    """
    if not rank or rank <= 0:
        return 50.0
    
    if rank <= 3:
        return 100
    elif rank <= 5:
        return 95
    elif rank <= 10:
        return 88
    elif rank <= 15:
        return 80
    elif rank <= 20:
        return 70
    elif rank <= 30:
        return 60
    elif rank <= 40:
        return 50
    elif rank <= 50:
        return 40
    else:
        return max(20, 40 - (rank - 50))


# ==============================================================================
# OVERALL GRADE CALCULATION
# ==============================================================================

def calculate_prospect_grade(
    position: str,
    rank: int,
    hs_stars: Optional[int] = None,
    hs_rank: Optional[int] = None,
    hs_rating: Optional[float] = None,
    college_stats: Optional[Dict] = None,
    college_games: int = 0,
    projected_round: Optional[int] = None,
    projected_pick: Optional[int] = None,
    height: Optional[float] = None,
    weight: Optional[float] = None,
    forty_time: Optional[float] = None,
) -> Dict:
    """
    Calculate overall prospect grade (0-100) with component breakdown.
    
    Returns:
        Dict with overall grade and component scores
    """
    # Calculate component scores
    hs_score = score_hs_recruiting(hs_stars, hs_rank, hs_rating)
    production_score = score_college_production(position, college_stats, college_games)
    draft_score = score_draft_projection(projected_round, projected_pick)
    physical_score = score_physical_measurables(position, height, weight, forty_time)
    consensus_score = score_expert_consensus(rank)
    
    # Calculate weighted overall grade
    overall = (
        hs_score * GRADE_WEIGHTS['hs_recruiting'] +
        production_score * GRADE_WEIGHTS['college_production'] +
        draft_score * GRADE_WEIGHTS['draft_projection'] +
        physical_score * GRADE_WEIGHTS['physical_measurables'] +
        consensus_score * GRADE_WEIGHTS['expert_consensus']
    )
    
    return {
        'overall_grade': round(overall, 1),
        'hs_recruiting_score': hs_score,
        'college_production_score': production_score,
        'draft_projection_score': draft_score,
        'physical_measurables_score': physical_score,
        'expert_consensus_score': consensus_score,
        'grade_tier': get_grade_tier(overall),
    }


def get_grade_tier(grade: float) -> str:
    """Convert numeric grade to tier label."""
    if grade >= 90:
        return 'Elite'
    elif grade >= 80:
        return 'Blue Chip'
    elif grade >= 70:
        return 'Starter'
    elif grade >= 60:
        return 'Rotational'
    elif grade >= 50:
        return 'Depth'
    else:
        return 'Longshot'


# ==============================================================================
# HISTORICAL COMPARISON
# ==============================================================================

def get_historical_percentile(
    position: str,
    grade: float,
    historical_df: pd.DataFrame
) -> float:
    """
    Calculate where this prospect ranks among historical prospects.
    
    Returns:
        Percentile (0-100) - higher is better
    """
    if historical_df.empty:
        return 50.0
    
    # Filter to position
    pos_df = historical_df[historical_df['position'] == position]
    
    if pos_df.empty:
        return 50.0
    
    # Calculate percentile based on pre_draft_grade
    if 'pre_draft_grade' in pos_df.columns:
        grades = pos_df['pre_draft_grade'].dropna()
        if len(grades) > 0:
            percentile = (grades < grade).sum() / len(grades) * 100
            return round(percentile, 1)
    
    return 50.0


# ==============================================================================
# MAIN TEST
# ==============================================================================

if __name__ == '__main__':
    # Test with sample prospect (Jeremiyah Love - RB)
    grade = calculate_prospect_grade(
        position='RB',
        rank=1,
        hs_stars=5,
        hs_rank=15,
        hs_rating=0.9920,
        college_stats={'rush_yds': 2500, 'rush_tds': 25, 'rec_yds': 400},
        college_games=35,
        projected_round=1,
        projected_pick=12,
        height=70,
        weight=205,
    )
    
    print("=" * 60)
    print("PROSPECT GRADE: Jeremiyah Love (RB)")
    print("=" * 60)
    for key, value in grade.items():
        print(f"  {key}: {value}")
    
    print("\n" + "=" * 60)
    print("GRADE WEIGHTS:")
    print("=" * 60)
    for key, value in GRADE_WEIGHTS.items():
        print(f"  {key}: {value*100:.0f}%")

