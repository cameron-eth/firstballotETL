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
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import outcome range helper (lazy import to avoid circular deps at module level)
def _get_outcome_range(position: str, grade: Optional[float]) -> Tuple[str, str]:
    """Thin wrapper so prospect_grading stays self-contained at import time."""
    try:
        from tiers.definitions import get_prospect_outcome_range
        return get_prospect_outcome_range(position, grade)
    except Exception:
        return ('Unknown', 'Unknown')


# ==============================================================================
# GRADE WEIGHTS - Gold Standard Configuration
# ==============================================================================

GRADE_WEIGHTS = {
    'hs_recruiting': 0.10,      # HS stars/ranking (kept as-is)
    'college_production': 0.30, # College stats vs peers
    'draft_projection': 0.42,   # Heavier draft-capital influence
    'physical_measurables': 0.15, # Height/weight/speed for position (kept as-is)
    'expert_consensus': 0.16,   # Reduced to let draft signal drive more
    'age_factor': 0.10,         # Younger prospects receive a meaningful boost
}

# Forward-looking blend for future classes.
# Draft projection is ESTIMATED (not real), so it should NOT dominate.
# College production carries the heaviest weight — actual on-field tape.
# Physical measurables matter more when combine data exists.
FUTURE_GRADE_WEIGHTS = {
    'hs_recruiting': 0.08,
    'college_production': 0.38,
    'draft_projection': 0.22,       # Much lower — this is rank-estimated, not verified
    'physical_measurables': 0.18,
    'expert_consensus': 0.08,
    'age_factor': 0.06,
}

# Already-drafted classes should lean harder on realized draft capital.
DRAFTED_CLASS_WEIGHTS = {
    'hs_recruiting': 0.04,
    'college_production': 0.24,
    'draft_projection': 0.45,
    'physical_measurables': 0.14,
    'expert_consensus': 0.07,
    'age_factor': 0.06,
}

# 2025+ drafted/current classes with actual draft capital:
# make draft projection a stronger driver than older historical eras.
RECENT_DRAFT_CAP_HEAVY_WEIGHTS = {
    'hs_recruiting': 0.03,
    'college_production': 0.22,
    'draft_projection': 0.56,
    'physical_measurables': 0.12,
    'expert_consensus': 0.05,
    'age_factor': 0.05,
}


def get_grade_weights(
    draft_year: Optional[int],
    projected_round: Optional[int] = None,
    projected_pick: Optional[int] = None,
) -> Dict[str, float]:
    """Use context-aware weights by class timing and draft-capital certainty."""
    try:
        year = int(draft_year) if draft_year is not None else None
        current_year = datetime.now().year
        has_capital = bool(projected_round or projected_pick)
        drafted_context = year is not None and (
            year < current_year or (has_capital and year <= current_year)
        )

        # Historical classes (or current-year players with real draft capital)
        # should emphasize draft signal more than projection-era classes.
        if drafted_context:
            if year is not None and year >= 2025:
                return RECENT_DRAFT_CAP_HEAVY_WEIGHTS
            return DRAFTED_CLASS_WEIGHTS

        # Current draft year without verified capital yet — treat like a
        # recently-drafted class so college tape & estimated picks drive the grade.
        if year is not None and year == current_year:
            return RECENT_DRAFT_CAP_HEAVY_WEIGHTS

        # Only truly future classes (next year+) get the lighter future weights.
        if year is not None and year > current_year:
            return FUTURE_GRADE_WEIGHTS
    except Exception:
        pass
    return GRADE_WEIGHTS

# Star Effect: rank-aware NUDGE for top future-class stars.
# This is NOT a floor — it's a small additive bonus that decays with rank,
# so top-ranked stars separate from lower-ranked ones instead of clustering.
# Max nudge of 3 pts for rank 1, decaying by 0.4 per rank, zero at rank 8+.
STAR_EFFECT_MAX_NUDGE = 3.0

# ==============================================================================
# MANUAL EXPERT BONUSES
# Expert scouts can assign an additive bonus to specific prospects to reflect
# intangibles, dynasty community consensus, or film grades the model can't see.
# Keyed by normalized player name, valued by draft_year.
# These are applied AFTER all model components and star nudges.
# Capped at 100.0.
# ==============================================================================

MANUAL_EXPERT_BONUSES: dict[str, dict] = {
    # 2027 class — expert consensus adjustments
    # Bonuses back-calculated so final grade hits the target values below.
    # Jeremiah Smith  → 97.3  |  Cam Coleman  → 94.6
    # Ahmad Hardy     → 92.3  |  Kewan Lacy   → 91.5
    # Isaac Brown     → 85.5  |  Julian Sayin → 82.5  |  Singleton → 80.0
    'jeremiahsmith':     {'draft_year': 2027, 'bonus': 17.0},
    'camcoleman':        {'draft_year': 2027, 'bonus': 14.8},
    'ahmadhardy':        {'draft_year': 2027, 'bonus': 16.8},
    'kewanlacy':         {'draft_year': 2027, 'bonus': 23.2},
    'isaacbrown':        {'draft_year': 2027, 'bonus': 11.0},
    'issacbrown':        {'draft_year': 2027, 'bonus': 11.0},  # alternate spelling
    'juliansayin':       {'draft_year': 2027, 'bonus': 10.0},
    'nicholassingleton': {'draft_year': 2027, 'bonus':  7.0},
}


def apply_expert_bonus(
    name: Optional[str],
    overall_grade: float,
    draft_year: Optional[int] = None,
) -> Tuple[float, float]:
    """
    Apply a manual expert bonus to a prospect's grade.

    Returns:
        (adjusted_grade, bonus_applied)
    """
    key = normalize_player_name(name)
    entry = MANUAL_EXPERT_BONUSES.get(key)
    if not entry:
        return (overall_grade, 0.0)

    # Only apply if the draft year matches
    try:
        if draft_year is not None and int(draft_year) != entry['draft_year']:
            return (overall_grade, 0.0)
    except Exception:
        return (overall_grade, 0.0)

    bonus = float(entry['bonus'])
    adjusted = min(100.0, overall_grade + bonus)
    return (adjusted, bonus)
STAR_EFFECT_PROSPECTS = {
    'jeremiahsmith',
    'kewanlacy',
    'ahmadhardy',
    'archmanning',
    'juliansayin',
    'camcoleman',
    'ryanwilliams',
    'isaacbrown',
    'issacbrown',
    'ryanwingo',
    'jamarijohnson',
    'kenyonsadiq',
    'fernandomendoza',
    'carnelltate',
    'jeremiyahlove',
}


def normalize_player_name(name: Optional[str]) -> str:
    """Normalize player names for robust matching."""
    s = (name or '').lower().replace('’', "'")
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", '', s)
    return re.sub(r'[^a-z0-9]+', '', s)


def _coerce_float(value: Optional[object]) -> Optional[float]:
    try:
        if value is None or value == '':
            return None
        return float(value)
    except Exception:
        return None


def _coerce_int(value: Optional[object]) -> Optional[int]:
    numeric = _coerce_float(value)
    if numeric is None:
        return None
    return int(round(numeric))


def get_external_consensus_context(prospect: Dict) -> Dict[str, Optional[float]]:
    """
    Return the external consensus inputs for a prospect.

    Falls back to legacy `rank` only when the newer explicit consensus fields
    are absent so old data can still be graded.
    """
    consensus_rank = _coerce_int(prospect.get('consensus_rank'))
    avg_rank = _coerce_float(prospect.get('consensus_avg_rank'))
    rank_stddev = _coerce_float(prospect.get('consensus_rank_stddev'))
    fallback_rank = _coerce_int(prospect.get('rank'))

    seed_rank = consensus_rank or (_coerce_int(avg_rank) if avg_rank is not None else None) or fallback_rank

    return {
        'consensus_rank': consensus_rank,
        'consensus_avg_rank': avg_rank,
        'consensus_rank_stddev': rank_stddev,
        'seed_rank': seed_rank,
    }


def apply_star_effect(
    name: Optional[str],
    overall_grade: float,
    draft_year: Optional[int] = None,
    rank: Optional[int] = None,
) -> Tuple[float, bool]:
    """
    Apply a rank-aware nudge for future-class star prospects.

    Unlike the old floor-based approach, this gives a DECAYING bonus:
      rank 1 → +6, rank 2 → +5.5, … rank 12 → +0.5, rank 13+ → 0.

    This creates natural spread instead of clustering everyone at one number.

    Returns:
        (adjusted_grade, was_applied)
    """
    # Only apply to future classes — historical/current classes earned their grades.
    try:
        year = int(draft_year) if draft_year is not None else None
    except Exception:
        year = None
    current_year = datetime.now().year
    if year is None or year <= current_year:
        return (overall_grade, False)

    if normalize_player_name(name) in STAR_EFFECT_PROSPECTS:
        r = max(1, int(rank or 50))
        nudge = max(0.0, STAR_EFFECT_MAX_NUDGE - (r - 1) * 0.4)
        return (overall_grade + nudge, nudge > 0)
    return (overall_grade, False)


# ==============================================================================
# POSITION-SPECIFIC IDEAL MEASURABLES
# ==============================================================================

IDEAL_MEASURABLES = {
    'QB': {
        'height': (74, 79),    # 6'2" - 6'7" ideal (taller QBs not penalized)
        'weight': (215, 240),
        'height_weight': 0.6,  # Height more important for QB
    },
    'RB': {
        'height': (68, 72),    # 5'8" - 6'0" ideal
        'weight': (205, 230),
        'height_weight': 0.4,  # Weight more important for RB
    },
    'WR': {
        'height': (71, 79),    # 5'11" - 6'7" ideal — expanded upper bound so
                                # elite-size receivers (6'4"+) aren't penalized.
                                # Being tall for WR is a positive, not negative.
        'weight': (185, 225),
        'height_weight': 0.5,  # Balanced
    },
    'TE': {
        'height': (75, 80),    # 6'3" - 6'8" ideal
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
    
    Missing stat fields use position medians (not zero) to avoid unfairly
    crushing players with partial stat ingestion.
    """
    if not stats:
        return 50.0  # Default for unknown
    
    def _num_or_none(d: Dict, key: str) -> Optional[float]:
        v = d.get(key)
        if v is None or v == '':
            return None
        try:
            return float(v)
        except Exception:
            return None

    g = max(float(games or 0), 1.0)
    score = 50.0

    production_keys_by_position = {
        'QB': ('pass_yds', 'pass_tds', 'pass_int', 'rush_yds'),
        'RB': ('rush_yds', 'rush_tds', 'rec_yds', 'rec'),
        'WR': ('rec_yds', 'rec_tds', 'rec'),
        'TE': ('rec_yds', 'rec_tds', 'rec'),
    }
    production_keys = production_keys_by_position.get(position, ())
    has_relevant_production = any(_num_or_none(stats, key) is not None for key in production_keys)
    if not has_relevant_production:
        # Prevent combine-only blobs (forty, vertical, arm length, etc.)
        # from being mistaken for actual production input.
        return 50.0
    
    if position == 'QB':
        pass_yds = _num_or_none(stats, 'pass_yds')
        pass_tds = _num_or_none(stats, 'pass_tds')
        pass_int = _num_or_none(stats, 'pass_int')
        rush_yds = _num_or_none(stats, 'rush_yds')

        ypg = (pass_yds / g) if pass_yds is not None else 220.0
        tdpg = (pass_tds / g) if pass_tds is not None else 2.2
        rush_ypg = (rush_yds / g) if rush_yds is not None else 18.0
        if pass_tds is not None and pass_int is not None:
            td_int = pass_tds / max(pass_int, 1.0)
        else:
            td_int = 2.5

        ypg_score = np.interp(ypg, [120, 320], [28, 92])
        td_score = np.interp(tdpg, [0.8, 3.6], [35, 96])
        ball_security = np.interp(td_int, [1.0, 4.0], [45, 96])
        rush_bonus = np.interp(rush_ypg, [0, 70], [0, 12])
        score = 0.38 * ypg_score + 0.30 * td_score + 0.22 * ball_security + rush_bonus
    
    elif position == 'RB':
        rush_yds = _num_or_none(stats, 'rush_yds')
        rush_tds = _num_or_none(stats, 'rush_tds')
        rec_yds = _num_or_none(stats, 'rec_yds')
        rec = _num_or_none(stats, 'rec')

        if rush_yds is not None or rec_yds is not None:
            scrim_yds = (rush_yds or 0.0) + (rec_yds or 0.0)
            scrim_ypg = scrim_yds / g
            pass_game_share = (rec_yds or 0.0) / max(scrim_yds, 1.0)
        else:
            scrim_ypg = 92.5
            pass_game_share = 0.20

        tdpg = (rush_tds / g) if rush_tds is not None else 0.75
        rec_pg = (rec / g) if rec is not None else 2.15

        score = (
            0.46 * np.interp(scrim_ypg, [35, 150], [28, 94]) +
            0.26 * np.interp(tdpg, [0.2, 1.3], [35, 95]) +
            0.18 * np.interp(rec_pg, [0.3, 4.0], [35, 90]) +
            0.10 * np.interp(pass_game_share, [0.02, 0.38], [40, 92])
        )
    
    elif position in ['WR', 'TE']:
        rec_yds = _num_or_none(stats, 'rec_yds')
        rec_tds = _num_or_none(stats, 'rec_tds')
        rec = _num_or_none(stats, 'rec')

        if position == 'WR':
            rec_ypg = (rec_yds / g) if rec_yds is not None else 72.5
            rec_pg = (rec / g) if rec is not None else 4.0
            tdpg = (rec_tds / g) if rec_tds is not None else 0.675
            ypr = (rec_yds / max(rec, 1.0)) if rec_yds is not None and rec is not None else 14.75
            score = (
                0.50 * np.interp(rec_ypg, [25, 120], [28, 95]) +
                0.22 * np.interp(rec_pg, [1.0, 7.0], [30, 92]) +
                0.18 * np.interp(tdpg, [0.15, 1.2], [35, 94]) +
                0.10 * np.interp(ypr, [9.5, 20.0], [40, 90])
            )
        else:  # TE
            rec_ypg = (rec_yds / g) if rec_yds is not None else 51.5
            rec_pg = (rec / g) if rec is not None else 3.15
            tdpg = (rec_tds / g) if rec_tds is not None else 0.525
            ypr = (rec_yds / max(rec, 1.0)) if rec_yds is not None and rec is not None else 12.25
            score = (
                0.48 * np.interp(rec_ypg, [18, 85], [30, 93]) +
                0.22 * np.interp(rec_pg, [0.8, 5.5], [30, 90]) +
                0.20 * np.interp(tdpg, [0.10, 0.95], [35, 92]) +
                0.10 * np.interp(ypr, [8.0, 16.5], [40, 88])
            )

    return min(100, max(0, round(float(score), 1)))

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
    
    # Round anchor keeps explicit capital signal, pick adds continuous spread.
    # Tuned to make early first-round capital materially more valuable.
    round_scores = {1: 95, 2: 72, 3: 56, 4: 42, 5: 30, 6: 20, 7: 12}
    base = float(round_scores.get(int(projected_round), 12))

    if projected_pick:
        p = min(max(float(projected_pick), 1.0), 280.0)
        # Steeper top-end curve to separate top-12 / top-32 draft capital.
        pick_component = 100.0 - 88.0 * ((p - 1.0) / 279.0) ** 0.48
        score = 0.45 * base + 0.55 * pick_component
    else:
        score = base

    return round(min(100.0, max(0.0, score)), 1)


# ==============================================================================
# PHYSICAL MEASURABLES SCORING
# ==============================================================================

def score_physical_measurables(
    position: str,
    height: Optional[float],
    weight: Optional[float],
    forty_time: Optional[float] = None,
    vertical: Optional[float] = None,
    broad_jump: Optional[float] = None,
    bench: Optional[float] = None,
    three_cone: Optional[float] = None,
    shuttle: Optional[float] = None,
    draft_year: Optional[int] = None,
) -> float:
    """
    Score physical measurables (0-100).
    
    Gold standard: Ideal height/weight for position, elite speed
    """
    if not height and not weight:
        return 50.0  # Default for unknown
    
    ideals = IDEAL_MEASURABLES.get(position.upper(), IDEAL_MEASURABLES['WR'])
    
    # Continuous size scoring around position midpoint (removes flat 90 plateaus).
    h_min, h_max = ideals['height']
    w_min, w_max = ideals['weight']
    h_mid = (h_min + h_max) / 2.0
    w_mid = (w_min + w_max) / 2.0
    h_half = max((h_max - h_min) / 2.0, 1.0)
    w_half = max((w_max - w_min) / 2.0, 1.0)

    pos = position.upper()

    height_score = 52.0
    if height:
        h_val = float(height)
        # Asymmetric height scoring:
        # Being ABOVE the ideal range is treated more leniently than being below.
        # For WR/TE/QB: extra height is generally a positive (catch radius, contested
        # catches, passing windows). For RB: extra height is slightly negative.
        # Penalty below ideal: steep (14 pts/half-width + quadratic 7).
        # Penalty above ideal: shallow (6 pts/half-width + quadratic 3).
        if h_val <= h_mid:
            # Below midpoint — symmetric penalty
            h_dist = (h_mid - h_val) / h_half
            height_score = 87.0 - (h_dist * 14.0) - ((h_dist**2) * 7.0)
        else:
            # Above midpoint — gentler penalty (taller is usually fine for skill positions)
            h_dist = (h_val - h_mid) / h_half
            if pos == 'RB':
                # RBs: extra height hurts leverage/contact balance
                height_score = 87.0 - (h_dist * 10.0) - ((h_dist**2) * 5.0)
            else:
                # WR/QB/TE: extra height is a positive, minimal penalty
                height_score = 87.0 - (h_dist * 4.0) - ((h_dist**2) * 2.0)
        height_score = max(30.0, min(92.0, height_score))

    weight_score = 52.0
    if weight:
        w_dist = abs(float(weight) - w_mid) / w_half
        weight_score = 87.0 - (w_dist * 12.0) - ((w_dist**2) * 8.5)
        weight_score = max(30.0, min(90.0, weight_score))

    # Combine size sub-scores with position-specific weighting.
    h_weight = ideals.get('height_weight', 0.5)
    score = (height_score * h_weight) + (weight_score * (1 - h_weight))
    
    combine_parts = []

    # 40-yard (lower is better)
    # np.interp clamps at endpoints, so times faster than the lower bound still score max.
    if forty_time:
        ft = float(forty_time)
        if pos == 'QB':
            combine_parts.append(float(np.interp(ft, [5.15, 4.45], [35, 97])))
        elif pos == 'RB':
            # Sub-4.30 is Tyreek-at-RB territory → cap at 99
            combine_parts.append(float(np.interp(ft, [4.85, 4.28], [30, 99])))
        elif pos == 'WR':
            # 4.28 is near the all-time record; sub-4.35 is elite.
            # Extended upper range so a 4.30 WR scores ~96+ not artificially floored.
            combine_parts.append(float(np.interp(ft, [4.78, 4.25], [30, 99])))
        elif pos == 'TE':
            # A TE running 4.30 is historically exceptional (Kelce runs ~4.61).
            # Extended fast end to reward jumbo-speed TEs.
            combine_parts.append(float(np.interp(ft, [5.00, 4.38], [30, 99])))

    # Explosiveness
    if vertical:
        combine_parts.append(np.interp(float(vertical), [26, 42], [35, 96]))
    if broad_jump:
        combine_parts.append(np.interp(float(broad_jump), [96, 132], [35, 96]))

    # Strength/endurance
    if bench:
        combine_parts.append(np.interp(float(bench), [8, 30], [35, 92]))

    # Agility (lower is better)
    if three_cone:
        combine_parts.append(np.interp(float(three_cone), [8.25, 6.60], [30, 96]))
    if shuttle:
        combine_parts.append(np.interp(float(shuttle), [4.90, 3.85], [30, 95]))

    has_combine = len(combine_parts) > 0
    combine_score = float(np.mean(combine_parts)) if has_combine else 50.0

    # Penalize sparse combine profiles so one average metric doesn't inflate the score.
    # However, a single ELITE metric (40-time, vertical) is genuine signal and shouldn't
    # be capped aggressively — the cap only prevents mediocre single metrics from hitting 90+.
    if has_combine:
        if len(combine_parts) == 1:
            # Allow elite single metrics (score >= 92) through at 91; cap average ones at 85.
            single = combine_score
            if single >= 92.0:
                combine_score = min(single, 94.0)   # Elite verified metric — trust it
            else:
                combine_score = min(single, 85.0)   # Average single metric — cap
        elif len(combine_parts) == 2:
            combine_score = min(combine_score, 92.0)

    # Weight size/profile + combine blend.
    # RBs should lean more on verified speed than other positions.
    if pos == 'RB':
        size_w = 0.58
        combine_w = 0.42
    else:
        size_w = 0.66
        combine_w = 0.34
    final = (score * size_w) + (combine_score * combine_w if has_combine else 50.0 * combine_w)

    # Jumbo WR speed bonus: a tall WR (6'4"+, i.e. ≥76 inches) who also runs elite
    # is an exceptionally rare and valuable prospect profile. Add an additive bonus
    # so the overall physical score properly reflects that combination.
    if pos == 'WR' and forty_time and height and float(height) >= 76.0:
        # Scale: 4.25 or faster → +8, 4.35 → +5, 4.45 → +2, 4.55+ → 0
        wr_jumbo_bonus = float(np.interp(float(forty_time), [4.55, 4.25], [0.0, 8.0]))
        final += wr_jumbo_bonus

    # Tight-end speed matters more for fantasy upside: reward verified TE burst.
    if pos == 'TE' and forty_time:
        te_speed_bonus = float(np.interp(float(forty_time), [5.10, 4.38], [-4.0, 10.0]))
        final += te_speed_bonus

    # Running-back speed is a primary fantasy ceiling driver; make 40 carry more.
    if pos == 'RB' and forty_time:
        rb_speed_bonus = float(np.interp(float(forty_time), [4.90, 4.30], [-5.0, 12.0]))
        final += rb_speed_bonus

    # For modern classes, missing combine data should limit ceiling.
    try:
        if draft_year is not None and int(draft_year) >= 2026 and not has_combine:
            final = min(final, 82.0)
        if draft_year is not None and int(draft_year) >= 2026 and has_combine and len(combine_parts) < 2:
            final = min(final, 88.0)
    except Exception:
        pass

    return min(100, max(0, round(float(final), 1)))


# ==============================================================================
# EXPERT CONSENSUS SCORING
# ==============================================================================

def score_expert_consensus(
    rank: Optional[int] = None,
    total_prospects: int = 50,
    avg_rank: Optional[float] = None,
    rank_stddev: Optional[float] = None,
) -> float:
    """
    Score expert consensus ranking (0-100).
    
    Gold standard: Top 5 dynasty prospect ranking
    """
    rank_value = _coerce_float(avg_rank)
    if rank_value is None:
        rank_value = float(rank) if rank and rank > 0 else None

    if rank_value is None or rank_value <= 0:
        return 50.0
    
    # Continuous decay reduces giant score clusters from bucket thresholds.
    r = float(max(rank_value, 1.0))
    base_score = 104.0 - (12.5 * np.log10(r + 1.0) * 2.0)

    # Lower stddev means stronger market agreement and slightly more confidence.
    if rank_stddev is not None:
        std = max(float(rank_stddev), 0.0)
        stability_bonus = float(np.interp(std, [0.0, 2.0, 5.0, 10.0, 20.0], [4.0, 3.0, 0.5, -3.0, -6.0]))
        base_score += stability_bonus

    return round(max(20.0, min(100.0, base_score)), 1)


# ==============================================================================
# AGE FACTOR SCORING
# ==============================================================================

def score_age_factor(
    class_year: Optional[str] = None,
    age_at_draft: Optional[float] = None,
) -> float:
    """
    Score age profile (0-100), where younger is better.

    Preferred source is numeric age_at_draft when available.
    Falls back to class-year proxy from profile data.
    """
    if age_at_draft is not None:
        try:
            age = float(age_at_draft)
            # Continuous curve: younger classes get meaningful separation.
            # 19.5 -> ~96, 21.0 -> ~86, 22.5 -> ~72, 24.0 -> ~56
            score = 108.0 - ((age - 18.5) * 13.0)
            return max(45.0, min(96.0, round(score, 1)))
        except Exception:
            pass

    label = (class_year or '').strip().lower()
    if not label:
        return 70.0  # Neutral when unknown

    if 'freshman' in label or label in {'fr', 'f'}:
        return 95.0
    if 'sophomore' in label or label in {'so', 'soph'}:
        return 88.0
    if 'junior' in label or label in {'jr', 'j'}:
        return 78.0
    if 'senior' in label or label in {'sr', 's'}:
        return 65.0
    if 'graduate' in label or '5th' in label or '6th' in label:
        return 50.0
    return 70.0


# ==============================================================================
# OVERALL GRADE CALCULATION
# ==============================================================================

def calculate_prospect_grade(
    position: str,
    rank: int,
    name: Optional[str] = None,
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
    vertical: Optional[float] = None,
    broad_jump: Optional[float] = None,
    bench: Optional[float] = None,
    three_cone: Optional[float] = None,
    shuttle: Optional[float] = None,
    draft_year: Optional[int] = None,
    class_year: Optional[str] = None,
    age_at_draft: Optional[float] = None,
    consensus_avg_rank: Optional[float] = None,
    consensus_rank_stddev: Optional[float] = None,
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
    physical_score = score_physical_measurables(
        position,
        height,
        weight,
        forty_time,
        vertical=vertical,
        broad_jump=broad_jump,
        bench=bench,
        three_cone=three_cone,
        shuttle=shuttle,
        draft_year=draft_year,
    )
    consensus_score = score_expert_consensus(
        rank,
        avg_rank=consensus_avg_rank,
        rank_stddev=consensus_rank_stddev,
    )
    age_score = score_age_factor(class_year, age_at_draft)
    weights = get_grade_weights(draft_year, projected_round, projected_pick)
    
    # Calculate weighted overall grade.
    # Normalize by total weight so we can increase draft capital emphasis
    # without needing to reduce other factors.
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

    # ── Class-context adjustments ──
    try:
        year = int(draft_year) if draft_year is not None else None
    except Exception:
        year = None
    current_year = datetime.now().year

    # Historical drafted classes with verified draft capital:
    # Stretch the grade range so top picks clearly separate from mid/late rounds.
    # A #2 pick (Saquon) should grade in the high 80s, not low 80s.
    if year is not None and year < current_year and projected_pick:
        overall = 60.0 + (overall - 60.0) * 1.25
        overall = min(overall, 100.0)

    # ── Future classes: confidence-based evidence regression ──
    # Instead of a hard ceiling, we use a Bayesian "prove-it" mechanism:
    # The grade is regressed toward a conservative prior based on how much
    # verified evidence exists.  More data → grade stays near raw.
    # Less data → pulled toward neutral.  This is elegant because
    # it creates natural spread (data-rich prospects separate from
    # data-sparse ones) without an arbitrary cap.
    if year is not None and year > current_year:
        has_hs = bool(hs_stars or hs_rank or hs_rating)
        has_production = bool(college_stats and college_games and college_games > 0)
        has_combine = bool(forty_time or vertical or broad_jump or bench or three_cone or shuttle)

        evidence = float(sum([has_hs, has_production, has_combine]))
        max_evidence = 3.0
        confidence = evidence / max_evidence  # 0.0 – 1.0

        # Blend range: 0.60 (no evidence) → 1.0 (full evidence)
        prior = 64.0
        blend = 0.60 + 0.40 * confidence
        overall = prior + (overall - prior) * blend

    overall, _ = apply_star_effect(name, overall, draft_year, rank)
    overall, _ = apply_expert_bonus(name, overall, draft_year)

    outcome_ceiling, outcome_floor = _get_outcome_range(position, round(overall, 1))

    return {
        'overall_grade': round(overall, 1),
        'hs_recruiting_score': hs_score,
        'college_production_score': production_score,
        'draft_projection_score': draft_score,
        'physical_measurables_score': physical_score,
        'expert_consensus_score': consensus_score,
        'grade_tier': get_grade_tier(overall),
        'outcome_ceiling': outcome_ceiling,
        'outcome_floor': outcome_floor,
    }


def get_grade_tier(grade: float) -> str:
    """Convert numeric grade to tier label."""
    # Evidence-weighted tiering:
    # - Elite: historically proven difference-makers (90+)
    # - Blue Chip: strong primary starters (85-89.9)
    # - Starter: reliable weekly contributors (78-84.9)
    # - Flex/QB3: usable depth/streamer profile (70-77.9)
    # - Depth: rostered but not startable (60-69.9)
    # - Longshot: stash/speculative (<60)
    if grade >= 90:
        return 'Elite'
    elif grade >= 85:
        return 'Blue Chip'
    elif grade >= 78:
        return 'Starter'
    elif grade >= 70:
        return 'Rotational'
    elif grade >= 60:
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

