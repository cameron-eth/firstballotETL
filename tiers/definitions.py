"""
Tier Definitions - Single Source of Truth
All tier breakpoints and definitions are centralized here.
"""

from typing import Dict, List, Tuple, Optional

# Prospect Tier Definitions (based on rank)
# These match the frontend expectations for display tiers
PROSPECT_TIER_DEFINITIONS: Dict[str, Dict[str, int]] = {
    'Elite Prospect': {'min_rank': 1, 'max_rank': 12},
    'First Round': {'min_rank': 13, 'max_rank': 36},
    'Second Round': {'min_rank': 37, 'max_rank': 72},
    'Third Round': {'min_rank': 73, 'max_rank': 120},
    'Mid Round': {'min_rank': 121, 'max_rank': 200},
    'Late Round': {'min_rank': 201, 'max_rank': 300},
    'Undrafted': {'min_rank': 301, 'max_rank': 9999},
}

# Numeric tier breakpoints (for database storage and sorting)
PROSPECT_TIER_BREAKPOINTS: List[Tuple[int, int, str, int]] = [
    (1, 5, 'Tier 1', 1),      # Elite prospects (1-5)
    (6, 12, 'Tier 2', 2),     # First round talent (6-12)
    (13, 18, 'Tier 3', 3),    # Early 2nd round (13-18)
    (19, 25, 'Tier 4', 4),    # Late 2nd/Early 3rd (19-25)
    (26, 9999, 'Tier 5', 5),  # Mid-late round (26+)
]

# Display tier names mapping (for frontend consistency)
TIER_DISPLAY_NAMES: Dict[str, str] = {
    'Tier 1': 'Elite Prospect',
    'Tier 2': 'First Round',
    'Tier 3': 'Second Round',
    'Tier 4': 'Third Round',
    'Tier 5': 'Mid Round',
}

# Physical attribute thresholds by position (for tier adjustments)
PHYSICAL_THRESHOLDS: Dict[str, Dict[str, Dict[str, float]]] = {
    'QB': {
        'height': {'ideal_min': 75, 'ideal_max': 79},  # 6'3" - 6'7"
        'weight': {'ideal_min': 210, 'ideal_max': 250},  # 210-250 lbs
    },
    'RB': {
        'height': {'ideal_min': 68, 'ideal_max': 72},  # 5'8" - 6'0"
        'weight': {'ideal_min': 210, 'ideal_max': 230},  # 210-230 lbs ideal
    },
    'WR': {
        'height': {'ideal_min': 72, 'ideal_max': 78},  # 6'0" - 6'6"
        'weight': {'ideal_min': 185, 'ideal_max': 225},  # 185-225 lbs
    },
    'TE': {
        'height': {'ideal_min': 76, 'ideal_max': 82},  # 6'4" - 6'10"
        'weight': {'ideal_min': 240, 'ideal_max': 270},  # 240-270 lbs
    },
}

# ==============================================================================
# NFL DYNASTY PLAYER TIER LABELS
# Position-specific labels that replace the old generic "Elite (Tier 1)" style.
# Keyed by position then tier_numeric (0=Generational … 8=Cut Candidate).
# These mirror the definitions in firstballotmodel/tiering.py and serve as the
# single source of truth for display names across the ETL pipeline.
# ==============================================================================

NFL_TIER_LABELS: Dict[str, Dict[int, str]] = {
    'QB': {
        0: 'Generational QB',
        1: 'QB1 Elite',
        2: 'QB1',
        3: 'High-End QB2',
        4: 'QB2',
        5: 'Streaming QB',
        6: 'Deep League QB',
        7: 'Bench QB',
        8: 'Cut Candidate',
    },
    'RB': {
        0: 'Generational RB',
        1: 'RB1 Elite',
        2: 'RB1',
        3: 'High-End RB2',
        4: 'RB2',
        5: 'Flex RB',
        6: 'Handcuff RB',
        7: 'Bench RB',
        8: 'Cut Candidate',
    },
    'WR': {
        0: 'Generational WR',
        1: 'WR1 Elite',
        2: 'WR1',
        3: 'High-End WR2',
        4: 'WR2',
        5: 'Flex WR',
        6: 'Depth WR',
        7: 'Bench WR',
        8: 'Cut Candidate',
    },
    'TE': {
        0: 'Generational TE',
        1: 'TE1 Elite',
        2: 'TE1',
        3: 'High-End TE2',
        4: 'TE2',
        5: 'Streaming TE',
        6: 'Depth TE',
        7: 'Bench TE',
        8: 'Cut Candidate',
    },
}

# NFL score band → tier_numeric mapping (mirrors tiering.py thresholds)
NFL_SCORE_TIER_BREAKPOINTS: List[Tuple[float, int]] = [
    (10500.0, 0),  # Generational
    (9844.0,  1),  # [Pos]1 Elite
    (9188.0,  2),  # [Pos]1
    (8531.0,  3),  # High-End [Pos]2
    (7219.0,  4),  # [Pos]2
    (5906.0,  5),  # Flex/Streaming
    (4594.0,  6),  # Depth/Handcuff
    (3281.0,  7),  # Bench
    (0.0,     8),  # Cut Candidate
]


def get_nfl_tier_label(position: str, score: float) -> tuple[str, int]:
    """Return (tier_label, tier_numeric) for an established NFL player."""
    tier_num = 8
    for min_score, num in NFL_SCORE_TIER_BREAKPOINTS:
        if score >= min_score:
            tier_num = num
            break
    pos = position if position in NFL_TIER_LABELS else 'WR'
    return NFL_TIER_LABELS[pos][tier_num], tier_num


# ==============================================================================
# PROSPECT OUTCOME RANGES
# Maps (position, grade) → (outcome_ceiling, outcome_floor).
# Grades are 0-100. Buckets defined by plan statistical anchors.
# ==============================================================================

# Format: list of (min_grade_inclusive, ceiling_label, floor_label), checked top-down.
PROSPECT_OUTCOME_RANGES: Dict[str, List[Tuple[float, str, str]]] = {
    'WR': [
        (98.0, 'Generational WR',  'WR1 Elite'),
        (93.0, 'WR1 Elite',        'WR1'),
        (88.0, 'WR1',              'High-End WR2'),
        (82.0, 'WR1',              'WR2'),
        (75.0, 'High-End WR2',     'WR2'),
        (66.0, 'WR2',              'Flex WR'),
        (55.0, 'Flex WR',          'Depth'),
        (40.0, 'Depth',            'Bust'),
        (0.0,  'High Bust Risk',   'High Bust Risk'),
    ],
    'RB': [
        (98.0, 'Generational RB',  'RB1 Elite'),
        (93.0, 'RB1 Elite',        'RB1'),
        (88.0, 'RB1',              'High-End RB2'),
        (82.0, 'RB1',              'RB2'),
        (75.0, 'High-End RB2',     'RB2'),
        (66.0, 'RB2',              'Handcuff'),
        (55.0, 'Handcuff',         'Depth'),
        (40.0, 'Depth',            'Bust'),
        (0.0,  'High Bust Risk',   'High Bust Risk'),
    ],
    'QB': [
        (98.0, 'Generational QB',  'QB1 Elite'),
        (93.0, 'QB1 Elite',        'QB1'),
        (88.0, 'QB1',              'High-End QB2'),
        (82.0, 'QB1',              'QB2'),
        (75.0, 'QB2',              'Streaming QB'),
        (66.0, 'Streaming QB',     'Deep League QB'),
        (40.0, 'Deep League QB',   'Bench QB'),
        (0.0,  'High Bust Risk',   'High Bust Risk'),
    ],
    'TE': [
        (98.0, 'Generational TE',  'TE1 Elite'),
        (93.0, 'TE1 Elite',        'TE1'),
        (88.0, 'TE1',              'High-End TE2'),
        (82.0, 'TE1',              'TE2'),
        (75.0, 'High-End TE2',     'TE2'),
        (66.0, 'TE2',              'Streaming TE'),
        (55.0, 'Streaming TE',     'Depth'),
        (0.0,  'High Bust Risk',   'High Bust Risk'),
    ],
}


def get_prospect_outcome_range(
    position: str,
    grade: Optional[float],
) -> tuple[str, str]:
    """
    Return (outcome_ceiling, outcome_floor) for a prospect given position + grade.

    Args:
        position: 'QB', 'RB', 'WR', or 'TE'
        grade: 0-100 overall grade

    Returns:
        Tuple of (ceiling_label, floor_label)
    """
    if grade is None:
        return ('Unknown', 'Unknown')

    pos = (position or '').upper()
    ranges = PROSPECT_OUTCOME_RANGES.get(pos, PROSPECT_OUTCOME_RANGES['WR'])

    for min_grade, ceiling, floor in ranges:
        if grade >= min_grade:
            return ceiling, floor

    return ('High Bust Risk', 'High Bust Risk')


# Tier color mappings (for frontend reference)
TIER_COLORS: Dict[str, Dict[str, str]] = {
    'Elite Prospect': {
        'bg': 'bg-yellow-500/20',
        'text': 'text-yellow-300',
        'border': 'border-yellow-500/30',
        'weight': 'font-bold',
    },
    'First Round': {
        'bg': 'bg-blue-500/20',
        'text': 'text-blue-300',
        'border': 'border-blue-500/30',
        'weight': 'font-semibold',
    },
    'Second Round': {
        'bg': 'bg-green-500/20',
        'text': 'text-green-300',
        'border': 'border-green-500/30',
        'weight': 'font-semibold',
    },
    'Third Round': {
        'bg': 'bg-purple-500/20',
        'text': 'text-purple-300',
        'border': 'border-purple-500/30',
        'weight': 'font-medium',
    },
    'Mid Round': {
        'bg': 'bg-orange-500/20',
        'text': 'text-orange-300',
        'border': 'border-orange-500/30',
        'weight': 'font-medium',
    },
    'Late Round': {
        'bg': 'bg-slate-500/20',
        'text': 'text-slate-300',
        'border': 'border-slate-500/30',
        'weight': 'font-normal',
    },
    'Undrafted': {
        'bg': 'bg-gray-500/20',
        'text': 'text-gray-300',
        'border': 'border-gray-500/30',
        'weight': 'font-normal',
    },
}

