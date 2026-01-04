"""
Tier Definitions - Single Source of Truth
All tier breakpoints and definitions are centralized here.
"""

from typing import Dict, List, Tuple

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

