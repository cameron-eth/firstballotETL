"""
Prospect Valuation Calculations
Hybrid Exponential-Tiered Value Curve for prospects.
All calculations happen at write time, stored in database.
"""

import math
from typing import Optional

# Position multipliers for prospects
POSITION_MULTIPLIERS: dict[str, float] = {
    'QB': 1.4,  # QB premium due to scarcity
    'RB': 0.8,  # RB discount due to short shelf life and injury risk
    'WR': 1.0,  # Baseline
    'TE': 1.2,  # TE premium due to scarcity
}

# Prospect valuation parameters by tier
# Format: (min_rank, max_rank, base_value, tier_floor, k, tier_start)
PROSPECT_VALUATION_PARAMS: list[tuple[int, int, float, float, float, int]] = [
    (1, 12, 70.0, 26.0, 0.1, 1),      # Tier 1 Prospects (1-12) - High upside, moderate risk
    (13, 36, 52.0, 15.0, 0.07, 13),   # Tier 2 Prospects (13-36) - Good upside, higher risk
    (37, 72, 35.0, 8.0, 0.05, 37),    # Tier 3 Prospects (37-72) - Moderate upside, high risk
    (73, 9999, 15.0, 3.0, 0.02, 73),  # Tier 4+ Prospects (73+) - Low upside, very high risk
]


def calculate_prospect_value(rank: int, position: Optional[str] = None) -> float:
    """
    Calculate prospect value using Hybrid Exponential-Tiered Value Curve.
    
    Formula: Value(rank) = BaseValue(tier) × e^(-k × (rank - TierStart)) + TierFloor
    
    Args:
        rank: Prospect rank (1-based)
        position: Optional position (QB, RB, WR, TE) for position multiplier
        
    Returns:
        Calculated value (rounded to 2 decimal places)
    """
    if not rank or rank <= 0 or rank > 1000:
        return 1.0  # Unranked
    
    # Find tier parameters
    base_value = 15.0
    tier_floor = 3.0
    k = 0.02
    tier_start = 73
    
    for min_rank, max_rank, bv, tf, k_val, ts in PROSPECT_VALUATION_PARAMS:
        if min_rank <= rank <= max_rank:
            base_value = bv
            tier_floor = tf
            k = k_val
            tier_start = ts
            break
    
    # Calculate value using exponential decay within tier
    value = base_value * math.exp(-k * (rank - tier_start)) + tier_floor
    
    # Apply position multiplier if provided
    if position:
        multiplier = POSITION_MULTIPLIERS.get(position.upper(), 1.0)
        value *= multiplier
    
    # Round to 2 decimal places
    return round(value, 2)


def get_position_multiplier(position: str) -> float:
    """
    Get position multiplier for a given position.
    
    Args:
        position: Player position (QB, RB, WR, TE)
        
    Returns:
        Position multiplier value
    """
    return POSITION_MULTIPLIERS.get(position.upper(), 1.0)

