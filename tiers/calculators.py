"""
Tier Calculation Functions
All tier calculations use definitions from definitions.py
"""

from typing import Optional
from .definitions import (
    PROSPECT_TIER_DEFINITIONS,
    PROSPECT_TIER_BREAKPOINTS,
    TIER_DISPLAY_NAMES,
)

# Valuation-based tier breakpoints
# These ensure players with higher valuations get higher tiers
# Format: (min_valuation, tier_name, tier_numeric)
VALUATION_TIER_BREAKPOINTS: list[tuple[float, str, int]] = [
    (50.0, 'Tier 1', 1),      # Elite prospects (50+)
    (30.0, 'Tier 2', 2),      # First round talent (30-49.99)
    (20.0, 'Tier 3', 3),      # Early 2nd round (20-29.99)
    (10.0, 'Tier 4', 4),      # Late 2nd/Early 3rd (10-19.99)
    (0.0, 'Tier 5', 5),       # Mid-late round (<10)
]


def calculate_prospect_tier(rank: int) -> str:
    """
    Calculate numeric tier (Tier 1-5) based on rank.
    
    Args:
        rank: Prospect rank (1-based)
        
    Returns:
        Tier string: 'Tier 1', 'Tier 2', 'Tier 3', 'Tier 4', or 'Tier 5'
    """
    if not rank or rank <= 0:
        return 'Tier 5'
    
    for min_rank, max_rank, tier_name, _ in PROSPECT_TIER_BREAKPOINTS:
        if min_rank <= rank <= max_rank:
            return tier_name
    
    # Fallback to Tier 5
    return 'Tier 5'


def calculate_prospect_display_tier(rank: int) -> str:
    """
    Calculate display tier name based on rank.
    This matches the frontend tier display names.
    
    Args:
        rank: Prospect rank (1-based)
        
    Returns:
        Display tier: 'Elite Prospect', 'First Round', 'Second Round', etc.
    """
    if not rank or rank <= 0:
        return 'Undrafted'
    
    # Check each tier definition
    for tier_name, bounds in PROSPECT_TIER_DEFINITIONS.items():
        if bounds['min_rank'] <= rank <= bounds['max_rank']:
            return tier_name
    
    # Fallback to Undrafted
    return 'Undrafted'


def get_tier_from_rank(rank: int) -> tuple[str, str, int]:
    """
    Get both numeric tier and display tier from rank.
    
    Args:
        rank: Prospect rank (1-based)
        
    Returns:
        Tuple of (tier, display_tier, tier_numeric)
    """
    tier = calculate_prospect_tier(rank)
    display_tier = calculate_prospect_display_tier(rank)
    
    # Get numeric tier value for sorting
    tier_numeric = 5  # Default
    for min_rank, max_rank, tier_name, tier_num in PROSPECT_TIER_BREAKPOINTS:
        if min_rank <= rank <= max_rank:
            tier_numeric = tier_num
            break
    
    return tier, display_tier, tier_numeric


def get_tier_numeric(rank: int) -> int:
    """
    Get numeric tier value (1-5) for sorting purposes.
    
    Args:
        rank: Prospect rank (1-based)
        
    Returns:
        Numeric tier: 1, 2, 3, 4, or 5
    """
    _, _, tier_numeric = get_tier_from_rank(rank)
    return tier_numeric


def calculate_prospect_tier_from_valuation(valuation: float) -> tuple[str, int]:
    """
    Calculate tier based on valuation instead of rank.
    This ensures players with higher valuations get higher tiers,
    regardless of their rank or position.
    
    Args:
        valuation: Prospect valuation (calculated with position multipliers)
        
    Returns:
        Tuple of (tier_name, tier_numeric)
    """
    if not valuation or valuation <= 0:
        return 'Tier 5', 5
    
    # Check valuation ranges from highest to lowest
    for min_val, tier_name, tier_num in VALUATION_TIER_BREAKPOINTS:
        if valuation >= min_val:
            return tier_name, tier_num
    
    # Fallback to Tier 5
    return 'Tier 5', 5

