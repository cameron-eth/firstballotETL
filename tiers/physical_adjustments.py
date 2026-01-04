"""
Physical Attribute Tier Adjustments
Height/weight can adjust tier assignments based on position-specific thresholds.
"""

from typing import Optional
from .definitions import PHYSICAL_THRESHOLDS


def calculate_physical_adjustment(
    position: str,
    height: Optional[float],
    weight: Optional[float],
    base_tier_numeric: int
) -> int:
    """
    Calculate tier adjustment based on physical attributes.
    
    Args:
        position: Player position (QB, RB, WR, TE)
        height: Height in inches
        weight: Weight in pounds
        base_tier_numeric: Base tier (1-5) from rank
        
    Returns:
        Adjusted tier numeric (can be +/- 1 from base, min 1, max 5)
    """
    if not height and not weight:
        return base_tier_numeric
    
    thresholds = PHYSICAL_THRESHOLDS.get(position.upper(), {})
    if not thresholds:
        return base_tier_numeric
    
    adjustment = 0
    
    # Check height
    if height:
        height_thresholds = thresholds.get('height', {})
        ideal_min = height_thresholds.get('ideal_min', 0)
        ideal_max = height_thresholds.get('ideal_max', 100)
        
        if ideal_min <= height <= ideal_max:
            # Ideal height range - slight boost for lower tiers
            if base_tier_numeric >= 3:
                adjustment += 0.5  # Half tier boost
        elif height < ideal_min - 2:
            # Significantly undersized - penalty
            if position == 'QB' or position == 'TE':
                adjustment -= 1  # Height matters more for these positions
        elif height > ideal_max + 2:
            # Significantly oversized - usually fine, but can be negative for RB
            if position == 'RB':
                adjustment -= 0.5
    
    # Check weight
    if weight:
        weight_thresholds = thresholds.get('weight', {})
        ideal_min = weight_thresholds.get('ideal_min', 0)
        ideal_max = weight_thresholds.get('ideal_max', 500)
        
        if ideal_min <= weight <= ideal_max:
            # Ideal weight range - slight boost
            if base_tier_numeric >= 3:
                adjustment += 0.5
        elif weight < ideal_min - 15:
            # Significantly underweight - penalty
            if position in ['RB', 'TE']:
                adjustment -= 1  # Weight matters more for these
        elif weight > ideal_max + 30:
            # Significantly overweight - penalty
            if position == 'RB':
                adjustment -= 1
    
    # Apply adjustment (round to nearest integer)
    adjusted_tier = base_tier_numeric + round(adjustment)
    
    # Clamp between 1 and 5
    return max(1, min(5, adjusted_tier))


def get_physical_score(
    position: str,
    height: Optional[float],
    weight: Optional[float]
) -> float:
    """
    Get a physical score (0-1) based on how well attributes match ideal ranges.
    
    Args:
        position: Player position
        height: Height in inches
        weight: Weight in pounds
        
    Returns:
        Score from 0.0 (poor) to 1.0 (ideal)
    """
    if not height and not weight:
        return 0.5  # Neutral if unknown
    
    thresholds = PHYSICAL_THRESHOLDS.get(position.upper(), {})
    if not thresholds:
        return 0.5
    
    score = 0.0
    factors = 0
    
    # Height score
    if height:
        height_thresholds = thresholds.get('height', {})
        ideal_min = height_thresholds.get('ideal_min', 0)
        ideal_max = height_thresholds.get('ideal_max', 100)
        ideal_center = (ideal_min + ideal_max) / 2
        
        # Score based on distance from ideal center
        distance = abs(height - ideal_center)
        max_distance = (ideal_max - ideal_min) / 2
        height_score = max(0, 1.0 - (distance / max_distance))
        score += height_score
        factors += 1
    
    # Weight score
    if weight:
        weight_thresholds = thresholds.get('weight', {})
        ideal_min = weight_thresholds.get('ideal_min', 0)
        ideal_max = weight_thresholds.get('ideal_max', 500)
        ideal_center = (ideal_min + ideal_max) / 2
        
        # Score based on distance from ideal center
        distance = abs(weight - ideal_center)
        max_distance = (ideal_max - ideal_min) / 2
        weight_score = max(0, 1.0 - (distance / max_distance))
        score += weight_score
        factors += 1
    
    # Average if both factors present
    return score / factors if factors > 0 else 0.5

