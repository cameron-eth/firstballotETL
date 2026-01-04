"""
Tier Constants - Shared constants for tier system
These can be imported by both ETL and frontend (if needed).
"""

# Tier display names (for frontend reference)
TIER_DISPLAY_NAMES = {
    'Tier 1': 'Elite Prospect',
    'Tier 2': 'First Round',
    'Tier 3': 'Second Round',
    'Tier 4': 'Third Round',
    'Tier 5': 'Mid Round',
}

# CSS class mappings for frontend (for reference)
TIER_CSS_CLASSES = {
    'Elite Prospect': 'bg-yellow-500/20 text-yellow-300 border border-yellow-500/30 font-bold',
    'First Round': 'bg-blue-500/20 text-blue-300 border border-blue-500/30 font-semibold',
    'Second Round': 'bg-green-500/20 text-green-300 border border-green-500/30 font-semibold',
    'Third Round': 'bg-purple-500/20 text-purple-300 border border-purple-500/30 font-medium',
    'Mid Round': 'bg-orange-500/20 text-orange-300 border border-orange-500/30 font-medium',
    'Late Round': 'bg-slate-500/20 text-slate-300 border border-slate-500/30 font-normal',
    'Undrafted': 'bg-gray-500/20 text-gray-300 border border-gray-500/30 font-normal',
}

