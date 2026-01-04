#!/usr/bin/env python3
"""
Update prospect heights and re-run NFL comparisons.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from supabase import create_client, Client
from college_ranking_pipeline import CollegeRankingPipeline
from config import config
import pandas as pd

def update_heights_and_comps():
    """Update heights and re-run comparisons for specific players."""
    
    # Initialize Supabase client
    supabase = config.get_supabase_client()
    if not supabase:
        print("❌ Failed to get Supabase client")
        return
    
    # Height updates: name -> height in inches
    # 6'1" = 73 inches, 6'3" = 75 inches
    updates = [
        {'name': 'Jordyn Tyson', 'height': 73.0},  # 6'1"
        {'name': 'Carnell Tate', 'height': 75.0},   # 6'3"
    ]
    
    print("🔄 Updating prospect heights...")
    
    # Update heights first
    for update in updates:
        name = update['name']
        height = update['height']
        
        # Find the prospect
        response = supabase.from_('dynasty_prospects').select('id, name, height, position').eq('name', name).execute()
        
        if not response.data:
            print(f"⚠ Prospect '{name}' not found")
            continue
        
        prospect = response.data[0]
        current_height = prospect.get('height')
        
        # Update height
        supabase.from_('dynasty_prospects').update({
            'height': height
        }).eq('id', prospect['id']).execute()
        
        print(f"✓ Updated {name}: {current_height} → {height} inches ({int(height // 12)}'{int(height % 12)}\")")
    
    print("\n🔄 Re-running NFL comparisons...")
    
    # Initialize pipeline
    pipeline = CollegeRankingPipeline()
    
    # Fetch NFL stats for comparisons (same way as pipeline does)
    print("   Fetching NFL stats...")
    skill_positions = ['QB', 'RB', 'WR', 'TE']
    nfl_result = supabase.from_('master_player_stats')\
        .select('player_display_name, position, fantasy_ppg, games_played')\
        .eq('season', 2025)\
        .in_('position', skill_positions)\
        .gte('games_played', 1)\
        .execute()
    
    if nfl_result.data:
        nfl_stats_df = pd.DataFrame(nfl_result.data)
        print(f"   ✓ Found {len(nfl_stats_df)} NFL players")
    else:
        print("⚠ No NFL stats available, skipping comparisons")
        return
    
    # Re-run comparisons for updated players
    for update in updates:
        name = update['name']
        
        # Fetch prospect data
        response = supabase.from_('dynasty_prospects').select('*').eq('name', name).execute()
        
        if not response.data:
            continue
        
        prospect = response.data[0]
        position = prospect.get('position')
        tier = prospect.get('tier')
        
        if not position or not tier:
            print(f"⚠ Missing position or tier for {name}")
            continue
        
        # Fetch college stats if available
        college_stats = prospect.get('college_stats')
        stats = college_stats if college_stats else None
        
        # Find comparisons
        if stats:
            comps = pipeline.find_nfl_comparisons(
                name, position, stats, tier, nfl_stats_df
            )
        else:
            comps = pipeline.find_tier_based_comps(position, tier, nfl_stats_df)
        
        if comps:
            # Format comparisons string
            comps_str = ', '.join(comps)
            
            # Update database
            supabase.from_('dynasty_prospects').update({
                'nfl_comparisons': comps_str
            }).eq('id', prospect['id']).execute()
            
            print(f"✓ Updated {name} comparisons: {comps_str}")
        else:
            print(f"⚠ No comparisons found for {name}")
    
    print("\n✅ Height and comparison updates complete")

if __name__ == '__main__':
    update_heights_and_comps()

