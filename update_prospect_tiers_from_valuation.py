#!/usr/bin/env python3
"""
Update prospect tiers based on valuation instead of rank.
This ensures players with higher valuations get higher tiers.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from supabase import create_client, Client
from config import config
from valuation import calculate_prospect_value
from tiers import calculate_prospect_tier_from_valuation

def update_prospect_tiers():
    """Update all prospect tiers based on their valuations."""
    
    # Initialize Supabase client
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    if not supabase_url or not supabase_key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    
    supabase: Client = create_client(supabase_url, supabase_key)
    
    print("🔄 Fetching all prospects from database...")
    
    # Fetch all prospects
    response = supabase.table('dynasty_prospects').select('*').execute()
    prospects = response.data
    
    if not prospects:
        print("⚠ No prospects found in database")
        return
    
    print(f"✓ Found {len(prospects)} prospects")
    print("\n🔄 Updating tiers based on valuations...")
    
    updates = []
    tier_changes = {}
    
    for prospect in prospects:
        rank = prospect.get('rank')
        position = prospect.get('position')
        current_tier = prospect.get('tier')
        current_valuation = prospect.get('valuation')
        
        if not rank or not position:
            continue
        
        # Calculate valuation if not present
        if current_valuation is None:
            valuation = calculate_prospect_value(rank, position)
        else:
            valuation = float(current_valuation)
        
        # Calculate tier from valuation
        new_tier, new_tier_numeric = calculate_prospect_tier_from_valuation(valuation)
        
        # Track changes
        if current_tier != new_tier:
            if current_tier not in tier_changes:
                tier_changes[current_tier] = {}
            if new_tier not in tier_changes[current_tier]:
                tier_changes[current_tier][new_tier] = 0
            tier_changes[current_tier][new_tier] += 1
        
        # Prepare update
        updates.append({
            'id': prospect['id'],
            'tier': new_tier,
            'tier_numeric': new_tier_numeric,
            'valuation': valuation,
        })
    
    # Batch update (Supabase allows up to 1000 rows per request)
    batch_size = 1000
    total_updated = 0
    
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        supabase.table('dynasty_prospects').upsert(batch).execute()
        total_updated += len(batch)
        print(f"   ✓ Updated {total_updated}/{len(updates)} prospects")
    
    print(f"\n✅ Successfully updated {total_updated} prospects")
    
    # Print tier change summary
    if tier_changes:
        print("\n📊 Tier Changes Summary:")
        for old_tier, new_tiers in tier_changes.items():
            for new_tier, count in new_tiers.items():
                print(f"   {old_tier} → {new_tier}: {count} prospects")
    else:
        print("\n✓ No tier changes needed (all tiers already match valuations)")

if __name__ == '__main__':
    update_prospect_tiers()

