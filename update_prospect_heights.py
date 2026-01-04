#!/usr/bin/env python3
"""
Update prospect heights in the database.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from supabase import create_client, Client

def update_prospect_heights():
    """Update prospect heights."""
    
    # Initialize Supabase client
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    if not supabase_url or not supabase_key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    
    supabase: Client = create_client(supabase_url, supabase_key)
    
    # Height updates: name -> height in inches
    # 6'1" = 73 inches, 6'3" = 75 inches
    updates = [
        {'name': 'Jordyn Tyson', 'height': 73.0},  # 6'1"
        {'name': 'Carnell Tate', 'height': 75.0},   # 6'3"
    ]
    
    print("🔄 Updating prospect heights...")
    
    for update in updates:
        name = update['name']
        height = update['height']
        
        # Find the prospect
        response = supabase.table('dynasty_prospects').select('id, name, height').eq('name', name).execute()
        
        if not response.data:
            print(f"⚠ Prospect '{name}' not found")
            continue
        
        prospect = response.data[0]
        current_height = prospect.get('height')
        
        # Update height
        supabase.table('dynasty_prospects').update({
            'height': height
        }).eq('id', prospect['id']).execute()
        
        print(f"✓ Updated {name}: {current_height} → {height} inches ({int(height // 12)}'{int(height % 12)}\")")
    
    print("\n✅ Height updates complete")

if __name__ == '__main__':
    update_prospect_heights()

