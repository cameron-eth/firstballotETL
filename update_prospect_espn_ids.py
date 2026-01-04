#!/usr/bin/env python3
"""
Update dynasty_prospects with ESPN IDs from the ESPN players JSON file.
Matches prospects to NFL players by name and updates espn_id + headshot_url.
"""

import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import config


def normalize_name(name: str) -> str:
    """Normalize player name for matching."""
    if not name:
        return ""
    # Remove suffixes like Jr., III, II, etc.
    suffixes = [' jr.', ' jr', ' iii', ' ii', ' iv', ' sr.', ' sr']
    normalized = name.lower().strip()
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()
    return normalized


def load_espn_players(json_path: str) -> dict:
    """Load ESPN players JSON and create a lookup dict by normalized name."""
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Create lookup dict: normalized_name -> espn player data
        lookup = {}
        for player in data:
            if not player.get('active', True):
                continue
            
            full_name = player.get('fullName', '')
            if full_name:
                key = normalize_name(full_name)
                # Store the player with highest ID (most recent) if duplicates
                if key not in lookup or int(player.get('id', 0)) > int(lookup[key].get('id', 0)):
                    lookup[key] = player
        
        print(f"✓ Loaded {len(lookup)} active NFL players from ESPN JSON")
        return lookup
        
    except FileNotFoundError:
        print(f"❌ ESPN JSON file not found at: {json_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse ESPN JSON: {e}")
        return {}


def update_prospects_with_espn_ids():
    """Match prospects to ESPN players and update the database."""
    
    # Path to ESPN JSON file (relative to ETL folder)
    espn_json_path = Path(__file__).parent.parent / 'firstballotfantasy' / 'firstballot' / 'public' / 'espn_all_active_nfl_players.json'
    
    print("=" * 80)
    print("UPDATE DYNASTY_PROSPECTS WITH ESPN IDs")
    print("=" * 80)
    
    # Load ESPN data
    espn_lookup = load_espn_players(str(espn_json_path))
    if not espn_lookup:
        return False
    
    # Get Supabase client
    supabase = config.get_supabase_client()
    if not supabase:
        print("❌ Failed to get Supabase client")
        return False
    
    # Fetch prospects missing ESPN IDs
    print("\n📊 Fetching prospects without ESPN IDs...")
    result = supabase.table('dynasty_prospects').select('id, name, position, draft_year, espn_id').is_('espn_id', 'null').execute()
    
    if not result.data:
        print("✓ All prospects already have ESPN IDs!")
        return True
    
    prospects = result.data
    print(f"   Found {len(prospects)} prospects without ESPN IDs")
    
    # Match and update
    matches = 0
    no_match = []
    
    print("\n🔄 Matching prospects to ESPN players...")
    
    for prospect in prospects:
        name = prospect.get('name', '')
        prospect_id = prospect.get('id')
        normalized = normalize_name(name)
        
        espn_player = espn_lookup.get(normalized)
        
        if espn_player:
            espn_id = espn_player.get('id')
            # Use NFL headshot URL
            headshot_url = f"https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/{espn_id}.png&w=350&h=254"
            
            # Update database
            try:
                supabase.table('dynasty_prospects').update({
                    'espn_id': int(espn_id),
                    'headshot_url': headshot_url
                }).eq('id', prospect_id).execute()
                
                matches += 1
                print(f"   ✓ {name} -> ESPN ID: {espn_id}")
            except Exception as e:
                print(f"   ❌ Failed to update {name}: {e}")
        else:
            no_match.append(f"{name} ({prospect.get('draft_year', 'N/A')})")
    
    # Summary
    print("\n" + "=" * 80)
    print("UPDATE COMPLETE")
    print("=" * 80)
    print(f"   ✓ Matched: {matches}")
    print(f"   ✗ No match: {len(no_match)}")
    
    if no_match and len(no_match) <= 20:
        print("\n   Unmatched prospects:")
        for name in no_match:
            print(f"      - {name}")
    elif no_match:
        print(f"\n   (Showing first 20 of {len(no_match)} unmatched)")
        for name in no_match[:20]:
            print(f"      - {name}")
    
    return True


if __name__ == '__main__':
    success = update_prospects_with_espn_ids()
    sys.exit(0 if success else 1)

