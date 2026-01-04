#!/usr/bin/env python3
"""
Quick script to refresh the master_player_stats materialized view.
"""

import sys
from pathlib import Path

# Add parent directory to path to import config and utils
sys.path.insert(0, str(Path(__file__).parent))

from config import config

def main():
    """Refresh master stats view."""
    print("=" * 80)
    print("REFRESHING MASTER PLAYER STATS VIEW")
    print("=" * 80)
    
    # Get Supabase clients
    supabase_primary = config.get_supabase_client()
    supabase_secondary = config.get_supabase_client_2()
    
    clients = []
    labels = []
    
    if supabase_primary and config.enable_database:
        clients.append(supabase_primary)
        labels.append("Primary DB")
    
    if supabase_secondary and config.enable_database_2:
        clients.append(supabase_secondary)
        labels.append("Secondary DB")
    
    if not clients:
        print("❌ No Supabase clients configured. Please check your config.")
        sys.exit(1)
    
    # Refresh the view by calling the RPC function directly
    for client, label in zip(clients, labels):
        try:
            print(f"\n📊 Refreshing materialized view on {label}...")
            result = client.rpc('refresh_master_stats').execute()
            print(f"✅ Successfully refreshed master_player_stats on {label}")
            if hasattr(result, 'data'):
                print(f"   Result: {result.data}")
        except Exception as e:
            print(f"⚠ Could not refresh on {label}: {str(e)}")
            print(f"   Trying SQL command directly...")
            try:
                # Try calling REFRESH MATERIALIZED VIEW directly
                result = client.rpc('exec_sql', {
                    'query': 'REFRESH MATERIALIZED VIEW CONCURRENTLY master_player_stats;'
                }).execute()
                print(f"✅ Successfully refreshed via SQL on {label}")
            except Exception as e2:
                print(f"❌ SQL refresh also failed: {str(e2)}")
                print(f"   Please run this SQL manually in Supabase:")
                print(f"   REFRESH MATERIALIZED VIEW CONCURRENTLY master_player_stats;")
    
    print("\n✅ Master stats refresh complete!")

if __name__ == "__main__":
    main()

