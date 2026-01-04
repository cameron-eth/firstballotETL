#!/usr/bin/env python3
"""
Copy 2025 draft class data from historical_prospects to dynasty_prospects table.
This ensures 2025 data is available for the Historical Rankings comparison page.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import config


def copy_2025_to_dynasty_prospects():
    """Copy 2025 prospects from historical_prospects to dynasty_prospects."""
    supabase = config.get_supabase_client()
    if not supabase:
        print("❌ Failed to get Supabase client")
        return False

    print("=" * 80)
    print("COPYING 2025 DRAFT CLASS TO DYNASTY_PROSPECTS")
    print("=" * 80)

    # Fetch 2025 prospects from historical_prospects
    print("\n📊 Fetching 2025 prospects from historical_prospects...")
    result = supabase.table('historical_prospects').select('*').eq('draft_year', 2025).execute()

    if not result.data:
        print("❌ No 2025 prospects found in historical_prospects table")
        print("   Run the historical_prospect_pipeline.py first with --end-year 2025")
        return False

    historical_prospects = result.data
    print(f"   Found {len(historical_prospects)} prospects from 2025 draft class")

    # Transform data to match dynasty_prospects schema
    print("\n🔄 Transforming data for dynasty_prospects schema...")
    dynasty_prospects = []
    for hist in historical_prospects:
        # Map fields from historical_prospects to dynasty_prospects
        # Required fields (always included)
        dynasty_prospect = {
            'name': hist.get('name'),
            'position': hist.get('position'),
            'draft_year': 2025,
            # Use pre_draft_rank as rank if available, otherwise use draft_pick, or default to 999
            'rank': hist.get('pre_draft_rank') or hist.get('draft_pick') or 999,
        }
        
        # Optional fields (only include if not None)
        optional_fields = {
            'school': hist.get('college'),  # historical uses 'college', dynasty uses 'school'
            'height': hist.get('height'),
            'weight': hist.get('weight'),
            'hs_rank': hist.get('hs_rank'),
            'hs_stars': hist.get('hs_stars'),
            'hs_rating': hist.get('hs_rating'),
            'hs_school': hist.get('hs_school'),
            'hs_state': hist.get('hs_state'),
            'draft_round_projection': hist.get('draft_round'),
        }
        
        # Add optional fields only if they have values
        for k, v in optional_fields.items():
            if v is not None:
                dynasty_prospect[k] = v

        dynasty_prospects.append(dynasty_prospect)

    print(f"   Transformed {len(dynasty_prospects)} prospects")

    # Upsert to dynasty_prospects (using name, draft_year, position as unique key)
    print("\n💾 Upserting to dynasty_prospects table...")
    batch_size = 100
    uploaded = 0
    failed = 0

    for i in range(0, len(dynasty_prospects), batch_size):
        batch = dynasty_prospects[i:i + batch_size]
        try:
            result = supabase.table('dynasty_prospects').upsert(
                batch,
                on_conflict='name,draft_year,position'
            ).execute()
            uploaded += len(batch)
            print(f"   ✓ Uploaded {uploaded}/{len(dynasty_prospects)}")
        except Exception as e:
            failed += len(batch)
            print(f"   ❌ Error uploading batch: {str(e)[:100]}")

    print(f"\n✅ Copy complete!")
    print(f"   Uploaded: {uploaded}")
    print(f"   Failed: {failed}")
    print(f"   Total: {len(dynasty_prospects)}")

    return uploaded > 0


if __name__ == '__main__':
    try:
        success = copy_2025_to_dynasty_prospects()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Script failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

