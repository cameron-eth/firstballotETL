#!/usr/bin/env python3
"""
Fantasy Football Rankings Updater
Updates dynasty_sf_top_150 table with latest fantasy rankings from nflreadpy
"""

import pandas as pd
import nflreadpy as nflread
from datetime import datetime
from config import config
from typing import Optional
import argparse


def fetch_fantasy_rankings(ranking_type: str = "draft") -> pd.DataFrame:
    """
    Fetch fantasy football rankings from nflreadpy.
    
    Args:
        ranking_type: Type of rankings to load:
            - "draft": Draft rankings/projections (default)
            - "week": Weekly rankings/projections
            - "all": All historical rankings/projections
    
    Returns:
        Pandas DataFrame with fantasy rankings data
    """
    print(f"📥 Fetching {ranking_type} rankings from nflreadpy...")
    
    # Load rankings (returns Polars DataFrame)
    df_polars = nflread.load_ff_rankings(type=ranking_type)
    
    # Convert to pandas
    df = df_polars.to_pandas()
    
    print(f"   ✓ Fetched {len(df)} ranking records")
    print(f"   Columns: {list(df.columns)}")
    
    return df


def transform_rankings_for_dynasty(df: pd.DataFrame, top_n: int = 400) -> pd.DataFrame:
    """
    Transform nflreadpy rankings data to match dynasty_sf_top_150 table schema.
    
    The dynasty_sf_top_150 table expects:
    - 'PLAYER NAME': Player display name
    - 'RK': Rank (integer)
    - 'POS': Position
    - 'TEAM': Team abbreviation
    
    Args:
        df: Raw rankings DataFrame from nflreadpy
        top_n: Number of top players to include (default 400)
    
    Returns:
        Transformed DataFrame ready for database upload
    """
    print(f"\n🔄 Transforming rankings data...")
    
    # Show sample of raw data
    print(f"\nSample raw data:")
    print(df.head(3))
    
    # Filter for dynasty superflex rankings (if available)
    if 'page_type' in df.columns:
        # Look for dynasty-sf or dynasty-superflex rankings
        dynasty_types = df['page_type'].unique()
        print(f"   Available ranking types: {list(dynasty_types)[:10]}...")  # Show first 10
        
        # Priority order: dynasty-op (superflex), dynasty-overall, or any dynasty rankings
        if 'dynasty-op' in dynasty_types:
            df = df[df['page_type'] == 'dynasty-op'].copy()
            print(f"   ✓ Filtered to dynasty-op (superflex) rankings")
        elif 'dynasty-sf' in dynasty_types:
            df = df[df['page_type'] == 'dynasty-sf'].copy()
            print(f"   ✓ Filtered to dynasty-sf rankings")
        elif 'dynasty-overall' in dynasty_types:
            df = df[df['page_type'] == 'dynasty-overall'].copy()
            print(f"   ✓ Filtered to dynasty-overall rankings")
        else:
            # Fallback: use position-specific dynasty rankings
            df = df[df['page_type'].str.contains('dynasty', case=False, na=False)].copy()
            print(f"   ✓ Filtered to all dynasty rankings (mixed positions)")
    
    # Filter for most recent rankings (if multiple dates exist)
    if 'scrape_date' in df.columns or 'scraped_date' in df.columns:
        date_col = 'scrape_date' if 'scrape_date' in df.columns else 'scraped_date'
        df['date_parsed'] = pd.to_datetime(df[date_col], errors='coerce')
        latest_date = df['date_parsed'].max()
        df = df[df['date_parsed'] == latest_date].copy()
        print(f"   Filtered to latest rankings date: {latest_date.strftime('%Y-%m-%d')}")
    
    # Map column names from nflreadpy to our schema
    # nflreadpy columns: player, ecr (expert consensus rank), pos, team
    column_mapping = {
        'player': 'PLAYER NAME',
        'ecr': 'RK',
        'pos': 'POS',
        'team': 'TEAM'
    }
    
    # Check which columns exist and map them
    available_cols = {}
    for src, dest in column_mapping.items():
        if src in df.columns:
            available_cols[src] = dest
    
    # Select and rename columns
    df_clean = df[list(available_cols.keys())].copy()
    df_clean = df_clean.rename(columns=available_cols)
    
    # Ensure RK is numeric and sort by it
    if 'RK' in df_clean.columns:
        df_clean['RK'] = pd.to_numeric(df_clean['RK'], errors='coerce')
        df_clean = df_clean.sort_values('RK').reset_index(drop=True)
    
    # Filter out rows with missing critical data
    df_clean = df_clean.dropna(subset=['PLAYER NAME'])
    
    # Remove duplicates - keep first occurrence of each player
    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['PLAYER NAME'], keep='first')
    after_dedup = len(df_clean)
    if before_dedup != after_dedup:
        print(f"   Removed {before_dedup - after_dedup} duplicate players")
    
    # Take top N players
    df_clean = df_clean.head(top_n)
    
    # Ensure rank is sequential after deduplication
    df_clean['RK'] = range(1, len(df_clean) + 1)
    
    # Fill missing values
    df_clean['POS'] = df_clean['POS'].fillna('UNKNOWN')
    df_clean['TEAM'] = df_clean['TEAM'].fillna('FA')
    
    print(f"   ✓ Prepared {len(df_clean)} rankings for upload")
    
    # Show sample of transformed data
    print(f"\nSample transformed data:")
    print(df_clean.head(10))
    
    # Show position breakdown
    print(f"\nPosition breakdown:")
    pos_counts = df_clean['POS'].value_counts()
    for pos, count in pos_counts.items():
        print(f"   {pos}: {count}")
    
    return df_clean


def upload_rankings_to_supabase(
    df: pd.DataFrame,
    table_name: str = 'dynasty_sf_top_150',
    clear_existing: bool = True
) -> None:
    """
    Upload rankings DataFrame to Supabase table.
    
    Args:
        df: DataFrame with rankings data
        table_name: Name of the Supabase table (default: 'dynasty_sf_top_150')
        clear_existing: If True, delete existing records before upload
    """
    supabase = config.get_supabase_client()
    
    if supabase is None:
        print("❌ Error: Supabase client not configured. Check your .env file.")
        return
    
    print(f"\n📤 Uploading rankings to {table_name}...")
    
    # Clear existing rankings if requested
    if clear_existing:
        try:
            print(f"   Clearing existing records from {table_name}...")
            # Delete all records (Supabase doesn't have truncate via API)
            result = supabase.table(table_name).delete().neq('RK', 0).execute()
            print(f"   ✓ Cleared existing records")
        except Exception as e:
            print(f"   ⚠ Warning: Could not clear existing records: {e}")
            print(f"   Continuing with upsert...")
    
    # Convert DataFrame to records
    records = df.to_dict('records')
    
    # Upload in batches
    batch_size = 100
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    print(f"   Uploading {len(records)} records in {total_batches} batches...")
    
    uploaded = 0
    errors = []
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        try:
            # Insert records
            response = supabase.table(table_name).insert(batch).execute()
            uploaded += len(batch)
            print(f"   ✓ Batch {i//batch_size + 1}/{total_batches} uploaded")
        except Exception as e:
            errors.append(str(e))
            print(f"   ✗ Batch {i//batch_size + 1}/{total_batches} failed: {e}")
    
    if errors:
        print(f"\n⚠ Upload completed with {len(errors)} errors")
    else:
        print(f"\n✅ Successfully uploaded {uploaded} rankings to {table_name}")


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description="Update dynasty fantasy football rankings from nflreadpy"
    )
    
    parser.add_argument(
        '--type',
        choices=['draft', 'week', 'all'],
        default='draft',
        help='Type of rankings to fetch (default: draft)'
    )
    
    parser.add_argument(
        '--top-n',
        type=int,
        default=400,
        help='Number of top players to include (default: 400)'
    )
    
    parser.add_argument(
        '--no-clear',
        action='store_true',
        help='Do not clear existing rankings before upload'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Fetch and transform data but do not upload to database'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("FANTASY FOOTBALL RANKINGS UPDATER")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Ranking type: {args.type}")
    print(f"Top N players: {args.top_n}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 80)
    
    try:
        # Step 1: Fetch rankings
        df_raw = fetch_fantasy_rankings(ranking_type=args.type)
        
        # Step 2: Transform data
        df_clean = transform_rankings_for_dynasty(df_raw, top_n=args.top_n)
        
        # Step 3: Upload to database (unless dry run)
        if args.dry_run:
            print("\n🏃 DRY RUN - Skipping database upload")
            print(f"\nPreview of top 20 rankings:")
            print(df_clean.head(20).to_string(index=False))
        else:
            upload_rankings_to_supabase(
                df_clean,
                clear_existing=not args.no_clear
            )
        
        # Save to CSV for backup/review
        output_file = f"ff_rankings_{args.type}_{datetime.now().strftime('%Y%m%d')}.csv"
        df_clean.to_csv(output_file, index=False)
        print(f"\n💾 Saved rankings to: {output_file}")
        
        print("\n" + "=" * 80)
        print("✅ RANKINGS UPDATE COMPLETE")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

