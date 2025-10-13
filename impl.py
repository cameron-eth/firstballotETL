#!/usr/bin/env python3
"""
NFL Data Pipeline Implementation
Main entry point for fetching and storing NFL weekly stats and NGS data.
"""

import argparse
import sys
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from config import config
from utils import (
    get_weekly_data,
    get_seasonal_data,
    get_ngs_data,
    get_player_stats,
    get_play_by_play_data,
    get_weekly_roster_data,
    get_ftn_data,
    clean_weekly_data,
    clean_ngs_data,
    add_fantasy_scoring,
    save_dataframe,
    upload_to_supabase,
    upload_to_multiple_databases,
    refresh_master_stats_view
)


def fetch_weekly_stats(years: Optional[List[int]] = None) -> None:
    """
    Fetch weekly NFL player statistics.
    
    Args:
        years: Optional list of years to fetch. If None, uses config range.
    """
    print("=" * 80)
    print("FETCHING WEEKLY STATS")
    print("=" * 80)
    
    if years is None:
        years = config.get_year_range()
    
    # Fetch data
    df = get_weekly_data(years, verbose=config.verbose)
    
    # Clean data
    df = clean_weekly_data(df, positions=config.positions)
    
    print(f"\nCleaned data: {len(df)} records")
    print(f"Columns ({len(df.columns)}): {list(df.columns)[:10]}...")
    
    # Show sample data
    print(f"\n--- Sample Data (first 5 rows) ---")
    print(df.head())
    
    # Show data info
    print(f"\n--- Data Summary ---")
    print(f"Shape: {df.shape}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    # Save to files
    filename = f"weekly_stats_{'_'.join(map(str, years))}"
    save_dataframe(
        df=df,
        filename=filename,
        output_dir=config.output_dir,
        save_csv=config.save_to_csv,
        save_json=config.save_to_json,
        verbose=config.verbose
    )
    
    # Upload to Supabase (both databases)
    if config.save_to_database:
        table_name = 'nfl_weekly_stats'
        
        # Get both database clients
        supabase_primary = config.get_supabase_client()
        supabase_secondary = config.get_supabase_client_2()
        
        # Collect active clients and labels
        clients = []
        labels = []
        
        if supabase_primary and config.enable_database:
            clients.append(supabase_primary)
            labels.append("Primary DB")
        
        if supabase_secondary and config.enable_database_2:
            clients.append(supabase_secondary)
            labels.append("Secondary DB")
        
        # Upload to all configured databases
        if clients:
            upload_to_multiple_databases(
                df=df,
                table_name=table_name,
                supabase_clients=clients,
                db_labels=labels,
                batch_size=config.batch_size,
                verbose=config.verbose
            )
        else:
            if config.verbose:
                print("⚠ No database clients configured, skipping upload")
    
    print(f"\n✓ Weekly stats processing complete")


def fetch_seasonal_stats(years: Optional[List[int]] = None) -> None:
    """
    Fetch seasonal NFL player statistics.
    
    Args:
        years: Optional list of years to fetch. If None, uses config range.
    """
    print("=" * 80)
    print("FETCHING SEASONAL STATS")
    print("=" * 80)
    
    if years is None:
        years = config.get_year_range()
    
    # Fetch data
    df = get_seasonal_data(years, verbose=config.verbose)
    
    print(f"\nFetched data: {len(df)} records")
    print(f"Columns ({len(df.columns)}): {list(df.columns)[:10]}...")
    
    # Show sample data
    print(f"\n--- Sample Data (first 5 rows) ---")
    print(df.head())
    
    # Save to files
    filename = f"seasonal_stats_{'_'.join(map(str, years))}"
    save_dataframe(
        df=df,
        filename=filename,
        output_dir=config.output_dir,
        save_csv=config.save_to_csv,
        save_json=config.save_to_json,
        verbose=config.verbose
    )
    
    print(f"\n✓ Seasonal stats processing complete")


def fetch_pbp_data(years: Optional[List[int]] = None) -> None:
    """
    Fetch play-by-play data.
    
    Args:
        years: Optional list of years to fetch. If None, uses config range.
    """
    print("=" * 80)
    print("FETCHING PLAY-BY-PLAY DATA")
    print("=" * 80)
    
    if years is None:
        years = config.get_year_range()
    
    # Fetch data
    df = get_play_by_play_data(years, verbose=config.verbose)
    
    print(f"\nFetched data: {len(df)} records")
    print(f"Columns ({len(df.columns)}): {list(df.columns)[:10]}...")
    
    # Show sample data (first 3 rows for PBP since it's huge)
    print(f"\n--- Sample Data (first 3 rows) ---")
    print(df.head(3))
    
    # Save to files
    filename = f"play_by_play_{'_'.join(map(str, years))}"
    save_dataframe(
        df=df,
        filename=filename,
        output_dir=config.output_dir,
        save_csv=config.save_to_csv,
        save_json=config.save_to_json,
        verbose=config.verbose
    )
    
    print(f"\n✓ Play-by-play data processing complete")


def fetch_roster_data(years: Optional[List[int]] = None) -> None:
    """
    Fetch weekly roster data.
    
    Args:
        years: Optional list of years to fetch. If None, uses config range.
    """
    print("=" * 80)
    print("FETCHING WEEKLY ROSTER DATA")
    print("=" * 80)
    
    if years is None:
        years = config.get_year_range()
    
    # Fetch data
    df = get_weekly_roster_data(years, verbose=config.verbose)
    
    print(f"\nFetched data: {len(df)} records")
    print(f"Columns ({len(df.columns)}): {list(df.columns)[:10]}...")
    
    # Show sample data
    print(f"\n--- Sample Data (first 5 rows) ---")
    print(df.head())
    
    # Save to files
    filename = f"weekly_rosters_{'_'.join(map(str, years))}"
    save_dataframe(
        df=df,
        filename=filename,
        output_dir=config.output_dir,
        save_csv=config.save_to_csv,
        save_json=config.save_to_json,
        verbose=config.verbose
    )
    
    print(f"\n✓ Weekly roster data processing complete")


def fetch_ftn_data(years: Optional[List[int]] = None) -> None:
    """
    Fetch FTN fantasy data.
    
    Args:
        years: Optional list of years to fetch. If None, uses config range.
    """
    print("=" * 80)
    print("FETCHING FTN DATA")
    print("=" * 80)
    
    if years is None:
        years = config.get_year_range()
    
    # Fetch data
    df = get_ftn_data(years, verbose=config.verbose)
    
    print(f"\nFetched data: {len(df)} records")
    print(f"Columns ({len(df.columns)}): {list(df.columns)[:10]}...")
    
    # Show sample data
    print(f"\n--- Sample Data (first 5 rows) ---")
    print(df.head())
    
    # Save to files
    filename = f"ftn_data_{'_'.join(map(str, years))}"
    save_dataframe(
        df=df,
        filename=filename,
        output_dir=config.output_dir,
        save_csv=config.save_to_csv,
        save_json=config.save_to_json,
        verbose=config.verbose
    )
    
    print(f"\n✓ FTN data processing complete")


def fetch_player_stats(years: Optional[List[int]] = None) -> None:
    """
    Fetch complete player stats from nflreadpy and upload to database.
    
    This includes all stat types (passing + rushing + receiving) with
    pre-calculated fantasy points in one table.
    
    Args:
        years: Optional list of years to fetch. If None, uses config range.
    """
    print("=" * 80)
    print("FETCHING PLAYER STATS (nflreadpy)")
    print("=" * 80)
    
    if years is None:
        years = config.get_year_range()
    
    # Fetch data
    df = get_player_stats(years, verbose=config.verbose)
    
    print(f"\nFetched {len(df)} player stat records")
    print(f"Sample data:")
    print(df.head(3))
    
    # Upload to database
    if config.save_to_database:
        table_name = 'nfl_player_stats'
        
        # Upload to primary database
        if config.enable_database:
            supabase_primary = config.get_supabase_client()
            if supabase_primary:
                upload_to_supabase(
                    df=df,
                    table_name=table_name,
                    supabase_client=supabase_primary,
                    verbose=config.verbose,
                    db_label="Primary DB"
                )
        
        # Upload to secondary database
        if config.enable_database_2:
            supabase_secondary = config.get_supabase_client_2()
            if supabase_secondary:
                upload_to_supabase(
                    df=df,
                    table_name=table_name,
                    supabase_client=supabase_secondary,
                    verbose=config.verbose,
                    db_label="Secondary DB"
                )
    
    # Save to CSV if enabled
    if config.save_to_csv:
        output_dir = Path(config.output_dir)
        output_dir.mkdir(exist_ok=True)
        
        years_str = '_'.join(map(str, sorted(years)))
        filename = f"player_stats_{years_str}.csv"
        filepath = output_dir / filename
        
        df.to_csv(filepath, index=False)
        if config.verbose:
            print(f"\nSaved to: {filepath}")


def fetch_ngs_stats(
    stat_types: Optional[List[str]] = None,
    years: Optional[List[int]] = None
) -> None:
    """
    Fetch and upload Next Gen Stats data (advanced metrics only).
    
    Note: Fantasy scoring is now handled by fetch_player_stats().
    NGS data is kept for advanced metrics like target share, air yards, EPA, etc.
    
    Args:
        stat_types: Optional list of stat types ('passing', 'rushing', 'receiving').
                   If None, uses config types.
        years: Optional list of years to fetch. If None, uses config range.
    """
    print("=" * 80)
    print("FETCHING NGS STATS")
    print("=" * 80)
    
    if stat_types is None:
        stat_types = config.ngs_stat_types
    
    if years is None:
        years = config.get_year_range()
    
    for stat_type in stat_types:
        print(f"\n--- Processing {stat_type.upper()} stats ---")
        
        # Fetch data
        df = get_ngs_data(stat_type, years, verbose=config.verbose)
        
        # Clean data
        df = clean_ngs_data(df)
        
        # NOTE: Fantasy scoring removed - now handled by fetch_player_stats()
        # NGS data is kept for advanced metrics only (target share, air yards, EPA, etc.)
        
        print(f"Cleaned data: {len(df)} records")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        
        # Show sample NGS data
        if stat_type == 'passing':
            print(f"\n{'='*80}")
            print(f"QB NGS Metrics Sample")
            print(f"{'='*80}")
            
            # Filter for QBs only
            qb_df = df[df['player_position'] == 'QB'].copy()
            top_qbs = qb_df.nlargest(10, 'pass_yards')[[
                'player_display_name', 'team_abbr', 'attempts', 'pass_yards', 'pass_touchdowns',
                'avg_time_to_throw', 'completion_percentage_above_expectation', 'passer_rating'
            ]].copy()
            
            print(top_qbs.to_string(index=False))
            
        elif stat_type == 'rushing':
            print(f"\n{'='*80}")
            print(f"RB NGS Metrics Sample")
            print(f"{'='*80}")
            
            rb_df = df[df['player_position'] == 'RB'].copy()
            top_rbs = rb_df.nlargest(10, 'rush_yards')[[
                'player_display_name', 'team_abbr', 'rush_attempts', 'rush_yards',
                'avg_rush_yards', 'efficiency', 'rush_yards_over_expected_per_att'
            ]].copy()
            
            print(top_rbs.to_string(index=False))
                
        elif stat_type == 'receiving':
            print(f"\n{'='*80}")
            print(f"WR NGS Metrics Sample")
            print(f"{'='*80}")
            
            wr_df = df[df['player_position'] == 'WR'].copy()
            top_wrs = wr_df.nlargest(10, 'yards')[[
                'player_display_name', 'team_abbr', 'targets', 'receptions', 'yards',
                'avg_separation', 'avg_cushion', 'catch_percentage'
            ]].copy()
            
            print(top_wrs.to_string(index=False))
        
        # Save to CSV (backup)
        if config.save_to_csv:
            filename = f"ngs_{stat_type}_{'_'.join(map(str, years))}"
            save_dataframe(
                df=df,
                filename=filename,
                output_dir=config.output_dir,
                save_csv=config.save_to_csv,
                save_json=config.save_to_json,
                verbose=config.verbose
            )
        
        # Upload to Supabase (both databases)
        if config.save_to_database:
            table_map = {
                'passing': 'nfl_ngs_passing_stats',
                'rushing': 'nfl_ngs_rushing_stats',
                'receiving': 'nfl_ngs_receiving_stats'
            }
            table_name = table_map.get(stat_type)
            
            if table_name:
                # Get both database clients
                supabase_primary = config.get_supabase_client()
                supabase_secondary = config.get_supabase_client_2()
                
                # Collect active clients and labels
                clients = []
                labels = []
                
                if supabase_primary and config.enable_database:
                    clients.append(supabase_primary)
                    labels.append("Primary DB")
                
                if supabase_secondary and config.enable_database_2:
                    clients.append(supabase_secondary)
                    labels.append("Secondary DB")
                
                # Upload to all configured databases
                if clients:
                    upload_to_multiple_databases(
                        df=df,
                        table_name=table_name,
                        supabase_clients=clients,
                        db_labels=labels,
                        batch_size=config.batch_size,
                        verbose=config.verbose
                    )
                else:
                    if config.verbose:
                        print("⚠ No database clients configured, skipping upload")
        
        print(f"\n✓ {stat_type.capitalize()} stats processing complete")


def fetch_all_data(years: Optional[List[int]] = None) -> None:
    """
    Fetch complete player stats and NGS stats, then refresh master view.
    
    Pipeline flow:
    1. Fetch player stats from nflreadpy (complete fantasy data)
    2. Fetch NGS stats (advanced metrics)
    3. Refresh master_player_stats view (combines both sources)
    
    Args:
        years: Optional list of years to fetch. If None, uses config range.
    """
    start_time = datetime.now()
    
    print("=" * 80)
    print("NFL DATA PIPELINE - FULL FETCH")
    print("=" * 80)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 1: Fetch complete player stats (fantasy data)
        fetch_player_stats(years=years)
        
        print("\n")
        
        # Step 2: Fetch NGS stats (advanced metrics)
        fetch_ngs_stats(years=years)
        
        print("\n")
        
        # Step 3: Refresh master player stats materialized view
        if config.save_to_database:
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
            
            if clients:
                refresh_master_stats_view(
                    supabase_clients=clients,
                    db_labels=labels,
                    verbose=config.verbose
                )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 80)
        print("PIPELINE COMPLETE")
        print("=" * 80)
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration:.2f} seconds")
        
    except Exception as e:
        print(f"\n✗ Pipeline failed with error: {e}")
        raise


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="NFL Data Pipeline - Fetch and store NFL statistics"
    )
    
    parser.add_argument(
        '--mode',
        choices=['weekly', 'seasonal', 'ngs', 'pbp', 'roster', 'ftn', 'all'],
        default='all',
        help='Data fetch mode (default: all)'
    )
    
    parser.add_argument(
        '--years',
        type=int,
        nargs='+',
        help='Specific years to fetch (e.g., --years 2023 2024)'
    )
    
    parser.add_argument(
        '--ngs-types',
        choices=['passing', 'rushing', 'receiving'],
        nargs='+',
        help='Specific NGS stat types to fetch (only used with --mode ngs or all)'
    )
    
    parser.add_argument(
        '--year',
        type=int,
        help='Single year to fetch (shorthand for --years)'
    )
    
    args = parser.parse_args()
    
    # Determine years to fetch
    years = None
    if args.years:
        years = args.years
    elif args.year:
        years = [args.year]
    
    # Execute based on mode
    try:
        if args.mode == 'weekly':
            fetch_weekly_stats(years)
        elif args.mode == 'seasonal':
            fetch_seasonal_stats(years)
        elif args.mode == 'ngs':
            fetch_ngs_stats(stat_types=args.ngs_types, years=years)
        elif args.mode == 'pbp':
            fetch_pbp_data(years)
        elif args.mode == 'roster':
            fetch_roster_data(years)
        elif args.mode == 'ftn':
            fetch_ftn_data(years)
        elif args.mode == 'all':
            fetch_all_data(years)
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nPipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

