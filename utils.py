"""Utility functions for NFL data pipeline."""

import pandas as pd
import nfl_data_py as nfl
import nflreadpy as nflread
from typing import List, Optional
from pathlib import Path
from tqdm import tqdm
import os
import math


def get_weekly_data(years: List[int], verbose: bool = True) -> pd.DataFrame:
    """
    Fetch weekly NFL player stats for specified years.
    
    Args:
        years: List of years to fetch data for
        verbose: Whether to show progress
        
    Returns:
        DataFrame with weekly player statistics
    """
    if verbose:
        print(f"Fetching weekly data for years: {years}")
    
    df = nfl.import_weekly_data(years)
    
    if verbose:
        print(f"Fetched {len(df)} weekly stat records")
    
    return df


def get_ngs_data(stat_type: str, years: List[int], verbose: bool = True) -> pd.DataFrame:
    """
    Fetch Next Gen Stats data for specified stat type and years.
    
    Args:
        stat_type: Type of NGS data ('passing', 'rushing', 'receiving')
        years: List of years to fetch data for
        verbose: Whether to show progress
        
    Returns:
        DataFrame with NGS statistics
    """
    if verbose:
        print(f"Fetching NGS {stat_type} data for years: {years}")
    
    df = nfl.import_ngs_data(stat_type, years)
    
    if verbose:
        print(f"Fetched {len(df)} NGS {stat_type} records")
    
    return df


def get_player_stats(years: List[int], verbose: bool = True) -> pd.DataFrame:
    """
    Fetch complete player stats from nflreadpy.
    
    This includes all stat types (passing + rushing + receiving) in one row per player per week.
    Fantasy points are pre-calculated by nflreadpy.
    
    Args:
        years: List of years to fetch data for
        verbose: Whether to show progress
        
    Returns:
        DataFrame with complete weekly player statistics
    """
    if verbose:
        print(f"Fetching player stats from nflreadpy for years: {years}")
    
    # Load player stats from nflreadpy (returns Polars DataFrame)
    df_polars = nflread.load_player_stats(years)
    
    # Convert to pandas
    df = df_polars.to_pandas()
    
    if verbose:
        print(f"Fetched {len(df)} player stat records")
        print(f"Columns: {len(df.columns)} total")
    
    # Rename columns to match our schema
    column_mapping = {
        'player_id': 'player_id',
        'player_display_name': 'player_display_name',
        'player_name': 'player_name',
        'position': 'position',
        'position_group': 'position_group',
        'team': 'team',
        'opponent_team': 'opponent_team',
        'season': 'season',
        'week': 'week',
        'season_type': 'season_type',
        'headshot_url': 'headshot_url',
        # Passing
        'completions': 'completions',
        'attempts': 'attempts',
        'passing_yards': 'passing_yards',
        'passing_tds': 'passing_tds',
        'passing_interceptions': 'passing_interceptions',
        'passing_2pt_conversions': 'passing_2pt_conversions',
        # Rushing
        'carries': 'carries',
        'rushing_yards': 'rushing_yards',
        'rushing_tds': 'rushing_tds',
        'rushing_2pt_conversions': 'rushing_2pt_conversions',
        # Receiving
        'receptions': 'receptions',
        'targets': 'targets',
        'receiving_yards': 'receiving_yards',
        'receiving_tds': 'receiving_tds',
        'receiving_2pt_conversions': 'receiving_2pt_conversions',
        # Advanced metrics
        'target_share': 'target_share',
        'air_yards_share': 'air_yards_share',
        # Fantasy points
        'fantasy_points': 'fantasy_points',
        'fantasy_points_ppr': 'fantasy_points_ppr',
    }
    
    # Select and rename columns
    columns_to_keep = [col for col in column_mapping.keys() if col in df.columns]
    df = df[columns_to_keep].rename(columns=column_mapping)
    
    # Add player_gsis_id column (same as player_id in nflreadpy)
    df['player_gsis_id'] = df['player_id']
    
    # Filter out rows with null player_id (defense/special teams units)
    df = df[df['player_id'].notna()].copy()
    
    if verbose:
        print(f"Prepared {len(df)} records for upload")
        sample_player = df[df['fantasy_points_ppr'] > 0].head(1)
        if not sample_player.empty:
            print(f"Sample: {sample_player.iloc[0]['player_display_name']} - "
                  f"Week {sample_player.iloc[0]['week']}: {sample_player.iloc[0]['fantasy_points_ppr']:.2f} PPR pts")
    
    return df


def get_seasonal_data(years: List[int], verbose: bool = True) -> pd.DataFrame:
    """
    Fetch seasonal NFL player stats with player IDs merged.
    
    Args:
        years: List of years to fetch data for
        verbose: Whether to show progress
        
    Returns:
        DataFrame with seasonal player statistics
    """
    if verbose:
        print(f"Fetching seasonal data for years: {years}")
    
    df = nfl.import_seasonal_data(years)
    id_df = nfl.import_ids()
    id_df = id_df[['gsis_id', 'name']]
    
    df = pd.merge(
        df,
        id_df,
        left_on='player_id',
        right_on='gsis_id',
        how='left'
    )
    
    # Reorder columns to put name first
    cols = df.columns.tolist()
    if 'name' in cols:
        cols = ['name'] + [col for col in cols if col != 'name' and col != 'gsis_id']
        df = df[cols]
    
    df = df.sort_values('name')
    
    if verbose:
        print(f"Fetched {len(df)} seasonal stat records")
    
    return df


def get_play_by_play_data(years: List[int], verbose: bool = True) -> pd.DataFrame:
    """
    Fetch play-by-play NFL data for specified years.
    
    Args:
        years: List of years to fetch data for
        verbose: Whether to show progress
        
    Returns:
        DataFrame with play-by-play data
    """
    if verbose:
        print(f"Fetching play-by-play data for years: {years}")
    
    df = nfl.import_pbp_data(years)
    
    if verbose:
        print(f"Fetched {len(df)} play-by-play records")
    
    return df


def get_weekly_roster_data(years: List[int], verbose: bool = True) -> pd.DataFrame:
    """
    Fetch weekly roster data for specified years.
    
    Args:
        years: List of years to fetch data for
        verbose: Whether to show progress
        
    Returns:
        DataFrame with weekly roster data
    """
    if verbose:
        print(f"Fetching weekly roster data for years: {years}")
    
    df = nfl.import_weekly_rosters(years)
    
    if verbose:
        print(f"Fetched {len(df)} roster records")
    
    return df


def get_ftn_data(years: List[int], verbose: bool = True) -> pd.DataFrame:
    """
    Fetch FTN (Fantasy) data for specified years.
    
    Args:
        years: List of years to fetch data for
        verbose: Whether to show progress
        
    Returns:
        DataFrame with FTN data
    """
    if verbose:
        print(f"Fetching FTN data for years: {years}")
    
    df = nfl.import_ftn_data(years)
    
    if verbose:
        print(f"Fetched {len(df)} FTN records")
    
    return df


def clean_weekly_data(df: pd.DataFrame, positions: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Clean and filter weekly data.
    
    Args:
        df: Raw weekly data DataFrame
        positions: Optional list of positions to filter for
        
    Returns:
        Cleaned DataFrame
    """
    # Remove records with missing critical data
    df = df.dropna(subset=['player_id'])
    
    # Filter by position if specified
    if positions:
        df = df[df['position'].isin(positions)]
    
    # Convert data types
    if 'week' in df.columns:
        df['week'] = df['week'].astype(int)
    if 'season' in df.columns:
        df['season'] = df['season'].astype(int)
    
    return df


def clean_ngs_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and prepare NGS data for database insertion.
    
    Args:
        df: Raw NGS data DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    # Remove records with missing player IDs
    df = df.dropna(subset=['player_gsis_id'])
    
    # Convert data types
    if 'week' in df.columns:
        df['week'] = pd.to_numeric(df['week'], errors='coerce')
    if 'season' in df.columns:
        df['season'] = pd.to_numeric(df['season'], errors='coerce')
    
    # Replace NaN, inf, and -inf with None for proper NULL handling in database
    # This ensures JSON compliance when uploading to Supabase
    df = df.replace([pd.NA, pd.NaT, float('nan'), float('inf'), float('-inf')], None)
    df = df.where(pd.notnull(df), None)
    
    return df


def add_fantasy_scoring(df: pd.DataFrame, stat_type: str) -> pd.DataFrame:
    """
    Add fantasy scoring calculations to NGS data.
    
    Args:
        df: NGS DataFrame
        stat_type: Type of stats ('passing', 'rushing', 'receiving')
        
    Returns:
        DataFrame with fantasy scoring columns added
    """
    # Filter out week 0 (cumulative stats) - we'll aggregate our own from individual weeks
    df = df[df['week'] != 0].copy()
    
    if stat_type == 'passing':
        # Passing scoring: 0.04 per yard (1 pt per 25 yards), 6 per TD, -2 per INT
        df['fantasy_points'] = (
            (df['pass_yards'].fillna(0) * 0.04) +
            (df['pass_touchdowns'].fillna(0) * 6) +
            (df['interceptions'].fillna(0) * -2)
        )
        # Calculate PPG as average of all weeks for each player
        df['fantasy_ppg'] = df.groupby('player_gsis_id')['fantasy_points'].transform('mean')
        df['fantasy_points_per_attempt'] = df['fantasy_points'] / df['attempts'].replace(0, 1)
        
    elif stat_type == 'rushing':
        # Rushing scoring: 0.1 per yard (1 pt per 10 yards), 6 per TD
        df['fantasy_points'] = (
            (df['rush_yards'].fillna(0) * 0.1) +
            (df['rush_touchdowns'].fillna(0) * 6)
        )
        # Calculate PPG as average of all weeks for each player
        df['fantasy_ppg'] = df.groupby('player_gsis_id')['fantasy_points'].transform('mean')
        df['fantasy_points_per_rush'] = df['fantasy_points'] / df['rush_attempts'].replace(0, 1)
        
    elif stat_type == 'receiving':
        # Receiving scoring (PPR): 1 per reception, 0.1 per yard (1 pt per 10 yards), 6 per TD
        df['fantasy_points'] = (
            (df['receptions'].fillna(0) * 1) +
            (df['yards'].fillna(0) * 0.1) +
            (df['rec_touchdowns'].fillna(0) * 6)
        )
        # Calculate PPG as average of all weeks for each player
        df['fantasy_ppg'] = df.groupby('player_gsis_id')['fantasy_points'].transform('mean')
        df['fantasy_points_per_reception'] = df['fantasy_points'] / df['receptions'].replace(0, 1)
        df['fantasy_points_per_target'] = df['fantasy_points'] / df['targets'].replace(0, 1)
    
    return df


def batch_dataframe(df: pd.DataFrame, batch_size: int):
    """
    Generator to yield batches of DataFrame rows.
    
    Args:
        df: DataFrame to batch
        batch_size: Number of rows per batch
        
    Yields:
        DataFrame batches
    """
    for i in range(0, len(df), batch_size):
        yield df.iloc[i:i + batch_size]


def save_dataframe(
    df: pd.DataFrame,
    filename: str,
    output_dir: str,
    save_csv: bool = True,
    save_json: bool = False,
    verbose: bool = True
):
    """
    Save DataFrame to local files.
    
    Args:
        df: DataFrame to save
        filename: Base filename (without extension)
        output_dir: Output directory path
        save_csv: Whether to save as CSV
        save_json: Whether to save as JSON
        verbose: Whether to show progress
    """
    if len(df) == 0:
        if verbose:
            print(f"No data to save for {filename}")
        return
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    if save_csv:
        csv_path = output_path / f"{filename}.csv"
        df.to_csv(csv_path, index=False)
        if verbose:
            print(f"✓ Saved to {csv_path}")
    
    if save_json:
        json_path = output_path / f"{filename}.json"
        df.to_json(json_path, orient='records', indent=2)
        if verbose:
            print(f"✓ Saved to {json_path}")


def upload_to_supabase(
    df: pd.DataFrame,
    table_name: str,
    supabase_client,
    batch_size: int = 1000,
    verbose: bool = True,
    db_label: str = "database"
):
    """
    Upload DataFrame to Supabase table with upsert.
    
    Args:
        df: DataFrame to upload
        table_name: Name of Supabase table
        supabase_client: Supabase client instance
        batch_size: Number of records per batch
        verbose: Whether to show progress
        db_label: Label for the database (for logging purposes)
    """
    if supabase_client is None:
        if verbose:
            print(f"⚠ Supabase client not configured for {db_label}, skipping upload")
        return
    
    if len(df) == 0:
        if verbose:
            print(f"No data to upload to {table_name} ({db_label})")
        return
    
    # Replace NaN, inf, and -inf with None for proper NULL handling
    # This ensures JSON compliance when uploading to Supabase
    df_clean = df.replace([pd.NA, pd.NaT, float('nan'), float('inf'), float('-inf')], None)
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    # Convert DataFrame to list of dicts, ensuring no NaN values slip through
    records = df_clean.to_dict('records')
    
    # Final pass: replace any remaining NaN/Inf values in records
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                record[key] = None
    
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    if verbose:
        print(f"📤 Uploading {len(records)} records to {table_name} ({db_label}) in {total_batches} batches...")
        batches = tqdm(range(0, len(records), batch_size), total=total_batches, desc=f"Uploading to {db_label}")
    else:
        batches = range(0, len(records), batch_size)
    
    errors = []
    uploaded_count = 0
    
    for i in batches:
        batch_records = records[i:i + batch_size]
        
        try:
            # Upsert to handle duplicates (based on unique constraint)
            # Different tables have different primary keys
            if table_name == 'nfl_player_stats':
                conflict_key = 'player_id,season,season_type,week'
            else:
                conflict_key = 'player_gsis_id,season,season_type,week'
            
            response = supabase_client.table(table_name).upsert(
                batch_records,
                on_conflict=conflict_key
            ).execute()
            uploaded_count += len(batch_records)
        except Exception as e:
            errors.append(str(e))
            if verbose:
                print(f"\n⚠ Error uploading batch to {db_label}: {e}")
    
    if errors:
        if verbose:
            print(f"⚠ Completed {db_label} upload with {len(errors)} errors")
    else:
        if verbose:
            print(f"✓ Successfully uploaded {uploaded_count} records to {table_name} ({db_label})")


def upload_to_multiple_databases(
    df: pd.DataFrame,
    table_name: str,
    supabase_clients: list,
    db_labels: list,
    batch_size: int = 1000,
    verbose: bool = True
):
    """
    Upload DataFrame to multiple Supabase databases.
    
    Args:
        df: DataFrame to upload
        table_name: Name of Supabase table
        supabase_clients: List of Supabase client instances
        db_labels: List of labels for each database (for logging)
        batch_size: Number of records per batch
        verbose: Whether to show progress
    """
    for client, label in zip(supabase_clients, db_labels):
        if client is not None:
            upload_to_supabase(
                df=df,
                table_name=table_name,
                supabase_client=client,
                batch_size=batch_size,
                verbose=verbose,
                db_label=label
            )


def refresh_master_stats_view(supabase_clients: List, db_labels: List[str], verbose: bool = True) -> None:
    """
    Refresh the master_player_stats materialized view.
    
    This aggregates weekly stats to create season-level fantasy PPG.
    Should be run after weekly stats are uploaded.
    
    Args:
        supabase_clients: List of Supabase client instances
        db_labels: List of labels for each database
        verbose: Whether to show progress
    """
    if verbose:
        print("\n" + "=" * 80)
        print("REFRESHING MASTER PLAYER STATS VIEW")
        print("=" * 80)
    
    # Read SQL script
    sql_file = Path(__file__).parent / 'create_master_stats.sql'
    
    if not sql_file.exists():
        print(f"⚠ SQL file not found: {sql_file}")
        return
    
    with open(sql_file, 'r') as f:
        sql_script = f.read()
    
    # Execute on each database
    for client, label in zip(supabase_clients, db_labels):
        if client is None:
            continue
            
        try:
            if verbose:
                print(f"\n📊 Refreshing materialized view on {label}...")
            
            # Split into individual statements (simple split on semicolon)
            statements = [s.strip() for s in sql_script.split(';') if s.strip() and not s.strip().startswith('--')]
            
            for stmt in statements:
                if stmt and not stmt.startswith('COMMENT'):
                    try:
                        result = client.rpc('exec_sql', {'query': stmt}).execute()
                    except Exception as e:
                        # Some statements might not work via RPC, that's ok
                        if verbose:
                            print(f"  Note: {str(e)[:100]}")
            
            # Call the refresh function
            try:
                client.rpc('refresh_master_stats').execute()
                if verbose:
                    print(f"✅ Successfully refreshed master_player_stats on {label}")
            except Exception as e:
                # Fallback: just print instructions
                if verbose:
                    print(f"⚠ Could not auto-refresh on {label}: {str(e)[:100]}")
                    print(f"   Please run this SQL manually in Supabase:")
                    print(f"   SELECT refresh_master_stats();")
                    
        except Exception as e:
            if verbose:
                print(f"⚠ Error refreshing view on {label}: {e}")
                print(f"   Please run the SQL script manually: {sql_file}")

