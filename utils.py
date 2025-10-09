"""Utility functions for NFL data pipeline."""

import pandas as pd
import nfl_data_py as nfl
from typing import List, Optional
from pathlib import Path
from tqdm import tqdm


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
    
    # Replace NaN with None for proper NULL handling in database
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
    if stat_type == 'passing':
        # Passing scoring: 0.04 per yard, 4 per TD, -2 per INT
        df['fantasy_points'] = (
            (df['pass_yards'].fillna(0) * 0.04) +
            (df['pass_touchdowns'].fillna(0) * 4) +
            (df['interceptions'].fillna(0) * -2)
        )
        df['fantasy_ppg'] = df['fantasy_points'] / df.groupby('player_gsis_id')['week'].transform('count')
        df['fantasy_points_per_attempt'] = df['fantasy_points'] / df['attempts'].replace(0, 1)
        
    elif stat_type == 'rushing':
        # Rushing scoring: 0.1 per yard, 6 per TD
        df['fantasy_points'] = (
            (df['rush_yards'].fillna(0) * 0.1) +
            (df['rush_touchdowns'].fillna(0) * 6)
        )
        df['fantasy_ppg'] = df['fantasy_points'] / df.groupby('player_gsis_id')['week'].transform('count')
        df['fantasy_points_per_rush'] = df['fantasy_points'] / df['rush_attempts'].replace(0, 1)
        
    elif stat_type == 'receiving':
        # Receiving scoring (PPR): 1 per catch, 0.1 per yard, 6 per TD
        df['fantasy_points'] = (
            (df['receptions'].fillna(0) * 1) +
            (df['yards'].fillna(0) * 0.1) +
            (df['rec_touchdowns'].fillna(0) * 6)
        )
        df['fantasy_ppg'] = df['fantasy_points'] / df.groupby('player_gsis_id')['week'].transform('count')
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
    verbose: bool = True
):
    """
    Upload DataFrame to Supabase table with upsert.
    
    Args:
        df: DataFrame to upload
        table_name: Name of Supabase table
        supabase_client: Supabase client instance
        batch_size: Number of records per batch
        verbose: Whether to show progress
    """
    if supabase_client is None:
        if verbose:
            print("⚠ Supabase client not configured, skipping database upload")
        return
    
    if len(df) == 0:
        if verbose:
            print(f"No data to upload to {table_name}")
        return
    
    # Replace NaN with None for proper NULL handling
    df_clean = df.where(pd.notnull(df), None)
    
    # Convert DataFrame to list of dicts
    records = df_clean.to_dict('records')
    
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    if verbose:
        print(f"📤 Uploading {len(records)} records to {table_name} in {total_batches} batches...")
        batches = tqdm(range(0, len(records), batch_size), total=total_batches, desc="Uploading")
    else:
        batches = range(0, len(records), batch_size)
    
    errors = []
    uploaded_count = 0
    
    for i in batches:
        batch_records = records[i:i + batch_size]
        
        try:
            # Upsert to handle duplicates (based on unique constraint)
            response = supabase_client.table(table_name).upsert(
                batch_records,
                on_conflict='player_gsis_id,season,season_type,week'
            ).execute()
            uploaded_count += len(batch_records)
        except Exception as e:
            errors.append(str(e))
            if verbose:
                print(f"\n⚠ Error uploading batch: {e}")
    
    if errors:
        if verbose:
            print(f"⚠ Completed with {len(errors)} errors")
    else:
        if verbose:
            print(f"✓ Successfully uploaded {uploaded_count} records to {table_name}")

