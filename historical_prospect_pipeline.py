#!/usr/bin/env python3
"""
Historical Prospect Pipeline
Fetches historical draft data from CFBD, links with NFL outcomes,
and creates grading baselines for current prospect evaluation.
"""

import os
import sys
import time
import argparse
import requests
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import config


class HistoricalProspectPipeline:
    """
    Pipeline for building historical prospect database with NFL outcomes.
    This creates the baseline for grading current prospects.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize CFBD API client."""
        self.api_key = api_key or os.getenv('CFBD_API_KEY')
        if not self.api_key:
            raise ValueError("CFBD_API_KEY environment variable not set")
        
        self.base_url = 'https://api.collegefootballdata.com'
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json'
        }
        
        self.request_delay = 0.15  # Rate limiting
        
        # Skill position mappings (API uses full names)
        self.skill_positions = ['QB', 'RB', 'WR', 'TE']
        self.skill_positions_full = ['Quarterback', 'Running Back', 'Wide Receiver', 'Tight End']
        self.position_map = {
            'Quarterback': 'QB',
            'Running Back': 'RB',
            'Wide Receiver': 'WR',
            'Tight End': 'TE',
        }
    
    def fetch_draft_picks(self, year: int) -> List[Dict]:
        """
        Fetch all draft picks for a given year from CFBD.
        
        Returns:
            List of draft pick records with player info
        """
        try:
            time.sleep(self.request_delay)
            
            url = f'{self.base_url}/draft/picks'
            params = {'year': year}
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                picks = response.json()
                # Filter to skill positions only (API uses full names)
                skill_picks = [
                    p for p in picks 
                    if p.get('position') in self.skill_positions_full
                ]
                # Normalize position names to abbreviations
                for p in skill_picks:
                    p['position'] = self.position_map.get(p.get('position'), p.get('position'))
                return skill_picks
            elif response.status_code == 429:
                print(f"  ⚠ API quota exceeded for year {year}")
                return []
            else:
                print(f"  ⚠ API error ({response.status_code}) for year {year}")
                return []
                
        except Exception as e:
            print(f"  ⚠ Error fetching draft picks for {year}: {str(e)[:50]}")
            return []
    
    def fetch_player_college_stats(
        self, 
        player_name: str, 
        team: str, 
        position: str,
        years: List[int]
    ) -> Optional[Dict]:
        """
        Fetch college stats for a drafted player.
        
        Returns:
            Dict with aggregated college stats
        """
        try:
            all_stats = {}
            
            for year in years:
                time.sleep(self.request_delay)
                
                category_map = {
                    'QB': 'passing',
                    'RB': 'rushing',
                    'WR': 'receiving',
                    'TE': 'receiving',
                }
                category = category_map.get(position.upper(), 'rushing')
                
                url = f'{self.base_url}/stats/player/season'
                params = {
                    'year': year,
                    'team': team,
                    'category': category
                }
                
                response = requests.get(url, headers=self.headers, params=params)
                
                if response.status_code == 200:
                    stats_list = response.json()
                    
                    # Find matching player
                    for stat in stats_list:
                        stat_name = stat.get('player', '')
                        if player_name.lower() in stat_name.lower():
                            stat_type = stat.get('statType', '')
                            stat_value = stat.get('stat', 0)
                            
                            if year not in all_stats:
                                all_stats[year] = {}
                            all_stats[year][stat_type] = stat_value
            
            if all_stats:
                return self._aggregate_college_stats(all_stats, position)
            return None
            
        except Exception as e:
            return None
    
    def _aggregate_college_stats(self, stats_by_year: Dict, position: str) -> Dict:
        """Aggregate college stats across years."""
        agg = {
            'seasons': len(stats_by_year),
            'years': list(stats_by_year.keys()),
        }
        
        if position == 'QB':
            agg['pass_yards'] = sum(
                int(s.get('YDS', 0) or 0) 
                for s in stats_by_year.values()
            )
            agg['pass_tds'] = sum(
                int(s.get('TD', 0) or 0) 
                for s in stats_by_year.values()
            )
            agg['pass_int'] = sum(
                int(s.get('INT', 0) or 0) 
                for s in stats_by_year.values()
            )
        else:
            agg['rush_yards'] = sum(
                int(s.get('YDS', 0) or 0) 
                for s in stats_by_year.values()
            )
            agg['rush_tds'] = sum(
                int(s.get('TD', 0) or 0) 
                for s in stats_by_year.values()
            )
            agg['receptions'] = sum(
                int(s.get('REC', 0) or 0) 
                for s in stats_by_year.values()
            )
            agg['rec_yards'] = sum(
                int(s.get('YDS', 0) or 0) 
                for s in stats_by_year.values()
            )
        
        return agg
    
    def fetch_recruiting_data(self, year: int) -> List[Dict]:
        """
        Fetch recruiting rankings for a class year.
        
        Returns:
            List of recruit records
        """
        try:
            time.sleep(self.request_delay)
            
            url = f'{self.base_url}/recruiting/players'
            params = {'year': year}
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                recruits = response.json()
                # Filter to skill positions (recruits may use either format)
                skill_recruits = [
                    r for r in recruits 
                    if r.get('position') in self.skill_positions or 
                       r.get('position') in self.skill_positions_full
                ]
                # Normalize position names
                for r in skill_recruits:
                    if r.get('position') in self.position_map:
                        r['position'] = self.position_map[r['position']]
                return skill_recruits
            else:
                return []
                
        except Exception as e:
            return []
    
    def calculate_historical_grade(
        self,
        hs_rank: Optional[int],
        hs_stars: Optional[int],
        draft_round: int,
        draft_pick: int,
        college_stats: Optional[Dict],
        nfl_outcome: Optional[Dict]
    ) -> Dict:
        """
        Calculate a historical grade for a drafted player.
        This becomes the baseline for comparing current prospects.
        
        Returns:
            Dict with grade components and overall grade
        """
        grades = {
            'hs_recruiting_score': 0,
            'draft_capital_score': 0,
            'college_production_score': 0,
            'nfl_outcome_score': 0,
            'overall_grade': 0,
        }
        
        # HS Recruiting Score (0-100)
        if hs_stars:
            star_scores = {5: 95, 4: 80, 3: 60, 2: 40}
            grades['hs_recruiting_score'] = star_scores.get(hs_stars, 50)
            
            # Adjust based on national rank
            if hs_rank:
                if hs_rank <= 10:
                    grades['hs_recruiting_score'] = 100
                elif hs_rank <= 50:
                    grades['hs_recruiting_score'] = 95
                elif hs_rank <= 100:
                    grades['hs_recruiting_score'] = 90
                elif hs_rank <= 200:
                    grades['hs_recruiting_score'] = 85
        
        # Draft Capital Score (0-100)
        round_scores = {1: 95, 2: 80, 3: 65, 4: 50, 5: 35, 6: 25, 7: 15}
        grades['draft_capital_score'] = round_scores.get(draft_round, 10)
        
        # Adjust for pick position within round
        pick_in_round = ((draft_pick - 1) % 32) + 1
        if pick_in_round <= 10:
            grades['draft_capital_score'] += 5
        
        # College Production Score (0-100)
        if college_stats:
            # Normalize based on position
            grades['college_production_score'] = 70  # Default
            # Would add more sophisticated scoring here
        
        # NFL Outcome Score (0-100) - This is the ground truth
        if nfl_outcome:
            # Based on career stats, Pro Bowls, All-Pro, etc.
            grades['nfl_outcome_score'] = nfl_outcome.get('career_grade', 50)
        
        # Calculate overall grade
        # Weight NFL outcome heavily since that's the ultimate measure
        weights = {
            'hs_recruiting_score': 0.20,
            'draft_capital_score': 0.25,
            'college_production_score': 0.20,
            'nfl_outcome_score': 0.35,
        }
        
        grades['overall_grade'] = sum(
            grades[k] * weights[k] 
            for k in weights.keys()
        )
        
        return grades
    
    def fetch_recruiting_for_year(self, year: int) -> Dict[str, Dict]:
        """
        Fetch recruiting data for a class year and return as lookup dict.
        
        Returns:
            Dict mapping player names to their recruiting data
        """
        try:
            time.sleep(self.request_delay)
            
            url = f'{self.base_url}/recruiting/players'
            params = {'year': year}
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                recruits = response.json()
                
                # Build lookup by name (lowercase) -> recruit data
                lookup = {}
                for r in recruits:
                    name_key = r.get('name', '').lower().strip()
                    if name_key:
                        lookup[name_key] = {
                            'hs_rank': r.get('ranking'),
                            'hs_stars': r.get('stars'),
                            'hs_rating': r.get('rating'),
                            'hs_school': r.get('school'),
                            'hs_city': r.get('city'),
                            'hs_state': r.get('stateProvince'),
                            'committed_to': r.get('committedTo'),
                        }
                return lookup
            return {}
            
        except Exception as e:
            print(f"  ⚠ Error fetching recruiting for {year}: {str(e)[:50]}")
            return {}
    
    def build_historical_database(
        self,
        start_year: int = 2015,
        end_year: int = 2025
    ) -> pd.DataFrame:
        """
        Build comprehensive historical prospect database.
        
        Returns:
            DataFrame with all historical prospects and their outcomes
        """
        print("=" * 80)
        print("HISTORICAL PROSPECT DATABASE BUILDER")
        print("=" * 80)
        
        # Pre-fetch recruiting data for relevant years
        # Players drafted in year Y were typically recruited in year Y-3 to Y-4
        print("\n📚 Fetching HS recruiting data...")
        recruiting_data = {}
        for recruit_year in range(start_year - 5, end_year):
            print(f"   Fetching {recruit_year} recruiting class...")
            recruiting_data[recruit_year] = self.fetch_recruiting_for_year(recruit_year)
            print(f"   Found {len(recruiting_data[recruit_year])} recruits")
        
        all_prospects = []
        hs_matches = 0
        
        for year in range(start_year, end_year + 1):
            print(f"\n📅 Processing {year} draft class...")
            
            # Fetch draft picks
            picks = self.fetch_draft_picks(year)
            print(f"   Found {len(picks)} skill position picks")
            
            for pick in picks:
                name = pick.get('name', '')
                college = pick.get('collegeTeam', '') or pick.get('college', '')
                
                # Try to find HS recruiting data
                # Check recruit years Y-3, Y-4, Y-5 (typical college career lengths)
                hs_data = {}
                name_key = name.lower().strip()
                
                for offset in [3, 4, 5, 2]:
                    recruit_year = year - offset
                    if recruit_year in recruiting_data:
                        if name_key in recruiting_data[recruit_year]:
                            hs_data = recruiting_data[recruit_year][name_key]
                            hs_matches += 1
                            break
                        # Also try last name only for partial matches
                        last_name = name_key.split()[-1] if ' ' in name_key else name_key
                        for rname, rdata in recruiting_data[recruit_year].items():
                            committed = (rdata.get('committed_to') or '').lower()
                            if last_name in rname and committed == college.lower():
                                hs_data = rdata
                                hs_matches += 1
                                break
                        if hs_data:
                            break
                
                prospect = {
                    'draft_year': year,
                    'name': name,
                    'position': pick.get('position', ''),
                    'college': college,
                    'nfl_team': pick.get('nflTeam', ''),
                    'draft_round': pick.get('round', 0),
                    'draft_pick': pick.get('overall', 0),
                    'height': pick.get('height', None),
                    'weight': pick.get('weight', None),
                    
                    # HS Recruiting data
                    'hs_rank': hs_data.get('hs_rank'),
                    'hs_stars': hs_data.get('hs_stars'),
                    'hs_rating': hs_data.get('hs_rating'),
                    'hs_school': hs_data.get('hs_school'),
                    'hs_city': hs_data.get('hs_city'),
                    'hs_state': hs_data.get('hs_state'),
                    
                    # Pre-draft rankings
                    'pre_draft_rank': pick.get('preDraftRanking', None),
                    'pre_draft_position_rank': pick.get('preDraftPositionRanking', None),
                    'pre_draft_grade': pick.get('preDraftGrade', None),
                }
                
                all_prospects.append(prospect)
            
            # Rate limiting
            time.sleep(0.5)
        
        df = pd.DataFrame(all_prospects)
        
        print(f"\n✅ Built database with {len(df)} historical prospects")
        print(f"   Years: {start_year} - {end_year}")
        print(f"   Positions: {df['position'].value_counts().to_dict()}")
        print(f"   HS data matched: {hs_matches}/{len(df)} ({100*hs_matches/len(df):.1f}%)")
        
        # Show HS stars distribution
        if 'hs_stars' in df.columns:
            stars_dist = df['hs_stars'].value_counts().sort_index().to_dict()
            print(f"   HS Stars: {stars_dist}")
        
        return df
    
    def calculate_percentile_rankings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate percentile rankings for historical prospects.
        This creates the baseline for grading current prospects.
        """
        # Calculate percentiles by position
        for position in self.skill_positions:
            pos_mask = df['position'] == position
            
            # Draft round percentile (lower is better)
            df.loc[pos_mask, 'draft_round_percentile'] = (
                df.loc[pos_mask, 'draft_round'].rank(pct=True) * 100
            )
            
            # Draft pick percentile (lower is better)
            df.loc[pos_mask, 'draft_pick_percentile'] = (
                df.loc[pos_mask, 'draft_pick'].rank(pct=True) * 100
            )
        
        return df
    
    def upload_to_supabase(self, df: pd.DataFrame, table_name: str = 'historical_prospects'):
        """Upload historical prospects to Supabase."""
        supabase = config.get_supabase_client()
        if not supabase:
            print("❌ Failed to get Supabase client")
            return
        
        print(f"\n💾 Uploading {len(df)} records to {table_name}...")
        
        # Integer columns that need proper conversion
        int_columns = ['draft_year', 'draft_round', 'draft_pick', 'hs_rank', 'hs_stars', 
                       'pre_draft_rank', 'pre_draft_position_rank']
        
        # Convert to records and clean data
        records = []
        for _, row in df.iterrows():
            record = {}
            for key, value in row.items():
                # Handle NaN/None
                if pd.isna(value):
                    record[key] = None
                # Convert integers properly
                elif key in int_columns:
                    record[key] = int(value) if not pd.isna(value) else None
                # Convert floats
                elif isinstance(value, (np.floating, float)):
                    record[key] = float(value) if not np.isnan(value) else None
                else:
                    record[key] = value
            records.append(record)
        
        # Batch upload
        batch_size = 100
        uploaded = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                result = supabase.table(table_name).upsert(batch).execute()
                uploaded += len(batch)
                print(f"   ✓ Uploaded {uploaded}/{len(records)}")
            except Exception as e:
                print(f"   ⚠ Error: {str(e)[:100]}")
        
        print(f"✅ Upload complete: {uploaded} records")
    
    def run_pipeline(
        self,
        start_year: int = 2015,
        end_year: int = 2025,
        upload: bool = True
    ):
        """Run the full historical prospect pipeline."""
        # Build database
        df = self.build_historical_database(start_year, end_year)
        
        # Calculate percentiles
        df = self.calculate_percentile_rankings(df)
        
        # Save to CSV backup
        csv_path = Path(__file__).parent / 'data_output' / 'historical_prospects.csv'
        df.to_csv(csv_path, index=False)
        print(f"\n📁 Saved to {csv_path}")
        
        # Upload to Supabase
        if upload:
            self.upload_to_supabase(df)
        
        return df


def main():
    parser = argparse.ArgumentParser(description='Historical Prospect Pipeline')
    parser.add_argument('--start-year', type=int, default=2015)
    parser.add_argument('--end-year', type=int, default=2025)
    parser.add_argument('--no-upload', action='store_true')
    parser.add_argument('--api-key', type=str)
    
    args = parser.parse_args()
    
    try:
        pipeline = HistoricalProspectPipeline(api_key=args.api_key)
        pipeline.run_pipeline(
            start_year=args.start_year,
            end_year=args.end_year,
            upload=not args.no_upload
        )
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

