#!/usr/bin/env python3
"""
College Player Ranking Pipeline
Fetches college stats from CFBD API, calculates tiers, and assigns NFL comparisons.
Now includes height/weight, draft year, and top players by class.
"""

import os
import sys
import time
import argparse
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# HTTP requests for CFBD API
import requests

from config import config
from tiers import (
    calculate_prospect_tier,
    get_tier_from_rank,
    calculate_prospect_tier_from_valuation,
)
from tiers.physical_adjustments import (
    calculate_physical_adjustment,
    get_physical_score,
)
from valuation import (
    calculate_prospect_value,
    get_position_multiplier,
)


class CollegeRankingPipeline:
    """Pipeline for fetching college stats and calculating tiers/comparisons."""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize CFBD API client using direct HTTP requests."""
        self.api_key = api_key or os.getenv('CFBD_API_KEY')
        if not self.api_key:
            raise ValueError("CFBD_API_KEY environment variable not set")
        
        # Base URL for CFBD API
        self.base_url = 'https://api.collegefootballdata.com'
        
        # HTTP headers with authentication
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json'
        }
        
        # Rate limiting: CFBD allows ~1000 requests/hour
        self.request_delay = 0.1  # 100ms between requests = ~36k/hour (safe)
        
        # Skill positions only
        self.skill_positions = ['QB', 'RB', 'WR', 'TE']
    
    def fetch_player_physical_attributes(
        self,
        player_name: str,
        school: str,
        position: str
    ) -> Optional[Dict]:
        """
        Fetch player height, weight, and other physical attributes from roster.
        
        Args:
            player_name: Player's name
            school: College school name
            position: Player position
            
        Returns:
            Dict with height, weight, class (year), or None if not found
        """
        try:
            time.sleep(self.request_delay)
            
            # Normalize school name
            school_normalized = self._normalize_school_name(school)
            
            # Get roster for the school
            current_year = datetime.now().year
            url = f'{self.base_url}/roster'
            params = {'year': current_year, 'team': school_normalized}
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 404:
                return None
            elif response.status_code != 200:
                print(f"  ⚠ API error fetching roster ({response.status_code}): {response.text[:100]}")
                return None
            
            roster = response.json()
            
            # Find matching player
            for player in roster:
                roster_name = f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
                roster_pos = player.get('position', '')
                
                # Match by name and position
                if (player_name.lower() in roster_name.lower() or 
                    roster_name.lower() in player_name.lower()) and \
                   roster_pos.upper() == position.upper():
                    
                    height = player.get('height')
                    weight = player.get('weight')
                    player_class = player.get('year')  # Freshman, Sophomore, etc.
                    
                    return {
                        'height': height,
                        'weight': weight,
                        'class': player_class,
                    }
            
            return None
            
        except Exception as e:
            print(f"  ⚠ Error fetching physical attributes: {str(e)[:50]}")
            return None
    
    def fetch_player_info(self, player_name: str, position: str) -> Optional[Dict]:
        """
        Fetch player's school and physical attributes from CFBD API by searching for the player.
        
        Args:
            player_name: Player's name
            position: Player position
            
        Returns:
            Dict with school, height, weight, class, or None if not found
        """
        try:
            time.sleep(self.request_delay)
            
            # Search for player using CFBD player search
            url = f'{self.base_url}/player/search'
            params = {
                'searchTerm': player_name,
                'position': position.upper()
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                players = response.json()
                
                # Find best match by name
                for player in players:
                    # API returns firstName/lastName (camelCase) or first_name/last_name (snake_case)
                    first_name = player.get('firstName') or player.get('first_name', '')
                    last_name = player.get('lastName') or player.get('last_name', '')
                    player_full_name = f"{first_name} {last_name}".strip()
                    player_pos = player.get('position', '')
                    
                    # Match by name and position
                    if (player_name.lower() in player_full_name.lower() or 
                        player_full_name.lower() in player_name.lower()) and \
                       player_pos.upper() == position.upper():
                        
                        # Get team, height, weight from player record
                        # Note: API doesn't return 'year' in search results, only in roster
                        team = player.get('team', '')
                        height = player.get('height', None)
                        weight = player.get('weight', None)
                        
                        if team or height or weight:
                            return {
                                'school': team,
                                'height': height,
                                'weight': weight,
                                'class': None,  # Not available in search results
                            }
            
            return None
            
        except Exception as e:
            return None
    
    def fetch_player_school(self, player_name: str, position: str) -> Optional[str]:
        """
        Fetch player's school from CFBD API by searching for the player.
        
        Args:
            player_name: Player's name
            position: Player position
            
        Returns:
            School name or None if not found
        """
        info = self.fetch_player_info(player_name, position)
        return info.get('school') if info else None
    
    def fetch_draft_year(self, player_name: str) -> Optional[int]:
        """
        Fetch draft year from CFBD draft API.
        
        Args:
            player_name: Player's name
            
        Returns:
            Draft year (e.g., 2026) or None if not found
        """
        try:
            time.sleep(self.request_delay)
            
            # Get recent draft picks (last 3 years)
            current_year = datetime.now().year
            draft_years = [current_year - 2, current_year - 1, current_year, current_year + 1]
            
            url = f'{self.base_url}/draft/picks'
            
            for year in draft_years:
                try:
                    params = {'year': year}
                    response = requests.get(url, headers=self.headers, params=params)
                    
                    if response.status_code == 200:
                        picks = response.json()
                        
                        for pick in picks:
                            pick_name = pick.get('name', '') or ''
                            if player_name.lower() in pick_name.lower() or pick_name.lower() in player_name.lower():
                                return year
                            
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            return None
    
    def fetch_college_stats(
        self, 
        player_name: str, 
        school: str, 
        position: str,
        years_back: int = 3
    ) -> Optional[Dict]:
        """
        Fetch college stats for a player using the CFBD library.
        
        Args:
            player_name: Player's name
            school: College school name
            position: Player position (QB, RB, WR, TE)
            years_back: Number of years to fetch stats for
            
        Returns:
            Dict with aggregated stats or None if not found
        """
        try:
            current_year = datetime.now().year
            search_years = list(range(current_year - years_back, current_year + 1))
            
            # Normalize school name for CFBD API (handle common variations)
            school_normalized = self._normalize_school_name(school)
            
            # Fetch player season stats using HTTP requests
            all_stats = []
            for year in reversed(search_years):
                try:
                    time.sleep(self.request_delay)
                    
                    # Map position to CFBD category
                    category_map = {
                        'QB': 'passing',
                        'RB': 'rushing',
                        'WR': 'receiving',
                        'TE': 'receiving',
                    }
                    category = category_map.get(position.upper(), 'rushing')
                    
                    # Call CFBD API
                    url = f'{self.base_url}/stats/player/season'
                    params = {
                        'year': year,
                        'team': school_normalized,
                        'category': category
                    }
                    
                    response = requests.get(url, headers=self.headers, params=params)
                    
                    if response.status_code == 404:
                        continue
                    elif response.status_code != 200:
                        print(f"  ⚠ API error ({response.status_code}) for {player_name} ({year}): {response.text[:100]}")
                        continue
                    
                    stats_list = response.json()
                    
                    # Find matching player by name
                    for stat in stats_list:
                        stat_player_name = stat.get('player', '') or ''
                        if player_name.lower() in stat_player_name.lower() or stat_player_name.lower() in player_name.lower():
                            # Convert stat dict (already JSON)
                            stat_dict = self._stat_to_dict(stat, position)
                            all_stats.append({
                                'year': year,
                                'stat': stat_dict
                            })
                            break
                        
                except Exception as e:
                    print(f"  ⚠ Error fetching stats for {player_name} ({year}): {str(e)[:50]}")
                    continue
            
            if not all_stats:
                return None
            
            # Aggregate stats
            return self._aggregate_stats(all_stats, position)
            
        except Exception as e:
            print(f"  ❌ Error fetching stats for {player_name}: {str(e)[:100]}")
            return None
    
    def _normalize_school_name(self, school: str) -> str:
        """Normalize school name for CFBD API."""
        school_normalized = school.lower().strip()
        
        # CFBD API school name mappings
        school_mappings = {
            'notre dame': 'Notre Dame',
            'nd': 'Notre Dame',
            'ohio state': 'Ohio State',
            'osu': 'Ohio State',
            'usc': 'USC',
            'southern california': 'USC',
            'miami': 'Miami',
            'miami (fl)': 'Miami',
            'miami fl': 'Miami',
        }
        
        return school_mappings.get(school_normalized, school)
    
    def _stat_to_dict(self, stat_dict, position: str) -> Dict:
        """Convert CFBD stat dict to our format.
        
        CFBD API returns JSON with various key formats.
        We normalize to camelCase keys to match our aggregation logic.
        """
        result = {}
        
        # Helper to safely get value with fallback
        def get_val(d, *keys, default=0):
            for key in keys:
                if key in d:
                    val = d[key]
                    return val if val is not None else default
            return default
        
        if position == 'QB':
            result = {
                'passingYards': get_val(stat_dict, 'passingYards', 'passing_yards', 'passingYds'),
                'passingTouchdowns': get_val(stat_dict, 'passingTouchdowns', 'passing_tds', 'passingTDs'),
                'passingInterceptions': get_val(stat_dict, 'passingInterceptions', 'passing_int', 'passingInt'),
                'passingAttempts': get_val(stat_dict, 'passingAttempts', 'attempts', 'passingAtt'),
                'passingCompletions': get_val(stat_dict, 'passingCompletions', 'completions', 'passingComp'),
                'rushingYards': get_val(stat_dict, 'rushingYards', 'rushing_yards', 'rushingYds'),
                'rushingTouchdowns': get_val(stat_dict, 'rushingTouchdowns', 'rushing_tds', 'rushingTDs'),
                'rushingAttempts': get_val(stat_dict, 'rushingAttempts', 'rushing_att', 'rushingAtt'),
                'games': get_val(stat_dict, 'games', 'gamesPlayed'),
            }
        else:  # RB, WR, TE
            result = {
                'rushingYards': get_val(stat_dict, 'rushingYards', 'rushing_yards', 'rushingYds'),
                'rushingTouchdowns': get_val(stat_dict, 'rushingTouchdowns', 'rushing_tds', 'rushingTDs'),
                'rushingAttempts': get_val(stat_dict, 'rushingAttempts', 'rushing_att', 'rushingAtt'),
                'receptions': get_val(stat_dict, 'receptions', 'receptions', 'rec'),
                'receivingYards': get_val(stat_dict, 'receivingYards', 'receiving_yards', 'receivingYds'),
                'receivingTouchdowns': get_val(stat_dict, 'receivingTouchdowns', 'receiving_tds', 'receivingTDs'),
                'targets': get_val(stat_dict, 'targets', 'targets'),
                'games': get_val(stat_dict, 'games', 'gamesPlayed'),
            }
        
        return result
    
    def _aggregate_stats(self, stats_data: List[Dict], position: str) -> Dict:
        """Aggregate stats across multiple seasons from JSON responses."""
        agg = {
            'seasons': len(stats_data),
            'total_games': 0,
        }
        
        if position == 'QB':
            agg.update({
                'pass_yds': 0,
                'pass_tds': 0,
                'pass_int': 0,
                'pass_att': 0,
                'pass_comp': 0,
                'rush_yds': 0,
                'rush_tds': 0,
                'rush_att': 0,
            })
            
            for data in stats_data:
                stat = data['stat']  # This is now a dict, not an object
                # CFBD API returns stats in a flat structure
                agg['pass_yds'] += stat.get('passingYards', 0) or 0
                agg['pass_tds'] += stat.get('passingTouchdowns', 0) or 0
                agg['pass_int'] += stat.get('passingInterceptions', 0) or 0
                agg['pass_att'] += stat.get('passingAttempts', 0) or 0
                agg['pass_comp'] += stat.get('passingCompletions', 0) or 0
                agg['rush_yds'] += stat.get('rushingYards', 0) or 0
                agg['rush_tds'] += stat.get('rushingTouchdowns', 0) or 0
                agg['rush_att'] += stat.get('rushingAttempts', 0) or 0
                agg['total_games'] += stat.get('games', 0) or 0
            
            # Calculate per-game averages
            if agg['total_games'] > 0:
                agg['pass_yds_per_game'] = agg['pass_yds'] / agg['total_games']
                agg['pass_tds_per_game'] = agg['pass_tds'] / agg['total_games']
                agg['completion_pct'] = (agg['pass_comp'] / agg['pass_att'] * 100) if agg['pass_att'] > 0 else 0
                agg['rush_yds_per_game'] = agg['rush_yds'] / agg['total_games']
        
        elif position in ['RB', 'WR', 'TE']:
            agg.update({
                'rush_yds': 0,
                'rush_tds': 0,
                'rush_att': 0,
                'rec': 0,
                'rec_yds': 0,
                'rec_tds': 0,
                'targets': 0,
            })
            
            for data in stats_data:
                stat = data['stat']  # This is now a dict
                agg['rush_yds'] += stat.get('rushingYards', 0) or 0
                agg['rush_tds'] += stat.get('rushingTouchdowns', 0) or 0
                agg['rush_att'] += stat.get('rushingAttempts', 0) or 0
                agg['rec'] += stat.get('receptions', 0) or 0
                agg['rec_yds'] += stat.get('receivingYards', 0) or 0
                agg['rec_tds'] += stat.get('receivingTouchdowns', 0) or 0
                agg['targets'] += stat.get('targets', 0) or 0
                agg['total_games'] += stat.get('games', 0) or 0
            
            # Calculate per-game averages
            if agg['total_games'] > 0:
                agg['rush_yds_per_game'] = agg['rush_yds'] / agg['total_games']
                agg['rec_per_game'] = agg['rec'] / agg['total_games']
                agg['rec_yds_per_game'] = agg['rec_yds'] / agg['total_games']
                agg['targets_per_game'] = agg['targets'] / agg['total_games']
                agg['yards_per_catch'] = (agg['rec_yds'] / agg['rec']) if agg['rec'] > 0 else 0
        
        return agg
    
    def calculate_tier(self, rank: int, stats: Optional[Dict], position: str) -> str:
        """
        Calculate tier based on rank using centralized tier system.
        
        This method now uses the centralized tier calculator from tiers module.
        """
        return calculate_prospect_tier(rank)
    
    def calculate_tier_with_physicals(
        self,
        rank: int,
        height: Optional[float],
        weight: Optional[float],
        position: str
    ) -> tuple[str, int]:
        """
        Calculate tier with height/weight adjustments.
        
        Physical attributes can adjust tier:
        - QB: Height premium (6'3"+), weight for durability
        - RB: Weight matters (210-230 ideal), height can be negative
        - WR: Height premium (6'2"+), weight for strength
        - TE: Height/weight both premium (6'4"+ 240+)
        
        Returns:
            Tuple of (tier_name, adjusted_tier_numeric)
        """
        base_tier = calculate_prospect_tier(rank)
        _, _, base_tier_numeric = get_tier_from_rank(rank)
        
        # Calculate physical adjustment
        adjusted_tier_numeric = calculate_physical_adjustment(
            position, height, weight, base_tier_numeric
        )
        
        # Map adjusted numeric tier back to tier name
        tier_mapping = {
            1: 'Tier 1',
            2: 'Tier 2',
            3: 'Tier 3',
            4: 'Tier 4',
            5: 'Tier 5',
        }
        
        adjusted_tier = tier_mapping.get(adjusted_tier_numeric, base_tier)
        
        return adjusted_tier, adjusted_tier_numeric
    
    def find_nfl_comparisons(
        self,
        player_name: str,
        position: str,
        stats: Optional[Dict],
        tier: str,
        nfl_players_df: pd.DataFrame
    ) -> List[str]:
        """
        Find NFL player comparisons based on similar statistical profiles.
        
        Args:
            player_name: College player name
            position: Player position
            stats: Aggregated college stats
            tier: Player tier
            nfl_players_df: DataFrame of NFL players with stats
            
        Returns:
            List of NFL player names (max 3)
        """
        if stats is None or nfl_players_df.empty:
            return []
        
        # Filter NFL players by position and similar tier
        nfl_filtered = nfl_players_df[
            (nfl_players_df['position'] == position.upper()) &
            (nfl_players_df['games_played'] >= 8)  # Minimum sample size
        ].copy()
        
        if nfl_filtered.empty:
            return []
        
        # Calculate similarity scores
        similarities = []
        
        for _, nfl_player in nfl_filtered.iterrows():
            score = self._calculate_similarity(stats, nfl_player, position)
            if score > 0:
                similarities.append({
                    'name': nfl_player.get('player_display_name', 'Unknown'),
                    'score': score
                })
        
        # Sort by similarity and return top 3
        similarities.sort(key=lambda x: x['score'], reverse=True)
        return [comp['name'] for comp in similarities[:3]]
    
    def _calculate_similarity(
        self,
        college_stats: Dict,
        nfl_player: pd.Series,
        position: str
    ) -> float:
        """Calculate similarity score between college stats and NFL player."""
        score = 0.0
        
        if position == 'QB':
            # Compare passing stats
            if 'pass_yds_per_game' in college_stats and 'fantasy_ppg' in nfl_player:
                college_ppg = college_stats.get('pass_yds_per_game', 0) / 25  # Rough conversion
                nfl_ppg = nfl_player.get('fantasy_ppg', 0) or 0
                if nfl_ppg > 0:
                    score += 1.0 - abs(college_ppg - nfl_ppg) / max(college_ppg, nfl_ppg)
            
            # Compare completion percentage
            if 'completion_pct' in college_stats:
                college_comp = college_stats['completion_pct']
                # NFL completion % would need to be fetched separately
                # For now, use fantasy PPG as proxy
                score += 0.5
        
        elif position in ['RB', 'WR', 'TE']:
            # Compare receiving stats
            if 'rec_yds_per_game' in college_stats and 'fantasy_ppg' in nfl_player:
                college_ppg = college_stats.get('rec_yds_per_game', 0) / 10  # Rough conversion
                nfl_ppg = nfl_player.get('fantasy_ppg', 0) or 0
                if nfl_ppg > 0:
                    score += 1.0 - abs(college_ppg - nfl_ppg) / max(college_ppg, nfl_ppg)
            
            # Compare rushing stats for RB
            if position == 'RB' and 'rush_yds_per_game' in college_stats:
                college_rush = college_stats['rush_yds_per_game']
                # Use fantasy PPG as proxy for NFL rushing production
                score += 0.3
        
        return max(0.0, min(1.0, score))
    
    def find_tier_based_comps(
        self,
        position: str,
        tier: str,
        nfl_players_df: pd.DataFrame
    ) -> List[str]:
        """
        Find NFL comparisons based on tier and position when stats aren't available.
        
        Args:
            position: Player position
            tier: Player tier (Tier 1-5)
            nfl_players_df: DataFrame of NFL players with stats
            
        Returns:
            List of NFL player names (max 3)
        """
        # Filter NFL players by position
        nfl_filtered = nfl_players_df[
            (nfl_players_df['position'] == position.upper()) &
            (nfl_players_df['games_played'] >= 8)
        ].copy()
        
        if nfl_filtered.empty:
            return []
        
        # Map tier to fantasy PPG ranges for comparison
        tier_ppg_ranges = {
            'Tier 1': (18, 30),  # Elite prospects -> elite NFL players
            'Tier 2': (15, 22),  # First round -> high-end starters
            'Tier 3': (12, 18),  # Early 2nd -> solid starters
            'Tier 4': (8, 15),   # Late 2nd/Early 3rd -> flex players
            'Tier 5': (0, 12),   # Mid-late round -> depth players
        }
        
        min_ppg, max_ppg = tier_ppg_ranges.get(tier, (0, 20))
        
        # Filter by fantasy PPG range
        nfl_filtered = nfl_filtered[
            (nfl_filtered['fantasy_ppg'] >= min_ppg) &
            (nfl_filtered['fantasy_ppg'] <= max_ppg)
        ]
        
        if nfl_filtered.empty:
            return []
        
        # Sort by fantasy PPG (descending) and return top 3
        nfl_filtered = nfl_filtered.sort_values('fantasy_ppg', ascending=False)
        comps = nfl_filtered.head(3)['player_display_name'].tolist()
        
        return comps
    
    def get_top_players_by_class(
        self,
        year: int,
        position: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get top players from a specific class (year) using CFBD API.
        
        Args:
            year: Class year (e.g., 2026 for 2026 draft class)
            position: Optional position filter (QB, RB, WR, TE)
            limit: Maximum number of players to return
            
        Returns:
            List of player dicts with stats and rankings
        """
        try:
            # Search for players by class year
            # Note: CFBD doesn't have direct class year search, so we'll use stats API
            # and filter by players who are draft-eligible
            
            # Get current year to determine class
            current_year = datetime.now().year
            draft_year = year
            
            # Fetch stats for the most recent season
            top_players = []
            
            # For each skill position
            positions_to_search = [position] if position else self.skill_positions
            
            for pos in positions_to_search:
                try:
                    time.sleep(self.request_delay)
                    
                    category_map = {
                        'QB': 'passing',
                        'RB': 'rushing',
                        'WR': 'receiving',
                        'TE': 'receiving',
                    }
                    category = category_map.get(pos.upper(), 'rushing')
                    
                    # Get stats for current season
                    url = f'{self.base_url}/stats/player/season'
                    params = {
                        'year': current_year,
                        'category': category
                    }
                    
                    response = requests.get(url, headers=self.headers, params=params)
                    
                    if response.status_code != 200:
                        continue
                    
                    stats_list = response.json()
                    
                    # Sort by production and take top players
                    # This is a simplified approach - ideally we'd use recruiting rankings
                    # or combine with draft projections
                    
                    for stat in stats_list[:limit]:
                        player_name = stat.get('player', '') or ''
                        if player_name:
                            top_players.append({
                                'name': player_name,
                                'position': pos,
                                'school': stat.get('team', ''),
                                'stats': self._stat_to_dict(stat, pos),
                            })
                            
                except Exception:
                    continue
            
            return top_players[:limit]
            
        except Exception as e:
            print(f"  ⚠ Error fetching top players by class: {str(e)[:50]}")
            return []
    
    def run_pipeline(
        self,
        years_back: int = 3,
        update_tiers: bool = True,
        update_comps: bool = True,
        fetch_physicals: bool = True,
        fetch_draft_year: bool = True
    ) -> None:
        """
        Run the full pipeline: fetch stats, calculate tiers, find comparisons.
        
        Args:
            years_back: Number of years of college stats to fetch
            update_tiers: Whether to update tier assignments
            update_comps: Whether to find and update NFL comparisons
            fetch_physicals: Whether to fetch height/weight
            fetch_draft_year: Whether to fetch draft year
        """
        print("=" * 80)
        print("COLLEGE PLAYER RANKING PIPELINE")
        print("=" * 80)
        
        supabase = config.get_supabase_client()
        if not supabase:
            print("❌ Failed to get Supabase client")
            return
        
        # Fetch current rookie rankings (skill positions only)
        print("\n📊 Fetching current rookie rankings (skill positions only)...")
        result = supabase.from_('dynasty_prospects')\
            .select('*')\
            .in_('position', self.skill_positions)\
            .order('rank')\
            .execute()
        
        if not result.data:
            print("❌ No rookie rankings found")
            return
        
        df_rookies = pd.DataFrame(result.data)
        print(f"   Found {len(df_rookies)} prospects (skill positions only)")
        
        # Fetch NFL player stats for comparisons
        nfl_stats_df = pd.DataFrame()
        if update_comps:
            print("\n📊 Fetching NFL player stats for comparisons...")
            nfl_result = supabase.from_('master_player_stats')\
                .select('player_display_name, position, fantasy_ppg, games_played')\
                .eq('season', 2025)\
                .in_('position', self.skill_positions)\
                .gte('games_played', 1)\
                .execute()
            
            if nfl_result.data:
                nfl_stats_df = pd.DataFrame(nfl_result.data)
                print(f"   Found {len(nfl_stats_df)} NFL players")
        
        # Process each prospect
        updates = []
        stats_fetched = 0
        stats_failed = 0
        physicals_fetched = 0
        draft_years_fetched = 0
        
        print(f"\n🔄 Processing {len(df_rookies)} prospects...")
        print(f"   Years back: {years_back}")
        print(f"   Update tiers: {update_tiers}")
        print(f"   Update comps: {update_comps}")
        print(f"   Fetch physicals: {fetch_physicals}")
        print(f"   Fetch draft year: {fetch_draft_year}")
        
        for idx, row in df_rookies.iterrows():
            player_name = row.get('name', '')
            school = row.get('school', '')
            position = row.get('position', '')
            rank = row.get('rank', 999)
            player_id = row.get('id')
            
            if not player_name or position not in self.skill_positions:
                continue
            
            # Try to fetch school and physicals if missing or TBD
            fetched_info = None
            if (not school or school == 'TBD') or (fetch_physicals and (not row.get('height') or not row.get('weight'))):
                fetched_info = self.fetch_player_info(player_name, position)
                if fetched_info:
                    # Update school if missing
                    if (not school or school == 'TBD') and fetched_info.get('school'):
                        school = fetched_info.get('school')
                        print(f"   ✓ Found school: {school}")
                        # Update school in database
                        try:
                            supabase.from_('dynasty_prospects')\
                                .update({'school': school})\
                                .eq('id', player_id)\
                                .execute()
                        except Exception:
                            pass  # Continue even if update fails
            
            # Skip if school is TBD but still process for tier updates
            skip_stats_fetch = (not school or school == 'TBD')
            
            print(f"\n[{idx+1}/{len(df_rookies)}] {player_name} ({position}, {school if school else 'TBD'})")
            
            # Fetch college stats (skip if school is TBD)
            stats = None
            if not skip_stats_fetch:
                stats = self.fetch_college_stats(player_name, school, position, years_back)
                
                if stats:
                    stats_fetched += 1
                    print(f"   ✓ Stats: {stats.get('seasons', 0)} seasons, {stats.get('total_games', 0)} games")
                else:
                    stats_failed += 1
                    print(f"   ⚠ No stats found")
            else:
                print(f"   ⚠ Skipping stats fetch (school TBD)")
            
            # Fetch physical attributes (height, weight, class)
            # Try from fetched_info first, then from roster API if we have a school
            height = None
            weight = None
            player_class = None
            
            if fetch_physicals:
                # Use fetched info if available
                if fetched_info:
                    height = fetched_info.get('height')
                    weight = fetched_info.get('weight')
                    player_class = fetched_info.get('class')
                    if height or weight:
                        physicals_fetched += 1
                        print(f"   ✓ Physicals (from search): {height}\" {weight}lbs ({player_class})")
                # Otherwise try roster API if we have a school
                elif not skip_stats_fetch:
                    physicals = self.fetch_player_physical_attributes(player_name, school, position)
                    if physicals:
                        height = physicals.get('height')
                        weight = physicals.get('weight')
                        player_class = physicals.get('class')
                        physicals_fetched += 1
                        if height or weight:
                            print(f"   ✓ Physicals (from roster): {height}\" {weight}lbs ({player_class})")
            
            # Fetch draft year
            draft_year = None
            if fetch_draft_year:
                draft_year = self.fetch_draft_year(player_name)
                if draft_year:
                    draft_years_fetched += 1
                    print(f"   ✓ Draft Year: {draft_year}")
            
            # Calculate valuation first (needed for tier assignment)
            valuation = calculate_prospect_value(rank, position)
            position_multiplier = get_position_multiplier(position)
            
            # Calculate tier based on valuation (ensures higher valuations = higher tiers)
            tier, tier_numeric = calculate_prospect_tier_from_valuation(valuation)
            
            # Apply physical adjustments if height/weight available
            # Physical adjustments can only improve tier, not lower it
            if (height is not None or weight is not None) and fetch_physicals:
                # Get base tier numeric from rank for physical adjustment calculation
                _, _, base_tier_numeric_from_rank = get_tier_from_rank(rank)
                adjusted_tier_numeric = calculate_physical_adjustment(
                    position, height, weight, base_tier_numeric_from_rank
                )
                # Only apply physical adjustment if it improves the tier
                # (we don't want to lower tiers based on physicals when valuation is higher)
                if adjusted_tier_numeric < tier_numeric:
                    tier_mapping = {
                        1: 'Tier 1',
                        2: 'Tier 2',
                        3: 'Tier 3',
                        4: 'Tier 4',
                        5: 'Tier 5',
                    }
                    tier = tier_mapping.get(adjusted_tier_numeric, tier)
                    tier_numeric = adjusted_tier_numeric
            
            # Find NFL comparisons (try even without stats, use tier-based matching)
            comps = []
            if update_comps and not nfl_stats_df.empty:
                if stats:
                    # Use stats-based comparison if available
                    comps = self.find_nfl_comparisons(
                        player_name, position, stats, tier, nfl_stats_df
                    )
                else:
                    # Fallback: find comps based on tier and position only
                    comps = self.find_tier_based_comps(position, tier, nfl_stats_df)
                
                if comps:
                    print(f"   ✓ Comps: {', '.join(comps)}")
            
            # Prepare update with all calculated fields
            update_data = {'id': row.get('id')}
            if update_tiers:
                update_data['tier'] = tier
                update_data['tier_numeric'] = tier_numeric
                update_data['valuation'] = float(valuation)
                update_data['position_multiplier'] = float(position_multiplier)
                print(f"   ✓ Tier: {tier} | Value: {valuation:.2f}")
            
            # Add physical attributes
            if fetch_physicals:
                if height is not None:
                    update_data['height'] = float(height) if height else None
                if weight is not None:
                    update_data['weight'] = float(weight) if weight else None
                if player_class:
                    update_data['class'] = player_class
            
            # Add draft year
            if fetch_draft_year and draft_year:
                update_data['draft_year'] = int(draft_year)
            
            if update_comps and comps:
                update_data['nfl_comparisons'] = ', '.join(comps)
            
            updates.append(update_data)
        
        # Batch update database
        if updates:
            print(f"\n💾 Updating database ({len(updates)} players)...")
            updated_count = 0
            failed_count = 0
            
            for update in updates:
                try:
                    player_id = update.pop('id')  # Remove id from update dict
                    update_data = update
                    
                    result = supabase.from_('dynasty_prospects')\
                        .update(update_data)\
                        .eq('id', player_id)\
                        .execute()
                    
                    if result.data:
                        updated_count += 1
                        if updated_count % 10 == 0:
                            print(f"   ✓ Updated {updated_count}/{len(updates)}...")
                    else:
                        failed_count += 1
                        print(f"   ⚠ No rows updated for id {player_id}")
                        
                except Exception as e:
                    failed_count += 1
                    print(f"   ❌ Failed to update: {str(e)[:100]}")
            
            print(f"\n✅ Pipeline complete!")
            print(f"   Stats fetched: {stats_fetched}")
            print(f"   Stats failed: {stats_failed}")
            print(f"   Physicals fetched: {physicals_fetched}")
            print(f"   Draft years fetched: {draft_years_fetched}")
            print(f"   Updates attempted: {len(updates)}")
            print(f"   Updates successful: {updated_count}")
            print(f"   Updates failed: {failed_count}")
        else:
            print("\n⚠ No updates to apply")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='College Player Ranking Pipeline')
    parser.add_argument(
        '--years-back',
        type=int,
        default=3,
        help='Number of years of college stats to fetch (default: 3)'
    )
    parser.add_argument(
        '--no-tiers',
        action='store_true',
        help='Skip tier updates'
    )
    parser.add_argument(
        '--no-comps',
        action='store_true',
        help='Skip NFL comparison updates'
    )
    parser.add_argument(
        '--no-physicals',
        action='store_true',
        help='Skip height/weight fetching'
    )
    parser.add_argument(
        '--no-draft-year',
        action='store_true',
        help='Skip draft year fetching'
    )
    parser.add_argument(
        '--api-key',
        type=str,
        help='CFBD API key (or set CFBD_API_KEY env var)'
    )
    
    args = parser.parse_args()
    
    try:
        pipeline = CollegeRankingPipeline(api_key=args.api_key)
        pipeline.run_pipeline(
            years_back=args.years_back,
            update_tiers=not args.no_tiers,
            update_comps=not args.no_comps,
            fetch_physicals=not args.no_physicals,
            fetch_draft_year=not args.no_draft_year
        )
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    import sys
    main()
