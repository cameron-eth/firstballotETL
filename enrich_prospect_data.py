#!/usr/bin/env python3
"""
Prospect Data Enrichment Pipeline

Enriches dynasty_prospects with:
1. CFBD player data (cfbd_id, height, weight, hometown, jersey)
2. ESPN athlete ID (for headshots)
3. First/Last name parsing

ESPN Headshot URL Pattern:
- College: https://a.espncdn.com/combiner/i?img=/i/headshots/college-football/players/full/{espn_id}.png
- NFL: https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/{espn_id}.png
"""

import os
import sys
import time
import argparse
import requests
from typing import Dict, Optional, List, Tuple
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import config


class ProspectEnrichmentPipeline:
    """Pipeline for enriching prospect data from CFBD and ESPN APIs."""
    
    def __init__(self, cfbd_api_key: Optional[str] = None):
        """Initialize API clients."""
        self.cfbd_api_key = cfbd_api_key or os.getenv('CFBD_API_KEY')
        if not self.cfbd_api_key:
            raise ValueError("CFBD_API_KEY environment variable not set")
        
        # CFBD API config
        self.cfbd_base_url = 'https://api.collegefootballdata.com'
        self.cfbd_headers = {
            'Authorization': f'Bearer {self.cfbd_api_key}',
            'Accept': 'application/json'
        }
        
        # ESPN API config (no auth required for search)
        self.espn_search_url = 'https://site.api.espn.com/apis/common/v3/search'
        self.espn_player_url = 'https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes'
        
        # Rate limiting
        self.request_delay = 0.15  # 150ms between requests
        
        # Skill positions
        self.skill_positions = ['QB', 'RB', 'WR', 'TE']
    
    def parse_name(self, full_name: str) -> Tuple[str, str]:
        """
        Parse full name into first and last name.
        
        Args:
            full_name: Full player name (e.g., "Jeremiyah Love")
            
        Returns:
            Tuple of (first_name, last_name)
        """
        parts = full_name.strip().split()
        if len(parts) >= 2:
            return parts[0], ' '.join(parts[1:])
        elif len(parts) == 1:
            return parts[0], ''
        return '', ''
    
    def fetch_cfbd_player(
        self,
        player_name: str,
        position: str,
        school: Optional[str] = None,
        year: Optional[int] = None
    ) -> Optional[Dict]:
        """
        Fetch player data from CFBD /player/search endpoint.
        
        Args:
            player_name: Player name to search
            position: Position filter (QB, RB, WR, TE)
            school: Optional school/team filter
            year: Optional year filter
            
        Returns:
            Dict with player data or None if not found
        """
        try:
            time.sleep(self.request_delay)
            
            url = f'{self.cfbd_base_url}/player/search'
            params = {
                'searchTerm': player_name,
                'position': position.upper()
            }
            
            if school:
                params['team'] = school
            if year:
                params['year'] = year
            
            response = requests.get(url, headers=self.cfbd_headers, params=params)
            
            if response.status_code != 200:
                return None
            
            players = response.json()
            
            if not players:
                return None
            
            # Find best match
            for player in players:
                # Extract fields (API uses both camelCase and snake_case)
                first_name = player.get('firstName') or player.get('first_name', '')
                last_name = player.get('lastName') or player.get('last_name', '')
                api_name = f"{first_name} {last_name}".strip()
                api_team = player.get('team', '')
                api_pos = player.get('position', '')
                
                # Match by name similarity and position
                name_match = (
                    player_name.lower() in api_name.lower() or 
                    api_name.lower() in player_name.lower()
                )
                pos_match = api_pos.upper() == position.upper()
                
                # School match (if provided)
                school_match = True
                if school:
                    school_match = (
                        school.lower() in api_team.lower() or
                        api_team.lower() in school.lower()
                    )
                
                if name_match and pos_match and school_match:
                    return {
                        'cfbd_id': player.get('id'),
                        'first_name': first_name,
                        'last_name': last_name,
                        'team': api_team,
                        'position': api_pos,
                        'height': player.get('height'),
                        'weight': player.get('weight'),
                        'jersey': player.get('jersey'),
                        'hometown': player.get('hometown'),
                        'team_color': player.get('teamColor'),
                        'team_color_secondary': player.get('teamColorSecondary'),
                    }
            
            # If no exact match, return first result if only one
            if len(players) == 1:
                player = players[0]
                first_name = player.get('firstName') or player.get('first_name', '')
                last_name = player.get('lastName') or player.get('last_name', '')
                return {
                    'cfbd_id': player.get('id'),
                    'first_name': first_name,
                    'last_name': last_name,
                    'team': player.get('team', ''),
                    'position': player.get('position', ''),
                    'height': player.get('height'),
                    'weight': player.get('weight'),
                    'jersey': player.get('jersey'),
                    'hometown': player.get('hometown'),
                    'team_color': player.get('teamColor'),
                    'team_color_secondary': player.get('teamColorSecondary'),
                }
            
            return None
            
        except Exception as e:
            print(f"  ⚠ CFBD search error: {str(e)[:50]}")
            return None
    
    def fetch_espn_id(
        self,
        player_name: str,
        school: Optional[str] = None,
        position: Optional[str] = None
    ) -> Optional[int]:
        """
        Fetch ESPN athlete ID using ESPN's search API.
        
        Args:
            player_name: Player name to search
            school: School name for verification
            position: Position for verification
            
        Returns:
            ESPN athlete ID or None if not found
        """
        try:
            time.sleep(self.request_delay)
            
            # Use ESPN's search API
            params = {
                'query': player_name,
                'limit': 20,
                'type': 'player'
            }
            
            response = requests.get(self.espn_search_url, params=params)
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            
            # Search results are in 'results' array
            results = data.get('results', [])
            
            for result in results:
                # Check if it's a college football player
                display_name = result.get('displayName', '')
                
                # Get the entity type and sport
                # ESPN search returns mixed results (NFL, college, etc.)
                # We need to filter for college football
                
                # Check for college football in the result
                athletes = result.get('athletes', [])
                
                for athlete in athletes:
                    athlete_name = athlete.get('displayName', '')
                    athlete_id = athlete.get('id')
                    
                    # Check if name matches
                    if not (player_name.lower() in athlete_name.lower() or 
                            athlete_name.lower() in player_name.lower()):
                        continue
                    
                    # Try to verify school/position from athlete details
                    # The search result may have limited info
                    team = athlete.get('team', {})
                    team_name = team.get('displayName', '') or team.get('name', '')
                    
                    # School verification (if provided)
                    if school:
                        if not (school.lower() in team_name.lower() or
                                team_name.lower() in school.lower()):
                            continue
                    
                    if athlete_id:
                        return int(athlete_id)
            
            # Fallback: Try direct search in results
            for result in results:
                if result.get('type') == 'athlete':
                    name = result.get('displayName', '')
                    if player_name.lower() in name.lower():
                        # Extract ID from link if available
                        link = result.get('link', '')
                        if '/id/' in link:
                            try:
                                id_part = link.split('/id/')[1].split('/')[0]
                                return int(id_part)
                            except (IndexError, ValueError):
                                pass
            
            return None
            
        except Exception as e:
            print(f"  ⚠ ESPN search error: {str(e)[:50]}")
            return None
    
    def fetch_espn_athlete_details(self, espn_id: int) -> Optional[Dict]:
        """
        Fetch detailed athlete info from ESPN's athlete API.
        
        Args:
            espn_id: ESPN athlete ID
            
        Returns:
            Dict with athlete details or None
        """
        try:
            time.sleep(self.request_delay)
            
            url = f'{self.espn_player_url}/{espn_id}'
            response = requests.get(url)
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            athlete = data.get('athlete', {})
            
            return {
                'espn_id': espn_id,
                'display_name': athlete.get('displayName'),
                'first_name': athlete.get('firstName'),
                'last_name': athlete.get('lastName'),
                'height': athlete.get('height'),
                'weight': athlete.get('weight'),
                'position': athlete.get('position', {}).get('abbreviation'),
                'team': athlete.get('team', {}).get('displayName'),
                'headshot': athlete.get('headshot', {}).get('href'),
            }
            
        except Exception as e:
            return None
    
    def get_headshot_url(self, espn_id: int, is_nfl: bool = False) -> str:
        """
        Generate ESPN headshot URL from athlete ID.
        
        Args:
            espn_id: ESPN athlete ID
            is_nfl: Whether this is an NFL player (vs college)
            
        Returns:
            Headshot URL string
        """
        sport = 'nfl' if is_nfl else 'college-football'
        return f"https://a.espncdn.com/combiner/i?img=/i/headshots/{sport}/players/full/{espn_id}.png&w=350&h=254"
    
    def run_pipeline(
        self,
        update_cfbd: bool = True,
        update_espn: bool = True,
        limit: Optional[int] = None,
        missing_only: bool = True
    ) -> None:
        """
        Run the full enrichment pipeline.
        
        Args:
            update_cfbd: Whether to fetch CFBD data
            update_espn: Whether to fetch ESPN IDs
            limit: Limit number of prospects to process
            missing_only: Only process prospects missing data
        """
        print("=" * 80)
        print("PROSPECT DATA ENRICHMENT PIPELINE")
        print("=" * 80)
        
        supabase = config.get_supabase_client()
        if not supabase:
            print("❌ Failed to get Supabase client")
            return
        
        # Fetch prospects
        print("\n📊 Fetching prospects...")
        query = supabase.from_('dynasty_prospects')\
            .select('*')\
            .in_('position', self.skill_positions)\
            .order('rank')
        
        if limit:
            query = query.limit(limit)
        
        result = query.execute()
        
        if not result.data:
            print("❌ No prospects found")
            return
        
        prospects = result.data
        print(f"   Found {len(prospects)} prospects")
        
        # Process each prospect
        stats = {
            'cfbd_found': 0,
            'cfbd_failed': 0,
            'espn_found': 0,
            'espn_failed': 0,
            'updated': 0,
            'skipped': 0,
        }
        
        print(f"\n🔄 Processing prospects...")
        print(f"   CFBD enrichment: {update_cfbd}")
        print(f"   ESPN enrichment: {update_espn}")
        print(f"   Missing data only: {missing_only}")
        
        for idx, prospect in enumerate(prospects):
            name = prospect.get('name', '')
            position = prospect.get('position', '')
            school = prospect.get('school', '')
            prospect_id = prospect.get('id')
            
            # Check if we should skip
            if missing_only:
                has_cfbd = prospect.get('cfbd_id') is not None
                has_espn = prospect.get('espn_id') is not None
                has_height = prospect.get('height') is not None
                has_weight = prospect.get('weight') is not None
                
                if has_cfbd and has_espn and has_height and has_weight:
                    stats['skipped'] += 1
                    continue
            
            print(f"\n[{idx+1}/{len(prospects)}] {name} ({position}, {school or 'TBD'})")
            
            update_data = {}
            
            # Parse name into first/last
            first_name, last_name = self.parse_name(name)
            if first_name:
                update_data['first_name'] = first_name
            if last_name:
                update_data['last_name'] = last_name
            
            # CFBD enrichment
            if update_cfbd:
                cfbd_data = self.fetch_cfbd_player(name, position, school)
                
                if cfbd_data:
                    stats['cfbd_found'] += 1
                    
                    # Store CFBD ID
                    if cfbd_data.get('cfbd_id'):
                        update_data['cfbd_id'] = cfbd_data['cfbd_id']
                        print(f"   ✓ CFBD ID: {cfbd_data['cfbd_id']}")
                    
                    # Update height/weight if missing
                    if cfbd_data.get('height') and not prospect.get('height'):
                        update_data['height'] = float(cfbd_data['height'])
                        print(f"   ✓ Height: {cfbd_data['height']}")
                    
                    if cfbd_data.get('weight') and not prospect.get('weight'):
                        update_data['weight'] = float(cfbd_data['weight'])
                        print(f"   ✓ Weight: {cfbd_data['weight']}")
                    
                    # Update school if TBD
                    if cfbd_data.get('team') and (not school or school == 'TBD'):
                        update_data['school'] = cfbd_data['team']
                        school = cfbd_data['team']  # Use for ESPN search
                        print(f"   ✓ School: {cfbd_data['team']}")
                    
                    # Store hometown
                    if cfbd_data.get('hometown'):
                        update_data['hometown'] = cfbd_data['hometown']
                        print(f"   ✓ Hometown: {cfbd_data['hometown']}")
                    
                    # Store jersey number
                    if cfbd_data.get('jersey'):
                        update_data['jersey'] = cfbd_data['jersey']
                        print(f"   ✓ Jersey: #{cfbd_data['jersey']}")
                    
                    # Use CFBD names if better
                    if cfbd_data.get('first_name'):
                        update_data['first_name'] = cfbd_data['first_name']
                    if cfbd_data.get('last_name'):
                        update_data['last_name'] = cfbd_data['last_name']
                else:
                    stats['cfbd_failed'] += 1
                    print(f"   ⚠ CFBD: Not found")
            
            # ESPN enrichment
            if update_espn and not prospect.get('espn_id'):
                espn_id = self.fetch_espn_id(name, school, position)
                
                if espn_id:
                    stats['espn_found'] += 1
                    update_data['espn_id'] = espn_id
                    headshot_url = self.get_headshot_url(espn_id)
                    print(f"   ✓ ESPN ID: {espn_id}")
                    print(f"   ✓ Headshot: {headshot_url[:60]}...")
                else:
                    stats['espn_failed'] += 1
                    print(f"   ⚠ ESPN: Not found")
            
            # Update database if we have new data
            if update_data:
                update_data['updated_at'] = datetime.now().isoformat()
                
                try:
                    supabase.from_('dynasty_prospects')\
                        .update(update_data)\
                        .eq('id', prospect_id)\
                        .execute()
                    stats['updated'] += 1
                except Exception as e:
                    print(f"   ❌ Update failed: {str(e)[:50]}")
        
        # Print summary
        print("\n" + "=" * 80)
        print("ENRICHMENT COMPLETE")
        print("=" * 80)
        print(f"   CFBD Found: {stats['cfbd_found']}")
        print(f"   CFBD Failed: {stats['cfbd_failed']}")
        print(f"   ESPN Found: {stats['espn_found']}")
        print(f"   ESPN Failed: {stats['espn_failed']}")
        print(f"   Records Updated: {stats['updated']}")
        print(f"   Records Skipped: {stats['skipped']}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Prospect Data Enrichment Pipeline')
    parser.add_argument(
        '--no-cfbd',
        action='store_true',
        help='Skip CFBD data fetching'
    )
    parser.add_argument(
        '--no-espn',
        action='store_true',
        help='Skip ESPN ID fetching'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of prospects to process'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all prospects (not just those missing data)'
    )
    parser.add_argument(
        '--api-key',
        type=str,
        help='CFBD API key (or set CFBD_API_KEY env var)'
    )
    
    args = parser.parse_args()
    
    try:
        pipeline = ProspectEnrichmentPipeline(cfbd_api_key=args.api_key)
        pipeline.run_pipeline(
            update_cfbd=not args.no_cfbd,
            update_espn=not args.no_espn,
            limit=args.limit,
            missing_only=not args.all
        )
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

