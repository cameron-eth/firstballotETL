#!/usr/bin/env python3
"""
ESPN College Football Athlete Data Fetcher

Fetches detailed athlete data from ESPN's college football API using stored ESPN IDs.
No authentication required.

ESPN Endpoints:
- Overview: site.web.api.espn.com/apis/common/v3/sports/football/college-football/athletes/{id}/overview
- Stats: site.web.api.espn.com/apis/common/v3/sports/football/college-football/athletes/{id}/stats
- Headshot: a.espncdn.com/i/headshots/college-football/players/full/{id}.png
"""

import os
import sys
import time
import argparse
import requests
from typing import Dict, Optional, List
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import config


class ESPNAthleteFetcher:
    """Fetches college football athlete data from ESPN API."""
    
    def __init__(self):
        """Initialize ESPN API client."""
        # ESPN API base URLs (no auth required)
        self.overview_url = 'https://site.web.api.espn.com/apis/common/v3/sports/football/college-football/athletes/{id}/overview'
        self.stats_url = 'https://site.web.api.espn.com/apis/common/v3/sports/football/college-football/athletes/{id}/stats'
        
        # Headshot URL pattern
        self.headshot_pattern = 'https://a.espncdn.com/i/headshots/college-football/players/full/{id}.png'
        
        # Rate limiting (be respectful - no official limits)
        self.request_delay = 0.2  # 200ms between requests
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
        })
    
    def fetch_athlete_overview(self, espn_id: int) -> Optional[Dict]:
        """
        Fetch athlete overview data from ESPN.
        
        Args:
            espn_id: ESPN athlete ID
            
        Returns:
            Dict with athlete data or None if not found
        """
        try:
            time.sleep(self.request_delay)
            
            url = self.overview_url.format(id=espn_id)
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 404:
                return None
            
            if response.status_code != 200:
                print(f"  ⚠ ESPN API error ({response.status_code})")
                return None
            
            data = response.json()
            athlete = data.get('athlete', {})
            
            # Extract key fields
            result = {
                'espn_id': espn_id,
                'display_name': athlete.get('displayName'),
                'first_name': athlete.get('firstName'),
                'last_name': athlete.get('lastName'),
                'jersey': athlete.get('jersey'),
                'position': athlete.get('position', {}).get('abbreviation'),
                'height_display': athlete.get('displayHeight'),  # "6'1""
                'weight_display': athlete.get('displayWeight'),  # "205 lbs"
                'height': athlete.get('height'),  # inches
                'weight': athlete.get('weight'),  # lbs
                'experience': athlete.get('experience', {}).get('displayValue'),  # "Junior"
                'experience_years': athlete.get('experience', {}).get('years'),
                'team_name': athlete.get('team', {}).get('displayName'),
                'team_abbreviation': athlete.get('team', {}).get('abbreviation'),
                'team_color': athlete.get('team', {}).get('color'),
                'headshot_url': athlete.get('headshot', {}).get('href'),
            }
            
            # Extract birthplace
            birthplace = athlete.get('birthPlace', {})
            if birthplace:
                city = birthplace.get('city', '')
                state = birthplace.get('state', '')
                result['birthplace'] = f"{city}, {state}".strip(', ') if city or state else None
            
            # Extract college (for verification)
            college = athlete.get('college', {})
            if college:
                result['college_name'] = college.get('name')
            
            return result
            
        except requests.exceptions.Timeout:
            print(f"  ⚠ Request timeout for ESPN ID {espn_id}")
            return None
        except Exception as e:
            print(f"  ⚠ Error fetching ESPN data: {str(e)[:50]}")
            return None
    
    def fetch_athlete_stats(self, espn_id: int) -> Optional[Dict]:
        """
        Fetch athlete statistics from ESPN.
        
        Args:
            espn_id: ESPN athlete ID
            
        Returns:
            Dict with stats data or None if not found
        """
        try:
            time.sleep(self.request_delay)
            
            url = self.stats_url.format(id=espn_id)
            response = self.session.get(url, timeout=10)
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            
            # Parse statistics
            stats_result = {}
            
            # Career stats are in 'statistics' array
            statistics = data.get('statistics', [])
            
            for stat_category in statistics:
                category_name = stat_category.get('name', '').lower()
                
                # Get career totals
                if 'career' in category_name or stat_category.get('type') == 'career':
                    stats = stat_category.get('stats', [])
                    for stat in stats:
                        stat_name = stat.get('name', '').lower().replace(' ', '_')
                        stat_value = stat.get('value')
                        if stat_value is not None:
                            stats_result[f'career_{stat_name}'] = stat_value
            
            # Also check for season stats
            splits = data.get('splits', {})
            categories = splits.get('categories', [])
            
            for category in categories:
                cat_name = category.get('name', '').lower()
                stats = category.get('stats', [])
                
                for stat in stats:
                    stat_name = stat.get('name', '').lower().replace(' ', '_')
                    stat_value = stat.get('value')
                    if stat_value is not None:
                        stats_result[f'{cat_name}_{stat_name}'] = stat_value
            
            return stats_result if stats_result else None
            
        except Exception as e:
            return None
    
    def get_headshot_url(self, espn_id: int) -> str:
        """Generate headshot URL from ESPN ID."""
        return self.headshot_pattern.format(id=espn_id)
    
    def run_pipeline(
        self,
        limit: Optional[int] = None,
        missing_only: bool = True,
        fetch_stats: bool = True
    ) -> None:
        """
        Run the ESPN data fetch pipeline.
        
        Args:
            limit: Limit number of prospects to process
            missing_only: Only process prospects missing headshot data
            fetch_stats: Whether to also fetch stats
        """
        print("=" * 80)
        print("ESPN COLLEGE FOOTBALL ATHLETE FETCHER")
        print("=" * 80)
        
        supabase = config.get_supabase_client()
        if not supabase:
            print("❌ Failed to get Supabase client")
            return
        
        # Fetch prospects with ESPN IDs
        print("\n📊 Fetching prospects with ESPN IDs...")
        
        query = supabase.from_('dynasty_prospects')\
            .select('id, name, position, school, espn_id, headshot_url, height, weight')\
            .not_.is_('espn_id', 'null')\
            .order('rank')
        
        if missing_only:
            # Only fetch those missing headshot data
            query = query.is_('headshot_url', 'null')
        
        if limit:
            query = query.limit(limit)
        
        result = query.execute()
        
        if not result.data:
            print("❌ No prospects with ESPN IDs found (or all already have headshots)")
            return
        
        prospects = result.data
        print(f"   Found {len(prospects)} prospects to process")
        
        # Process each prospect
        stats = {
            'fetched': 0,
            'failed': 0,
            'updated': 0,
            'stats_fetched': 0,
        }
        
        print(f"\n🔄 Fetching ESPN data...")
        print(f"   Fetch stats: {fetch_stats}")
        print(f"   Missing only: {missing_only}")
        
        for idx, prospect in enumerate(prospects):
            name = prospect.get('name', '')
            espn_id = prospect.get('espn_id')
            prospect_id = prospect.get('id')
            
            print(f"\n[{idx+1}/{len(prospects)}] {name} (ESPN ID: {espn_id})")
            
            # Fetch overview data
            overview = self.fetch_athlete_overview(espn_id)
            
            if not overview:
                stats['failed'] += 1
                print(f"   ⚠ No ESPN data found")
                continue
            
            stats['fetched'] += 1
            
            # Prepare update data
            update_data = {
                'updated_at': datetime.now().isoformat()
            }
            
            # Headshot URL
            if overview.get('headshot_url'):
                update_data['headshot_url'] = overview['headshot_url']
                print(f"   ✓ Headshot: {overview['headshot_url'][:60]}...")
            else:
                # Fallback to generated URL
                update_data['headshot_url'] = self.get_headshot_url(espn_id)
                print(f"   ✓ Headshot (generated): {update_data['headshot_url'][:60]}...")
            
            # Height/Weight (update if missing)
            if overview.get('height') and not prospect.get('height'):
                update_data['height'] = float(overview['height'])
                print(f"   ✓ Height: {overview.get('height_display', overview['height'])}")
            
            if overview.get('weight') and not prospect.get('weight'):
                update_data['weight'] = float(overview['weight'])
                print(f"   ✓ Weight: {overview.get('weight_display', overview['weight'])}")
            
            # Experience/Class year
            if overview.get('experience'):
                update_data['class'] = overview['experience']
                print(f"   ✓ Class: {overview['experience']}")
            
            # Birthplace/Hometown
            if overview.get('birthplace'):
                update_data['hometown'] = overview['birthplace']
                print(f"   ✓ Hometown: {overview['birthplace']}")
            
            # Jersey number
            if overview.get('jersey'):
                update_data['jersey'] = int(overview['jersey'])
                print(f"   ✓ Jersey: #{overview['jersey']}")
            
            # First/Last name
            if overview.get('first_name'):
                update_data['first_name'] = overview['first_name']
            if overview.get('last_name'):
                update_data['last_name'] = overview['last_name']
            
            # Team color
            if overview.get('team_color'):
                update_data['team_color'] = f"#{overview['team_color']}"
            
            # Fetch stats if requested
            if fetch_stats:
                athlete_stats = self.fetch_athlete_stats(espn_id)
                if athlete_stats:
                    update_data['college_stats'] = athlete_stats
                    stats['stats_fetched'] += 1
                    
                    # Print key stats based on position
                    position = prospect.get('position', '')
                    if position == 'QB':
                        pass_yds = athlete_stats.get('career_passing_yards') or athlete_stats.get('passing_yards')
                        pass_tds = athlete_stats.get('career_passing_touchdowns') or athlete_stats.get('passing_touchdowns')
                        if pass_yds or pass_tds:
                            print(f"   ✓ Stats: {pass_yds or 0} yds, {pass_tds or 0} TDs")
                    elif position == 'RB':
                        rush_yds = athlete_stats.get('career_rushing_yards') or athlete_stats.get('rushing_yards')
                        rush_tds = athlete_stats.get('career_rushing_touchdowns') or athlete_stats.get('rushing_touchdowns')
                        if rush_yds or rush_tds:
                            print(f"   ✓ Stats: {rush_yds or 0} yds, {rush_tds or 0} TDs")
                    elif position in ['WR', 'TE']:
                        rec_yds = athlete_stats.get('career_receiving_yards') or athlete_stats.get('receiving_yards')
                        rec_tds = athlete_stats.get('career_receiving_touchdowns') or athlete_stats.get('receiving_touchdowns')
                        if rec_yds or rec_tds:
                            print(f"   ✓ Stats: {rec_yds or 0} yds, {rec_tds or 0} TDs")
            
            # Update database
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
        print("FETCH COMPLETE")
        print("=" * 80)
        print(f"   ESPN Data Fetched: {stats['fetched']}")
        print(f"   ESPN Data Failed: {stats['failed']}")
        print(f"   Stats Fetched: {stats['stats_fetched']}")
        print(f"   Records Updated: {stats['updated']}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='ESPN College Football Athlete Fetcher')
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of prospects to process'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all prospects (not just those missing headshots)'
    )
    parser.add_argument(
        '--no-stats',
        action='store_true',
        help='Skip fetching career stats'
    )
    
    args = parser.parse_args()
    
    try:
        fetcher = ESPNAthleteFetcher()
        fetcher.run_pipeline(
            limit=args.limit,
            missing_only=not args.all,
            fetch_stats=not args.no_stats
        )
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

