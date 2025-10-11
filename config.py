"""Configuration management for NFL data pipeline."""

import os
import toml
from pathlib import Path
from typing import Dict, Any, List, Optional

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, use system env vars

# Load configuration from TOML file
CONFIG_PATH = Path(__file__).parent / "config.toml"


class Config:
    """Configuration manager for the NFL data pipeline."""
    
    def __init__(self, config_path: str = None):
        """Initialize configuration from TOML file."""
        if config_path is None:
            config_path = CONFIG_PATH
        
        with open(config_path, 'r') as f:
            self._config = toml.load(f)
        
        # Load environment variables for Supabase
        self._load_env_overrides()
        
        # Create output directory if it doesn't exist
        output_dir = Path(__file__).parent / self.output_dir
        output_dir.mkdir(exist_ok=True)
    
    def _load_env_overrides(self):
        """Load environment variable overrides for database credentials."""
        # Primary database
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
        
        if 'database' not in self._config:
            self._config['database'] = {}
            
        if supabase_url:
            self._config['database']['supabase_url'] = supabase_url
        if supabase_key:
            self._config['database']['supabase_key'] = supabase_key
        
        # Secondary database
        supabase_url_2 = os.getenv('SUPABASE_URL_2')
        supabase_key_2 = os.getenv('SUPABASE_SERVICE_KEY_2')
        
        if 'database_2' not in self._config:
            self._config['database_2'] = {}
            
        if supabase_url_2:
            self._config['database_2']['supabase_url_2'] = supabase_url_2
        if supabase_key_2:
            self._config['database_2']['supabase_key_2'] = supabase_key_2
    
    @property
    def supabase_url(self) -> Optional[str]:
        """Get Supabase URL."""
        return self._config.get('database', {}).get('supabase_url', '')
    
    @property
    def supabase_key(self) -> Optional[str]:
        """Get Supabase service key."""
        return self._config.get('database', {}).get('supabase_key', '')
    
    @property
    def enable_database(self) -> bool:
        """Get database enable flag."""
        return self._config.get('database', {}).get('enable_database', True)
    
    @property
    def supabase_url_2(self) -> Optional[str]:
        """Get secondary Supabase URL."""
        return self._config.get('database_2', {}).get('supabase_url_2', '')
    
    @property
    def supabase_key_2(self) -> Optional[str]:
        """Get secondary Supabase service key."""
        return self._config.get('database_2', {}).get('supabase_key_2', '')
    
    @property
    def enable_database_2(self) -> bool:
        """Get secondary database enable flag."""
        return self._config.get('database_2', {}).get('enable_database_2', True)
    
    @property
    def start_year(self) -> int:
        """Get start year for data fetching."""
        return self._config['data']['start_year']
    
    @property
    def end_year(self) -> int:
        """Get end year for data fetching."""
        return self._config['data']['end_year']
    
    @property
    def current_season(self) -> int:
        """Get current season year."""
        return self._config['data']['current_season']
    
    @property
    def output_dir(self) -> str:
        """Get output directory path."""
        return self._config['data']['output_dir']
    
    @property
    def save_to_csv(self) -> bool:
        """Get save to CSV flag."""
        return self._config['data']['save_to_csv']
    
    @property
    def save_to_json(self) -> bool:
        """Get save to JSON flag."""
        return self._config['data']['save_to_json']
    
    @property
    def save_to_database(self) -> bool:
        """Get save to database flag."""
        return self._config['data'].get('save_to_database', True)
    
    def get_supabase_client(self):
        """Create and return primary Supabase client if credentials are available."""
        if not self.supabase_url or not self.supabase_key:
            return None
        
        try:
            from supabase import create_client
            return create_client(self.supabase_url, self.supabase_key)
        except ImportError:
            print("Warning: supabase-py not installed. Run: pip install supabase")
            return None
    
    def get_supabase_client_2(self):
        """Create and return secondary Supabase client if credentials are available."""
        if not self.supabase_url_2 or not self.supabase_key_2:
            return None
        
        try:
            from supabase import create_client
            return create_client(self.supabase_url_2, self.supabase_key_2)
        except ImportError:
            print("Warning: supabase-py not installed. Run: pip install supabase")
            return None
    
    @property
    def ngs_stat_types(self) -> List[str]:
        """Get list of NGS stat types to fetch."""
        return self._config['ngs']['stat_types']
    
    @property
    def batch_size(self) -> int:
        """Get batch size for database operations."""
        return self._config['pipeline']['batch_size']
    
    @property
    def verbose(self) -> bool:
        """Get verbose logging flag."""
        return self._config['pipeline']['verbose']
    
    @property
    def positions(self) -> List[str]:
        """Get list of positions to include."""
        return self._config['filters']['positions']
    
    @property
    def season_types(self) -> List[str]:
        """Get list of season types to include."""
        return self._config['filters']['season_types']
    
    def get_year_range(self) -> List[int]:
        """Get list of years to fetch data for."""
        return list(range(self.start_year, self.end_year + 1))


# Global config instance
config = Config()

