#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Configuration Manager
Centralized configuration management for all APIs and system settings
"""

import os
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

@dataclass
class APIConfig:
    """API configuration data class"""
    name: str
    base_url: str
    api_key: str
    rate_limit: int  # requests per minute
    timeout: int     # seconds
    retry_attempts: int

@dataclass
class ApifyActorConfig:
    """Apify actor configuration"""
    platform: str
    actor_id: str
    default_input: Dict[str, Any]
    timeout: int
    memory: int

class ConfigManager:
    """Centralized configuration manager for AIADMK system"""
    
    def __init__(self):
        self.load_config()
    
    def load_config(self):
        """Load all configuration from environment variables"""
        
        # Database Configuration
        self.database = {
            'supabase_url': os.getenv('SUPABASE_URL'),
            'supabase_key': os.getenv('SUPABASE_ANON_KEY'),
            'db_host': os.getenv('SUPABASE_DB_HOST'),
            'db_port': int(os.getenv('SUPABASE_DB_PORT', '5432')),
            'db_name': os.getenv('SUPABASE_DB_NAME', 'postgres'),
            'db_user': os.getenv('SUPABASE_DB_USER'),
            'db_password': os.getenv('SUPABASE_DB_PASSWORD'),
            'pool_min_size': 2,
            'pool_max_size': 10,
            'command_timeout': 30
        }
        
        # API Configurations
        self.apis = {
            'serpapi': APIConfig(
                name='SerpAPI',
                base_url='https://serpapi.com/search',
                api_key=os.getenv('SERPAPI_KEY'),
                rate_limit=100,  # requests per month (free tier)
                timeout=30,
                retry_attempts=3
            ),
            'brave': APIConfig(
                name='Brave Search',
                base_url='https://api.search.brave.com/res/v1/web/search',
                api_key=os.getenv('BRAVE_API_KEY'),
                rate_limit=2000,  # requests per month (free tier)
                timeout=15,
                retry_attempts=3
            ),
            'firecrawl': APIConfig(
                name='Firecrawl',
                base_url='https://api.firecrawl.dev/v0',
                api_key=os.getenv('FIRECRAWL_API_KEY'),
                rate_limit=500,   # requests per month (free tier)
                timeout=60,
                retry_attempts=2
            ),
            'apify': APIConfig(
                name='Apify',
                base_url='https://api.apify.com/v2',
                api_key=os.getenv('APIFY_API_TOKEN'),
                rate_limit=1000,  # requests per month (free tier)
                timeout=300,      # 5 minutes for actor runs
                retry_attempts=2
            )
        }
        
        # Apify Actor Configurations
        self.apify_actors = {
            'facebook': ApifyActorConfig(
                platform='facebook',
                actor_id=os.getenv('APIFY_FACEBOOK_ACTOR', 'apify/facebook-posts-scraper'),
                default_input={
                    'startUrls': [],
                    'maxPosts': 50,
                    'scrollTimeout': 3000,
                    'maxComments': 20,
                    'maxReplies': 5
                },
                timeout=600,  # 10 minutes
                memory=2048   # MB
            ),
            'youtube': ApifyActorConfig(
                platform='youtube',
                actor_id=os.getenv('APIFY_YOUTUBE_ACTOR', 'streamers/youtube-scraper'),
                default_input={
                    'startUrls': [],
                    'maxVideos': 50,
                    'maxComments': 20,
                    'includeCaptions': False
                },
                timeout=900,  # 15 minutes
                memory=2048
            ),
            'instagram': ApifyActorConfig(
                platform='instagram',
                actor_id=os.getenv('APIFY_INSTAGRAM_ACTOR', 'apify/instagram-scraper'),
                default_input={
                    'directUrls': [],
                    'resultsLimit': 50,
                    'includeComments': True,
                    'maxComments': 20
                },
                timeout=600,
                memory=2048
            ),
            'twitter': ApifyActorConfig(
                platform='twitter',
                actor_id=os.getenv('APIFY_TWITTER_ACTOR', 'apidojo/tweet-scraper'),
                default_input={
                    'searchTerms': [],
                    'maxTweets': 50,
                    'includeReplies': True,
                    'maxReplies': 10
                },
                timeout=600,
                memory=1024
            ),
            'reddit': ApifyActorConfig(
                platform='reddit',
                actor_id=os.getenv('APIFY_REDDIT_ACTOR', 'trudax/reddit-scraper-lite'),
                default_input={
                    'startUrls': [],
                    'maxPosts': 50,
                    'maxComments': 20,
                    'sortBy': 'hot'
                },
                timeout=600,
                memory=1024
            )
        }
        
        # AIADMK-specific keywords for monitoring
        self.aiadmk_keywords = {
            'tamil': [
                'அ.இ.அ.த.மு.க', 'அதிமுக', 'எடப்பாடி பழனிசாமி', 'ஓ.பன்னீர்செல்வம்',
                'ஜெயலலிதா', 'புரட்சித்தலைவி', 'இரட்டை இலை', 'தமிழக அரசியல்'
            ],
            'english': [
                'AIADMK', 'All India Anna Dravida Munnetra Kazhagam', 
                'Edappadi Palaniswami', 'EPS', 'O Panneerselvam', 'OPS',
                'Jayalalithaa', 'Amma', 'Tamil Nadu politics'
            ]
        }
        
        # System Configuration
        self.system = {
            'max_concurrent_scrapers': int(os.getenv('MAX_WORKERS', '3')),
            'batch_size': int(os.getenv('BATCH_SIZE', '50')),
            'default_delay_min': int(os.getenv('DELAY_MIN', '2')),
            'default_delay_max': int(os.getenv('DELAY_MAX', '5')),
            'request_timeout': int(os.getenv('REQUEST_TIMEOUT', '30')),
            'retry_attempts': int(os.getenv('RETRY_ATTEMPTS', '3')),
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'headless_mode': os.getenv('HEADLESS_MODE', 'true').lower() == 'true',
            'user_agent_rotation': os.getenv('USER_AGENT_ROTATION', 'true').lower() == 'true'
        }
        
        # Monitoring frequencies (in seconds)
        self.monitoring = {
            'keyword_search_frequency': 1800,    # 30 minutes
            'channel_check_frequency': 3600,     # 1 hour
            'news_crawl_frequency': 1800,        # 30 minutes
            'system_health_check': 300,          # 5 minutes
            'database_cleanup_frequency': 86400   # 24 hours
        }
        
        # Data retention policies
        self.retention = {
            'raw_data_days': 90,      # Keep raw data for 90 days
            'processed_data_days': 365, # Keep processed data for 1 year
            'log_files_days': 30,     # Keep logs for 30 days
            'temp_files_hours': 24    # Clean temp files after 24 hours
        }
        
        logger.info("✅ Configuration loaded successfully")
    
    def validate_config(self) -> Dict[str, bool]:
        """Validate all configuration settings"""
        validation_results = {}
        
        # Validate database config
        db_required = ['supabase_url', 'supabase_key', 'db_host', 'db_user', 'db_password']
        validation_results['database'] = all(self.database.get(key) for key in db_required)
        
        # Validate API keys
        for api_name, api_config in self.apis.items():
            validation_results[f'api_{api_name}'] = bool(api_config.api_key)
        
        # Validate Apify actors
        validation_results['apify_token'] = bool(self.apis['apify'].api_key)
        validation_results['apify_actors'] = len(self.apify_actors) == 5
        
        # Log validation results
        for component, is_valid in validation_results.items():
            status = "✅" if is_valid else "❌"
            logger.info(f"{status} {component}: {'Valid' if is_valid else 'Invalid/Missing'}")
        
        return validation_results
    
    def get_api_config(self, api_name: str) -> Optional[APIConfig]:
        """Get API configuration by name"""
        return self.apis.get(api_name)
    
    def get_apify_actor_config(self, platform: str) -> Optional[ApifyActorConfig]:
        """Get Apify actor configuration by platform"""
        return self.apify_actors.get(platform)
    
    def get_keywords_for_platform(self, platform: str) -> List[str]:
        """Get AIADMK keywords optimized for specific platform"""
        all_keywords = self.aiadmk_keywords['tamil'] + self.aiadmk_keywords['english']
        
        # Platform-specific keyword optimization
        if platform == 'youtube':
            return all_keywords + ['AIADMK speech', 'Edappadi interview', 'அதிமுக பேச்சு']
        elif platform == 'facebook':
            return all_keywords + ['AIADMK page', 'அதிமுக அறிக்கை']
        elif platform == 'twitter':
            return all_keywords + ['#AIADMK', '#அதிமுக', '#EPS', '#OPS']
        elif platform == 'instagram':
            return all_keywords + ['#AIADMK', '#TamilNaduPolitics']
        elif platform == 'reddit':
            return all_keywords + ['TamilNadu politics', 'AIADMK discussion']
        elif platform == 'tamil_news':
            return all_keywords + ['அதிமுக செய்தி', 'AIADMK news', 'எடப்பாடி செய்தி']
        
        return all_keywords
    
    def get_search_frequency(self, search_type: str) -> int:
        """Get search frequency for different types"""
        return self.monitoring.get(f'{search_type}_frequency', 3600)
    
    def is_api_available(self, api_name: str) -> bool:
        """Check if API is configured and available"""
        config = self.get_api_config(api_name)
        return config is not None and bool(config.api_key)
    
    def get_enabled_platforms(self) -> List[str]:
        """Get list of platforms that have valid configurations"""
        enabled = []
        
        # Check content discovery APIs
        if self.is_api_available('serpapi'):
            enabled.extend(['google_search', 'youtube_search'])
        if self.is_api_available('brave'):
            enabled.append('web_search')
        if self.is_api_available('firecrawl'):
            enabled.append('news_crawl')
        
        # Check Apify actors
        if self.is_api_available('apify'):
            for platform in self.apify_actors.keys():
                enabled.append(platform)
        
        return enabled
    
    def get_cost_estimates(self) -> Dict[str, Dict[str, float]]:
        """Get estimated costs for API usage"""
        return {
            'serpapi': {'free_limit': 100, 'cost_per_1000': 5.0},
            'brave': {'free_limit': 2000, 'cost_per_1000': 0.5},
            'firecrawl': {'free_limit': 500, 'cost_per_1000': 3.0},
            'apify': {'free_limit': 10, 'cost_per_hour': 0.25}  # compute units
        }
    
    def export_config_summary(self) -> Dict[str, Any]:
        """Export configuration summary for debugging"""
        return {
            'database_configured': bool(self.database['supabase_url']),
            'api_keys_configured': {name: bool(config.api_key) for name, config in self.apis.items()},
            'total_keywords': len(self.aiadmk_keywords['tamil'] + self.aiadmk_keywords['english']),
            'enabled_platforms': self.get_enabled_platforms(),
            'monitoring_frequencies': self.monitoring,
            'system_limits': {
                'max_concurrent_scrapers': self.system['max_concurrent_scrapers'],
                'batch_size': self.system['batch_size']
            }
        }

# Global configuration instance
config_manager = ConfigManager()

def get_config() -> ConfigManager:
    """Get global configuration manager instance"""
    return config_manager

def validate_system_config() -> bool:
    """Validate system configuration and return overall status"""
    config = get_config()
    validation_results = config.validate_config()
    
    # Check critical components
    critical_components = ['database', 'api_serpapi', 'apify_token']
    critical_valid = all(validation_results.get(comp, False) for comp in critical_components)
    
    if not critical_valid:
        logger.error("❌ Critical configuration components are missing")
        return False
    
    optional_count = sum(1 for key, val in validation_results.items() 
                        if key not in critical_components and val)
    
    logger.info(f"✅ System configuration valid. {optional_count} optional components configured")
    return True

# Test function
def test_configuration():
    """Test configuration loading and validation"""
    try:
        config = get_config()
        
        print("🔧 AIADMK Political Intelligence System Configuration")
        print("=" * 60)
        
        # Validation
        is_valid = validate_system_config()
        print(f"\n📊 Overall System Status: {'✅ READY' if is_valid else '❌ NEEDS SETUP'}")
        
        # Summary
        summary = config.export_config_summary()
        print(f"\n📈 Configuration Summary:")
        print(f"   Enabled Platforms: {len(summary['enabled_platforms'])}")
        print(f"   Total Keywords: {summary['total_keywords']}")
        print(f"   Max Concurrent Scrapers: {summary['system_limits']['max_concurrent_scrapers']}")
        
        # Cost estimates
        costs = config.get_cost_estimates()
        print(f"\n💰 API Cost Estimates (monthly):")
        for api, cost_info in costs.items():
            if 'free_limit' in cost_info:
                print(f"   {api}: {cost_info['free_limit']} free, then ${cost_info.get('cost_per_1000', 0)}/1k")
        
        return is_valid
        
    except Exception as e:
        logger.error(f"❌ Configuration test failed: {e}")
        return False

if __name__ == "__main__":
    test_configuration()