#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Database Layer
Production-ready Supabase PostgreSQL integration with party-platform segregated tables
"""

import os
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import json

# Database imports
try:
    import asyncpg
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    logging.error("Supabase libraries not available. Install: pip install supabase asyncpg")

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Production database manager for AIADMK Political Intelligence System"""
    
    def __init__(self):
        if not SUPABASE_AVAILABLE:
            raise ImportError("Supabase libraries required for database operations")
        
        # Supabase configuration
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        self.db_host = os.getenv('SUPABASE_DB_HOST')
        self.db_port = int(os.getenv('SUPABASE_DB_PORT', '5432'))
        self.db_name = os.getenv('SUPABASE_DB_NAME', 'postgres')
        self.db_user = os.getenv('SUPABASE_DB_USER')
        self.db_password = os.getenv('SUPABASE_DB_PASSWORD')
        
        if not all([self.supabase_url, self.supabase_key, self.db_host, self.db_user, self.db_password]):
            raise ValueError("Missing required Supabase environment variables")
        
        # Initialize clients
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        self.db_pool = None
        
        logger.info("Database manager initialized")
    
    async def connect(self):
        """Establish database connection pool"""
        try:
            self.db_pool = await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                min_size=2,
                max_size=10,
                command_timeout=30,
                statement_cache_size=0,  # Disable prepared statement cache for pgbouncer compatibility
                server_settings={
                    'search_path': 'public'
                }
            )
            logger.info("✅ Database connection pool created")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    async def close(self):
        """Close database connections"""
        if self.db_pool:
            await self.db_pool.close()
            logger.info("✅ Database connections closed")
    
    async def execute_query(self, query: str, params: tuple = None):
        """Execute database query with connection pool"""
        if not self.db_pool:
            await self.connect()
        
        async with self.db_pool.acquire() as conn:
            try:
                if params:
                    result = await conn.fetch(query, *params)
                else:
                    result = await conn.fetch(query)
                return result
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                raise
    
    async def insert_data(self, table: str, data: Dict[str, Any]):
        """Insert data into specified table"""
        if not self.db_pool:
            await self.connect()
        
        # Prepare columns and values
        columns = list(data.keys())
        placeholders = [f"${i+1}" for i in range(len(columns))]
        values = list(data.values())
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING id
        """
        
        async with self.db_pool.acquire() as conn:
            try:
                result = await conn.fetchrow(query, *values)
                return result['id'] if result else None
            except Exception as e:
                logger.error(f"Insert failed for table {table}: {e}")
                raise
    
    async def upsert_data(self, table: str, data: Dict[str, Any], conflict_column: str = 'url'):
        """Upsert data into specified table (insert or update on conflict)"""
        if not self.db_pool:
            await self.connect()
        
        columns = list(data.keys())
        placeholders = [f"${i+1}" for i in range(len(columns))]
        values = list(data.values())
        
        # Update clause for conflict resolution
        update_columns = [f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_column]
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({conflict_column}) DO UPDATE SET
            {', '.join(update_columns)},
            updated_at = NOW()
            RETURNING id
        """
        
        async with self.db_pool.acquire() as conn:
            try:
                result = await conn.fetchrow(query, *values)
                return result['id'] if result else None
            except Exception as e:
                logger.error(f"Upsert failed for table {table}: {e}")
                raise
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics across all AIADMK tables"""
        stats = {
            'total_posts': 0,
            'total_comments': 0,
            'platforms': {},
            'last_updated': None
        }
        
        platforms = ['youtube', 'facebook', 'instagram', 'twitter', 'reddit', 'tamil_news']
        
        for platform in platforms:
            table_name = f"admk_{platform}"
            try:
                # Get post count
                posts_query = f"SELECT COUNT(*) as count FROM {table_name}"
                posts_result = await self.execute_query(posts_query)
                post_count = posts_result[0]['count'] if posts_result else 0
                
                # Get latest post date
                latest_query = f"SELECT MAX(created_at) as latest FROM {table_name}"
                latest_result = await self.execute_query(latest_query)
                latest_date = latest_result[0]['latest'] if latest_result and latest_result[0]['latest'] else None
                
                stats['platforms'][platform] = {
                    'posts': post_count,
                    'latest': latest_date.isoformat() if latest_date else None
                }
                stats['total_posts'] += post_count
                
                if latest_date and (not stats['last_updated'] or latest_date > stats['last_updated']):
                    stats['last_updated'] = latest_date.isoformat()
                
            except Exception as e:
                logger.warning(f"Failed to get stats for {platform}: {e}")
                stats['platforms'][platform] = {'posts': 0, 'latest': None}
        
        return stats
    
    # Platform-specific insert methods
    async def insert_youtube_data(self, data: Dict[str, Any]):
        """Insert YouTube data into admk_youtube table"""
        return await self.upsert_data('admk_youtube', data, 'video_id')
    
    async def insert_facebook_data(self, data: Dict[str, Any]):
        """Insert Facebook data into admk_facebook table"""
        return await self.upsert_data('admk_facebook', data, 'post_id')
    
    async def insert_instagram_data(self, data: Dict[str, Any]):
        """Insert Instagram data into admk_instagram table"""
        return await self.upsert_data('admk_instagram', data, 'post_id')
    
    async def insert_twitter_data(self, data: Dict[str, Any]):
        """Insert Twitter data into admk_twitter table"""
        return await self.upsert_data('admk_twitter', data, 'tweet_id')
    
    async def insert_reddit_data(self, data: Dict[str, Any]):
        """Insert Reddit data into admk_reddit table"""
        return await self.upsert_data('admk_reddit', data, 'post_id')
    
    async def insert_tamil_news_data(self, data: Dict[str, Any]):
        """Insert Tamil News data into admk_tamil_news table"""
        return await self.upsert_data('admk_tamil_news', data, 'article_url')
    
    # Management table operations
    async def add_to_url_queue(self, url: str, platform: str, priority: int = 1, metadata: Dict = None):
        """Add URL to processing queue"""
        data = {
            'url': url,
            'platform': platform,
            'priority': priority,
            'status': 'pending',
            'metadata': json.dumps(metadata or {}),
            'created_at': datetime.now(),
            'attempts': 0
        }
        return await self.insert_data('url_queue', data)
    
    async def get_pending_urls(self, platform: str = None, limit: int = 100):
        """Get pending URLs from queue"""
        query = """
            SELECT * FROM url_queue 
            WHERE status = 'pending' AND attempts < 3
        """
        params = []
        
        if platform:
            query += " AND platform = $1"
            params.append(platform)
        
        query += " ORDER BY priority DESC, created_at ASC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        return await self.execute_query(query, tuple(params))
    
    async def update_url_status(self, url_id: int, status: str, error_message: str = None):
        """Update URL processing status"""
        query = """
            UPDATE url_queue 
            SET status = $1, error_message = $2, updated_at = NOW(), attempts = attempts + 1
            WHERE id = $3
        """
        await self.execute_query(query, (status, error_message, url_id))
    
    async def add_monitored_channel(self, platform: str, channel_id: str, channel_name: str, 
                                  check_frequency: int = 3600, metadata: Dict = None):
        """Add channel to monitoring list"""
        data = {
            'platform': platform,
            'channel_id': channel_id,
            'channel_name': channel_name,
            'check_frequency_seconds': check_frequency,
            'is_active': True,
            'metadata': json.dumps(metadata or {}),
            'created_at': datetime.now(),
            'last_checked': None
        }
        return await self.upsert_data('monitored_channels', data, 'channel_id')
    
    async def get_channels_to_check(self, platform: str = None):
        """Get channels that need to be checked"""
        query = """
            SELECT * FROM monitored_channels 
            WHERE is_active = true 
            AND (last_checked IS NULL OR 
                 last_checked < NOW() - INTERVAL '1 second' * check_frequency_seconds)
        """
        params = []
        
        if platform:
            query += " AND platform = $1"
            params.append(platform)
        
        query += " ORDER BY priority DESC, last_checked ASC NULLS FIRST"
        
        return await self.execute_query(query, tuple(params))
    
    async def update_channel_check(self, channel_id: str, posts_found: int = 0):
        """Update channel last check time"""
        query = """
            UPDATE monitored_channels 
            SET last_checked = NOW(), posts_found_last_check = $1
            WHERE channel_id = $2
        """
        await self.execute_query(query, (posts_found, channel_id))
    
    async def add_search_keyword(self, keyword: str, platform: str, is_active: bool = True, 
                               search_frequency: int = 1800, metadata: Dict = None):
        """Add keyword to search monitoring"""
        data = {
            'keyword': keyword,
            'platform': platform,
            'is_active': is_active,
            'search_frequency_seconds': search_frequency,
            'metadata': json.dumps(metadata or {}),
            'created_at': datetime.now(),
            'last_searched': None,
            'results_found_last_search': 0
        }
        return await self.upsert_data('search_keywords', data, 'keyword')
    
    async def get_keywords_to_search(self, platform: str = None):
        """Get keywords that need to be searched"""
        query = """
            SELECT * FROM search_keywords 
            WHERE is_active = true 
            AND (last_searched IS NULL OR 
                 last_searched < NOW() - INTERVAL '1 second' * search_frequency_seconds)
        """
        params = []
        
        if platform:
            query += " AND platform = $1"
            params.append(platform)
        
        query += " ORDER BY last_searched ASC NULLS FIRST"
        
        return await self.execute_query(query, tuple(params))
    
    async def update_keyword_search(self, keyword: str, results_found: int):
        """Update keyword search results"""
        query = """
            UPDATE search_keywords 
            SET last_searched = NOW(), results_found_last_search = $1
            WHERE keyword = $2
        """
        await self.execute_query(query, (results_found, keyword))

# Global database manager instance
db_manager = None

async def get_database():
    """Get global database manager instance"""
    global db_manager
    if not db_manager:
        db_manager = DatabaseManager()
        await db_manager.connect()
    return db_manager

async def close_database():
    """Close global database manager"""
    global db_manager
    if db_manager:
        await db_manager.close()
        db_manager = None

# Test function
async def test_database_connection():
    """Test database connection and basic operations"""
    try:
        db = await get_database()
        
        # Test basic connectivity
        result = await db.execute_query("SELECT 1 as test")
        logger.info(f"✅ Database connection test passed: {result}")
        
        # Test statistics
        stats = await db.get_statistics()
        logger.info(f"📊 Database statistics: {stats}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        return False
    finally:
        await close_database()

if __name__ == "__main__":
    asyncio.run(test_database_connection())