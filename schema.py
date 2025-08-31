#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Database Schema
Complete schema for party-platform segregated tables and management tables
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List
from database import DatabaseManager, get_database

logger = logging.getLogger(__name__)

class SchemaManager:
    """Manages database schema creation and updates for AIADMK system"""
    
    def __init__(self):
        self.db_manager = None
    
    async def initialize(self):
        """Initialize schema manager"""
        self.db_manager = await get_database()
        return self.db_manager
    
    async def create_all_tables(self):
        """Create all required tables for AIADMK Political Intelligence System"""
        
        # AIADMK Platform Tables
        await self.create_admk_youtube_table()
        await self.create_admk_facebook_table()
        await self.create_admk_instagram_table()
        await self.create_admk_twitter_table()
        await self.create_admk_reddit_table()
        await self.create_admk_tamil_news_table()
        
        # Management Tables
        await self.create_url_queue_table()
        await self.create_monitored_channels_table()
        await self.create_search_keywords_table()
        
        # System Tables
        await self.create_scraping_sessions_table()
        await self.create_system_metrics_table()
        
        # Create indexes for performance
        await self.create_all_indexes()
        
        logger.info("✅ All database tables created successfully")
    
    async def create_admk_youtube_table(self):
        """Create AIADMK YouTube content table"""
        query = """
        CREATE TABLE IF NOT EXISTS admk_youtube (
            id BIGSERIAL PRIMARY KEY,
            video_id TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            description TEXT,
            channel_name TEXT,
            channel_id TEXT,
            author TEXT,
            published_date TIMESTAMPTZ,
            duration TEXT,
            views_count BIGINT DEFAULT 0,
            likes_count BIGINT DEFAULT 0,
            dislikes_count BIGINT DEFAULT 0,
            comments_count BIGINT DEFAULT 0,
            subscriber_count BIGINT DEFAULT 0,
            video_category TEXT,
            language TEXT DEFAULT 'tamil',
            thumbnail_url TEXT,
            tags TEXT[],
            keywords_matched TEXT[],
            sentiment_score DECIMAL(3,2),
            engagement_rate DECIMAL(5,4) DEFAULT 0,
            raw_apify_data JSONB,
            scrape_method TEXT DEFAULT 'apify',
            scrape_status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created admk_youtube table")
    
    async def create_admk_facebook_table(self):
        """Create AIADMK Facebook content table"""
        query = """
        CREATE TABLE IF NOT EXISTS admk_facebook (
            id BIGSERIAL PRIMARY KEY,
            post_id TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            content TEXT,
            page_name TEXT,
            page_id TEXT,
            author TEXT,
            published_date TIMESTAMPTZ,
            post_type TEXT,
            likes_count BIGINT DEFAULT 0,
            shares_count BIGINT DEFAULT 0,
            comments_count BIGINT DEFAULT 0,
            reactions_count BIGINT DEFAULT 0,
            reactions_breakdown JSONB,
            page_followers BIGINT DEFAULT 0,
            is_verified BOOLEAN DEFAULT FALSE,
            hashtags TEXT[],
            mentions TEXT[],
            keywords_matched TEXT[],
            sentiment_score DECIMAL(3,2),
            engagement_rate DECIMAL(5,4) DEFAULT 0,
            media_urls TEXT[],
            raw_apify_data JSONB,
            scrape_method TEXT DEFAULT 'apify',
            scrape_status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created admk_facebook table")
    
    async def create_admk_instagram_table(self):
        """Create AIADMK Instagram content table"""
        query = """
        CREATE TABLE IF NOT EXISTS admk_instagram (
            id BIGSERIAL PRIMARY KEY,
            post_id TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            caption TEXT,
            username TEXT,
            user_id TEXT,
            full_name TEXT,
            published_date TIMESTAMPTZ,
            post_type TEXT,
            likes_count BIGINT DEFAULT 0,
            comments_count BIGINT DEFAULT 0,
            user_followers BIGINT DEFAULT 0,
            user_following BIGINT DEFAULT 0,
            hashtags TEXT[],
            mentions TEXT[],
            location TEXT,
            media_urls TEXT[],
            keywords_matched TEXT[],
            sentiment_score DECIMAL(3,2),
            engagement_rate DECIMAL(5,4) DEFAULT 0,
            raw_apify_data JSONB,
            scrape_method TEXT DEFAULT 'apify',
            scrape_status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created admk_instagram table")
    
    async def create_admk_twitter_table(self):
        """Create AIADMK Twitter content table"""
        query = """
        CREATE TABLE IF NOT EXISTS admk_twitter (
            id BIGSERIAL PRIMARY KEY,
            tweet_id TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            text TEXT,
            user_handle TEXT,
            user_name TEXT,
            user_id TEXT,
            published_date TIMESTAMPTZ,
            likes_count BIGINT DEFAULT 0,
            retweets_count BIGINT DEFAULT 0,
            quotes_count BIGINT DEFAULT 0,
            replies_count BIGINT DEFAULT 0,
            views_count BIGINT DEFAULT 0,
            user_followers BIGINT DEFAULT 0,
            user_following BIGINT DEFAULT 0,
            is_retweet BOOLEAN DEFAULT FALSE,
            is_quote_tweet BOOLEAN DEFAULT FALSE,
            parent_tweet_id TEXT,
            hashtags TEXT[],
            mentions TEXT[],
            urls TEXT[],
            media_urls TEXT[],
            keywords_matched TEXT[],
            sentiment_score DECIMAL(3,2),
            engagement_rate DECIMAL(5,4) DEFAULT 0,
            raw_apify_data JSONB,
            scrape_method TEXT DEFAULT 'apify',
            scrape_status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created admk_twitter table")
    
    async def create_admk_reddit_table(self):
        """Create AIADMK Reddit content table"""
        query = """
        CREATE TABLE IF NOT EXISTS admk_reddit (
            id BIGSERIAL PRIMARY KEY,
            post_id TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            content TEXT,
            subreddit TEXT,
            author TEXT,
            published_date TIMESTAMPTZ,
            post_type TEXT,
            upvotes BIGINT DEFAULT 0,
            downvotes BIGINT DEFAULT 0,
            upvote_ratio DECIMAL(3,2) DEFAULT 0,
            comments_count BIGINT DEFAULT 0,
            awards_count INTEGER DEFAULT 0,
            gilded INTEGER DEFAULT 0,
            flair TEXT,
            is_nsfw BOOLEAN DEFAULT FALSE,
            is_spoiler BOOLEAN DEFAULT FALSE,
            keywords_matched TEXT[],
            sentiment_score DECIMAL(3,2),
            engagement_rate DECIMAL(5,4) DEFAULT 0,
            raw_apify_data JSONB,
            scrape_method TEXT DEFAULT 'apify',
            scrape_status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created admk_reddit table")
    
    async def create_admk_tamil_news_table(self):
        """Create AIADMK Tamil News content table"""
        query = """
        CREATE TABLE IF NOT EXISTS admk_tamil_news (
            id BIGSERIAL PRIMARY KEY,
            article_url TEXT UNIQUE NOT NULL,
            title TEXT,
            content TEXT,
            summary TEXT,
            author TEXT,
            news_source TEXT,
            published_date TIMESTAMPTZ,
            category TEXT,
            tags TEXT[],
            word_count INTEGER DEFAULT 0,
            reading_time INTEGER DEFAULT 0,
            related_articles TEXT[],
            keywords_matched TEXT[],
            sentiment_score DECIMAL(3,2),
            importance_score DECIMAL(3,2),
            raw_firecrawl_data JSONB,
            scrape_method TEXT DEFAULT 'firecrawl',
            scrape_status TEXT DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created admk_tamil_news table")
    
    async def create_url_queue_table(self):
        """Create URL processing queue table"""
        query = """
        CREATE TABLE IF NOT EXISTS url_queue (
            id BIGSERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            platform TEXT NOT NULL,
            source TEXT DEFAULT 'manual',
            priority INTEGER DEFAULT 1,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            metadata JSONB,
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            processed_at TIMESTAMPTZ
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created url_queue table")
    
    async def create_monitored_channels_table(self):
        """Create monitored channels table"""
        query = """
        CREATE TABLE IF NOT EXISTS monitored_channels (
            id BIGSERIAL PRIMARY KEY,
            platform TEXT NOT NULL,
            channel_id TEXT UNIQUE NOT NULL,
            channel_name TEXT,
            channel_url TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            priority INTEGER DEFAULT 1,
            check_frequency_seconds INTEGER DEFAULT 3600,
            last_checked TIMESTAMPTZ,
            posts_found_last_check INTEGER DEFAULT 0,
            total_posts_found INTEGER DEFAULT 0,
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created monitored_channels table")
    
    async def create_search_keywords_table(self):
        """Create search keywords monitoring table"""
        query = """
        CREATE TABLE IF NOT EXISTS search_keywords (
            id BIGSERIAL PRIMARY KEY,
            keyword TEXT NOT NULL,
            platform TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            search_frequency_seconds INTEGER DEFAULT 1800,
            last_searched TIMESTAMPTZ,
            results_found_last_search INTEGER DEFAULT 0,
            total_results_found INTEGER DEFAULT 0,
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(keyword, platform)
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created search_keywords table")
    
    async def create_scraping_sessions_table(self):
        """Create scraping sessions tracking table"""
        query = """
        CREATE TABLE IF NOT EXISTS scraping_sessions (
            id BIGSERIAL PRIMARY KEY,
            session_id TEXT UNIQUE NOT NULL,
            session_type TEXT NOT NULL,
            platform TEXT,
            status TEXT DEFAULT 'started',
            started_at TIMESTAMPTZ DEFAULT NOW(),
            completed_at TIMESTAMPTZ,
            duration_seconds INTEGER,
            items_processed INTEGER DEFAULT 0,
            items_successful INTEGER DEFAULT 0,
            items_failed INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created scraping_sessions table")
    
    async def create_system_metrics_table(self):
        """Create system performance metrics table"""
        query = """
        CREATE TABLE IF NOT EXISTS system_metrics (
            id BIGSERIAL PRIMARY KEY,
            metric_date DATE NOT NULL,
            total_posts_today INTEGER DEFAULT 0,
            total_comments_today INTEGER DEFAULT 0,
            api_calls_made INTEGER DEFAULT 0,
            api_costs_usd DECIMAL(10,4) DEFAULT 0,
            platform_breakdown JSONB,
            performance_metrics JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(metric_date)
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created system_metrics table")
    
    async def create_all_indexes(self):
        """Create performance indexes for all tables"""
        
        indexes = [
            # AIADMK platform tables indexes
            "CREATE INDEX IF NOT EXISTS idx_admk_youtube_published ON admk_youtube(published_date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_youtube_channel ON admk_youtube(channel_id);",
            "CREATE INDEX IF NOT EXISTS idx_admk_youtube_views ON admk_youtube(views_count DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_youtube_engagement ON admk_youtube(engagement_rate DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_youtube_keywords ON admk_youtube USING GIN(keywords_matched);",
            
            "CREATE INDEX IF NOT EXISTS idx_admk_facebook_published ON admk_facebook(published_date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_facebook_page ON admk_facebook(page_id);",
            "CREATE INDEX IF NOT EXISTS idx_admk_facebook_engagement ON admk_facebook(engagement_rate DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_facebook_keywords ON admk_facebook USING GIN(keywords_matched);",
            
            "CREATE INDEX IF NOT EXISTS idx_admk_instagram_published ON admk_instagram(published_date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_instagram_user ON admk_instagram(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_admk_instagram_engagement ON admk_instagram(engagement_rate DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_instagram_keywords ON admk_instagram USING GIN(keywords_matched);",
            
            "CREATE INDEX IF NOT EXISTS idx_admk_twitter_published ON admk_twitter(published_date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_twitter_user ON admk_twitter(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_admk_twitter_engagement ON admk_twitter(engagement_rate DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_twitter_keywords ON admk_twitter USING GIN(keywords_matched);",
            
            "CREATE INDEX IF NOT EXISTS idx_admk_reddit_published ON admk_reddit(published_date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_reddit_subreddit ON admk_reddit(subreddit);",
            "CREATE INDEX IF NOT EXISTS idx_admk_reddit_upvotes ON admk_reddit(upvotes DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_reddit_keywords ON admk_reddit USING GIN(keywords_matched);",
            
            "CREATE INDEX IF NOT EXISTS idx_admk_tamil_news_published ON admk_tamil_news(published_date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_admk_tamil_news_source ON admk_tamil_news(news_source);",
            "CREATE INDEX IF NOT EXISTS idx_admk_tamil_news_keywords ON admk_tamil_news USING GIN(keywords_matched);",
            
            # Management tables indexes
            "CREATE INDEX IF NOT EXISTS idx_url_queue_status ON url_queue(status, priority DESC);",
            "CREATE INDEX IF NOT EXISTS idx_url_queue_platform ON url_queue(platform, status);",
            "CREATE INDEX IF NOT EXISTS idx_url_queue_created ON url_queue(created_at);",
            
            "CREATE INDEX IF NOT EXISTS idx_monitored_channels_platform ON monitored_channels(platform);",
            "CREATE INDEX IF NOT EXISTS idx_monitored_channels_active ON monitored_channels(is_active, last_checked);",
            
            "CREATE INDEX IF NOT EXISTS idx_search_keywords_platform ON search_keywords(platform);",
            "CREATE INDEX IF NOT EXISTS idx_search_keywords_active ON search_keywords(is_active, last_searched);",
            
            "CREATE INDEX IF NOT EXISTS idx_scraping_sessions_type ON scraping_sessions(session_type, started_at);",
            "CREATE INDEX IF NOT EXISTS idx_scraping_sessions_platform ON scraping_sessions(platform, started_at);",
            
            "CREATE INDEX IF NOT EXISTS idx_system_metrics_date ON system_metrics(metric_date DESC);",
        ]
        
        for index in indexes:
            try:
                await self.db_manager.execute_query(index)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
        
        logger.info("✅ All indexes created successfully")
    
    async def seed_initial_data(self):
        """Seed initial data for AIADMK monitoring"""
        
        # Add AIADMK keywords for monitoring
        aiadmk_keywords = [
            'அ.இ.அ.த.மு.க', 'அதிமுக', 'AIADMK', 
            'எடப்பாடி பழனிசாமி', 'Edappadi Palaniswami', 'EPS',
            'ஓ.பன்னீர்செல்வம்', 'O Panneerselvam', 'OPS',
            'ஜெயலலிதா', 'Jayalalithaa', 'புரட்சித்தலைவி'
        ]
        
        platforms = ['youtube', 'facebook', 'instagram', 'twitter', 'reddit', 'tamil_news']
        
        for keyword in aiadmk_keywords:
            for platform in platforms:
                try:
                    await self.db_manager.add_search_keyword(
                        keyword=keyword,
                        platform=platform,
                        search_frequency=1800,  # 30 minutes
                        metadata={'category': 'party_name', 'language': 'tamil' if any(c > '\u0b80' for c in keyword) else 'english'}
                    )
                except Exception as e:
                    logger.warning(f"Keyword insertion warning: {e}")
        
        # Add known AIADMK channels for monitoring
        channels = [
            {'platform': 'youtube', 'channel_id': 'UCExample1', 'channel_name': 'AIADMK Official'},
            {'platform': 'facebook', 'channel_id': 'aiadmk.official', 'channel_name': 'AIADMK Official'},
            {'platform': 'instagram', 'channel_id': 'aiadmk_official', 'channel_name': 'AIADMK Official'},
            {'platform': 'twitter', 'channel_id': 'AIADMKOfficial', 'channel_name': 'AIADMK Official'},
        ]
        
        for channel in channels:
            try:
                await self.db_manager.add_monitored_channel(
                    platform=channel['platform'],
                    channel_id=channel['channel_id'],
                    channel_name=channel['channel_name'],
                    check_frequency=3600,  # 1 hour
                    metadata={'verified': True, 'priority': 'high'}
                )
            except Exception as e:
                logger.warning(f"Channel insertion warning: {e}")
        
        logger.info("✅ Initial data seeded successfully")
    
    async def get_schema_stats(self):
        """Get database schema statistics"""
        tables = [
            'admk_youtube', 'admk_facebook', 'admk_instagram', 
            'admk_twitter', 'admk_reddit', 'admk_tamil_news',
            'url_queue', 'monitored_channels', 'search_keywords',
            'scraping_sessions', 'system_metrics'
        ]
        
        stats = {}
        for table in tables:
            try:
                result = await self.db_manager.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                stats[table] = result[0]['count'] if result else 0
            except Exception as e:
                stats[table] = f"Error: {e}"
        
        return stats

async def main():
    """Main function to create database schema"""
    schema_manager = SchemaManager()
    
    try:
        await schema_manager.initialize()
        
        print("🚀 Creating AIADMK Political Intelligence System Database Schema...")
        
        # Create all tables
        await schema_manager.create_all_tables()
        
        # Seed initial data
        await schema_manager.seed_initial_data()
        
        # Get statistics
        stats = await schema_manager.get_schema_stats()
        
        print("✅ Database schema created successfully!")
        print(f"📊 Table Statistics: {stats}")
        
    except Exception as e:
        logger.error(f"❌ Schema creation failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())