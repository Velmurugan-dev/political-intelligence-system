#!/usr/bin/env python3
"""
Multi-Competitor Political Intelligence System - Normalized Database Schema
Complete schema for 2-stage ETL pipeline with deduplication and analytics support
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List
from database import get_database

logger = logging.getLogger(__name__)

class NewSchemaManager:
    """Manages normalized multi-competitor database schema"""
    
    def __init__(self):
        self.db_manager = None
    
    async def initialize(self):
        """Initialize schema manager"""
        self.db_manager = await get_database()
        return self.db_manager
    
    async def create_normalized_schema(self):
        """Create complete normalized schema for multi-competitor system"""
        
        logger.info("🚀 Creating Normalized Multi-Competitor Schema...")
        
        # Core system tables
        await self.create_competitors_table()
        await self.create_platforms_table() 
        await self.create_keywords_table()
        await self.create_sources_table()
        
        # Pipeline tables (2-stage ETL)
        await self.create_stage_results_table()
        await self.create_final_results_table()
        await self.create_manual_queue_table()
        await self.create_monitoring_schedule_table()
        
        # Queue and processing
        await self.create_scraping_jobs_table()
        await self.create_job_status_table()
        
        # Analytics and reporting
        await self.create_analytics_summary_table()
        await self.create_engagement_metrics_table()
        
        # System management
        await self.create_system_logs_table()
        await self.create_api_usage_table()
        
        # Create indexes and constraints
        await self.create_indexes()
        await self.create_constraints()
        
        logger.info("✅ Normalized schema created successfully")
    
    async def create_competitors_table(self):
        """Political competitors/parties table"""
        query = """
        CREATE TABLE IF NOT EXISTS competitors (
            competitor_id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL,
            display_name VARCHAR(200) NOT NULL,
            short_code VARCHAR(10) UNIQUE NOT NULL,
            description TEXT,
            party_color VARCHAR(7), -- Hex color code
            founded_year INTEGER,
            headquarters VARCHAR(200),
            official_website VARCHAR(500),
            is_active BOOLEAN DEFAULT TRUE,
            priority_level INTEGER DEFAULT 1, -- 1=high, 2=medium, 3=low
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created competitors table")
    
    async def create_platforms_table(self):
        """Social media and news platforms table"""
        query = """
        CREATE TABLE IF NOT EXISTS platforms (
            platform_id SERIAL PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL,
            display_name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL, -- social_media, news, video, etc.
            base_url VARCHAR(200),
            api_available BOOLEAN DEFAULT FALSE,
            scraping_difficulty INTEGER DEFAULT 1, -- 1=easy, 5=very hard
            rate_limit_per_hour INTEGER DEFAULT 100,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created platforms table")
    
    async def create_keywords_table(self):
        """Keywords per competitor per platform"""
        query = """
        CREATE TABLE IF NOT EXISTS keywords (
            keyword_id SERIAL PRIMARY KEY,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            keyword VARCHAR(200) NOT NULL,
            language VARCHAR(10) DEFAULT 'ta', -- ta=Tamil, en=English
            keyword_type VARCHAR(50) DEFAULT 'general', -- party_name, leader_name, slogan, etc.
            search_frequency_hours INTEGER DEFAULT 4, -- How often to search
            last_searched TIMESTAMPTZ,
            total_results_found INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(competitor_id, platform_id, keyword)
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created keywords table")
    
    async def create_sources_table(self):
        """Manual monitoring sources (channels, accounts, websites)"""
        query = """
        CREATE TABLE IF NOT EXISTS sources (
            source_id SERIAL PRIMARY KEY,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            name VARCHAR(200) NOT NULL,
            url VARCHAR(1000) NOT NULL,
            source_type VARCHAR(50) NOT NULL, -- channel, profile, website, page
            identifier VARCHAR(200), -- channel_id, username, etc.
            followers_count BIGINT DEFAULT 0,
            verification_status VARCHAR(20) DEFAULT 'unknown', -- verified, unverified, unknown
            monitoring_frequency_hours INTEGER DEFAULT 6,
            last_monitored TIMESTAMPTZ,
            total_content_found INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(competitor_id, platform_id, url)
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created sources table")
    
    async def create_stage_results_table(self):
        """Stage 1: Raw discovery results with deduplication"""
        query = """
        CREATE TABLE IF NOT EXISTS stage_results (
            stage_id BIGSERIAL PRIMARY KEY,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            keyword_id INTEGER REFERENCES keywords(keyword_id) ON DELETE SET NULL,
            source_id INTEGER REFERENCES sources(source_id) ON DELETE SET NULL,
            url VARCHAR(2000) NOT NULL,
            url_hash VARCHAR(64) NOT NULL, -- SHA256 hash for fast dedup
            title TEXT,
            snippet TEXT,
            author VARCHAR(200),
            author_id VARCHAR(200),
            published_at TIMESTAMPTZ,
            discovery_method VARCHAR(50) NOT NULL, -- serpapi, brave, firecrawl, manual, monitoring
            content_type VARCHAR(50), -- post, video, article, tweet, etc.
            language VARCHAR(10),
            keywords_matched TEXT[],
            status VARCHAR(20) DEFAULT 'pending', -- pending, processing, scraped, failed, duplicate
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            priority INTEGER DEFAULT 1,
            raw_data JSONB, -- Original discovery data
            inserted_at TIMESTAMPTZ DEFAULT NOW(),
            processed_at TIMESTAMPTZ,
            UNIQUE(competitor_id, platform_id, url_hash)
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created stage_results table")
    
    async def create_final_results_table(self):
        """Stage 2: Enriched engagement data with metrics"""
        query = """
        CREATE TABLE IF NOT EXISTS final_results (
            result_id BIGSERIAL PRIMARY KEY,
            stage_id BIGINT REFERENCES stage_results(stage_id) ON DELETE CASCADE,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            url VARCHAR(2000) NOT NULL,
            content_id VARCHAR(200), -- Platform-specific ID
            title TEXT,
            content TEXT,
            author VARCHAR(200),
            author_id VARCHAR(200),
            author_followers BIGINT DEFAULT 0,
            published_at TIMESTAMPTZ,
            
            -- Engagement Metrics (from Apify results)
            views_count BIGINT DEFAULT 0,
            likes_count BIGINT DEFAULT 0,
            shares_count BIGINT DEFAULT 0,
            comments_count BIGINT DEFAULT 0,
            reactions_count BIGINT DEFAULT 0,
            
            -- Platform-specific metrics
            retweets_count BIGINT DEFAULT 0, -- Twitter
            quotes_count BIGINT DEFAULT 0, -- Twitter  
            upvotes BIGINT DEFAULT 0, -- Reddit
            downvotes BIGINT DEFAULT 0, -- Reddit
            upvote_ratio DECIMAL(3,2) DEFAULT 0, -- Reddit
            subscriber_count BIGINT DEFAULT 0, -- YouTube
            
            -- Content analysis
            word_count INTEGER DEFAULT 0,
            reading_time INTEGER DEFAULT 0,
            language VARCHAR(10),
            content_type VARCHAR(50),
            hashtags TEXT[],
            mentions TEXT[],
            media_urls TEXT[],
            
            -- Calculated metrics
            engagement_rate DECIMAL(8,4) DEFAULT 0,
            viral_score DECIMAL(8,4) DEFAULT 0,
            sentiment_score DECIMAL(3,2), -- -1 to +1
            importance_score DECIMAL(8,4) DEFAULT 0,
            
            -- Comments and raw data
            top_comments JSONB,
            comments_json JSONB, -- Full comment threads
            raw_apify_data JSONB, -- Complete Apify response
            
            scraping_method VARCHAR(50), -- apify, playwright, api
            scraped_at TIMESTAMPTZ DEFAULT NOW(),
            
            -- Multiple snapshots support
            is_latest BOOLEAN DEFAULT TRUE,
            snapshot_number INTEGER DEFAULT 1
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created final_results table")
    
    async def create_manual_queue_table(self):
        """Manual URL submission queue"""
        query = """
        CREATE TABLE IF NOT EXISTS manual_queue (
            queue_id BIGSERIAL PRIMARY KEY,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            url VARCHAR(2000) NOT NULL,
            url_hash VARCHAR(64) NOT NULL,
            submitted_by VARCHAR(100), -- User/admin who submitted
            priority INTEGER DEFAULT 1,
            notes TEXT,
            status VARCHAR(20) DEFAULT 'pending', -- pending, processed, duplicate, invalid
            processed_at TIMESTAMPTZ,
            stage_result_id BIGINT REFERENCES stage_results(stage_id) ON DELETE SET NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created manual_queue table")
    
    async def create_monitoring_schedule_table(self):
        """Scheduled monitoring jobs"""
        query = """
        CREATE TABLE IF NOT EXISTS monitoring_schedule (
            schedule_id SERIAL PRIMARY KEY,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            source_id INTEGER REFERENCES sources(source_id) ON DELETE CASCADE,
            schedule_type VARCHAR(20) NOT NULL, -- hourly, daily, weekly, custom
            frequency_hours INTEGER NOT NULL,
            next_run TIMESTAMPTZ NOT NULL,
            last_run TIMESTAMPTZ,
            is_active BOOLEAN DEFAULT TRUE,
            max_items_per_run INTEGER DEFAULT 50,
            run_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created monitoring_schedule table")
    
    async def create_scraping_jobs_table(self):
        """Celery job tracking"""
        query = """
        CREATE TABLE IF NOT EXISTS scraping_jobs (
            job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_type VARCHAR(50) NOT NULL, -- discovery, engagement, monitoring
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            status VARCHAR(20) DEFAULT 'pending', -- pending, running, completed, failed, retrying
            priority INTEGER DEFAULT 1,
            celery_task_id VARCHAR(100),
            progress INTEGER DEFAULT 0, -- 0-100%
            items_total INTEGER DEFAULT 0,
            items_processed INTEGER DEFAULT 0,
            items_successful INTEGER DEFAULT 0,
            items_failed INTEGER DEFAULT 0,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            estimated_duration INTEGER, -- seconds
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created scraping_jobs table")
    
    async def create_job_status_table(self):
        """Real-time job status updates"""
        query = """
        CREATE TABLE IF NOT EXISTS job_status (
            status_id BIGSERIAL PRIMARY KEY,
            job_id UUID REFERENCES scraping_jobs(job_id) ON DELETE CASCADE,
            status VARCHAR(20) NOT NULL,
            message TEXT,
            progress INTEGER DEFAULT 0,
            items_processed INTEGER DEFAULT 0,
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created job_status table")
    
    async def create_analytics_summary_table(self):
        """Pre-computed analytics for dashboards"""
        query = """
        CREATE TABLE IF NOT EXISTS analytics_summary (
            summary_id BIGSERIAL PRIMARY KEY,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            date DATE NOT NULL,
            period_type VARCHAR(20) DEFAULT 'daily', -- daily, weekly, monthly
            
            -- Volume metrics
            total_mentions INTEGER DEFAULT 0,
            total_content INTEGER DEFAULT 0,
            total_engagement BIGINT DEFAULT 0,
            
            -- Engagement breakdown
            total_views BIGINT DEFAULT 0,
            total_likes BIGINT DEFAULT 0,
            total_shares BIGINT DEFAULT 0,
            total_comments BIGINT DEFAULT 0,
            
            -- Calculated metrics
            avg_engagement_rate DECIMAL(8,4) DEFAULT 0,
            viral_content_count INTEGER DEFAULT 0,
            avg_sentiment DECIMAL(3,2) DEFAULT 0,
            reach_estimate BIGINT DEFAULT 0,
            
            -- Top content
            top_content_ids BIGINT[],
            trending_hashtags TEXT[],
            
            -- Comparative metrics
            rank_by_mentions INTEGER,
            rank_by_engagement INTEGER,
            share_of_voice DECIMAL(5,2) DEFAULT 0, -- Percentage
            
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(competitor_id, platform_id, date, period_type)
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created analytics_summary table")
    
    async def create_engagement_metrics_table(self):
        """Time-series engagement tracking"""
        query = """
        CREATE TABLE IF NOT EXISTS engagement_metrics (
            metric_id BIGSERIAL PRIMARY KEY,
            result_id BIGINT REFERENCES final_results(result_id) ON DELETE CASCADE,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE CASCADE,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE CASCADE,
            
            -- Engagement snapshot
            views_count BIGINT DEFAULT 0,
            likes_count BIGINT DEFAULT 0,
            shares_count BIGINT DEFAULT 0,
            comments_count BIGINT DEFAULT 0,
            
            -- Growth calculations
            views_growth INTEGER DEFAULT 0,
            likes_growth INTEGER DEFAULT 0,
            shares_growth INTEGER DEFAULT 0,
            comments_growth INTEGER DEFAULT 0,
            
            -- Rates and percentages
            engagement_rate DECIMAL(8,4) DEFAULT 0,
            growth_rate DECIMAL(8,4) DEFAULT 0,
            virality_coefficient DECIMAL(8,4) DEFAULT 0,
            
            recorded_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created engagement_metrics table")
    
    async def create_system_logs_table(self):
        """System operation logs"""
        query = """
        CREATE TABLE IF NOT EXISTS system_logs (
            log_id BIGSERIAL PRIMARY KEY,
            log_level VARCHAR(10) NOT NULL, -- INFO, WARN, ERROR
            component VARCHAR(50) NOT NULL, -- discovery, engagement, api, etc.
            operation VARCHAR(100) NOT NULL,
            message TEXT NOT NULL,
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE SET NULL,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE SET NULL,
            job_id UUID REFERENCES scraping_jobs(job_id) ON DELETE SET NULL,
            execution_time INTEGER, -- milliseconds
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created system_logs table")
    
    async def create_api_usage_table(self):
        """API usage tracking and cost monitoring"""
        query = """
        CREATE TABLE IF NOT EXISTS api_usage (
            usage_id BIGSERIAL PRIMARY KEY,
            api_name VARCHAR(50) NOT NULL, -- serpapi, brave, firecrawl, apify
            operation VARCHAR(50) NOT NULL, -- search, scrape, crawl
            competitor_id INTEGER REFERENCES competitors(competitor_id) ON DELETE SET NULL,
            platform_id INTEGER REFERENCES platforms(platform_id) ON DELETE SET NULL,
            
            -- Usage metrics
            requests_made INTEGER DEFAULT 1,
            requests_successful INTEGER DEFAULT 0,
            requests_failed INTEGER DEFAULT 0,
            data_transferred_kb INTEGER DEFAULT 0,
            
            -- Cost tracking
            cost_usd DECIMAL(10,4) DEFAULT 0,
            credits_used INTEGER DEFAULT 0,
            
            -- Performance
            response_time_ms INTEGER,
            rate_limit_hit BOOLEAN DEFAULT FALSE,
            
            -- Metadata
            request_metadata JSONB,
            response_metadata JSONB,
            error_details TEXT,
            
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        await self.db_manager.execute_query(query)
        logger.info("✅ Created api_usage table")
    
    async def create_indexes(self):
        """Create performance indexes"""
        indexes = [
            # Core table indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_competitors_active ON competitors(is_active, priority_level);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_platforms_active ON platforms(is_active, category);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_keywords_competitor_platform ON keywords(competitor_id, platform_id, is_active);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sources_competitor_platform ON sources(competitor_id, platform_id, is_active);",
            
            # Stage results indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stage_results_status ON stage_results(status, priority DESC);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stage_results_competitor_platform ON stage_results(competitor_id, platform_id);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stage_results_url_hash ON stage_results(url_hash);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stage_results_inserted ON stage_results(inserted_at DESC);",
            
            # Final results indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_competitor_platform ON final_results(competitor_id, platform_id);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_published ON final_results(published_at DESC);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_engagement ON final_results(engagement_rate DESC);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_viral ON final_results(viral_score DESC);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_latest ON final_results(is_latest, scraped_at DESC);",
            
            # Queue and job indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_manual_queue_status ON manual_queue(status, priority DESC);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scraping_jobs_status ON scraping_jobs(status, priority DESC);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scraping_jobs_celery ON scraping_jobs(celery_task_id);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_status_job_time ON job_status(job_id, created_at DESC);",
            
            # Analytics indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_analytics_summary_date ON analytics_summary(date DESC, competitor_id, platform_id);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_engagement_metrics_time ON engagement_metrics(recorded_at DESC, result_id);",
            
            # System indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_system_logs_component_time ON system_logs(component, created_at DESC);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_usage_api_time ON api_usage(api_name, created_at DESC);",
            
            # Full-text search indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stage_results_fts ON stage_results USING gin(to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(snippet, '')));",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_fts ON final_results USING gin(to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(content, '')));",
            
            # JSONB indexes
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_hashtags ON final_results USING gin(hashtags);",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_final_results_mentions ON final_results USING gin(mentions);"
        ]
        
        for index in indexes:
            try:
                await self.db_manager.execute_query(index)
                logger.debug(f"Created index: {index[:50]}...")
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
        
        logger.info("✅ All indexes created successfully")
    
    async def create_constraints(self):
        """Create additional constraints and triggers"""
        constraints = [
            # URL hash generation trigger
            """
            CREATE OR REPLACE FUNCTION generate_url_hash()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.url_hash = encode(sha256(NEW.url::bytea), 'hex');
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            
            # Trigger for stage_results
            """
            DROP TRIGGER IF EXISTS trigger_stage_results_url_hash ON stage_results;
            CREATE TRIGGER trigger_stage_results_url_hash
                BEFORE INSERT OR UPDATE ON stage_results
                FOR EACH ROW EXECUTE FUNCTION generate_url_hash();
            """,
            
            # Trigger for manual_queue
            """
            DROP TRIGGER IF EXISTS trigger_manual_queue_url_hash ON manual_queue;
            CREATE TRIGGER trigger_manual_queue_url_hash
                BEFORE INSERT OR UPDATE ON manual_queue
                FOR EACH ROW EXECUTE FUNCTION generate_url_hash();
            """,
            
            # Updated_at triggers
            """
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            
            # Apply updated_at to relevant tables
            """
            DROP TRIGGER IF EXISTS trigger_competitors_updated_at ON competitors;
            CREATE TRIGGER trigger_competitors_updated_at
                BEFORE UPDATE ON competitors
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """,
        ]
        
        for constraint in constraints:
            try:
                await self.db_manager.execute_query(constraint)
            except Exception as e:
                logger.warning(f"Constraint creation warning: {e}")
        
        logger.info("✅ All constraints and triggers created successfully")
    
    async def seed_initial_data(self):
        """Seed initial data for the multi-competitor system"""
        
        # Insert competitors
        competitors = [
            ('admk', 'All India Anna Dravida Munnetra Kazhagam', 'AIADMK', 'AIADMK party description', '#ff6b6b', 1972, 'Chennai', 'https://aiadmk.com', True, 1),
            ('dmk', 'Dravida Munnetra Kazhagam', 'DMK', 'DMK party description', '#ffdd59', 1949, 'Chennai', 'https://dmk.in', True, 1),
            ('bjp', 'Bharatiya Janata Party', 'BJP', 'BJP Tamil Nadu description', '#ff9933', 1980, 'New Delhi', 'https://bjp.org', True, 1),
            ('ntk', 'Naam Tamilar Katchi', 'NTK', 'NTK party description', '#4ecdc4', 2010, 'Chennai', 'https://naamtamilar.org', True, 2),
            ('pmk', 'Pattali Makkal Katchi', 'PMK', 'PMK party description', '#45b7d1', 1989, 'Chennai', 'https://pmk.in', True, 2),
            ('tvk', 'Tamilaga Vettri Kazhagam', 'TVK', 'TVK party description', '#96ceb4', 2023, 'Chennai', 'https://tvk.in', True, 2),
            ('dmdk', 'Desiya Murpokku Dravida Kazhagam', 'DMDK', 'DMDK party description', '#feca57', 2005, 'Chennai', 'https://dmdk.in', True, 3)
        ]
        
        for comp in competitors:
            try:
                query = """
                INSERT INTO competitors (name, display_name, short_code, description, party_color, founded_year, headquarters, official_website, is_active, priority_level)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (name) DO NOTHING
                """
                await self.db_manager.execute_query(query, comp)
            except Exception as e:
                logger.warning(f"Competitor insertion warning: {e}")
        
        # Insert platforms
        platforms = [
            ('youtube', 'YouTube', 'video', 'https://youtube.com', True, 3, 100, True),
            ('facebook', 'Facebook', 'social_media', 'https://facebook.com', False, 4, 50, True),
            ('instagram', 'Instagram', 'social_media', 'https://instagram.com', False, 4, 50, True),
            ('twitter', 'Twitter/X', 'social_media', 'https://x.com', False, 5, 30, True),
            ('reddit', 'Reddit', 'social_media', 'https://reddit.com', True, 2, 100, True),
            ('tamil_news', 'Tamil News Sites', 'news', 'https://various.com', False, 2, 200, True)
        ]
        
        for plat in platforms:
            try:
                query = """
                INSERT INTO platforms (name, display_name, category, base_url, api_available, scraping_difficulty, rate_limit_per_hour, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (name) DO NOTHING
                """
                await self.db_manager.execute_query(query, plat)
            except Exception as e:
                logger.warning(f"Platform insertion warning: {e}")
        
        logger.info("✅ Initial data seeded successfully")

async def main():
    """Main function to create normalized schema"""
    schema_manager = NewSchemaManager()
    
    try:
        await schema_manager.initialize()
        
        print("🚀 Creating Normalized Multi-Competitor Database Schema...")
        
        # Create all tables
        await schema_manager.create_normalized_schema()
        
        # Seed initial data
        await schema_manager.seed_initial_data()
        
        print("✅ Normalized database schema created successfully!")
        print("📊 Ready for multi-competitor political intelligence system")
        
    except Exception as e:
        logger.error(f"❌ Schema creation failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())