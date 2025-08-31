#!/usr/bin/env python3
"""
Stage 1: Discovery Engine
Finds URLs from keywords/sources and stores in staging with deduplication
"""

import asyncio
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json

import sys
sys.path.append('..')
from database import get_database
from services.serpapi_service import get_serpapi_service
from services.brave_search_service import get_brave_service
from services.firecrawl_service import get_firecrawl_service

logger = logging.getLogger(__name__)

class DiscoveryEngine:
    """Stage 1: URL Discovery Engine with deduplication"""
    
    def __init__(self):
        self.db = None
        self.serpapi = None
        self.brave = None
        self.firecrawl = None
        
        self.stats = {
            'total_keywords_processed': 0,
            'total_sources_monitored': 0,
            'total_urls_found': 0,
            'total_urls_added': 0,
            'total_duplicates_found': 0,
            'errors': []
        }
    
    async def initialize(self):
        """Initialize discovery engine"""
        try:
            self.db = await get_database()
            self.serpapi = await get_serpapi_service()
            self.brave = await get_brave_service()
            self.firecrawl = await get_firecrawl_service()
            
            logger.info("✅ Discovery Engine initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Discovery Engine initialization failed: {e}")
            return False
    
    async def run_discovery_cycle(self, competitor_ids: List[int] = None, platform_ids: List[int] = None) -> Dict[str, Any]:
        """Run complete discovery cycle"""
        
        logger.info("🔍 Starting Discovery Cycle...")
        cycle_start = datetime.now()
        
        try:
            # Reset stats
            self.stats = {
                'total_keywords_processed': 0,
                'total_sources_monitored': 0,
                'total_urls_found': 0,
                'total_urls_added': 0,
                'total_duplicates_found': 0,
                'errors': []
            }
            
            # Get keywords to search
            keywords = await self.get_keywords_to_search(competitor_ids, platform_ids)
            logger.info(f"📝 Found {len(keywords)} keywords to search")
            
            # Get sources to monitor  
            sources = await self.get_sources_to_monitor(competitor_ids, platform_ids)
            logger.info(f"📺 Found {len(sources)} sources to monitor")
            
            # Process keywords in parallel
            keyword_tasks = []
            for keyword_batch in self.batch_items(keywords, 5):  # Process 5 keywords at a time
                keyword_tasks.append(self.process_keyword_batch(keyword_batch))
            
            if keyword_tasks:
                keyword_results = await asyncio.gather(*keyword_tasks, return_exceptions=True)
                self.process_keyword_results(keyword_results)
            
            # Process sources in parallel
            source_tasks = []
            for source_batch in self.batch_items(sources, 3):  # Process 3 sources at a time
                source_tasks.append(self.process_source_batch(source_batch))
            
            if source_tasks:
                source_results = await asyncio.gather(*source_tasks, return_exceptions=True)
                self.process_source_results(source_results)
            
            # Process manual queue
            await self.process_manual_queue()
            
            cycle_duration = (datetime.now() - cycle_start).total_seconds()
            
            logger.info(f"✅ Discovery Cycle completed in {cycle_duration:.1f}s")
            logger.info(f"📊 Stats: {self.stats['total_urls_found']} URLs found, {self.stats['total_urls_added']} added, {self.stats['total_duplicates_found']} duplicates")
            
            return {
                'success': True,
                'duration_seconds': cycle_duration,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Discovery Cycle failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_keywords_to_search(self, competitor_ids: List[int] = None, platform_ids: List[int] = None) -> List[Dict]:
        """Get keywords that need to be searched"""
        
        query = """
        SELECT k.*, c.name as competitor_name, p.name as platform_name
        FROM keywords k
        JOIN competitors c ON k.competitor_id = c.competitor_id
        JOIN platforms p ON k.platform_id = p.platform_id
        WHERE k.is_active = TRUE 
        AND c.is_active = TRUE 
        AND p.is_active = TRUE
        AND (k.last_searched IS NULL OR 
             k.last_searched < NOW() - INTERVAL '1 hour' * k.search_frequency_hours)
        """
        
        params = []
        if competitor_ids:
            query += f" AND k.competitor_id = ANY($1)"
            params.append(competitor_ids)
        
        if platform_ids:
            param_num = len(params) + 1
            query += f" AND k.platform_id = ANY(${param_num})"
            params.append(platform_ids)
        
        query += " ORDER BY k.last_searched ASC NULLS FIRST, c.priority_level ASC"
        
        results = await self.db.execute_query(query, tuple(params) if params else ())
        return [dict(row) for row in results]
    
    async def get_sources_to_monitor(self, competitor_ids: List[int] = None, platform_ids: List[int] = None) -> List[Dict]:
        """Get sources that need monitoring"""
        
        query = """
        SELECT s.*, c.name as competitor_name, p.name as platform_name
        FROM sources s
        JOIN competitors c ON s.competitor_id = c.competitor_id
        JOIN platforms p ON s.platform_id = p.platform_id
        WHERE s.is_active = TRUE 
        AND c.is_active = TRUE 
        AND p.is_active = TRUE
        AND (s.last_monitored IS NULL OR 
             s.last_monitored < NOW() - INTERVAL '1 hour' * s.monitoring_frequency_hours)
        """
        
        params = []
        if competitor_ids:
            query += f" AND s.competitor_id = ANY($1)"
            params.append(competitor_ids)
        
        if platform_ids:
            param_num = len(params) + 1
            query += f" AND s.platform_id = ANY(${param_num})"
            params.append(platform_ids)
        
        query += " ORDER BY s.last_monitored ASC NULLS FIRST, c.priority_level ASC"
        
        results = await self.db.execute_query(query, tuple(params) if params else ())
        return [dict(row) for row in results]
    
    async def process_keyword_batch(self, keywords: List[Dict]) -> List[Dict]:
        """Process a batch of keywords"""
        results = []
        
        for keyword in keywords:
            try:
                # Search using available services
                search_results = await self.search_keyword(keyword)
                
                # Add to stage_results with deduplication
                for result in search_results:
                    added = await self.add_to_stage_results(result, keyword)
                    if added:
                        self.stats['total_urls_added'] += 1
                    else:
                        self.stats['total_duplicates_found'] += 1
                
                # Update keyword last_searched
                await self.update_keyword_searched(keyword['keyword_id'], len(search_results))
                
                self.stats['total_keywords_processed'] += 1
                self.stats['total_urls_found'] += len(search_results)
                
                results.append({
                    'keyword_id': keyword['keyword_id'],
                    'keyword': keyword['keyword'],
                    'platform': keyword['platform_name'],
                    'results_found': len(search_results),
                    'success': True
                })
                
            except Exception as e:
                error_msg = f"Keyword search failed: {keyword['keyword']} on {keyword['platform_name']}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                
                results.append({
                    'keyword_id': keyword['keyword_id'],
                    'keyword': keyword['keyword'], 
                    'platform': keyword['platform_name'],
                    'results_found': 0,
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    async def search_keyword(self, keyword: Dict) -> List[Dict]:
        """Search for keyword using available services"""
        platform = keyword['platform_name']
        search_term = keyword['keyword']
        
        results = []
        
        try:
            # Choose search method based on platform
            if platform == 'youtube' and self.serpapi:
                # Use SerpAPI for YouTube search
                serpapi_results = await self.serpapi.search_youtube(search_term, limit=20)
                results.extend(self.format_serpapi_results(serpapi_results, keyword))
                
            elif platform in ['facebook', 'instagram', 'twitter'] and self.brave:
                # Use Brave Search for social media
                search_query = f"site:{platform}.com {search_term}"
                brave_results = await self.brave.search(search_query, limit=15)
                results.extend(self.format_brave_results(brave_results, keyword))
                
            elif platform == 'tamil_news' and self.firecrawl:
                # Use Firecrawl for news sites
                news_sites = ['dinamalar.com', 'thanthi.tv', 'polimer.in', 'vikatan.com']
                for site in news_sites:
                    try:
                        site_results = await self.firecrawl.search_site(site, search_term)
                        results.extend(self.format_firecrawl_results(site_results, keyword))
                    except Exception as e:
                        logger.warning(f"Firecrawl search failed for {site}: {e}")
                        
            elif platform == 'reddit' and self.brave:
                # Reddit search
                search_query = f"site:reddit.com {search_term}"
                reddit_results = await self.brave.search(search_query, limit=10)
                results.extend(self.format_brave_results(reddit_results, keyword))
            
            # Fallback to Brave Search for any platform
            if not results and self.brave:
                search_query = f"{search_term} {platform}"
                fallback_results = await self.brave.search(search_query, limit=10)
                results.extend(self.format_brave_results(fallback_results, keyword))
            
        except Exception as e:
            logger.error(f"Search failed for keyword '{search_term}' on {platform}: {e}")
        
        return results
    
    def format_serpapi_results(self, results: List[Dict], keyword: Dict) -> List[Dict]:
        """Format SerpAPI results for stage_results"""
        formatted = []
        
        for item in results:
            formatted.append({
                'competitor_id': keyword['competitor_id'],
                'platform_id': keyword['platform_id'],
                'keyword_id': keyword['keyword_id'],
                'url': item.get('link', ''),
                'title': item.get('title', ''),
                'snippet': item.get('snippet', ''),
                'author': item.get('channel', {}).get('name', ''),
                'author_id': item.get('channel', {}).get('id', ''),
                'published_at': self.parse_date(item.get('published_date')),
                'discovery_method': 'serpapi',
                'content_type': 'video',
                'language': keyword.get('language', 'ta'),
                'keywords_matched': [keyword['keyword']],
                'raw_data': item
            })
        
        return formatted
    
    def format_brave_results(self, results: List[Dict], keyword: Dict) -> List[Dict]:
        """Format Brave Search results for stage_results"""
        formatted = []
        
        for item in results:
            formatted.append({
                'competitor_id': keyword['competitor_id'],
                'platform_id': keyword['platform_id'],
                'keyword_id': keyword['keyword_id'],
                'url': item.get('url', ''),
                'title': item.get('title', ''),
                'snippet': item.get('description', ''),
                'author': self.extract_author_from_url(item.get('url', '')),
                'published_at': self.parse_date(item.get('age')),
                'discovery_method': 'brave',
                'content_type': self.detect_content_type(item.get('url', '')),
                'language': keyword.get('language', 'ta'),
                'keywords_matched': [keyword['keyword']],
                'raw_data': item
            })
        
        return formatted
    
    def format_firecrawl_results(self, results: List[Dict], keyword: Dict) -> List[Dict]:
        """Format Firecrawl results for stage_results"""
        formatted = []
        
        for item in results:
            formatted.append({
                'competitor_id': keyword['competitor_id'],
                'platform_id': keyword['platform_id'],
                'keyword_id': keyword['keyword_id'],
                'url': item.get('url', ''),
                'title': item.get('title', ''),
                'snippet': item.get('excerpt', ''),
                'author': item.get('author', ''),
                'published_at': self.parse_date(item.get('published_date')),
                'discovery_method': 'firecrawl',
                'content_type': 'article',
                'language': keyword.get('language', 'ta'),
                'keywords_matched': [keyword['keyword']],
                'raw_data': item
            })
        
        return formatted
    
    async def add_to_stage_results(self, result_data: Dict, keyword: Dict = None) -> bool:
        """Add result to stage_results with deduplication check"""
        
        try:
            # Generate URL hash for deduplication
            url = result_data['url']
            url_hash = hashlib.sha256(url.encode()).hexdigest()
            
            # Check for existing URL
            existing_query = """
            SELECT stage_id FROM stage_results 
            WHERE competitor_id = $1 AND platform_id = $2 AND url_hash = $3
            """
            existing = await self.db.execute_query(existing_query, (
                result_data['competitor_id'],
                result_data['platform_id'],
                url_hash
            ))
            
            if existing:
                # Mark as duplicate
                await self.db.execute_query(
                    "UPDATE stage_results SET status = 'duplicate' WHERE stage_id = $1",
                    (existing[0]['stage_id'],)
                )
                return False
            
            # Insert new result
            insert_query = """
            INSERT INTO stage_results (
                competitor_id, platform_id, keyword_id, url, url_hash,
                title, snippet, author, author_id, published_at,
                discovery_method, content_type, language, keywords_matched,
                status, raw_data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            RETURNING stage_id
            """
            
            stage_result = await self.db.execute_query(insert_query, (
                result_data['competitor_id'],
                result_data['platform_id'],
                result_data.get('keyword_id'),
                url,
                url_hash,
                result_data.get('title'),
                result_data.get('snippet'),
                result_data.get('author'),
                result_data.get('author_id'),
                result_data.get('published_at'),
                result_data.get('discovery_method'),
                result_data.get('content_type'),
                result_data.get('language'),
                result_data.get('keywords_matched', []),
                'pending',
                json.dumps(result_data.get('raw_data', {}))
            ))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to add result to stage_results: {e}")
            self.stats['errors'].append(f"Database insert failed: {e}")
            return False
    
    async def process_manual_queue(self):
        """Process pending manual URL submissions"""
        
        try:
            # Get pending manual URLs
            manual_urls = await self.db.execute_query("""
                SELECT mq.*, c.name as competitor_name, p.name as platform_name
                FROM manual_queue mq
                JOIN competitors c ON mq.competitor_id = c.competitor_id
                JOIN platforms p ON mq.platform_id = p.platform_id
                WHERE mq.status = 'pending'
                ORDER BY mq.priority DESC, mq.created_at ASC
                LIMIT 50
            """)
            
            for manual_url in manual_urls:
                try:
                    # Create result data for manual URL
                    result_data = {
                        'competitor_id': manual_url['competitor_id'],
                        'platform_id': manual_url['platform_id'],
                        'url': manual_url['url'],
                        'title': f"Manual submission: {manual_url['notes'] or 'No notes'}",
                        'snippet': manual_url['notes'] or '',
                        'author': manual_url.get('submitted_by', 'Unknown'),
                        'discovery_method': 'manual',
                        'content_type': self.detect_content_type(manual_url['url']),
                        'language': 'ta',
                        'keywords_matched': [],
                        'raw_data': {'manual_submission': True, 'queue_id': manual_url['queue_id']}
                    }
                    
                    # Add to stage_results
                    added = await self.add_to_stage_results(result_data)
                    
                    if added:
                        # Update manual queue status
                        await self.db.execute_query(
                            "UPDATE manual_queue SET status = 'processed', processed_at = NOW() WHERE queue_id = $1",
                            (manual_url['queue_id'],)
                        )
                        logger.info(f"✅ Manual URL processed: {manual_url['url']}")
                    else:
                        # Mark as duplicate
                        await self.db.execute_query(
                            "UPDATE manual_queue SET status = 'duplicate', processed_at = NOW() WHERE queue_id = $1",
                            (manual_url['queue_id'],)
                        )
                        logger.info(f"🔄 Manual URL duplicate: {manual_url['url']}")
                
                except Exception as e:
                    # Mark as failed
                    await self.db.execute_query(
                        "UPDATE manual_queue SET status = 'failed', processed_at = NOW() WHERE queue_id = $1",
                        (manual_url['queue_id'],)
                    )
                    logger.error(f"❌ Manual URL processing failed: {manual_url['url']}: {e}")
            
            logger.info(f"📎 Processed {len(manual_urls)} manual URLs")
            
        except Exception as e:
            logger.error(f"Manual queue processing failed: {e}")
            self.stats['errors'].append(f"Manual queue processing failed: {e}")
    
    async def update_keyword_searched(self, keyword_id: int, results_found: int):
        """Update keyword search timestamp and results count"""
        await self.db.execute_query("""
            UPDATE keywords 
            SET last_searched = NOW(), 
                total_results_found = total_results_found + $1
            WHERE keyword_id = $2
        """, (results_found, keyword_id))
    
    async def update_source_monitored(self, source_id: int, content_found: int):
        """Update source monitoring timestamp and content count"""
        await self.db.execute_query("""
            UPDATE sources 
            SET last_monitored = NOW(), 
                total_content_found = total_content_found + $1
            WHERE source_id = $2
        """, (content_found, source_id))
    
    def batch_items(self, items: List, batch_size: int) -> List[List]:
        """Split items into batches"""
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]
    
    def process_keyword_results(self, results: List):
        """Process keyword search results"""
        for result in results:
            if isinstance(result, Exception):
                self.stats['errors'].append(f"Keyword batch failed: {result}")
    
    def process_source_results(self, results: List):
        """Process source monitoring results"""
        for result in results:
            if isinstance(result, Exception):
                self.stats['errors'].append(f"Source batch failed: {result}")
    
    def extract_author_from_url(self, url: str) -> str:
        """Extract author/channel name from URL"""
        try:
            if 'youtube.com' in url and '@' in url:
                return url.split('@')[1].split('/')[0]
            elif 'facebook.com' in url:
                return url.split('facebook.com/')[1].split('/')[0]
            elif 'instagram.com' in url:
                return url.split('instagram.com/')[1].split('/')[0]
            elif 'twitter.com' in url or 'x.com' in url:
                return url.split('/')[-2] if url.count('/') > 3 else ''
            else:
                return ''
        except:
            return ''
    
    def detect_content_type(self, url: str) -> str:
        """Detect content type from URL"""
        if 'youtube.com/watch' in url:
            return 'video'
        elif 'facebook.com/posts' in url or 'facebook.com/photo' in url:
            return 'post'
        elif 'instagram.com/p/' in url:
            return 'post'
        elif 'twitter.com' in url or 'x.com' in url:
            return 'tweet'
        elif 'reddit.com' in url:
            return 'post'
        else:
            return 'article'
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats"""
        if not date_str:
            return None
        
        try:
            # Handle common date formats
            if 'ago' in date_str:
                # Parse relative dates like "2 hours ago"
                return datetime.now() - timedelta(hours=2)  # Simplified
            else:
                # Try ISO format
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None
    
    async def process_source_batch(self, sources: List[Dict]) -> List[Dict]:
        """Process a batch of sources for monitoring"""
        results = []
        
        for source in sources:
            try:
                # Monitor source for new content
                content_results = await self.monitor_source(source)
                
                # Add found content to stage_results
                for result in content_results:
                    added = await self.add_to_stage_results(result, None)
                    if added:
                        self.stats['total_urls_added'] += 1
                    else:
                        self.stats['total_duplicates_found'] += 1
                
                # Update source monitoring timestamp
                await self.update_source_monitored(source['source_id'], len(content_results))
                
                self.stats['total_sources_monitored'] += 1
                self.stats['total_urls_found'] += len(content_results)
                
                results.append({
                    'source_id': source['source_id'],
                    'name': source['name'],
                    'platform': source['platform_name'],
                    'content_found': len(content_results),
                    'success': True
                })
                
            except Exception as e:
                error_msg = f"Source monitoring failed: {source['name']}: {e}"
                logger.error(error_msg)
                self.stats['errors'].append(error_msg)
                
                results.append({
                    'source_id': source['source_id'],
                    'name': source['name'],
                    'platform': source['platform_name'],
                    'content_found': 0,
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    async def monitor_source(self, source: Dict) -> List[Dict]:
        """Monitor a specific source for new content"""
        platform = source['platform_name']
        source_url = source['url']
        
        results = []
        
        try:
            if platform == 'youtube' and self.serpapi:
                # Monitor YouTube channel
                channel_id = source.get('identifier') or self.extract_youtube_channel_id(source_url)
                if channel_id:
                    channel_results = await self.serpapi.get_channel_videos(channel_id, limit=10)
                    results.extend(self.format_channel_results(channel_results, source))
            
            elif platform in ['facebook', 'instagram', 'twitter'] and self.brave:
                # Use search to find recent posts from this source
                search_query = f"site:{platform}.com {source['name']}"
                search_results = await self.brave.search(search_query, limit=5)
                results.extend(self.format_source_search_results(search_results, source))
            
            elif platform == 'tamil_news' and self.firecrawl:
                # Crawl news website for recent articles
                crawl_results = await self.firecrawl.crawl_recent(source_url, limit=5)
                results.extend(self.format_news_crawl_results(crawl_results, source))
            
        except Exception as e:
            logger.error(f"Source monitoring failed for {source['name']}: {e}")
        
        return results
    
    def format_channel_results(self, results: List[Dict], source: Dict) -> List[Dict]:
        """Format channel monitoring results"""
        formatted = []
        
        for item in results:
            formatted.append({
                'competitor_id': source['competitor_id'],
                'platform_id': source['platform_id'],
                'source_id': source['source_id'],
                'url': item.get('link', ''),
                'title': item.get('title', ''),
                'snippet': item.get('description', ''),
                'author': source['name'],
                'author_id': source.get('identifier'),
                'published_at': self.parse_date(item.get('published_date')),
                'discovery_method': 'source_monitoring',
                'content_type': 'video',
                'language': 'ta',
                'keywords_matched': [],
                'raw_data': item
            })
        
        return formatted
    
    def format_source_search_results(self, results: List[Dict], source: Dict) -> List[Dict]:
        """Format source search results"""
        formatted = []
        
        for item in results:
            formatted.append({
                'competitor_id': source['competitor_id'],
                'platform_id': source['platform_id'],
                'source_id': source['source_id'],
                'url': item.get('url', ''),
                'title': item.get('title', ''),
                'snippet': item.get('description', ''),
                'author': source['name'],
                'published_at': self.parse_date(item.get('age')),
                'discovery_method': 'source_monitoring',
                'content_type': self.detect_content_type(item.get('url', '')),
                'language': 'ta',
                'keywords_matched': [],
                'raw_data': item
            })
        
        return formatted
    
    def format_news_crawl_results(self, results: List[Dict], source: Dict) -> List[Dict]:
        """Format news crawl results"""
        formatted = []
        
        for item in results:
            formatted.append({
                'competitor_id': source['competitor_id'],
                'platform_id': source['platform_id'],
                'source_id': source['source_id'],
                'url': item.get('url', ''),
                'title': item.get('title', ''),
                'snippet': item.get('excerpt', ''),
                'author': item.get('author', source['name']),
                'published_at': self.parse_date(item.get('published_date')),
                'discovery_method': 'source_monitoring',
                'content_type': 'article',
                'language': 'ta',
                'keywords_matched': [],
                'raw_data': item
            })
        
        return formatted
    
    def extract_youtube_channel_id(self, url: str) -> str:
        """Extract YouTube channel ID from URL"""
        try:
            if '/channel/' in url:
                return url.split('/channel/')[1].split('?')[0]
            elif '/@' in url:
                return url.split('/@')[1].split('?')[0]
            else:
                return ''
        except:
            return ''
    
    async def get_discovery_stats(self) -> Dict[str, Any]:
        """Get discovery engine statistics"""
        
        # Recent discovery stats
        stats_query = """
        SELECT 
            c.name as competitor,
            p.name as platform,
            COUNT(*) as urls_discovered,
            COUNT(CASE WHEN sr.status = 'pending' THEN 1 END) as pending_processing,
            COUNT(CASE WHEN sr.status = 'scraped' THEN 1 END) as processed,
            COUNT(CASE WHEN sr.status = 'duplicate' THEN 1 END) as duplicates,
            MAX(sr.inserted_at) as last_discovery
        FROM stage_results sr
        JOIN competitors c ON sr.competitor_id = c.competitor_id
        JOIN platforms p ON sr.platform_id = p.platform_id
        WHERE sr.inserted_at >= NOW() - INTERVAL '24 hours'
        GROUP BY c.name, p.name
        ORDER BY urls_discovered DESC
        """
        
        try:
            results = await self.db.execute_query(stats_query)
            return {
                'recent_stats': [dict(row) for row in results],
                'engine_stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get discovery stats: {e}")
            return {'error': str(e)}

# Global instance
discovery_engine = None

async def get_discovery_engine():
    """Get global discovery engine instance"""
    global discovery_engine
    if not discovery_engine:
        discovery_engine = DiscoveryEngine()
        await discovery_engine.initialize()
    return discovery_engine

async def run_discovery_cycle(competitor_ids: List[int] = None, platform_ids: List[int] = None) -> Dict[str, Any]:
    """Run discovery cycle (can be called from Celery)"""
    engine = await get_discovery_engine()
    return await engine.run_discovery_cycle(competitor_ids, platform_ids)

if __name__ == "__main__":
    async def test_discovery():
        engine = DiscoveryEngine()
        if await engine.initialize():
            result = await engine.run_discovery_cycle()
            print(f"Discovery test result: {result}")
    
    asyncio.run(test_discovery())