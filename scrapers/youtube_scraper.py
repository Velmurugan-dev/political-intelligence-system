#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - YouTube Scraper
YouTube videos, comments and channel scraper using Apify streamers/youtube-scraper
"""

import os
import logging
import asyncio
from typing import Dict, List, Optional, Any
import json
from datetime import datetime, timedelta

import sys
sys.path.append('..')
from services.apify_service import get_apify_service
from config import get_config
from database import get_database

logger = logging.getLogger(__name__)

class YouTubeScraper:
    """YouTube scraper for AIADMK political content"""
    
    def __init__(self):
        self.config = get_config()
        self.apify_service = get_apify_service()
        self.db = None
        
        # AIADMK-related YouTube channels to monitor
        self.aiadmk_channels = [
            "https://www.youtube.com/@JayaTVOfficial",
            "https://www.youtube.com/@AIADMKofficial", 
            "https://www.youtube.com/@EdappadiPalaniswamiOfficial",
            "https://www.youtube.com/@tamilnadu_aiadmk",
            "https://www.youtube.com/@aiadmktamil",
            "https://www.youtube.com/@puthiyathalaimurai",
            "https://www.youtube.com/@PolimerNews",
            "https://www.youtube.com/@ThanthiTV"
        ]
        
        # AIADMK search queries for content discovery
        self.search_queries = self.config.get_keywords_for_platform('youtube')
        
        logger.info("✅ YouTube scraper initialized")
    
    async def get_database(self):
        """Get database connection"""
        if not self.db:
            self.db = await get_database()
        return self.db
    
    async def discover_aiadmk_videos(self) -> Dict[str, List[str]]:
        """Discover AIADMK-related videos from database queue and channels"""
        db = await self.get_database()
        
        # Get YouTube URLs from the database queue
        pending_urls = await db.get_pending_urls('youtube', limit=20)
        youtube_urls = [record['url'] for record in pending_urls if 'youtube.com' in record['url']]
        
        # Combine with known channels
        all_channels = list(set(self.aiadmk_channels + 
                               [url for url in youtube_urls if '/channel/' in url or '/@' in url]))
        
        # Separate video URLs
        video_urls = [url for url in youtube_urls if '/watch?v=' in url]
        
        logger.info(f"📍 Discovered {len(all_channels)} channels and {len(video_urls)} videos to scrape")
        
        return {
            'channels': all_channels,
            'videos': video_urls,
            'search_queries': self.search_queries[:10]  # Limit to first 10 queries
        }
    
    async def scrape_youtube_content(self, search_queries: List[str] = None,
                                   channel_urls: List[str] = None,
                                   video_urls: List[str] = None,
                                   max_results: int = 100) -> Dict[str, Any]:
        """Scrape YouTube content using Apify service"""
        
        # Get content targets
        if not any([search_queries, channel_urls, video_urls]):
            discovered = await self.discover_aiadmk_videos()
            search_queries = discovered['search_queries']
            channel_urls = discovered['channels']
            video_urls = discovered['videos']
        
        # Combine URLs for scraping
        start_urls = (channel_urls or []) + (video_urls or [])
        
        try:
            # Use Apify service to scrape YouTube
            result = await self.apify_service.run_youtube_scraper(
                search_queries=search_queries or [],
                start_urls=start_urls,
                max_results=max_results
            )
            
            if not result['success']:
                return result
            
            # Normalize the data
            raw_items = result['items']
            normalized_items = self.apify_service.normalize_youtube_data(raw_items)
            
            # Filter for AIADMK-related content
            aiadmk_videos = self.filter_aiadmk_content(normalized_items)
            
            return {
                'success': True,
                'platform': 'youtube',
                'run_id': result['run_id'],
                'total_scraped': len(normalized_items),
                'aiadmk_videos': len(aiadmk_videos),
                'videos': aiadmk_videos,
                'scraped_at': result['scraped_at']
            }
            
        except Exception as e:
            logger.error(f"YouTube scraping failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_videos': 0
            }
    
    def filter_aiadmk_content(self, videos: List[Dict]) -> List[Dict]:
        """Filter videos for AIADMK-related content"""
        aiadmk_keywords = (self.config.aiadmk_keywords['tamil'] + 
                          self.config.aiadmk_keywords['english'])
        
        aiadmk_videos = []
        
        for video in videos:
            title = (video.get('title', '') or '').lower()
            description = (video.get('description', '') or '').lower()
            channel_name = (video.get('channel_name', '') or '').lower()
            
            # Check if video contains AIADMK keywords
            is_aiadmk_related = any(
                keyword.lower() in title or 
                keyword.lower() in description or 
                keyword.lower() in channel_name
                for keyword in aiadmk_keywords
            )
            
            # Also check comments for AIADMK mentions
            if not is_aiadmk_related and video.get('comments'):
                comments_text = ' '.join([comment.get('text', '') 
                                        for comment in video.get('comments', [])]).lower()
                is_aiadmk_related = any(keyword.lower() in comments_text 
                                      for keyword in aiadmk_keywords)
            
            # Check subtitles/captions if available
            if not is_aiadmk_related and video.get('subtitles'):
                subtitles_text = str(video.get('subtitles', '')).lower()
                is_aiadmk_related = any(keyword.lower() in subtitles_text 
                                      for keyword in aiadmk_keywords)
            
            if is_aiadmk_related:
                # Calculate engagement score
                video['engagement_score'] = (
                    video.get('views_count', 0) * 0.1 +  # Views weighted less
                    video.get('likes_count', 0) * 2 + 
                    video.get('comments_count', 0) * 3
                )
                
                # Add AIADMK relevance score
                aiadmk_mentions = sum(1 for keyword in aiadmk_keywords 
                                    if keyword.lower() in title or keyword.lower() in description)
                video['aiadmk_relevance_score'] = aiadmk_mentions
                
                # Add content type classification
                video['content_type'] = self._classify_content_type(title, description)
                
                aiadmk_videos.append(video)
        
        # Sort by engagement score and recency
        aiadmk_videos.sort(
            key=lambda x: (
                x.get('engagement_score', 0),
                self._parse_date_for_sorting(x.get('published_date'))
            ),
            reverse=True
        )
        
        return aiadmk_videos
    
    def _classify_content_type(self, title: str, description: str) -> str:
        """Classify video content type based on title and description"""
        text = (title + ' ' + description).lower()
        
        if any(word in text for word in ['speech', 'பேச்சு', 'address', 'announce']):
            return 'speech'
        elif any(word in text for word in ['interview', 'interview', 'கேள்வி', 'பதில்']):
            return 'interview'  
        elif any(word in text for word in ['news', 'செய்தி', 'update', 'latest']):
            return 'news'
        elif any(word in text for word in ['rally', 'meeting', 'கூட்டம்', 'conference']):
            return 'event'
        elif any(word in text for word in ['debate', 'discussion', 'பேட்டி']):
            return 'discussion'
        else:
            return 'other'
    
    def _parse_date_for_sorting(self, date_str: str) -> datetime:
        """Parse date for sorting purposes"""
        if not date_str:
            return datetime.min
        
        try:
            from dateutil import parser
            return parser.parse(date_str)
        except:
            return datetime.min
    
    async def store_youtube_data(self, videos: List[Dict]) -> Dict[str, Any]:
        """Store YouTube videos in database"""
        db = await self.get_database()
        
        storage_results = {
            'total_videos': len(videos),
            'stored_videos': 0,
            'updated_videos': 0,
            'errors': []
        }
        
        for video in videos:
            try:
                # Prepare data for database insertion
                db_data = {
                    'video_id': video['video_id'],
                    'url': video['url'],
                    'title': video['title'],
                    'description': video.get('description', ''),
                    'channel_name': video['channel_name'],
                    'channel_url': video.get('channel_url'),
                    'published_date': self._parse_date(video['published_date']),
                    'duration': video.get('duration'),
                    'views_count': video.get('views_count', 0),
                    'likes_count': video.get('likes_count', 0),
                    'comments_count': video.get('comments_count', 0),
                    'engagement_score': video.get('engagement_score', 0),
                    'aiadmk_relevance_score': video.get('aiadmk_relevance_score', 0),
                    'content_type': video.get('content_type', 'other'),
                    'comments': json.dumps(video.get('comments', [])[:20]),  # Store top 20 comments
                    'subtitles': json.dumps(video.get('subtitles', {})),
                    'raw_data': json.dumps(video.get('raw_data', {})),
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Insert/update in database
                video_id = await db.insert_youtube_data(db_data)
                
                if video_id:
                    storage_results['stored_videos'] += 1
                    logger.info(f"💾 Stored YouTube video: {video['video_id']} - {video['title'][:50]}...")
                else:
                    storage_results['updated_videos'] += 1
                    logger.info(f"🔄 Updated YouTube video: {video['video_id']}")
                
            except Exception as e:
                error_msg = f"Failed to store video {video.get('video_id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                storage_results['errors'].append(error_msg)
        
        logger.info(f"✅ YouTube storage completed: {storage_results['stored_videos']} new, {storage_results['updated_videos']} updated")
        return storage_results
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse YouTube date format"""
        if not date_str:
            return None
        
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.isoformat()
        except:
            return date_str
    
    async def run_youtube_monitoring(self) -> Dict[str, Any]:
        """Run complete YouTube monitoring pipeline"""
        logger.info("🚀 Starting YouTube monitoring pipeline...")
        
        try:
            # Step 1: Scrape YouTube content
            scrape_results = await self.scrape_youtube_content(max_results=200)
            
            if not scrape_results['success']:
                return {
                    'success': False,
                    'error': scrape_results.get('error'),
                    'pipeline_stage': 'scraping'
                }
            
            if not scrape_results['videos']:
                return {
                    'success': True,
                    'message': 'No AIADMK-related videos found',
                    'total_scraped': scrape_results['total_scraped'],
                    'aiadmk_videos': 0
                }
            
            # Step 2: Store data in database
            storage_results = await self.store_youtube_data(scrape_results['videos'])
            
            # Step 3: Update URL processing status
            db = await self.get_database()
            pending_urls = await db.get_pending_urls('youtube', limit=20)
            for record in pending_urls:
                await db.update_url_status(record['id'], 'completed')
            
            return {
                'success': True,
                'platform': 'youtube',
                'scraping': {
                    'total_scraped': scrape_results['total_scraped'],
                    'aiadmk_videos': scrape_results['aiadmk_videos'],
                    'run_id': scrape_results['run_id']
                },
                'storage': storage_results,
                'completed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"YouTube monitoring pipeline failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'pipeline_stage': 'pipeline_execution'
            }
    
    async def get_trending_aiadmk_videos(self, limit: int = 10, 
                                       content_type: str = None) -> List[Dict]:
        """Get trending AIADMK videos from YouTube"""
        db = await self.get_database()
        
        query = """
            SELECT * FROM admk_youtube 
            WHERE scraped_at >= NOW() - INTERVAL '7 days'
        """
        params = []
        
        if content_type:
            query += " AND content_type = $1"
            params.append(content_type)
        
        query += " ORDER BY engagement_score DESC, published_date DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        try:
            result = await db.execute_query(query, tuple(params))
            videos = [dict(record) for record in result]
            
            # Parse JSON fields
            for video in videos:
                video['comments'] = json.loads(video.get('comments', '[]'))
                video['subtitles'] = json.loads(video.get('subtitles', '{}'))
                video['raw_data'] = json.loads(video.get('raw_data', '{}'))
            
            return videos
            
        except Exception as e:
            logger.error(f"Failed to get trending videos: {e}")
            return []
    
    async def get_aiadmk_channels_analytics(self) -> Dict[str, Any]:
        """Get analytics for AIADMK YouTube channels"""
        db = await self.get_database()
        
        query = """
            SELECT 
                channel_name,
                COUNT(*) as video_count,
                AVG(views_count) as avg_views,
                AVG(likes_count) as avg_likes,
                AVG(engagement_score) as avg_engagement,
                MAX(published_date) as latest_video
            FROM admk_youtube 
            WHERE scraped_at >= NOW() - INTERVAL '30 days'
            GROUP BY channel_name
            ORDER BY avg_engagement DESC
        """
        
        try:
            result = await db.execute_query(query)
            return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Failed to get channel analytics: {e}")
            return []

# Global scraper instance
youtube_scraper = None

def get_youtube_scraper():
    """Get global YouTube scraper instance"""
    global youtube_scraper
    if not youtube_scraper:
        youtube_scraper = YouTubeScraper()
    return youtube_scraper

# Test function
async def test_youtube_scraper():
    """Test YouTube scraper functionality"""
    try:
        scraper = get_youtube_scraper()
        
        # Test discovery
        discovered = await scraper.discover_aiadmk_videos()
        logger.info(f"✅ Discovery test: {len(discovered['channels'])} channels, {len(discovered['videos'])} videos, {len(discovered['search_queries'])} queries")
        
        # Test content filtering (mock data)
        mock_videos = [
            {
                'video_id': 'test1',
                'title': 'AIADMK Leader Edappadi Palaniswami Speech',
                'description': 'Latest political speech',
                'channel_name': 'Political Channel',
                'views_count': 10000,
                'likes_count': 500,
                'comments': []
            },
            {
                'video_id': 'test2',
                'title': 'Cooking Recipe Tutorial',
                'description': 'How to make biryani',
                'channel_name': 'Food Channel',
                'views_count': 5000,
                'likes_count': 100,
                'comments': []
            }
        ]
        
        filtered_videos = scraper.filter_aiadmk_content(mock_videos)
        logger.info(f"✅ Content filtering test: {len(filtered_videos)} AIADMK videos from {len(mock_videos)} total")
        
        if filtered_videos:
            logger.info(f"   - Content type: {filtered_videos[0].get('content_type')}")
            logger.info(f"   - Engagement score: {filtered_videos[0].get('engagement_score')}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ YouTube scraper test failed: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_youtube_scraper())