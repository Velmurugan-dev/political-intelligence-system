#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Facebook Scraper
Facebook posts and comments scraper using Apify facebook-posts-scraper
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

class FacebookScraper:
    """Facebook scraper for AIADMK political content"""
    
    def __init__(self):
        self.config = get_config()
        self.apify_service = get_apify_service()
        self.db = None
        
        # AIADMK Facebook pages and groups to monitor
        self.aiadmk_pages = [
            "https://www.facebook.com/AIADMKOfficial",
            "https://www.facebook.com/edappadipalaniswamiofficial",
            "https://www.facebook.com/jaya.tv",
            "https://www.facebook.com/groups/aiadmksupporters",
            "https://www.facebook.com/AIADMK.Party",
        ]
        
        logger.info("✅ Facebook scraper initialized")
    
    async def get_database(self):
        """Get database connection"""
        if not self.db:
            self.db = await get_database()
        return self.db
    
    async def discover_aiadmk_pages(self) -> List[str]:
        """Discover AIADMK-related Facebook pages from database queue"""
        db = await self.get_database()
        
        # Get Facebook URLs from the database queue
        pending_urls = await db.get_pending_urls('facebook', limit=10)
        facebook_urls = [record['url'] for record in pending_urls if 'facebook.com' in record['url']]
        
        # Combine with known AIADMK pages
        all_urls = list(set(self.aiadmk_pages + facebook_urls))
        
        logger.info(f"📍 Discovered {len(all_urls)} Facebook URLs to scrape")
        return all_urls
    
    async def scrape_facebook_posts(self, urls: List[str] = None, 
                                  max_posts: int = 50) -> Dict[str, Any]:
        """Scrape Facebook posts using Apify service"""
        if not urls:
            urls = await self.discover_aiadmk_pages()
        
        if not urls:
            return {
                'success': False,
                'error': 'No Facebook URLs to scrape',
                'total_posts': 0
            }
        
        try:
            # Use Apify service to scrape Facebook
            result = await self.apify_service.run_facebook_scraper(
                urls=urls,
                max_posts=max_posts,
                max_comments=20
            )
            
            if not result['success']:
                return result
            
            # Normalize the data
            raw_items = result['items']
            normalized_items = self.apify_service.normalize_facebook_data(raw_items)
            
            # Filter for AIADMK-related content
            aiadmk_posts = self.filter_aiadmk_content(normalized_items)
            
            return {
                'success': True,
                'platform': 'facebook',
                'run_id': result['run_id'],
                'total_scraped': len(normalized_items),
                'aiadmk_posts': len(aiadmk_posts),
                'posts': aiadmk_posts,
                'scraped_at': result['scraped_at']
            }
            
        except Exception as e:
            logger.error(f"Facebook scraping failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_posts': 0
            }
    
    def filter_aiadmk_content(self, posts: List[Dict]) -> List[Dict]:
        """Filter posts for AIADMK-related content"""
        aiadmk_keywords = (self.config.aiadmk_keywords['tamil'] + 
                          self.config.aiadmk_keywords['english'])
        
        aiadmk_posts = []
        
        for post in posts:
            text = (post.get('text', '') or '').lower()
            author = (post.get('author_name', '') or '').lower()
            
            # Check if post contains AIADMK keywords
            is_aiadmk_related = any(keyword.lower() in text or keyword.lower() in author 
                                  for keyword in aiadmk_keywords)
            
            # Also check comments for AIADMK mentions
            if not is_aiadmk_related:
                comments_text = ' '.join([comment.get('text', '') 
                                        for comment in post.get('comments', [])]).lower()
                is_aiadmk_related = any(keyword.lower() in comments_text 
                                      for keyword in aiadmk_keywords)
            
            if is_aiadmk_related:
                # Calculate engagement score
                post['engagement_score'] = (
                    post.get('likes_count', 0) + 
                    (post.get('comments_count', 0) * 2) + 
                    (post.get('shares_count', 0) * 3)
                )
                
                # Add AIADMK relevance score
                aiadmk_mentions = sum(1 for keyword in aiadmk_keywords 
                                    if keyword.lower() in text)
                post['aiadmk_relevance_score'] = aiadmk_mentions
                
                aiadmk_posts.append(post)
        
        # Sort by engagement score
        aiadmk_posts.sort(key=lambda x: x.get('engagement_score', 0), reverse=True)
        
        return aiadmk_posts
    
    async def store_facebook_data(self, posts: List[Dict]) -> Dict[str, Any]:
        """Store Facebook posts in database"""
        db = await self.get_database()
        
        storage_results = {
            'total_posts': len(posts),
            'stored_posts': 0,
            'updated_posts': 0,
            'errors': []
        }
        
        for post in posts:
            try:
                # Prepare data for database insertion
                db_data = {
                    'post_id': post['post_id'],
                    'url': post['url'],
                    'text': post['text'],
                    'author_name': post['author_name'],
                    'author_url': post['author_url'],
                    'published_date': self._parse_date(post['published_date']),
                    'likes_count': post.get('likes_count', 0),
                    'comments_count': post.get('comments_count', 0),
                    'shares_count': post.get('shares_count', 0),
                    'engagement_score': post.get('engagement_score', 0),
                    'aiadmk_relevance_score': post.get('aiadmk_relevance_score', 0),
                    'comments': json.dumps(post.get('comments', [])[:10]),  # Store top 10 comments
                    'images': json.dumps(post.get('images', [])),
                    'video_url': post.get('video_url'),
                    'raw_data': json.dumps(post.get('raw_data', {})),
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Insert/update in database
                post_id = await db.insert_facebook_data(db_data)
                
                if post_id:
                    storage_results['stored_posts'] += 1
                    logger.info(f"💾 Stored Facebook post: {post['post_id']}")
                else:
                    storage_results['updated_posts'] += 1
                    logger.info(f"🔄 Updated Facebook post: {post['post_id']}")
                
            except Exception as e:
                error_msg = f"Failed to store post {post.get('post_id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                storage_results['errors'].append(error_msg)
        
        logger.info(f"✅ Facebook storage completed: {storage_results['stored_posts']} new, {storage_results['updated_posts']} updated")
        return storage_results
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse Facebook date format"""
        if not date_str:
            return None
        
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.isoformat()
        except:
            return date_str
    
    async def run_facebook_monitoring(self) -> Dict[str, Any]:
        """Run complete Facebook monitoring pipeline"""
        logger.info("🚀 Starting Facebook monitoring pipeline...")
        
        try:
            # Step 1: Scrape Facebook posts
            scrape_results = await self.scrape_facebook_posts(max_posts=100)
            
            if not scrape_results['success']:
                return {
                    'success': False,
                    'error': scrape_results.get('error'),
                    'pipeline_stage': 'scraping'
                }
            
            if not scrape_results['posts']:
                return {
                    'success': True,
                    'message': 'No AIADMK-related posts found',
                    'total_scraped': scrape_results['total_scraped'],
                    'aiadmk_posts': 0
                }
            
            # Step 2: Store data in database
            storage_results = await self.store_facebook_data(scrape_results['posts'])
            
            # Step 3: Update URL processing status
            db = await self.get_database()
            pending_urls = await db.get_pending_urls('facebook', limit=10)
            for record in pending_urls:
                await db.update_url_status(record['id'], 'completed')
            
            return {
                'success': True,
                'platform': 'facebook',
                'scraping': {
                    'total_scraped': scrape_results['total_scraped'],
                    'aiadmk_posts': scrape_results['aiadmk_posts'],
                    'run_id': scrape_results['run_id']
                },
                'storage': storage_results,
                'completed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Facebook monitoring pipeline failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'pipeline_stage': 'pipeline_execution'
            }
    
    async def get_trending_aiadmk_posts(self, limit: int = 10) -> List[Dict]:
        """Get trending AIADMK posts from Facebook"""
        db = await self.get_database()
        
        # Get recent high-engagement posts
        query = """
            SELECT * FROM admk_facebook 
            WHERE scraped_at >= NOW() - INTERVAL '7 days'
            ORDER BY engagement_score DESC, published_date DESC
            LIMIT $1
        """
        
        try:
            result = await db.execute_query(query, (limit,))
            posts = [dict(record) for record in result]
            
            # Parse JSON fields
            for post in posts:
                post['comments'] = json.loads(post.get('comments', '[]'))
                post['images'] = json.loads(post.get('images', '[]'))
                post['raw_data'] = json.loads(post.get('raw_data', '{}'))
            
            return posts
            
        except Exception as e:
            logger.error(f"Failed to get trending posts: {e}")
            return []

# Global scraper instance
facebook_scraper = None

def get_facebook_scraper():
    """Get global Facebook scraper instance"""
    global facebook_scraper
    if not facebook_scraper:
        facebook_scraper = FacebookScraper()
    return facebook_scraper

# Test function
async def test_facebook_scraper():
    """Test Facebook scraper functionality"""
    try:
        scraper = get_facebook_scraper()
        
        # Test URL discovery
        urls = await scraper.discover_aiadmk_pages()
        logger.info(f"✅ URL discovery test: {len(urls)} URLs found")
        
        # Test content filtering (mock data)
        mock_posts = [
            {
                'post_id': 'test1',
                'text': 'AIADMK latest news update',
                'author_name': 'Test Page',
                'likes_count': 100,
                'comments': []
            },
            {
                'post_id': 'test2', 
                'text': 'Random post about food',
                'author_name': 'Food Page',
                'likes_count': 50,
                'comments': []
            }
        ]
        
        filtered_posts = scraper.filter_aiadmk_content(mock_posts)
        logger.info(f"✅ Content filtering test: {len(filtered_posts)} AIADMK posts from {len(mock_posts)} total")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Facebook scraper test failed: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_facebook_scraper())