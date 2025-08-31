#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Reddit Scraper
Reddit posts and comments scraper using Apify reddit-scraper
"""

import os
import logging
import asyncio
from typing import Dict, List, Optional, Any
import json
from datetime import datetime

import sys
sys.path.append('..')
from services.apify_service import get_apify_service
from config import get_config
from database import get_database

logger = logging.getLogger(__name__)

class RedditScraper:
    """Reddit scraper for AIADMK political content"""
    
    def __init__(self):
        self.config = get_config()
        self.apify_service = get_apify_service()
        self.db = None
        
        # Relevant subreddits for Tamil Nadu politics
        self.target_subreddits = [
            "https://www.reddit.com/r/TamilNadu/",
            "https://www.reddit.com/r/Chennai/",
            "https://www.reddit.com/r/IndiaSpeaks/",
            "https://www.reddit.com/r/india/"
        ]
        
        logger.info("✅ Reddit scraper initialized")
    
    async def get_database(self):
        if not self.db:
            self.db = await get_database()
        return self.db
    
    async def scrape_reddit_content(self, max_posts: int = 100) -> Dict[str, Any]:
        """Scrape Reddit content using Apify service"""
        try:
            # Create search URLs for AIADMK terms
            search_urls = []
            for term in self.config.get_keywords_for_platform('reddit')[:5]:
                search_urls.append(f"https://www.reddit.com/search/?q={term}")
            
            result = await self.apify_service.run_reddit_scraper(
                start_urls=search_urls + self.target_subreddits,
                max_posts=max_posts
            )
            
            if not result['success']:
                return result
            
            # Normalize and filter data
            raw_items = result['items']
            normalized_items = self.apify_service.normalize_reddit_data(raw_items)
            aiadmk_posts = self.filter_aiadmk_content(normalized_items)
            
            return {
                'success': True,
                'platform': 'reddit',
                'total_scraped': len(normalized_items),
                'aiadmk_posts': len(aiadmk_posts),
                'posts': aiadmk_posts,
                'scraped_at': result['scraped_at']
            }
            
        except Exception as e:
            logger.error(f"Reddit scraping failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def filter_aiadmk_content(self, posts: List[Dict]) -> List[Dict]:
        """Filter posts for AIADMK-related content"""
        aiadmk_keywords = (self.config.aiadmk_keywords['tamil'] + 
                          self.config.aiadmk_keywords['english'])
        
        aiadmk_posts = []
        
        for post in posts:
            title = (post.get('title', '') or '').lower()
            text = (post.get('text', '') or '').lower()
            subreddit = (post.get('subreddit', '') or '').lower()
            
            is_aiadmk_related = (
                any(keyword.lower() in title for keyword in aiadmk_keywords) or
                any(keyword.lower() in text for keyword in aiadmk_keywords) or
                ('tamilnadu' in subreddit and any(keyword.lower() in title + text for keyword in ['aiadmk', 'அதிமுக']))
            )
            
            if is_aiadmk_related:
                post['engagement_score'] = (
                    post.get('upvotes', 0) * 2 +
                    post.get('comments_count', 0) * 3
                )
                aiadmk_posts.append(post)
        
        return sorted(aiadmk_posts, key=lambda x: x.get('engagement_score', 0), reverse=True)
    
    async def store_reddit_data(self, posts: List[Dict]) -> Dict[str, Any]:
        """Store Reddit data in database"""
        db = await self.get_database()
        storage_results = {'total_posts': len(posts), 'stored_posts': 0, 'errors': []}
        
        for post in posts:
            try:
                db_data = {
                    'post_id': post['post_id'],
                    'url': post['url'],
                    'title': post['title'],
                    'text': post['text'],
                    'author_name': post['author_name'],
                    'subreddit': post['subreddit'],
                    'published_date': self._parse_date(post['published_date']),
                    'upvotes': post.get('upvotes', 0),
                    'comments_count': post.get('comments_count', 0),
                    'engagement_score': post.get('engagement_score', 0),
                    'comments': json.dumps(post.get('comments', [])[:10]),
                    'raw_data': json.dumps(post.get('raw_data', {})),
                    'scraped_at': datetime.now().isoformat()
                }
                
                await db.insert_reddit_data(db_data)
                storage_results['stored_posts'] += 1
                
            except Exception as e:
                storage_results['errors'].append(str(e))
        
        return storage_results
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        if not date_str:
            return None
        try:
            from dateutil import parser
            return parser.parse(date_str).isoformat()
        except:
            return date_str

    async def run_reddit_monitoring(self) -> Dict[str, Any]:
        """Run complete Reddit monitoring pipeline"""
        logger.info("🚀 Starting Reddit monitoring pipeline...")
        
        scrape_results = await self.scrape_reddit_content(max_posts=150)
        if not scrape_results['success'] or not scrape_results.get('posts'):
            return scrape_results
        
        storage_results = await self.store_reddit_data(scrape_results['posts'])
        
        return {
            'success': True,
            'platform': 'reddit',
            'scraping': {
                'total_scraped': scrape_results['total_scraped'],
                'aiadmk_posts': scrape_results['aiadmk_posts']
            },
            'storage': storage_results,
            'completed_at': datetime.now().isoformat()
        }

def get_reddit_scraper():
    return RedditScraper()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(get_reddit_scraper().scrape_reddit_content(10))