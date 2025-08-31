#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Twitter Scraper
Twitter tweets and threads scraper using Apify tweet-scraper
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

class TwitterScraper:
    """Twitter scraper for AIADMK political content"""
    
    def __init__(self):
        self.config = get_config()
        self.apify_service = get_apify_service()
        self.db = None
        
        # AIADMK Twitter handles to monitor
        self.aiadmk_handles = [
            "aiadmkofficial", "epsofficialpage", "jayatvofficial",
            "aiadmk_youth", "aiadmktamil"
        ]
        
        # AIADMK hashtags and search terms
        self.search_terms = self.config.get_keywords_for_platform('twitter')
        
        logger.info("✅ Twitter scraper initialized")
    
    async def get_database(self):
        """Get database connection"""
        if not self.db:
            self.db = await get_database()
        return self.db
    
    async def scrape_twitter_content(self, max_tweets: int = 100) -> Dict[str, Any]:
        """Scrape Twitter content using Apify service"""
        try:
            result = await self.apify_service.run_twitter_scraper(
                search_terms=self.search_terms,
                twitter_handles=self.aiadmk_handles,
                max_items=max_tweets
            )
            
            if not result['success']:
                return result
            
            # Normalize and filter data
            raw_items = result['items']
            normalized_items = self.apify_service.normalize_twitter_data(raw_items)
            aiadmk_tweets = self.filter_aiadmk_content(normalized_items)
            
            return {
                'success': True,
                'platform': 'twitter',
                'total_scraped': len(normalized_items),
                'aiadmk_tweets': len(aiadmk_tweets),
                'tweets': aiadmk_tweets,
                'scraped_at': result['scraped_at']
            }
            
        except Exception as e:
            logger.error(f"Twitter scraping failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def filter_aiadmk_content(self, tweets: List[Dict]) -> List[Dict]:
        """Filter tweets for AIADMK-related content"""
        aiadmk_keywords = (self.config.aiadmk_keywords['tamil'] + 
                          self.config.aiadmk_keywords['english'])
        
        aiadmk_tweets = []
        
        for tweet in tweets:
            text = (tweet.get('text', '') or '').lower()
            author = (tweet.get('author_name', '') or '').lower()
            hashtags = ' '.join(tweet.get('hashtags', [])).lower()
            
            is_aiadmk_related = (
                any(keyword.lower() in text for keyword in aiadmk_keywords) or
                any(keyword.lower() in author for keyword in aiadmk_keywords) or
                any(keyword.lower() in hashtags for keyword in aiadmk_keywords)
            )
            
            if is_aiadmk_related:
                tweet['engagement_score'] = (
                    tweet.get('retweets_count', 0) * 2 +
                    tweet.get('likes_count', 0) +
                    tweet.get('replies_count', 0) * 3
                )
                aiadmk_tweets.append(tweet)
        
        return sorted(aiadmk_tweets, key=lambda x: x.get('engagement_score', 0), reverse=True)
    
    async def store_twitter_data(self, tweets: List[Dict]) -> Dict[str, Any]:
        """Store Twitter data in database"""
        db = await self.get_database()
        storage_results = {'total_tweets': len(tweets), 'stored_tweets': 0, 'errors': []}
        
        for tweet in tweets:
            try:
                db_data = {
                    'tweet_id': tweet['tweet_id'],
                    'url': tweet['url'],
                    'text': tweet['text'],
                    'author_name': tweet['author_name'],
                    'author_username': tweet['author_username'],
                    'published_date': self._parse_date(tweet['published_date']),
                    'retweets_count': tweet.get('retweets_count', 0),
                    'likes_count': tweet.get('likes_count', 0),
                    'replies_count': tweet.get('replies_count', 0),
                    'engagement_score': tweet.get('engagement_score', 0),
                    'hashtags': json.dumps(tweet.get('hashtags', [])),
                    'mentions': json.dumps(tweet.get('mentions', [])),
                    'is_retweet': tweet.get('is_retweet', False),
                    'raw_data': json.dumps(tweet.get('raw_data', {})),
                    'scraped_at': datetime.now().isoformat()
                }
                
                await db.insert_twitter_data(db_data)
                storage_results['stored_tweets'] += 1
                
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

    async def run_twitter_monitoring(self) -> Dict[str, Any]:
        """Run complete Twitter monitoring pipeline"""
        logger.info("🚀 Starting Twitter monitoring pipeline...")
        
        scrape_results = await self.scrape_twitter_content(max_tweets=200)
        if not scrape_results['success'] or not scrape_results.get('tweets'):
            return scrape_results
        
        storage_results = await self.store_twitter_data(scrape_results['tweets'])
        
        return {
            'success': True,
            'platform': 'twitter',
            'scraping': {
                'total_scraped': scrape_results['total_scraped'],
                'aiadmk_tweets': scrape_results['aiadmk_tweets']
            },
            'storage': storage_results,
            'completed_at': datetime.now().isoformat()
        }

def get_twitter_scraper():
    return TwitterScraper()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(get_twitter_scraper().scrape_twitter_content(10))