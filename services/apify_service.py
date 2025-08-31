#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Apify Service
Unified Apify client for all social media platform scrapers
"""

import os
import logging
import asyncio
from typing import Dict, List, Optional, Any, Union
import json
from datetime import datetime, timedelta

import sys
sys.path.append('..')
from config import get_config
from database import get_database

# Import Apify client
try:
    from apify_client import ApifyClient
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False
    logging.error("Apify client not available. Install: pip install apify-client")

logger = logging.getLogger(__name__)

class ApifyService:
    """Unified Apify service for all platform scrapers"""
    
    def __init__(self):
        if not APIFY_AVAILABLE:
            raise ImportError("Apify client required for platform scraping operations")
        
        self.config = get_config()
        self.api_config = self.config.get_api_config('apify')
        
        if not self.api_config or not self.api_config.api_key:
            raise ValueError("Apify configuration missing. Check APIFY_API_TOKEN in .env")
        
        # Initialize Apify client
        self.client = ApifyClient(self.api_config.api_key)
        
        # Actor configurations from config
        self.actors = {}
        for platform, actor_config in self.config.apify_actors.items():
            self.actors[platform] = {
                'id': actor_config.actor_id,
                'config': actor_config,
                'client': self.client.actor(actor_config.actor_id)
            }
        
        logger.info(f"✅ Apify service initialized with {len(self.actors)} actors")
    
    async def run_facebook_scraper(self, urls: List[str], max_posts: int = 50,
                                 max_comments: int = 20) -> Dict[str, Any]:
        """Run Facebook posts scraper (apify/facebook-posts-scraper)"""
        if 'facebook' not in self.actors:
            return {'success': False, 'error': 'Facebook actor not configured'}
        
        # Get AIADMK keywords for Facebook
        keywords = self.config.get_keywords_for_platform('facebook')
        
        run_input = {
            "startUrls": [{"url": url} for url in urls],
            "resultsLimit": max_posts,
            "maxPosts": max_posts,
            "maxComments": max_comments,
            "maxReplies": 5,
            "scrollTimeout": 3000,
            "captionText": True,
            "includeComments": True,
            "language": "any"
        }
        
        try:
            logger.info(f"🚀 Starting Facebook scraper for {len(urls)} URLs...")
            run = self.actors['facebook']['client'].call(run_input=run_input)
            
            if run is None:
                return {'success': False, 'error': 'Facebook Actor run failed'}
            
            # Fetch results from dataset
            dataset = self.client.dataset(run['defaultDatasetId'])
            items = list(dataset.iterate_items())
            
            logger.info(f"✅ Facebook scraping completed: {len(items)} posts extracted")
            
            return {
                'success': True,
                'platform': 'facebook',
                'run_id': run['id'],
                'dataset_id': run['defaultDatasetId'],
                'total_items': len(items),
                'items': items,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Facebook scraping failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def run_youtube_scraper(self, search_queries: List[str] = None,
                                start_urls: List[str] = None,
                                max_results: int = 50) -> Dict[str, Any]:
        """Run YouTube scraper (streamers/youtube-scraper)"""
        if 'youtube' not in self.actors:
            return {'success': False, 'error': 'YouTube actor not configured'}
        
        # Use AIADMK keywords if no specific queries provided
        if not search_queries and not start_urls:
            search_queries = self.config.get_keywords_for_platform('youtube')[:5]
        
        run_input = {
            "searchQueries": search_queries or [],
            "startUrls": [{"url": url} for url in (start_urls or [])],
            "maxResults": max_results,
            "maxResultsShorts": 10,
            "maxResultStreams": 5,
            "subtitlesLanguage": "any",
            "subtitlesFormat": "srt",
            "includeComments": True,
            "maxComments": 20
        }
        
        try:
            logger.info(f"🚀 Starting YouTube scraper for {len(search_queries or [])} queries...")
            run = self.actors['youtube']['client'].call(run_input=run_input)
            
            if run is None:
                return {'success': False, 'error': 'YouTube Actor run failed'}
            
            dataset = self.client.dataset(run['defaultDatasetId'])
            items = list(dataset.iterate_items())
            
            logger.info(f"✅ YouTube scraping completed: {len(items)} videos extracted")
            
            return {
                'success': True,
                'platform': 'youtube',
                'run_id': run['id'],
                'dataset_id': run['defaultDatasetId'],
                'total_items': len(items),
                'items': items,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"YouTube scraping failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def run_instagram_scraper(self, direct_urls: List[str],
                                  max_posts: int = 50) -> Dict[str, Any]:
        """Run Instagram scraper (apify/instagram-scraper)"""
        if 'instagram' not in self.actors:
            return {'success': False, 'error': 'Instagram actor not configured'}
        
        run_input = {
            "directUrls": direct_urls,
            "resultsType": "posts",
            "resultsLimit": max_posts,
            "searchType": "hashtag",
            "searchLimit": 1,
            "addParentData": True,
            "includeComments": True,
            "maxComments": 20
        }
        
        try:
            logger.info(f"🚀 Starting Instagram scraper for {len(direct_urls)} URLs...")
            run = self.actors['instagram']['client'].call(run_input=run_input)
            
            if run is None:
                return {'success': False, 'error': 'Instagram Actor run failed'}
            
            dataset = self.client.dataset(run['defaultDatasetId'])
            items = list(dataset.iterate_items())
            
            logger.info(f"✅ Instagram scraping completed: {len(items)} posts extracted")
            
            return {
                'success': True,
                'platform': 'instagram',
                'run_id': run['id'],
                'dataset_id': run['defaultDatasetId'],
                'total_items': len(items),
                'items': items,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Instagram scraping failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def run_twitter_scraper(self, search_terms: List[str] = None,
                                twitter_handles: List[str] = None,
                                start_urls: List[str] = None,
                                max_items: int = 50) -> Dict[str, Any]:
        """Run Twitter scraper (apidojo/tweet-scraper)"""
        if 'twitter' not in self.actors:
            return {'success': False, 'error': 'Twitter actor not configured'}
        
        # Use AIADMK keywords if no specific terms provided
        if not search_terms and not twitter_handles and not start_urls:
            search_terms = self.config.get_keywords_for_platform('twitter')[:5]
        
        # Calculate date range (last 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        run_input = {
            "searchTerms": search_terms or [],
            "twitterHandles": twitter_handles or [],
            "startUrls": start_urls or [],
            "maxItems": max_items,
            "sort": "Latest",
            "tweetLanguage": "any",
            "minimumRetweets": 0,
            "minimumFavorites": 0,
            "minimumReplies": 0,
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d"),
            "includeReplies": True,
            "includeRetweets": True
        }
        
        try:
            logger.info(f"🚀 Starting Twitter scraper for {len(search_terms or [])} terms...")
            run = self.actors['twitter']['client'].call(run_input=run_input)
            
            if run is None:
                return {'success': False, 'error': 'Twitter Actor run failed'}
            
            dataset = self.client.dataset(run['defaultDatasetId'])
            items = list(dataset.iterate_items())
            
            logger.info(f"✅ Twitter scraping completed: {len(items)} tweets extracted")
            
            return {
                'success': True,
                'platform': 'twitter',
                'run_id': run['id'],
                'dataset_id': run['defaultDatasetId'],
                'total_items': len(items),
                'items': items,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Twitter scraping failed: {e}")
            return {'success': False, 'error': str(e)}
    
    async def run_reddit_scraper(self, start_urls: List[str] = None,
                               search_terms: List[str] = None,
                               max_posts: int = 50,
                               max_comments: int = 20) -> Dict[str, Any]:
        """Run Reddit scraper (trudax/reddit-scraper-lite)"""
        if 'reddit' not in self.actors:
            return {'success': False, 'error': 'Reddit actor not configured'}
        
        # If no URLs provided, search for AIADMK terms
        if not start_urls:
            aiadmk_terms = self.config.get_keywords_for_platform('reddit')[:3]
            # Convert to Reddit search URLs
            start_urls = [f"https://www.reddit.com/search/?q={term}" for term in aiadmk_terms]
        
        run_input = {
            "startUrls": [{"url": url} for url in start_urls],
            "skipComments": False,
            "skipUserPosts": False,
            "skipCommunity": False,
            "searchPosts": True,
            "searchComments": False,
            "searchCommunities": False,
            "searchUsers": False,
            "sort": "new",
            "includeNSFW": False,
            "maxItems": max_posts,
            "maxPostCount": max_posts,
            "maxComments": max_comments,
            "maxCommunitiesCount": 2,
            "maxUserCount": 2,
            "scrollTimeout": 40,
            "proxy": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            },
            "debugMode": False
        }
        
        try:
            logger.info(f"🚀 Starting Reddit scraper for {len(start_urls)} URLs...")
            run = self.actors['reddit']['client'].call(run_input=run_input)
            
            if run is None:
                return {'success': False, 'error': 'Reddit Actor run failed'}
            
            dataset = self.client.dataset(run['defaultDatasetId'])
            items = list(dataset.iterate_items())
            
            logger.info(f"✅ Reddit scraping completed: {len(items)} posts extracted")
            
            return {
                'success': True,
                'platform': 'reddit',
                'run_id': run['id'],
                'dataset_id': run['defaultDatasetId'],
                'total_items': len(items),
                'items': items,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Reddit scraping failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def normalize_facebook_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Normalize Facebook data to common format"""
        normalized = []
        
        for item in raw_data:
            normalized_item = {
                'post_id': item.get('postId') or item.get('id'),
                'url': item.get('postUrl') or item.get('url'),
                'text': item.get('text') or item.get('caption'),
                'author_name': item.get('authorName') or item.get('user', {}).get('name'),
                'author_url': item.get('authorUrl') or item.get('user', {}).get('url'),
                'published_date': item.get('publishedTime') or item.get('createdAt'),
                'likes_count': item.get('likesCount', 0),
                'comments_count': item.get('commentsCount', 0),
                'shares_count': item.get('sharesCount', 0),
                'comments': item.get('comments', []),
                'images': item.get('images', []),
                'video_url': item.get('videoUrl'),
                'platform': 'facebook',
                'raw_data': item
            }
            normalized.append(normalized_item)
        
        return normalized
    
    def normalize_youtube_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Normalize YouTube data to common format"""
        normalized = []
        
        for item in raw_data:
            normalized_item = {
                'video_id': item.get('id') or item.get('videoId'),
                'url': item.get('url') or f"https://www.youtube.com/watch?v={item.get('id')}",
                'title': item.get('title'),
                'description': item.get('description'),
                'channel_name': item.get('channelName') or item.get('author'),
                'channel_url': item.get('channelUrl'),
                'published_date': item.get('uploadDate') or item.get('publishedAt'),
                'views_count': item.get('viewCount', 0),
                'likes_count': item.get('likeCount', 0),
                'comments_count': item.get('commentCount', 0),
                'duration': item.get('duration'),
                'comments': item.get('comments', []),
                'subtitles': item.get('subtitles'),
                'platform': 'youtube',
                'raw_data': item
            }
            normalized.append(normalized_item)
        
        return normalized
    
    def normalize_instagram_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Normalize Instagram data to common format"""
        normalized = []
        
        for item in raw_data:
            normalized_item = {
                'post_id': item.get('id') or item.get('shortcode'),
                'url': item.get('url') or f"https://www.instagram.com/p/{item.get('shortcode')}/",
                'text': item.get('caption') or item.get('text'),
                'author_name': item.get('ownerUsername') or item.get('username'),
                'author_url': f"https://www.instagram.com/{item.get('ownerUsername', '')}/",
                'published_date': item.get('timestamp') or item.get('createdAt'),
                'likes_count': item.get('likesCount', 0),
                'comments_count': item.get('commentsCount', 0),
                'comments': item.get('comments', []),
                'images': item.get('images', []),
                'video_url': item.get('videoUrl'),
                'hashtags': item.get('hashtags', []),
                'platform': 'instagram',
                'raw_data': item
            }
            normalized.append(normalized_item)
        
        return normalized
    
    def normalize_twitter_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Normalize Twitter data to common format"""
        normalized = []
        
        for item in raw_data:
            normalized_item = {
                'tweet_id': item.get('id') or item.get('tweetId'),
                'url': item.get('url') or item.get('tweetUrl'),
                'text': item.get('text') or item.get('fullText'),
                'author_name': item.get('authorName') or item.get('user', {}).get('name'),
                'author_username': item.get('authorUsername') or item.get('user', {}).get('username'),
                'author_url': f"https://twitter.com/{item.get('authorUsername', '')}",
                'published_date': item.get('createdAt') or item.get('publishedAt'),
                'retweets_count': item.get('retweetCount', 0),
                'likes_count': item.get('likeCount', 0),
                'replies_count': item.get('replyCount', 0),
                'quotes_count': item.get('quoteCount', 0),
                'hashtags': item.get('hashtags', []),
                'mentions': item.get('mentions', []),
                'images': item.get('images', []),
                'video_url': item.get('videoUrl'),
                'is_retweet': item.get('isRetweet', False),
                'platform': 'twitter',
                'raw_data': item
            }
            normalized.append(normalized_item)
        
        return normalized
    
    def normalize_reddit_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Normalize Reddit data to common format"""
        normalized = []
        
        for item in raw_data:
            normalized_item = {
                'post_id': item.get('id') or item.get('postId'),
                'url': item.get('url') or item.get('postUrl'),
                'title': item.get('title'),
                'text': item.get('text') or item.get('selftext'),
                'author_name': item.get('author') or item.get('authorName'),
                'subreddit': item.get('subreddit'),
                'subreddit_url': f"https://www.reddit.com/r/{item.get('subreddit', '')}/",
                'published_date': item.get('createdAt') or item.get('created'),
                'upvotes': item.get('upvotes', 0),
                'downvotes': item.get('downvotes', 0),
                'comments_count': item.get('numberOfComments', 0),
                'comments': item.get('comments', []),
                'awards': item.get('awards', []),
                'platform': 'reddit',
                'raw_data': item
            }
            normalized.append(normalized_item)
        
        return normalized
    
    async def get_actor_status(self, platform: str) -> Dict[str, Any]:
        """Get status of the last run for a specific platform"""
        if platform not in self.actors:
            return {'success': False, 'error': f'Platform {platform} not configured'}
        
        try:
            # Get last runs for the actor
            actor_runs = self.client.actor(self.actors[platform]['id']).runs()
            runs_list = list(actor_runs.list()['items'])
            
            if not runs_list:
                return {'success': True, 'status': 'no_runs', 'runs': []}
            
            latest_run = runs_list[0]
            return {
                'success': True,
                'platform': platform,
                'latest_run': {
                    'id': latest_run['id'],
                    'status': latest_run['status'],
                    'startedAt': latest_run['startedAt'],
                    'finishedAt': latest_run.get('finishedAt'),
                    'stats': latest_run.get('stats', {}),
                    'defaultDatasetId': latest_run.get('defaultDatasetId')
                },
                'total_runs': len(runs_list)
            }
            
        except Exception as e:
            logger.error(f"Failed to get actor status for {platform}: {e}")
            return {'success': False, 'error': str(e)}

# Global service instance
apify_service = None

def get_apify_service():
    """Get global Apify service instance"""
    global apify_service
    if not apify_service:
        apify_service = ApifyService()
    return apify_service

# Test function
async def test_apify_service():
    """Test Apify service functionality"""
    try:
        service = get_apify_service()
        
        logger.info(f"✅ Apify service test: {len(service.actors)} actors configured")
        
        # Test actor status for each platform
        for platform in service.actors.keys():
            status = await service.get_actor_status(platform)
            if status['success']:
                logger.info(f"✅ {platform.upper()} actor status: {status.get('latest_run', {}).get('status', 'no_runs')}")
            else:
                logger.warning(f"⚠️  {platform.upper()} actor status check failed: {status.get('error')}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Apify service test failed: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_apify_service())