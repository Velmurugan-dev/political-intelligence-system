#!/usr/bin/env python3
"""
Stage 2: Engagement Engine
Scrapes engagement metrics from discovered URLs and stores enriched data
"""

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import re

import sys
sys.path.append('..')
from database import get_database
from services.apify_service import get_apify_service

logger = logging.getLogger(__name__)

class EngagementEngine:
    """Stage 2: Engagement Scraping Engine"""
    
    def __init__(self):
        self.db = None
        self.apify = None
        
        self.stats = {
            'total_urls_processed': 0,
            'total_engagement_extracted': 0,
            'total_comments_extracted': 0,
            'failed_extractions': 0,
            'errors': []
        }
        
        # Apify field mapping based on analysis of Results folder
        self.apify_field_mappings = {
            'facebook': {
                'content_id': 'post_id',
                'url': 'url',
                'title': 'text',  # Facebook uses 'text' field
                'content': 'text',
                'author': 'author',
                'published_at': 'created_time',
                'likes_count': 'likes',
                'shares_count': 'shares',
                'comments_count': 'comments',
                'views_count': 'video_view_count',
                'media_urls': 'attachments'
            },
            'youtube': {
                'content_id': 'id',
                'url': 'url',
                'title': 'title',
                'content': 'description',
                'author': 'channelName',
                'author_id': 'channelUrl',
                'published_at': 'date',
                'views_count': 'viewCount',
                'likes_count': 'likes',
                'comments_count': 'numberOfComments',
                'subscriber_count': 'numberOfSubscribers',
                'duration': 'duration'
            },
            'instagram': {
                'content_id': 'id',
                'url': 'url',
                'title': 'caption',
                'content': 'caption',
                'author': 'ownerUsername',
                'author_followers': 'ownerFollowersCount',
                'published_at': 'timestamp',
                'likes_count': 'likesCount',
                'comments_count': 'commentsCount',
                'media_urls': 'displayUrl'
            },
            'twitter': {
                'content_id': 'id',
                'url': 'url',
                'title': 'text',
                'content': 'text',
                'author': 'author_username',
                'author_id': 'author_id',
                'published_at': 'created_at',
                'likes_count': 'likes',
                'retweets_count': 'retweets',
                'quotes_count': 'quotes',
                'replies_count': 'replies',
                'views_count': 'views'
            },
            'reddit': {
                'content_id': 'id',
                'url': 'url',
                'title': 'title',
                'content': 'body',
                'author': 'username',
                'published_at': 'createdAt',
                'upvotes': 'upVotes',
                'comments_count': 'numberOfComments',
                'upvote_ratio': 'upVoteRatio'
            }
        }
    
    async def initialize(self):
        """Initialize engagement engine"""
        try:
            self.db = await get_database()
            self.apify = await get_apify_service()
            
            logger.info("✅ Engagement Engine initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Engagement Engine initialization failed: {e}")
            return False
    
    async def run_engagement_cycle(self, competitor_ids: List[int] = None, platform_ids: List[int] = None, limit: int = 100) -> Dict[str, Any]:
        """Run complete engagement extraction cycle"""
        
        logger.info("⚡ Starting Engagement Cycle...")
        cycle_start = datetime.now()
        
        try:
            # Reset stats
            self.stats = {
                'total_urls_processed': 0,
                'total_engagement_extracted': 0,
                'total_comments_extracted': 0,
                'failed_extractions': 0,
                'errors': []
            }
            
            # Get pending URLs from stage_results
            pending_urls = await self.get_pending_urls(competitor_ids, platform_ids, limit)
            logger.info(f"📋 Found {len(pending_urls)} URLs pending engagement extraction")
            
            if not pending_urls:
                return {
                    'success': True,
                    'message': 'No pending URLs for engagement extraction',
                    'stats': self.stats
                }
            
            # Group URLs by platform for batch processing
            platform_groups = {}
            for url_data in pending_urls:
                platform = url_data['platform_name']
                if platform not in platform_groups:
                    platform_groups[platform] = []
                platform_groups[platform].append(url_data)
            
            # Process each platform group
            platform_tasks = []
            for platform, urls in platform_groups.items():
                platform_tasks.append(self.process_platform_batch(platform, urls))
            
            if platform_tasks:
                platform_results = await asyncio.gather(*platform_tasks, return_exceptions=True)
                self.process_platform_results(platform_results)
            
            cycle_duration = (datetime.now() - cycle_start).total_seconds()
            
            logger.info(f"✅ Engagement Cycle completed in {cycle_duration:.1f}s")
            logger.info(f"📊 Stats: {self.stats['total_urls_processed']} URLs processed, {self.stats['total_engagement_extracted']} with engagement data")
            
            return {
                'success': True,
                'duration_seconds': cycle_duration,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Engagement Cycle failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_pending_urls(self, competitor_ids: List[int] = None, platform_ids: List[int] = None, limit: int = 100) -> List[Dict]:
        """Get URLs pending engagement extraction"""
        
        query = """
        SELECT sr.*, c.name as competitor_name, p.name as platform_name
        FROM stage_results sr
        JOIN competitors c ON sr.competitor_id = c.competitor_id
        JOIN platforms p ON sr.platform_id = p.platform_id
        WHERE sr.status = 'pending' 
        AND sr.retry_count < sr.max_retries
        AND c.is_active = TRUE 
        AND p.is_active = TRUE
        """
        
        params = []
        if competitor_ids:
            query += f" AND sr.competitor_id = ANY($1)"
            params.append(competitor_ids)
        
        if platform_ids:
            param_num = len(params) + 1
            query += f" AND sr.platform_id = ANY(${param_num})"
            params.append(platform_ids)
        
        query += f" ORDER BY sr.priority DESC, sr.inserted_at ASC LIMIT ${len(params) + 1}"
        params.append(limit)
        
        results = await self.db.execute_query(query, tuple(params))
        return [dict(row) for row in results]
    
    async def process_platform_batch(self, platform: str, urls: List[Dict]) -> Dict[str, Any]:
        """Process a batch of URLs for a specific platform"""
        
        logger.info(f"🔄 Processing {len(urls)} {platform} URLs...")
        
        try:
            if platform in self.apify_field_mappings and self.apify:
                # Use Apify for scraping
                return await self.process_with_apify(platform, urls)
            else:
                # Fallback to browser automation
                return await self.process_with_browser(platform, urls)
                
        except Exception as e:
            logger.error(f"Platform batch processing failed for {platform}: {e}")
            return {
                'platform': platform,
                'success': False,
                'error': str(e),
                'urls_processed': 0
            }
    
    async def process_with_apify(self, platform: str, urls: List[Dict]) -> Dict[str, Any]:
        """Process URLs using Apify actors"""
        
        platform_config = self.apify_field_mappings[platform]
        urls_processed = 0
        urls_successful = 0
        
        try:
            # Prepare URLs for Apify
            url_list = [url_data['url'] for url_data in urls]
            
            # Configure Apify input based on platform
            apify_input = self.prepare_apify_input(platform, url_list)
            
            # Run Apify actor
            apify_results = await self.apify.run_actor(platform, apify_input)
            
            if apify_results and 'items' in apify_results:
                # Process each result
                for apify_item in apify_results['items']:
                    try:
                        # Find corresponding stage_result
                        item_url = apify_item.get('url', '')
                        stage_data = self.find_stage_data_by_url(urls, item_url)
                        
                        if stage_data:
                            # Extract engagement data
                            engagement_data = self.extract_engagement_data(apify_item, platform_config, stage_data)
                            
                            # Save to final_results
                            await self.save_final_result(engagement_data)
                            
                            # Update stage_results status
                            await self.update_stage_status(stage_data['stage_id'], 'scraped')
                            
                            urls_successful += 1
                            self.stats['total_engagement_extracted'] += 1
                            
                        urls_processed += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to process Apify item: {e}")
                        self.stats['failed_extractions'] += 1
            
            self.stats['total_urls_processed'] += urls_processed
            
            return {
                'platform': platform,
                'success': True,
                'urls_processed': urls_processed,
                'urls_successful': urls_successful,
                'method': 'apify'
            }
            
        except Exception as e:
            logger.error(f"Apify processing failed for {platform}: {e}")
            self.stats['errors'].append(f"Apify {platform}: {e}")
            
            # Mark URLs as failed
            for url_data in urls:
                await self.update_stage_status(url_data['stage_id'], 'failed', str(e))
            
            return {
                'platform': platform,
                'success': False,
                'error': str(e),
                'urls_processed': 0
            }
    
    def prepare_apify_input(self, platform: str, url_list: List[str]) -> Dict[str, Any]:
        """Prepare Apify input based on platform"""
        
        if platform == 'facebook':
            return {
                'startUrls': [{'url': url} for url in url_list],
                'maxPosts': len(url_list),
                'maxComments': 20,
                'maxReplies': 5,
                'scrapeComments': True,
                'scrapeReactions': True
            }
        
        elif platform == 'youtube':
            return {
                'startUrls': url_list,
                'maxVideos': len(url_list),
                'maxComments': 50,
                'includeVideoDetails': True,
                'includeChannelInfo': True
            }
        
        elif platform == 'instagram':
            return {
                'directUrls': url_list,
                'resultsLimit': len(url_list),
                'includeComments': True,
                'maxComments': 30,
                'addParentData': True
            }
        
        elif platform == 'twitter':
            return {
                'urls': url_list,
                'maxTweets': len(url_list),
                'includeReplies': True,
                'maxReplies': 20,
                'tweetLanguage': 'any'
            }
        
        elif platform == 'reddit':
            return {
                'startUrls': url_list,
                'maxPosts': len(url_list),
                'maxComments': 50,
                'sortBy': 'best',
                'includePostContent': True
            }
        
        else:
            return {'urls': url_list}
    
    def extract_engagement_data(self, apify_item: Dict, field_mapping: Dict, stage_data: Dict) -> Dict[str, Any]:
        """Extract engagement data from Apify response"""
        
        engagement_data = {
            'stage_id': stage_data['stage_id'],
            'competitor_id': stage_data['competitor_id'],
            'platform_id': stage_data['platform_id'],
            'url': stage_data['url'],
            'content_id': apify_item.get(field_mapping.get('content_id', 'id'), ''),
            'title': apify_item.get(field_mapping.get('title', 'title'), stage_data.get('title')),
            'content': apify_item.get(field_mapping.get('content', 'content'), ''),
            'author': apify_item.get(field_mapping.get('author', 'author'), stage_data.get('author')),
            'author_id': apify_item.get(field_mapping.get('author_id', 'author_id'), stage_data.get('author_id')),
            'published_at': self.parse_engagement_date(apify_item.get(field_mapping.get('published_at', 'published_at'))),
            
            # Engagement metrics
            'views_count': self.safe_int(apify_item.get(field_mapping.get('views_count', 'views'), 0)),
            'likes_count': self.safe_int(apify_item.get(field_mapping.get('likes_count', 'likes'), 0)),
            'shares_count': self.safe_int(apify_item.get(field_mapping.get('shares_count', 'shares'), 0)),
            'comments_count': self.safe_int(apify_item.get(field_mapping.get('comments_count', 'comments'), 0)),
            'reactions_count': self.safe_int(apify_item.get('reactions_count', 0)),
            
            # Platform-specific metrics
            'retweets_count': self.safe_int(apify_item.get('retweets_count', 0)),
            'quotes_count': self.safe_int(apify_item.get('quotes_count', 0)),
            'upvotes': self.safe_int(apify_item.get('upVotes', 0)),
            'downvotes': self.safe_int(apify_item.get('downvotes', 0)),
            'upvote_ratio': self.safe_float(apify_item.get('upVoteRatio', 0)),
            'subscriber_count': self.safe_int(apify_item.get(field_mapping.get('subscriber_count', 'subscribers'), 0)),
            'author_followers': self.safe_int(apify_item.get(field_mapping.get('author_followers', 'followers'), 0)),
            
            # Content analysis
            'word_count': len((apify_item.get(field_mapping.get('content', 'content'), '') or '').split()),
            'language': stage_data.get('language', 'ta'),
            'content_type': stage_data.get('content_type', 'post'),
            'hashtags': self.extract_hashtags(apify_item.get(field_mapping.get('content', 'content'), '')),
            'mentions': self.extract_mentions(apify_item.get(field_mapping.get('content', 'content'), '')),
            'media_urls': self.extract_media_urls(apify_item, field_mapping),
            
            # Comments data
            'top_comments': self.extract_top_comments(apify_item),
            'comments_json': apify_item.get('comments', []),
            
            # Raw data
            'raw_apify_data': apify_item,
            'scraping_method': 'apify',
            'scraped_at': datetime.now()
        }
        
        # Calculate engagement metrics
        engagement_data['engagement_rate'] = self.calculate_engagement_rate(engagement_data)
        engagement_data['viral_score'] = self.calculate_viral_score(engagement_data)
        engagement_data['importance_score'] = self.calculate_importance_score(engagement_data)
        
        return engagement_data
    
    def calculate_engagement_rate(self, data: Dict) -> float:
        """Calculate engagement rate"""
        try:
            views = data.get('views_count', 0) or data.get('author_followers', 0) or 1
            engagement = (data.get('likes_count', 0) + 
                         data.get('shares_count', 0) + 
                         data.get('comments_count', 0) +
                         data.get('retweets_count', 0))
            
            return min(engagement / views if views > 0 else 0, 1.0)
        except:
            return 0.0
    
    def calculate_viral_score(self, data: Dict) -> float:
        """Calculate viral potential score"""
        try:
            # Weighted score based on shares/retweets and engagement velocity
            shares = data.get('shares_count', 0) + data.get('retweets_count', 0)
            comments = data.get('comments_count', 0)
            likes = data.get('likes_count', 0)
            
            # Higher weight for shares (viral indicator)
            viral_score = (shares * 3 + comments * 2 + likes * 1) / 1000
            
            return min(viral_score, 10.0)  # Cap at 10
        except:
            return 0.0
    
    def calculate_importance_score(self, data: Dict) -> float:
        """Calculate content importance score"""
        try:
            # Factors: engagement, author influence, content quality
            engagement_factor = data.get('engagement_rate', 0) * 5
            author_factor = min((data.get('author_followers', 0) or 0) / 100000, 2)  # Cap at 2M followers = score 2
            content_factor = min(data.get('word_count', 0) / 100, 1)  # Longer content gets higher score
            
            return min(engagement_factor + author_factor + content_factor, 10.0)
        except:
            return 0.0
    
    def extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from text"""
        if not text:
            return []
        
        hashtags = re.findall(r'#[\w\u0B80-\u0BFF]+', text)  # Supports Tamil Unicode
        return list(set(hashtags))  # Remove duplicates
    
    def extract_mentions(self, text: str) -> List[str]:
        """Extract mentions from text"""
        if not text:
            return []
        
        mentions = re.findall(r'@[\w\u0B80-\u0BFF]+', text)  # Supports Tamil Unicode
        return list(set(mentions))
    
    def extract_media_urls(self, apify_item: Dict, field_mapping: Dict) -> List[str]:
        """Extract media URLs from Apify response"""
        media_urls = []
        
        # Try various fields where media might be stored
        media_fields = ['media_urls', 'attachments', 'displayUrl', 'thumbnail', 'images']
        
        for field in media_fields:
            if field in apify_item:
                value = apify_item[field]
                if isinstance(value, list):
                    media_urls.extend([item for item in value if isinstance(item, str) and item.startswith('http')])
                elif isinstance(value, str) and value.startswith('http'):
                    media_urls.append(value)
        
        return list(set(media_urls))  # Remove duplicates
    
    def extract_top_comments(self, apify_item: Dict, limit: int = 10) -> List[Dict]:
        """Extract top comments from Apify response"""
        comments = apify_item.get('comments', [])
        
        if not comments:
            return []
        
        # Sort by engagement (likes, replies) and take top comments
        try:
            sorted_comments = sorted(
                comments,
                key=lambda c: (c.get('likes', 0) + c.get('replies', 0)),
                reverse=True
            )
            
            return sorted_comments[:limit]
        except:
            return comments[:limit] if isinstance(comments, list) else []
    
    async def save_final_result(self, engagement_data: Dict):
        """Save engagement data to final_results table"""
        
        # Mark previous results as not latest
        await self.db.execute_query("""
            UPDATE final_results 
            SET is_latest = FALSE 
            WHERE stage_id = $1
        """, (engagement_data['stage_id'],))
        
        # Insert new result
        insert_query = """
        INSERT INTO final_results (
            stage_id, competitor_id, platform_id, url, content_id,
            title, content, author, author_id, author_followers, published_at,
            views_count, likes_count, shares_count, comments_count, reactions_count,
            retweets_count, quotes_count, upvotes, downvotes, upvote_ratio, subscriber_count,
            word_count, language, content_type, hashtags, mentions, media_urls,
            engagement_rate, viral_score, importance_score,
            top_comments, comments_json, raw_apify_data,
            scraping_method, scraped_at, is_latest, snapshot_number
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
            $31, $32, $33, $34, $35, $36, TRUE, 1
        )
        RETURNING result_id
        """
        
        result = await self.db.execute_query(insert_query, (
            engagement_data['stage_id'],
            engagement_data['competitor_id'],
            engagement_data['platform_id'],
            engagement_data['url'],
            engagement_data['content_id'],
            engagement_data['title'],
            engagement_data['content'],
            engagement_data['author'],
            engagement_data['author_id'],
            engagement_data['author_followers'],
            engagement_data['published_at'],
            engagement_data['views_count'],
            engagement_data['likes_count'],
            engagement_data['shares_count'],
            engagement_data['comments_count'],
            engagement_data['reactions_count'],
            engagement_data['retweets_count'],
            engagement_data['quotes_count'],
            engagement_data['upvotes'],
            engagement_data['downvotes'],
            engagement_data['upvote_ratio'],
            engagement_data['subscriber_count'],
            engagement_data['word_count'],
            engagement_data['language'],
            engagement_data['content_type'],
            engagement_data['hashtags'],
            engagement_data['mentions'],
            engagement_data['media_urls'],
            engagement_data['engagement_rate'],
            engagement_data['viral_score'],
            engagement_data['importance_score'],
            json.dumps(engagement_data['top_comments']),
            json.dumps(engagement_data['comments_json']),
            json.dumps(engagement_data['raw_apify_data']),
            engagement_data['scraping_method'],
            engagement_data['scraped_at']
        ))
        
        # Track comment count
        if engagement_data.get('comments_json'):
            self.stats['total_comments_extracted'] += len(engagement_data['comments_json'])
        
        return result[0]['result_id'] if result else None
    
    async def update_stage_status(self, stage_id: int, status: str, error_message: str = None):
        """Update stage_results status"""
        
        if status == 'failed':
            await self.db.execute_query("""
                UPDATE stage_results 
                SET status = $1, error_message = $2, retry_count = retry_count + 1, processed_at = NOW()
                WHERE stage_id = $3
            """, (status, error_message, stage_id))
        else:
            await self.db.execute_query("""
                UPDATE stage_results 
                SET status = $1, processed_at = NOW()
                WHERE stage_id = $2
            """, (status, stage_id))
    
    def find_stage_data_by_url(self, stage_urls: List[Dict], apify_url: str) -> Optional[Dict]:
        """Find stage_result data by matching URL"""
        for stage_data in stage_urls:
            if stage_data['url'] == apify_url:
                return stage_data
        
        # Try fuzzy matching (remove query parameters)
        clean_apify_url = apify_url.split('?')[0]
        for stage_data in stage_urls:
            clean_stage_url = stage_data['url'].split('?')[0]
            if clean_stage_url == clean_apify_url:
                return stage_data
        
        return None
    
    async def process_with_browser(self, platform: str, urls: List[Dict]) -> Dict[str, Any]:
        """Fallback browser automation processing"""
        
        logger.info(f"🌐 Using browser automation for {platform}")
        
        urls_processed = 0
        urls_successful = 0
        
        for url_data in urls:
            try:
                # Basic content extraction (placeholder for browser automation)
                engagement_data = {
                    'stage_id': url_data['stage_id'],
                    'competitor_id': url_data['competitor_id'],
                    'platform_id': url_data['platform_id'],
                    'url': url_data['url'],
                    'content_id': url_data['url'],  # Use URL as fallback ID
                    'title': url_data.get('title', 'Unknown Title'),
                    'content': url_data.get('snippet', ''),
                    'author': url_data.get('author', 'Unknown Author'),
                    'published_at': url_data.get('published_at'),
                    'scraping_method': 'browser_automation',
                    'scraped_at': datetime.now(),
                    
                    # Default values for browser automation
                    'views_count': 0,
                    'likes_count': 0,
                    'shares_count': 0,
                    'comments_count': 0,
                    'engagement_rate': 0.0,
                    'viral_score': 0.0,
                    'importance_score': 1.0,
                    'word_count': len((url_data.get('snippet', '') or '').split()),
                    'language': url_data.get('language', 'ta'),
                    'content_type': url_data.get('content_type', 'post'),
                    'hashtags': [],
                    'mentions': [],
                    'media_urls': [],
                    'top_comments': [],
                    'comments_json': [],
                    'raw_apify_data': {'browser_automation': True}
                }
                
                # Save result
                await self.save_final_result(engagement_data)
                await self.update_stage_status(url_data['stage_id'], 'scraped')
                
                urls_successful += 1
                
            except Exception as e:
                logger.error(f"Browser automation failed for {url_data['url']}: {e}")
                await self.update_stage_status(url_data['stage_id'], 'failed', str(e))
            
            urls_processed += 1
        
        self.stats['total_urls_processed'] += urls_processed
        
        return {
            'platform': platform,
            'success': True,
            'urls_processed': urls_processed,
            'urls_successful': urls_successful,
            'method': 'browser_automation'
        }
    
    def process_platform_results(self, results: List):
        """Process platform batch results"""
        for result in results:
            if isinstance(result, Exception):
                self.stats['errors'].append(f"Platform batch failed: {result}")
    
    def safe_int(self, value: Any) -> int:
        """Safely convert value to integer"""
        try:
            if isinstance(value, str):
                # Remove commas and other formatting
                value = re.sub(r'[^\d]', '', value)
                return int(value) if value else 0
            return int(value) if value is not None else 0
        except:
            return 0
    
    def safe_float(self, value: Any) -> float:
        """Safely convert value to float"""
        try:
            return float(value) if value is not None else 0.0
        except:
            return 0.0
    
    def parse_engagement_date(self, date_value: Any) -> Optional[datetime]:
        """Parse date from Apify response"""
        if not date_value:
            return None
        
        try:
            if isinstance(date_value, str):
                # Handle ISO format
                if 'T' in date_value:
                    return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                # Handle other formats
                else:
                    return datetime.strptime(date_value, '%Y-%m-%d')
            return date_value
        except:
            return None
    
    async def get_engagement_stats(self) -> Dict[str, Any]:
        """Get engagement engine statistics"""
        
        stats_query = """
        SELECT 
            c.name as competitor,
            p.name as platform,
            COUNT(*) as total_content,
            SUM(views_count) as total_views,
            SUM(likes_count) as total_likes,
            SUM(shares_count + retweets_count) as total_shares,
            SUM(comments_count) as total_comments,
            AVG(engagement_rate) as avg_engagement_rate,
            MAX(scraped_at) as last_scraped
        FROM final_results fr
        JOIN competitors c ON fr.competitor_id = c.competitor_id
        JOIN platforms p ON fr.platform_id = p.platform_id
        WHERE fr.scraped_at >= NOW() - INTERVAL '24 hours'
        AND fr.is_latest = TRUE
        GROUP BY c.name, p.name
        ORDER BY total_content DESC
        """
        
        try:
            results = await self.db.execute_query(stats_query)
            return {
                'recent_stats': [dict(row) for row in results],
                'engine_stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get engagement stats: {e}")
            return {'error': str(e)}

# Global instance
engagement_engine = None

async def get_engagement_engine():
    """Get global engagement engine instance"""
    global engagement_engine
    if not engagement_engine:
        engagement_engine = EngagementEngine()
        await engagement_engine.initialize()
    return engagement_engine

async def run_engagement_cycle(competitor_ids: List[int] = None, platform_ids: List[int] = None, limit: int = 100) -> Dict[str, Any]:
    """Run engagement cycle (can be called from Celery)"""
    engine = await get_engagement_engine()
    return await engine.run_engagement_cycle(competitor_ids, platform_ids, limit)

if __name__ == "__main__":
    async def test_engagement():
        engine = EngagementEngine()
        if await engine.initialize():
            result = await engine.run_engagement_cycle(limit=5)
            print(f"Engagement test result: {result}")
    
    asyncio.run(test_engagement())