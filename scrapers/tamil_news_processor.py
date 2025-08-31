#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Tamil News Processor
Tamil news content processing using Firecrawl integration
"""

import os
import logging
import asyncio
from typing import Dict, List, Optional, Any
import json
from datetime import datetime
from urllib.parse import urlparse

import sys
sys.path.append('..')
from services.firecrawl_service import get_firecrawl_service
from config import get_config
from database import get_database

logger = logging.getLogger(__name__)

class TamilNewsProcessor:
    """Tamil news processor for AIADMK political content"""
    
    def __init__(self):
        self.config = get_config()
        self.firecrawl_service = None
        self.db = None
        
        # Priority Tamil news sources
        self.priority_sources = {
            'dailythanthi.com': {'weight': 0.9, 'language': 'tamil'},
            'dinamalar.com': {'weight': 0.85, 'language': 'tamil'},
            'thehindu.com': {'weight': 0.8, 'language': 'english'},
            'puthiyathalaimurai.com': {'weight': 0.8, 'language': 'tamil'},
            'polimer.tv': {'weight': 0.75, 'language': 'tamil'},
            'thanthitv.com': {'weight': 0.7, 'language': 'tamil'},
            'news18.com': {'weight': 0.65, 'language': 'mixed'},
            'vikatan.com': {'weight': 0.6, 'language': 'tamil'}
        }
        
        logger.info("✅ Tamil News processor initialized")
    
    async def get_services(self):
        """Get required services"""
        if not self.firecrawl_service:
            self.firecrawl_service = await get_firecrawl_service()
        if not self.db:
            self.db = await get_database()
        return self.firecrawl_service, self.db
    
    async def discover_tamil_news_urls(self) -> List[str]:
        """Discover Tamil news URLs from database queue"""
        _, db = await self.get_services()
        
        # Get Tamil news URLs from database queue
        pending_urls = await db.get_pending_urls('tamil_news', limit=30)
        news_urls = [record['url'] for record in pending_urls 
                    if any(domain in record['url'] for domain in self.priority_sources.keys())]
        
        logger.info(f"📍 Discovered {len(news_urls)} Tamil news URLs to process")
        return news_urls
    
    async def process_tamil_news_articles(self, urls: List[str] = None) -> Dict[str, Any]:
        """Process Tamil news articles using Firecrawl service"""
        if not urls:
            urls = await self.discover_tamil_news_urls()
        
        if not urls:
            return {
                'success': False,
                'error': 'No Tamil news URLs to process',
                'total_articles': 0
            }
        
        firecrawl_service, _ = await self.get_services()
        
        try:
            # Process URLs using Firecrawl
            processing_results = await firecrawl_service.process_tamil_news_urls(urls)
            
            if not processing_results['articles']:
                return {
                    'success': True,
                    'message': 'No articles extracted',
                    'total_urls': processing_results['total_urls'],
                    'articles': []
                }
            
            # Filter and enhance articles for AIADMK content
            enhanced_articles = []
            for article in processing_results['articles']:
                enhanced_article = await self.enhance_article_data(article)
                if enhanced_article['is_aiadmk_related']:
                    enhanced_articles.append(enhanced_article)
            
            return {
                'success': True,
                'platform': 'tamil_news',
                'total_urls_processed': processing_results['total_urls'],
                'total_articles_extracted': len(processing_results['articles']),
                'aiadmk_articles': len(enhanced_articles),
                'articles': enhanced_articles,
                'processing_errors': processing_results.get('errors', []),
                'processed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Tamil news processing failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_articles': 0
            }
    
    async def enhance_article_data(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance article data with AIADMK analysis"""
        aiadmk_keywords = (self.config.aiadmk_keywords['tamil'] + 
                          self.config.aiadmk_keywords['english'])
        
        # Extract text for analysis
        title = article.get('title', '') or ''
        content = article.get('content', '') or ''
        combined_text = (title + ' ' + content).lower()
        
        # Check AIADMK relevance
        aiadmk_mentions = {}
        total_mentions = 0
        
        for keyword in aiadmk_keywords:
            count = combined_text.count(keyword.lower())
            if count > 0:
                aiadmk_mentions[keyword] = count
                total_mentions += count
        
        is_aiadmk_related = total_mentions > 0
        
        # Calculate relevance scores
        relevance_score = min(total_mentions * 10, 100)  # Cap at 100
        
        # Determine article priority based on source and content
        source_domain = urlparse(article.get('url', '')).netloc
        source_info = self.priority_sources.get(source_domain, {'weight': 0.5, 'language': 'unknown'})
        
        priority_score = (
            relevance_score * source_info['weight'] +
            (50 if 'அதிமுக' in title.lower() or 'aiadmk' in title.lower() else 0) +
            (article.get('word_count', 0) * 0.01)  # Longer articles get slight boost
        )
        
        # Classify article type
        article_type = self._classify_article_type(title, content)
        
        # Enhanced article data
        enhanced = {
            **article,  # Original article data
            'is_aiadmk_related': is_aiadmk_related,
            'aiadmk_mentions': aiadmk_mentions,
            'total_mentions': total_mentions,
            'relevance_score': relevance_score,
            'priority_score': priority_score,
            'source_domain': source_domain,
            'source_weight': source_info['weight'],
            'article_type': article_type,
            'processing_metadata': {
                'enhanced_at': datetime.now().isoformat(),
                'keywords_checked': len(aiadmk_keywords)
            }
        }
        
        return enhanced
    
    def _classify_article_type(self, title: str, content: str) -> str:
        """Classify article type based on content"""
        text = (title + ' ' + content).lower()
        
        if any(word in text for word in ['breaking', 'urgent', 'செய்தி வேகம்', 'அவசர']):
            return 'breaking_news'
        elif any(word in text for word in ['interview', 'பேட்டி', 'கேள்வி பதில்']):
            return 'interview'
        elif any(word in text for word in ['analysis', 'opinion', 'கருத்து', 'பகுப்பாய்வு']):
            return 'analysis'
        elif any(word in text for word in ['election', 'தேர்தல்', 'campaign', 'பிரச்சாரம்']):
            return 'election_news'
        elif any(word in text for word in ['statement', 'அறிக்கை', 'press release', 'செய்திக்குறிப்பு']):
            return 'official_statement'
        elif any(word in text for word in ['meeting', 'கூட்டம்', 'conference', 'மாநாடு']):
            return 'event_coverage'
        else:
            return 'general_news'
    
    async def store_tamil_news_data(self, articles: List[Dict]) -> Dict[str, Any]:
        """Store Tamil news articles in database"""
        _, db = await self.get_services()
        
        storage_results = {
            'total_articles': len(articles),
            'stored_articles': 0,
            'updated_articles': 0,
            'errors': []
        }
        
        for article in articles:
            try:
                # Prepare data for database insertion
                db_data = {
                    'article_url': article['url'],
                    'title': article['title'],
                    'content': article['content'][:10000],  # Truncate very long content
                    'author': article.get('author'),
                    'published_date': self._parse_date(article.get('published_date')),
                    'language': article.get('language', 'tamil'),
                    'word_count': article.get('word_count', 0),
                    'source_domain': article['source_domain'],
                    'article_type': article['article_type'],
                    'relevance_score': article['relevance_score'],
                    'priority_score': article['priority_score'],
                    'aiadmk_mentions': json.dumps(article['aiadmk_mentions']),
                    'total_mentions': article['total_mentions'],
                    'images': json.dumps(article.get('images', [])),
                    'raw_data': json.dumps({
                        'processing_metadata': article['processing_metadata'],
                        'source_weight': article['source_weight']
                    }),
                    'scraped_at': datetime.now().isoformat()
                }
                
                # Insert/update in database
                article_id = await db.insert_tamil_news_data(db_data)
                
                if article_id:
                    storage_results['stored_articles'] += 1
                    logger.info(f"💾 Stored Tamil news: {article['title'][:50]}...")
                else:
                    storage_results['updated_articles'] += 1
                    logger.info(f"🔄 Updated Tamil news: {article['title'][:30]}...")
                
            except Exception as e:
                error_msg = f"Failed to store article {article.get('url', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                storage_results['errors'].append(error_msg)
        
        logger.info(f"✅ Tamil news storage completed: {storage_results['stored_articles']} new, {storage_results['updated_articles']} updated")
        return storage_results
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse Tamil news date format"""
        if not date_str:
            return None
        
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.isoformat()
        except:
            return date_str
    
    async def run_tamil_news_monitoring(self) -> Dict[str, Any]:
        """Run complete Tamil news monitoring pipeline"""
        logger.info("🚀 Starting Tamil news monitoring pipeline...")
        
        try:
            # Step 1: Process Tamil news articles
            processing_results = await self.process_tamil_news_articles()
            
            if not processing_results['success']:
                return {
                    'success': False,
                    'error': processing_results.get('error'),
                    'pipeline_stage': 'processing'
                }
            
            if not processing_results['articles']:
                return {
                    'success': True,
                    'message': 'No AIADMK-related articles found',
                    'total_processed': processing_results['total_urls_processed'],
                    'aiadmk_articles': 0
                }
            
            # Step 2: Store data in database
            storage_results = await self.store_tamil_news_data(processing_results['articles'])
            
            # Step 3: Update URL processing status
            _, db = await self.get_services()
            pending_urls = await db.get_pending_urls('tamil_news', limit=30)
            for record in pending_urls:
                await db.update_url_status(record['id'], 'completed')
            
            return {
                'success': True,
                'platform': 'tamil_news',
                'processing': {
                    'total_urls_processed': processing_results['total_urls_processed'],
                    'articles_extracted': processing_results['total_articles_extracted'],
                    'aiadmk_articles': processing_results['aiadmk_articles']
                },
                'storage': storage_results,
                'completed_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Tamil news monitoring pipeline failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'pipeline_stage': 'pipeline_execution'
            }
    
    async def get_trending_aiadmk_news(self, limit: int = 10, 
                                     article_type: str = None) -> List[Dict]:
        """Get trending AIADMK news articles"""
        _, db = await self.get_services()
        
        query = """
            SELECT * FROM admk_tamil_news 
            WHERE scraped_at >= NOW() - INTERVAL '24 hours'
        """
        params = []
        
        if article_type:
            query += " AND article_type = $1"
            params.append(article_type)
        
        query += " ORDER BY priority_score DESC, published_date DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        try:
            result = await db.execute_query(query, tuple(params))
            articles = [dict(record) for record in result]
            
            # Parse JSON fields
            for article in articles:
                article['aiadmk_mentions'] = json.loads(article.get('aiadmk_mentions', '{}'))
                article['images'] = json.loads(article.get('images', '[]'))
                article['raw_data'] = json.loads(article.get('raw_data', '{}'))
            
            return articles
            
        except Exception as e:
            logger.error(f"Failed to get trending news: {e}")
            return []

# Global processor instance
tamil_news_processor = None

def get_tamil_news_processor():
    """Get global Tamil news processor instance"""
    global tamil_news_processor
    if not tamil_news_processor:
        tamil_news_processor = TamilNewsProcessor()
    return tamil_news_processor

# Test function
async def test_tamil_news_processor():
    """Test Tamil news processor functionality"""
    try:
        processor = get_tamil_news_processor()
        
        # Test URL discovery
        urls = await processor.discover_tamil_news_urls()
        logger.info(f"✅ URL discovery test: {len(urls)} URLs found")
        
        # Test article enhancement (mock data)
        mock_article = {
            'url': 'https://www.dailythanthi.com/test',
            'title': 'AIADMK தலைவர் எடப்பாடி பழனிசாமி அறிக்கை',
            'content': 'அ.இ.அ.த.மு.க கட்சியின் செயல்பாடுகள் குறித்து விளக்கம் அளித்தார்',
            'word_count': 150,
            'published_date': '2024-01-15T10:00:00Z'
        }
        
        enhanced = await processor.enhance_article_data(mock_article)
        logger.info(f"✅ Article enhancement test: Relevance={enhanced['relevance_score']}, Type={enhanced['article_type']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Tamil news processor test failed: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_tamil_news_processor())