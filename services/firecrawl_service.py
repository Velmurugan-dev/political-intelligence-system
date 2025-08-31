#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Firecrawl API Service
Tamil news content extraction and web scraping using Firecrawl
"""

import os
import logging
import asyncio
from typing import Dict, List, Optional, Any
import json
from datetime import datetime, timedelta
import httpx
from urllib.parse import urljoin, urlparse

import sys
sys.path.append('..')
from config import get_config
from database import get_database

logger = logging.getLogger(__name__)

class FirecrawlService:
    """Firecrawl API integration for Tamil news content extraction"""
    
    def __init__(self):
        self.config = get_config()
        self.api_config = self.config.get_api_config('firecrawl')
        
        if not self.api_config or not self.api_config.api_key:
            raise ValueError("Firecrawl API configuration missing. Check FIRECRAWL_API_KEY in .env")
        
        self.base_url = self.api_config.base_url
        self.api_key = self.api_config.api_key
        self.timeout = self.api_config.timeout
        
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'AIADMK-Intelligence-System/1.0'
        }
        
        self.session = httpx.AsyncClient(timeout=self.timeout, headers=self.headers)
        
        # Tamil news sites configuration
        self.tamil_news_sites = {
            'dailythanthi.com': {
                'name': 'Daily Thanthi',
                'language': 'tamil',
                'base_url': 'https://www.dailythanthi.com',
                'search_patterns': ['/news/politics/', '/news/state/', '/tag/aiadmk/']
            },
            'dinamalar.com': {
                'name': 'Dinamalar',
                'language': 'tamil',
                'base_url': 'https://www.dinamalar.com',
                'search_patterns': ['/news_detail.asp', '/politics/']
            },
            'thehindu.com': {
                'name': 'The Hindu Tamil',
                'language': 'mixed',
                'base_url': 'https://www.thehindu.com',
                'search_patterns': ['/news/national/tamil-nadu/', '/topic/aiadmk/']
            },
            'puthiyathalaimurai.com': {
                'name': 'Puthiya Thalaimurai',
                'language': 'tamil',
                'base_url': 'https://www.puthiyathalaimurai.com',
                'search_patterns': ['/news/', '/politics/']
            },
            'polimer.tv': {
                'name': 'Polimer TV',
                'language': 'tamil',
                'base_url': 'https://polimer.tv',
                'search_patterns': ['/news/', '/politics/']
            }
        }
        
        logger.info("✅ Firecrawl API service initialized")
    
    async def close(self):
        """Close HTTP session"""
        await self.session.aclose()
    
    async def scrape_url(self, url: str, formats: List[str] = None,
                        include_tags: List[str] = None,
                        exclude_tags: List[str] = None,
                        wait_for: int = 0) -> Dict[str, Any]:
        """Scrape a single URL using Firecrawl API"""
        if formats is None:
            formats = ['markdown', 'html', 'rawHtml']
        
        payload = {
            'url': url,
            'formats': formats,
            'onlyMainContent': True,
            'includeTags': include_tags or ['article', 'main', 'div.content', 'div.news-content'],
            'excludeTags': exclude_tags or ['nav', 'footer', 'aside', 'div.ads', 'div.advertisement'],
            'waitFor': wait_for
        }
        
        try:
            response = await self.session.post(f'{self.base_url}/scrape', json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('success'):
                return {
                    'success': True,
                    'url': url,
                    'content': data.get('data', {}),
                    'metadata': {
                        'title': data.get('data', {}).get('metadata', {}).get('title'),
                        'description': data.get('data', {}).get('metadata', {}).get('description'),
                        'language': data.get('data', {}).get('metadata', {}).get('language'),
                        'published_date': data.get('data', {}).get('metadata', {}).get('published_time'),
                        'author': data.get('data', {}).get('metadata', {}).get('author')
                    },
                    'extracted_at': datetime.now().isoformat()
                }
            else:
                return {
                    'success': False,
                    'url': url,
                    'error': data.get('error', 'Unknown error')
                }
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning(f"Firecrawl API rate limit hit for URL: {url}")
                return {'success': False, 'url': url, 'error': 'Rate limit exceeded'}
            else:
                logger.error(f"Firecrawl API HTTP error {e.response.status_code} for URL {url}: {e}")
                return {'success': False, 'url': url, 'error': f'HTTP {e.response.status_code}'}
        except httpx.RequestError as e:
            logger.error(f"Firecrawl API request failed for URL {url}: {e}")
            return {'success': False, 'url': url, 'error': str(e)}
        except Exception as e:
            logger.error(f"Firecrawl scraping failed for URL {url}: {e}")
            return {'success': False, 'url': url, 'error': str(e)}
    
    async def crawl_site(self, url: str, limit: int = 50,
                        include_paths: List[str] = None,
                        exclude_paths: List[str] = None) -> Dict[str, Any]:
        """Crawl a website to discover pages using Firecrawl API"""
        payload = {
            'url': url,
            'limit': min(limit, 100),  # Firecrawl limit
            'scrapeOptions': {
                'formats': ['markdown'],
                'onlyMainContent': True
            }
        }
        
        if include_paths:
            payload['includePaths'] = include_paths
        if exclude_paths:
            payload['excludePaths'] = exclude_paths
        
        try:
            response = await self.session.post(f'{self.base_url}/crawl', json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('success'):
                job_id = data.get('jobId')
                
                # Poll for completion (simplified - in production would need better polling)
                max_attempts = 30  # 5 minutes max wait
                for attempt in range(max_attempts):
                    await asyncio.sleep(10)  # Wait 10 seconds between polls
                    
                    status_response = await self.session.get(f'{self.base_url}/crawl/status/{job_id}')
                    status_data = status_response.json()
                    
                    if status_data.get('status') == 'completed':
                        return {
                            'success': True,
                            'job_id': job_id,
                            'url': url,
                            'pages': status_data.get('data', []),
                            'total_pages': len(status_data.get('data', [])),
                            'crawled_at': datetime.now().isoformat()
                        }
                    elif status_data.get('status') == 'failed':
                        return {
                            'success': False,
                            'job_id': job_id,
                            'url': url,
                            'error': 'Crawl job failed'
                        }
                
                # Timeout
                return {
                    'success': False,
                    'job_id': job_id,
                    'url': url,
                    'error': 'Crawl job timeout'
                }
            else:
                return {
                    'success': False,
                    'url': url,
                    'error': data.get('error', 'Unknown error')
                }
                
        except Exception as e:
            logger.error(f"Firecrawl crawling failed for URL {url}: {e}")
            return {'success': False, 'url': url, 'error': str(e)}
    
    def extract_article_data(self, scraped_content: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured article data from scraped content"""
        content = scraped_content.get('content', {})
        metadata = scraped_content.get('metadata', {})
        
        # Extract text content
        markdown_content = content.get('markdown', '')
        html_content = content.get('html', '')
        
        # Extract key information
        article_data = {
            'url': scraped_content.get('url'),
            'title': metadata.get('title') or self._extract_title_from_content(markdown_content),
            'content': markdown_content,
            'html_content': html_content,
            'description': metadata.get('description'),
            'author': metadata.get('author'),
            'published_date': self._parse_date(metadata.get('published_date')),
            'language': self._detect_language(markdown_content),
            'word_count': len(markdown_content.split()) if markdown_content else 0,
            'aiadmk_mentions': self._count_aiadmk_mentions(markdown_content),
            'extracted_at': datetime.now().isoformat()
        }
        
        # Extract images
        if 'rawHtml' in content:
            article_data['images'] = self._extract_images(content['rawHtml'])
        
        return article_data
    
    def _extract_title_from_content(self, content: str) -> Optional[str]:
        """Extract title from markdown content"""
        if not content:
            return None
        
        lines = content.split('\n')
        for line in lines[:5]:  # Check first 5 lines
            line = line.strip()
            if line.startswith('# '):
                return line[2:].strip()
            elif line and not line.startswith('#') and len(line) > 20:
                return line
        return None
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse and standardize date format"""
        if not date_str:
            return None
        
        try:
            # Try to parse various date formats
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.isoformat()
        except:
            return date_str  # Return as-is if parsing fails
    
    def _detect_language(self, content: str) -> str:
        """Simple language detection for Tamil/English content"""
        if not content:
            return 'unknown'
        
        # Count Tamil characters
        tamil_chars = sum(1 for char in content if '\u0B80' <= char <= '\u0BFF')
        english_chars = sum(1 for char in content if char.isalpha() and char.isascii())
        
        if tamil_chars > english_chars:
            return 'tamil'
        elif english_chars > tamil_chars:
            return 'english'
        else:
            return 'mixed'
    
    def _count_aiadmk_mentions(self, content: str) -> Dict[str, int]:
        """Count AIADMK-related mentions in content"""
        if not content:
            return {}
        
        content_lower = content.lower()
        mentions = {}
        
        # AIADMK keywords
        aiadmk_keywords = self.config.aiadmk_keywords
        
        for lang, keywords in aiadmk_keywords.items():
            for keyword in keywords:
                count = content_lower.count(keyword.lower())
                if count > 0:
                    mentions[keyword] = count
        
        return mentions
    
    def _extract_images(self, html_content: str) -> List[Dict[str, str]]:
        """Extract image URLs from HTML content"""
        images = []
        
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            for img in soup.find_all('img'):
                src = img.get('src')
                alt = img.get('alt', '')
                if src:
                    images.append({
                        'url': src,
                        'alt_text': alt,
                        'caption': img.get('title', '')
                    })
        except ImportError:
            # BeautifulSoup not available, skip image extraction
            logger.warning("BeautifulSoup not available for image extraction")
        
        return images
    
    async def process_tamil_news_urls(self, urls: List[str]) -> Dict[str, Any]:
        """Process multiple Tamil news URLs"""
        results = {
            'total_urls': len(urls),
            'successful_extractions': 0,
            'failed_extractions': 0,
            'articles': [],
            'errors': []
        }
        
        for url in urls:
            try:
                scraped = await self.scrape_url(url)
                
                if scraped['success']:
                    article_data = self.extract_article_data(scraped)
                    results['articles'].append(article_data)
                    results['successful_extractions'] += 1
                    
                    logger.info(f"✅ Extracted article: {article_data.get('title', 'No title')[:50]}...")
                else:
                    results['errors'].append(scraped)
                    results['failed_extractions'] += 1
                
                # Rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Failed to process URL {url}: {e}")
                results['errors'].append({'url': url, 'error': str(e)})
                results['failed_extractions'] += 1
        
        return results
    
    async def automated_news_monitoring(self) -> Dict[str, Any]:
        """Automated monitoring of Tamil news sites for AIADMK content"""
        db = await get_database()
        
        monitoring_results = {
            'sites_checked': 0,
            'articles_found': 0,
            'articles_processed': 0,
            'new_articles': 0,
            'errors': []
        }
        
        # Get URLs from database queue for Tamil news
        pending_urls = await db.get_pending_urls('tamil_news', limit=20)
        
        if pending_urls:
            url_list = [record['url'] for record in pending_urls]
            processing_results = await self.process_tamil_news_urls(url_list)
            
            # Store successful articles in database
            for article in processing_results['articles']:
                try:
                    # Prepare data for database insertion
                    db_data = {
                        'article_url': article['url'],
                        'title': article['title'],
                        'content': article['content'],
                        'author': article['author'],
                        'published_date': article['published_date'],
                        'language': article['language'],
                        'word_count': article['word_count'],
                        'aiadmk_mentions': json.dumps(article['aiadmk_mentions']),
                        'images': json.dumps(article.get('images', [])),
                        'source_domain': urlparse(article['url']).netloc,
                        'extracted_at': article['extracted_at']
                    }
                    
                    article_id = await db.insert_tamil_news_data(db_data)
                    
                    if article_id:
                        monitoring_results['new_articles'] += 1
                        logger.info(f"💾 Stored article ID {article_id}: {article['title'][:50]}...")
                
                except Exception as e:
                    logger.error(f"Failed to store article {article['url']}: {e}")
                    monitoring_results['errors'].append({
                        'url': article['url'],
                        'error': f'Database storage failed: {str(e)}'
                    })
            
            # Update URL processing status
            for record in pending_urls:
                url_id = record['id']
                url = record['url']
                
                # Check if URL was processed successfully
                processed = any(article['url'] == url for article in processing_results['articles'])
                
                if processed:
                    await db.update_url_status(url_id, 'completed')
                else:
                    error_info = next((error for error in processing_results['errors'] if error.get('url') == url), {})
                    await db.update_url_status(url_id, 'failed', error_info.get('error', 'Processing failed'))
            
            monitoring_results.update({
                'sites_checked': len(set(urlparse(url).netloc for url in url_list)),
                'articles_found': len(url_list),
                'articles_processed': processing_results['successful_extractions']
            })
        
        logger.info(f"✅ News monitoring completed: {monitoring_results['new_articles']} new articles stored")
        return monitoring_results

# Global service instance
firecrawl_service = None

async def get_firecrawl_service():
    """Get global Firecrawl service instance"""
    global firecrawl_service
    if not firecrawl_service:
        firecrawl_service = FirecrawlService()
    return firecrawl_service

async def close_firecrawl_service():
    """Close global Firecrawl service"""
    global firecrawl_service
    if firecrawl_service:
        await firecrawl_service.close()
        firecrawl_service = None

# Test function
async def test_firecrawl_service():
    """Test Firecrawl service functionality"""
    try:
        service = await get_firecrawl_service()
        
        # Test URL scraping with a news site
        test_url = "https://www.thehindu.com/news/national/tamil-nadu/"
        scraped = await service.scrape_url(test_url, formats=['markdown'])
        
        if scraped['success']:
            logger.info(f"✅ URL scraping test successful: {len(scraped['content'].get('markdown', ''))} characters extracted")
            
            # Test article data extraction
            article_data = service.extract_article_data(scraped)
            logger.info(f"✅ Article extraction: {article_data['word_count']} words, language: {article_data['language']}")
        else:
            logger.warning(f"⚠️  URL scraping test returned: {scraped.get('error')}")
        
        # Test news monitoring (if there are URLs in queue)
        monitoring = await service.automated_news_monitoring()
        logger.info(f"✅ News monitoring test: {monitoring['articles_processed']} articles processed")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Firecrawl service test failed: {e}")
        return False
    finally:
        await close_firecrawl_service()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_firecrawl_service())