#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Brave Search API Service
Web content discovery and real-time search monitoring using Brave Search API
"""

import os
import logging
import asyncio
from typing import Dict, List, Optional, Any
import json
from datetime import datetime, timedelta
import httpx

import sys
sys.path.append('..')
from config import get_config
from database import get_database

logger = logging.getLogger(__name__)

class BraveSearchService:
    """Brave Search API integration for AIADMK web content discovery"""
    
    def __init__(self):
        self.config = get_config()
        self.api_config = self.config.get_api_config('brave')
        
        if not self.api_config or not self.api_config.api_key:
            raise ValueError("Brave Search API configuration missing. Check BRAVE_API_KEY in .env")
        
        self.base_url = self.api_config.base_url
        self.api_key = self.api_config.api_key
        self.timeout = self.api_config.timeout
        
        self.headers = {
            'X-Subscription-Token': self.api_key,
            'Accept': 'application/json',
            'User-Agent': 'AIADMK-Intelligence-System/1.0'
        }
        
        self.session = httpx.AsyncClient(timeout=self.timeout, headers=self.headers)
        logger.info("✅ Brave Search API service initialized")
    
    async def close(self):
        """Close HTTP session"""
        await self.session.aclose()
    
    async def web_search(self, query: str, count: int = 20, offset: int = 0,
                        search_lang: str = 'en', country: str = 'IN',
                        freshness: str = None) -> Dict[str, Any]:
        """Perform web search using Brave Search API"""
        params = {
            'q': query,
            'count': min(count, 50),  # Brave API limit
            'offset': offset,
            'search_lang': search_lang,
            'country': country,
            'safesearch': 'off',
            'textDecorations': True,
            'spellcheck': True
        }
        
        # Add freshness filter if specified (d=day, w=week, m=month, y=year)
        if freshness:
            params['freshness'] = freshness
        
        try:
            response = await self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return {
                'success': True,
                'query': query,
                'results': data.get('web', {}).get('results', []),
                'total_count': data.get('web', {}).get('totalCount', 0),
                'deep_results': data.get('deepResults', {}),
                'query_context': data.get('query', {}),
                'timestamp': datetime.now().isoformat()
            }
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning(f"Brave API rate limit hit for query '{query}'")
                return {'success': False, 'error': 'Rate limit exceeded', 'query': query}
            else:
                logger.error(f"Brave API HTTP error {e.response.status_code} for query '{query}': {e}")
                return {'success': False, 'error': f'HTTP {e.response.status_code}', 'query': query}
        except httpx.RequestError as e:
            logger.error(f"Brave API request failed for query '{query}': {e}")
            return {'success': False, 'error': str(e), 'query': query}
        except Exception as e:
            logger.error(f"Brave API search failed for query '{query}': {e}")
            return {'success': False, 'error': str(e), 'query': query}
    
    async def news_search(self, query: str, count: int = 20, offset: int = 0,
                         freshness: str = 'pw') -> Dict[str, Any]:
        """Search for news articles using Brave Search"""
        # Use web search with news-focused parameters
        news_query = f'{query} news OR செய்தி OR சமீபத்திய'
        
        return await self.web_search(
            query=news_query,
            count=count,
            offset=offset,
            freshness=freshness  # pw = past week
        )
    
    async def discover_tamil_news_sites(self, keyword: str) -> Dict[str, Any]:
        """Discover Tamil news sites covering AIADMK"""
        tamil_news_domains = [
            'dailythanthi.com', 'dinamalar.com', 'thehindu.com',
            'tamilnews.com', 'polimer.tv', 'thanthitv.com',
            'puthiyathalaimurai.com', 'news18.com', 'vikatan.com'
        ]
        
        # Create site-specific search query
        site_query = f'{keyword} site:' + ' OR site:'.join(tamil_news_domains)
        
        results = await self.web_search(
            query=site_query,
            count=30,
            search_lang='ta',  # Tamil language
            freshness='pm'     # Past month
        )
        
        if results['success']:
            # Categorize results by news source
            categorized = {}
            for result in results['results']:
                url = result.get('url', '')
                for domain in tamil_news_domains:
                    if domain in url:
                        if domain not in categorized:
                            categorized[domain] = []
                        categorized[domain].append({
                            'title': result.get('title'),
                            'url': url,
                            'description': result.get('description'),
                            'age': result.get('age'),
                            'language': result.get('language')
                        })
                        break
            
            results['categorized_news'] = categorized
        
        return results
    
    async def discover_social_mentions(self, keyword: str) -> Dict[str, Any]:
        """Discover social media mentions and discussions"""
        social_platforms = [
            'youtube.com', 'facebook.com', 'instagram.com',
            'twitter.com', 'reddit.com', 'quora.com'
        ]
        
        # Create platform-specific search
        platform_query = f'"{keyword}" site:' + f' OR "{keyword}" site:'.join(social_platforms)
        
        results = await self.web_search(
            query=platform_query,
            count=40,
            freshness='pw'  # Past week
        )
        
        if results['success']:
            # Categorize by platform
            platform_mentions = {}
            for result in results['results']:
                url = result.get('url', '')
                for platform in social_platforms:
                    if platform in url:
                        platform_name = platform.replace('.com', '')
                        if platform_name not in platform_mentions:
                            platform_mentions[platform_name] = []
                        platform_mentions[platform_name].append({
                            'title': result.get('title'),
                            'url': url,
                            'description': result.get('description'),
                            'age': result.get('age')
                        })
                        break
            
            results['platform_mentions'] = platform_mentions
        
        return results
    
    def extract_urls_for_scraping(self, search_results: List[Dict]) -> Dict[str, List[str]]:
        """Extract URLs suitable for platform scraping"""
        platform_urls = {
            'youtube': [],
            'facebook': [],
            'instagram': [],
            'twitter': [],
            'reddit': [],
            'tamil_news': [],
            'web_articles': []
        }
        
        for result in search_results:
            url = result.get('url', '')
            
            # Categorize URLs by platform
            if 'youtube.com' in url or 'youtu.be' in url:
                platform_urls['youtube'].append(url)
            elif 'facebook.com' in url:
                platform_urls['facebook'].append(url)
            elif 'instagram.com' in url:
                platform_urls['instagram'].append(url)
            elif 'twitter.com' in url or 'x.com' in url:
                platform_urls['twitter'].append(url)
            elif 'reddit.com' in url:
                platform_urls['reddit'].append(url)
            elif any(domain in url for domain in [
                'dailythanthi.com', 'dinamalar.com', 'thehindu.com',
                'tamilnews.com', 'polimer.tv', 'thanthitv.com'
            ]):
                platform_urls['tamil_news'].append(url)
            else:
                platform_urls['web_articles'].append(url)
        
        return platform_urls
    
    async def automated_aiadmk_monitoring(self) -> Dict[str, Any]:
        """Automated AIADMK content monitoring using Brave Search"""
        db = await get_database()
        
        # Get AIADMK keywords for monitoring
        keywords = self.config.aiadmk_keywords['tamil'][:3] + self.config.aiadmk_keywords['english'][:3]
        
        monitoring_results = {
            'total_searches': 0,
            'total_results': 0,
            'new_urls_discovered': 0,
            'platform_breakdown': {},
            'monitoring_summary': [],
            'errors': []
        }
        
        for keyword in keywords:
            try:
                # Search for recent content (past day)
                search_results = await self.web_search(
                    query=f'"{keyword}" AIADMK OR அதிமுக',
                    count=20,
                    freshness='pd'  # Past day
                )
                
                if search_results['success']:
                    results = search_results['results']
                    platform_urls = self.extract_urls_for_scraping(results)
                    
                    urls_added = 0
                    # Add discovered URLs to database queue
                    for platform, urls in platform_urls.items():
                        if urls:
                            monitoring_results['platform_breakdown'][platform] = \
                                monitoring_results['platform_breakdown'].get(platform, 0) + len(urls)
                            
                            for url in urls:
                                await db.add_to_url_queue(
                                    url=url,
                                    platform=platform,
                                    priority=1,  # High priority for monitoring
                                    metadata={
                                        'source': 'brave_monitoring',
                                        'keyword': keyword,
                                        'discovered_at': datetime.now().isoformat(),
                                        'freshness': 'pd'
                                    }
                                )
                                urls_added += 1
                    
                    monitoring_results['monitoring_summary'].append({
                        'keyword': keyword,
                        'total_results': len(results),
                        'urls_added': urls_added,
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    monitoring_results['total_results'] += len(results)
                    monitoring_results['new_urls_discovered'] += urls_added
                
                else:
                    monitoring_results['errors'].append(search_results)
                
                monitoring_results['total_searches'] += 1
                
                # Rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Monitoring failed for keyword '{keyword}': {e}")
                monitoring_results['errors'].append({
                    'keyword': keyword,
                    'error': str(e)
                })
        
        logger.info(f"✅ Brave monitoring completed: {monitoring_results['new_urls_discovered']} URLs discovered")
        return monitoring_results
    
    async def deep_content_analysis(self, query: str) -> Dict[str, Any]:
        """Deep content analysis for AIADMK topics"""
        analysis_results = {
            'web_results': {},
            'news_results': {},
            'social_mentions': {},
            'content_themes': [],
            'recommendation': {}
        }
        
        try:
            # Parallel searches for comprehensive analysis
            web_task = self.web_search(query, count=30)
            news_task = self.news_search(query, count=20)
            social_task = self.discover_social_mentions(query)
            
            web_results, news_results, social_results = await asyncio.gather(
                web_task, news_task, social_task, return_exceptions=True
            )
            
            analysis_results['web_results'] = web_results if not isinstance(web_results, Exception) else {'success': False, 'error': str(web_results)}
            analysis_results['news_results'] = news_results if not isinstance(news_results, Exception) else {'success': False, 'error': str(news_results)}
            analysis_results['social_mentions'] = social_results if not isinstance(social_results, Exception) else {'success': False, 'error': str(social_results)}
            
            # Extract content themes from successful searches
            all_results = []
            if analysis_results['web_results'].get('success'):
                all_results.extend(analysis_results['web_results']['results'])
            if analysis_results['news_results'].get('success'):
                all_results.extend(analysis_results['news_results']['results'])
            
            # Simple theme extraction based on titles and descriptions
            themes = {}
            for result in all_results:
                title = result.get('title', '').lower()
                description = result.get('description', '').lower()
                
                # Look for common political themes
                if any(word in title or word in description for word in ['election', 'தேர்தல்', 'campaign']):
                    themes['election'] = themes.get('election', 0) + 1
                if any(word in title or word in description for word in ['policy', 'நீதி', 'announcement']):
                    themes['policy'] = themes.get('policy', 0) + 1
                if any(word in title or word in description for word in ['protest', 'போராட்டம்', 'opposition']):
                    themes['protest'] = themes.get('protest', 0) + 1
            
            analysis_results['content_themes'] = [{'theme': k, 'count': v} for k, v in themes.items()]
            
            # Generate recommendations
            total_results = sum(len(r.get('results', [])) for r in [analysis_results['web_results'], analysis_results['news_results']] if r.get('success'))
            
            analysis_results['recommendation'] = {
                'monitoring_priority': 'high' if total_results > 50 else 'medium' if total_results > 20 else 'low',
                'suggested_frequency': '1 hour' if total_results > 50 else '6 hours' if total_results > 20 else '24 hours',
                'key_platforms': list(analysis_results['social_mentions'].get('platform_mentions', {}).keys())[:3]
            }
            
        except Exception as e:
            logger.error(f"Deep analysis failed for query '{query}': {e}")
            analysis_results['error'] = str(e)
        
        return analysis_results

# Global service instance
brave_service = None

async def get_brave_service():
    """Get global Brave Search service instance"""
    global brave_service
    if not brave_service:
        brave_service = BraveSearchService()
    return brave_service

async def close_brave_service():
    """Close global Brave Search service"""
    global brave_service
    if brave_service:
        await brave_service.close()
        brave_service = None

# Test function
async def test_brave_service():
    """Test Brave Search service functionality"""
    try:
        service = await get_brave_service()
        
        # Test basic web search
        results = await service.web_search("AIADMK Edappadi Palaniswami", count=5)
        logger.info(f"✅ Web search test: {len(results.get('results', []))} results")
        
        # Test Tamil news discovery
        news_results = await service.discover_tamil_news_sites("அதிமுக")
        logger.info(f"✅ Tamil news discovery: {len(news_results.get('categorized_news', {}))} sources found")
        
        # Test automated monitoring
        monitoring = await service.automated_aiadmk_monitoring()
        logger.info(f"✅ Monitoring test: {monitoring['new_urls_discovered']} URLs discovered")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Brave Search service test failed: {e}")
        return False
    finally:
        await close_brave_service()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_brave_service())