#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - SerpAPI Service
Automated keyword discovery and content discovery using Google Search
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

class SerpAPIService:
    """SerpAPI integration for AIADMK content discovery"""
    
    def __init__(self):
        self.config = get_config()
        self.api_config = self.config.get_api_config('serpapi')
        
        if not self.api_config or not self.api_config.api_key:
            raise ValueError("SerpAPI configuration missing. Check SERPAPI_KEY in .env")
        
        self.base_url = self.api_config.base_url
        self.api_key = self.api_config.api_key
        self.timeout = self.api_config.timeout
        
        self.session = httpx.AsyncClient(timeout=self.timeout)
        logger.info("✅ SerpAPI service initialized")
    
    async def close(self):
        """Close HTTP session"""
        await self.session.aclose()
    
    async def search_google(self, query: str, num_results: int = 20, 
                          location: str = "Tamil Nadu, India") -> Dict[str, Any]:
        """Perform Google search using SerpAPI"""
        params = {
            'q': query,
            'api_key': self.api_key,
            'engine': 'google',
            'num': min(num_results, 100),  # SerpAPI limit
            'location': location,
            'gl': 'in',  # India
            'hl': 'en',  # Language
            'safe': 'off',
            'device': 'desktop'
        }
        
        try:
            response = await self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return {
                'success': True,
                'query': query,
                'results': data.get('organic_results', []),
                'total_results': data.get('search_information', {}).get('total_results', 0),
                'search_metadata': data.get('search_metadata', {}),
                'timestamp': datetime.now().isoformat()
            }
            
        except httpx.RequestError as e:
            logger.error(f"SerpAPI request failed for query '{query}': {e}")
            return {'success': False, 'error': str(e), 'query': query}
        except Exception as e:
            logger.error(f"SerpAPI search failed for query '{query}': {e}")
            return {'success': False, 'error': str(e), 'query': query}
    
    async def search_youtube(self, query: str, num_results: int = 20) -> Dict[str, Any]:
        """Search YouTube using SerpAPI"""
        params = {
            'search_query': query,
            'api_key': self.api_key,
            'engine': 'youtube',
            'gl': 'in',
            'hl': 'en'
        }
        
        try:
            response = await self.session.get('https://serpapi.com/search', params=params)
            response.raise_for_status()
            
            data = response.json()
            return {
                'success': True,
                'query': query,
                'videos': data.get('video_results', []),
                'channels': data.get('channel_results', []),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"YouTube search failed for query '{query}': {e}")
            return {'success': False, 'error': str(e), 'query': query}
    
    async def search_news(self, query: str, num_results: int = 20, 
                        days_back: int = 30) -> Dict[str, Any]:
        """Search Google News using SerpAPI"""
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        params = {
            'q': query,
            'api_key': self.api_key,
            'engine': 'google_news',
            'num': min(num_results, 100),
            'gl': 'in',
            'hl': 'en',
            'tbm': 'nws',
            'tbs': f"cdr:1,cd_min:{start_date.strftime('%m/%d/%Y')},cd_max:{end_date.strftime('%m/%d/%Y')}"
        }
        
        try:
            response = await self.session.get('https://serpapi.com/search', params=params)
            response.raise_for_status()
            
            data = response.json()
            return {
                'success': True,
                'query': query,
                'news_results': data.get('news_results', []),
                'date_range': {
                    'start': start_date.isoformat(),
                    'end': end_date.isoformat()
                },
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"News search failed for query '{query}': {e}")
            return {'success': False, 'error': str(e), 'query': query}
    
    def extract_platform_urls(self, search_results: List[Dict]) -> Dict[str, List[str]]:
        """Extract platform-specific URLs from search results"""
        platform_urls = {
            'youtube': [],
            'facebook': [],
            'instagram': [],
            'twitter': [],
            'reddit': [],
            'news_sites': []
        }
        
        for result in search_results:
            url = result.get('link', '')
            
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
                platform_urls['news_sites'].append(url)
        
        return platform_urls
    
    async def discover_aiadmk_content(self, platform: str = None) -> Dict[str, Any]:
        """Automated AIADMK content discovery across platforms"""
        db = await get_database()
        
        # Get keywords for search
        if platform:
            keywords = self.config.get_keywords_for_platform(platform)[:5]  # Limit for API quota
        else:
            keywords = (self.config.aiadmk_keywords['tamil'][:3] + 
                       self.config.aiadmk_keywords['english'][:3])
        
        discovery_results = {
            'total_searches': 0,
            'total_urls_found': 0,
            'platform_breakdown': {},
            'search_results': [],
            'errors': []
        }
        
        for keyword in keywords:
            try:
                # Google search
                google_results = await self.search_google(
                    query=f'"{keyword}" site:youtube.com OR site:facebook.com OR site:instagram.com OR site:twitter.com OR site:reddit.com',
                    num_results=20
                )
                
                if google_results['success']:
                    platform_urls = self.extract_platform_urls(google_results['results'])
                    
                    # Add URLs to database queue
                    for platform_name, urls in platform_urls.items():
                        if urls:
                            discovery_results['platform_breakdown'][platform_name] = \
                                discovery_results['platform_breakdown'].get(platform_name, 0) + len(urls)
                            
                            for url in urls:
                                await db.add_to_url_queue(
                                    url=url,
                                    platform=platform_name,
                                    priority=2,  # Discovery URLs get medium priority
                                    metadata={
                                        'source': 'serpapi',
                                        'keyword': keyword,
                                        'discovered_at': datetime.now().isoformat()
                                    }
                                )
                    
                    discovery_results['search_results'].append(google_results)
                    discovery_results['total_urls_found'] += len(google_results['results'])
                
                else:
                    discovery_results['errors'].append(google_results)
                
                discovery_results['total_searches'] += 1
                
                # Rate limiting - wait between searches
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Content discovery failed for keyword '{keyword}': {e}")
                discovery_results['errors'].append({
                    'keyword': keyword,
                    'error': str(e)
                })
        
        logger.info(f"✅ Content discovery completed: {discovery_results['total_urls_found']} URLs found across {len(discovery_results['platform_breakdown'])} platforms")
        return discovery_results
    
    async def discover_youtube_channels(self) -> List[Dict[str, Any]]:
        """Discover AIADMK-related YouTube channels"""
        channels = []
        
        channel_queries = [
            "AIADMK official channel",
            "Edappadi Palaniswami speeches",
            "அதிமுக அதிகாரப்பூர்வ",
            "Tamil Nadu AIADMK news"
        ]
        
        for query in channel_queries:
            try:
                results = await self.search_youtube(query)
                
                if results['success']:
                    for channel in results.get('channels', []):
                        channels.append({
                            'channel_id': channel.get('channel_id'),
                            'channel_name': channel.get('title'),
                            'channel_url': channel.get('link'),
                            'subscribers': channel.get('subscribers'),
                            'description': channel.get('description', ''),
                            'discovered_via': query,
                            'platform': 'youtube'
                        })
                
                await asyncio.sleep(2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Channel discovery failed for query '{query}': {e}")
        
        return channels
    
    async def automated_keyword_monitoring(self):
        """Automated monitoring of AIADMK keywords"""
        db = await get_database()
        
        # Get keywords that need to be searched
        keywords_to_search = await db.get_keywords_to_search('google_search')
        
        monitoring_results = {
            'keywords_processed': 0,
            'total_results_found': 0,
            'new_urls_discovered': 0
        }
        
        for keyword_record in keywords_to_search:
            keyword = keyword_record['keyword']
            
            try:
                # Search for recent content
                search_results = await self.search_google(
                    query=keyword,
                    num_results=10
                )
                
                if search_results['success']:
                    results_found = len(search_results['results'])
                    
                    # Extract and queue new URLs
                    platform_urls = self.extract_platform_urls(search_results['results'])
                    new_urls = 0
                    
                    for platform, urls in platform_urls.items():
                        for url in urls:
                            await db.add_to_url_queue(
                                url=url,
                                platform=platform,
                                priority=1,  # Monitoring gets high priority
                                metadata={
                                    'source': 'keyword_monitoring',
                                    'keyword': keyword,
                                    'monitored_at': datetime.now().isoformat()
                                }
                            )
                            new_urls += 1
                    
                    # Update keyword search record
                    await db.update_keyword_search(keyword, results_found)
                    
                    monitoring_results['keywords_processed'] += 1
                    monitoring_results['total_results_found'] += results_found
                    monitoring_results['new_urls_discovered'] += new_urls
                    
                    logger.info(f"🔍 Monitored keyword '{keyword}': {results_found} results, {new_urls} new URLs")
                
                await asyncio.sleep(3)  # Rate limiting for sustained monitoring
                
            except Exception as e:
                logger.error(f"Keyword monitoring failed for '{keyword}': {e}")
        
        return monitoring_results

# Global service instance
serpapi_service = None

async def get_serpapi_service():
    """Get global SerpAPI service instance"""
    global serpapi_service
    if not serpapi_service:
        serpapi_service = SerpAPIService()
    return serpapi_service

async def close_serpapi_service():
    """Close global SerpAPI service"""
    global serpapi_service
    if serpapi_service:
        await serpapi_service.close()
        serpapi_service = None

# Test function
async def test_serpapi_service():
    """Test SerpAPI service functionality"""
    try:
        service = await get_serpapi_service()
        
        # Test basic search
        results = await service.search_google("AIADMK எடப்பாடி பழனிசாமி", num_results=5)
        logger.info(f"✅ Test search completed: {len(results.get('results', []))} results")
        
        # Test content discovery
        discovery = await service.discover_aiadmk_content()
        logger.info(f"✅ Content discovery test: {discovery['total_urls_found']} URLs found")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ SerpAPI service test failed: {e}")
        return False
    finally:
        await close_serpapi_service()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_serpapi_service())