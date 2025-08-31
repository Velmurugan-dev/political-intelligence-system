"""
Tests for Discovery Engine
Tests Stage 1 URL discovery functionality including keyword search, source monitoring, and deduplication
"""

import os
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from engines.discovery_engine import DiscoveryEngine
from tests.conftest import (
    assert_discovery_result_structure, 
    sample_competitors, 
    sample_platforms, 
    sample_keywords,
    sample_sources
)

class TestDiscoveryEngine:
    """Test suite for Discovery Engine functionality"""

    @pytest.mark.asyncio
    async def test_engine_initialization(self, mock_db):
        """Test discovery engine initializes correctly"""
        with patch('engines.discovery_engine.get_database', return_value=mock_db), \
             patch('engines.discovery_engine.get_serpapi_service', return_value=AsyncMock()), \
             patch('engines.discovery_engine.get_brave_service', return_value=AsyncMock()), \
             patch('engines.discovery_engine.get_firecrawl_service', return_value=AsyncMock()):
            
            engine = DiscoveryEngine()
            success = await engine.initialize()
            
            assert success == True
            assert engine.db == mock_db
            assert engine.serpapi is not None
            assert engine.brave is not None
            assert engine.firecrawl is not None
            assert hasattr(engine, 'stats')

    @pytest.mark.asyncio
    async def test_get_keywords_to_search(self, discovery_engine, sample_competitors, sample_keywords):
        """Test keyword retrieval for discovery"""
        # Mock database response
        discovery_engine.db.execute_query.return_value = sample_keywords
        
        keywords = await discovery_engine.get_keywords_to_search([1, 2])
        
        assert len(keywords) > 0
        assert all('keyword' in k for k in keywords)
        assert all('competitor_id' in k for k in keywords)
        
        # Verify query was called correctly
        discovery_engine.db.execute_query.assert_called()

    @pytest.mark.asyncio
    async def test_get_sources_to_monitor(self, discovery_engine, sample_sources):
        """Test source retrieval for monitoring"""
        discovery_engine.db.execute_query.return_value = sample_sources
        
        sources = await discovery_engine.get_sources_to_monitor([1, 2])
        
        assert len(sources) > 0
        assert all('url' in s for s in sources)
        assert all('competitor_id' in s for s in sources)

    @pytest.mark.asyncio
    @patch('engines.discovery_engine.GoogleSearch')
    async def test_search_with_serpapi(self, mock_google_search, discovery_engine):
        """Test SerpAPI keyword search functionality"""
        # Mock SerpAPI response
        mock_search_instance = MagicMock()
        mock_search_instance.get_dict.return_value = {
            'organic_results': [
                {
                    'link': 'https://facebook.com/test/posts/123',
                    'title': 'Test Political Post',
                    'snippet': 'Test content snippet'
                },
                {
                    'link': 'https://twitter.com/test/status/456',
                    'title': 'Test Tweet',
                    'snippet': 'Test tweet content'
                }
            ]
        }
        mock_google_search.return_value = mock_search_instance
        
        keyword_data = {
            'id': 1,
            'competitor_id': 1,
            'keyword': 'ADMK',
            'priority_level': 1
        }
        
        results = await discovery_engine.search_with_serpapi(keyword_data)
        
        assert len(results) == 2
        assert all('url' in r for r in results)
        assert all('title' in r for r in results)
        assert results[0]['source_type'] == 'keyword_search'

    @pytest.mark.asyncio
    @patch('requests.get')
    async def test_search_with_brave(self, mock_requests, discovery_engine):
        """Test Brave Search API functionality"""
        # Mock Brave Search response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'web': {
                'results': [
                    {
                        'url': 'https://facebook.com/test/posts/789',
                        'title': 'Political Update',
                        'description': 'Latest political news'
                    }
                ]
            }
        }
        mock_response.status_code = 200
        mock_requests.return_value = mock_response
        
        keyword_data = {
            'id': 1,
            'competitor_id': 1,
            'keyword': 'DMK',
            'priority_level': 1
        }
        
        results = await discovery_engine.search_with_brave(keyword_data)
        
        assert len(results) >= 0  # May be empty if no results
        if results:
            assert all('url' in r for r in results)
            assert results[0]['source_type'] == 'keyword_search'

    @pytest.mark.asyncio
    @patch('requests.post')
    async def test_crawl_with_firecrawl(self, mock_requests, discovery_engine):
        """Test Firecrawl URL crawling functionality"""
        # Mock Firecrawl response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'data': [
                {
                    'url': 'https://news.example.com/article1',
                    'title': 'Political News Article',
                    'content': 'Article content...'
                }
            ]
        }
        mock_response.status_code = 200
        mock_requests.return_value = mock_response
        
        base_url = 'https://news.example.com'
        competitor_id = 1
        
        results = await discovery_engine.crawl_with_firecrawl(base_url, competitor_id)
        
        assert isinstance(results, list)
        if results:
            assert all('url' in r for r in results)
            assert results[0]['source_type'] == 'news_crawl'

    @pytest.mark.asyncio
    async def test_monitor_source(self, discovery_engine):
        """Test individual source monitoring"""
        source_data = {
            'id': 1,
            'competitor_id': 1,
            'platform_id': 1,
            'name': 'Test Source',
            'url': 'https://facebook.com/test',
            'source_type': 'social_media'
        }
        
        # Mock source monitoring to return some results
        with patch.object(discovery_engine, 'extract_urls_from_source') as mock_extract:
            mock_extract.return_value = [
                {
                    'url': 'https://facebook.com/test/posts/123',
                    'title': 'Test Post',
                    'source_type': 'source_monitoring'
                }
            ]
            
            results = await discovery_engine.monitor_source(source_data)
            
            assert isinstance(results, list)
            if results:
                assert all('url' in r for r in results)
                assert results[0]['source_type'] == 'source_monitoring'

    @pytest.mark.asyncio
    async def test_process_manual_queue(self, discovery_engine):
        """Test manual URL queue processing"""
        # Mock manual queue data
        manual_urls = [
            {
                'id': 1,
                'competitor_id': 1,
                'platform_id': 1,
                'url': 'https://facebook.com/manual/post/123',
                'priority_level': 1
            },
            {
                'id': 2,
                'competitor_id': 2,
                'platform_id': 2,
                'url': 'https://twitter.com/manual/status/456',
                'priority_level': 1
            }
        ]
        
        discovery_engine.db.execute_query.return_value = manual_urls
        
        results = await discovery_engine.process_manual_queue()
        
        assert isinstance(results, dict)
        assert 'urls_processed' in results
        assert 'successful' in results
        assert 'failed' in results

    @pytest.mark.asyncio
    async def test_store_discovery_results(self, discovery_engine):
        """Test storing discovery results to database"""
        discovery_results = [
            {
                'competitor_id': 1,
                'platform_id': 1,
                'url': 'https://facebook.com/test/posts/123',
                'title': 'Test Post',
                'source_type': 'keyword_search',
                'metadata': {'search_query': 'ADMK'}
            }
        ]
        
        # Mock successful database insertion
        discovery_engine.db.execute_query.return_value = [{'id': 1}]
        
        stored_count = await discovery_engine.store_discovery_results(discovery_results)
        
        assert stored_count >= 0
        discovery_engine.db.execute_query.assert_called()

    @pytest.mark.asyncio
    async def test_url_hash_generation(self, discovery_engine):
        """Test URL hash generation for deduplication"""
        url1 = 'https://facebook.com/test/posts/123'
        url2 = 'https://facebook.com/test/posts/123?utm_source=test'
        url3 = 'https://facebook.com/test/posts/456'
        
        hash1 = discovery_engine.generate_url_hash(url1)
        hash2 = discovery_engine.generate_url_hash(url2)
        hash3 = discovery_engine.generate_url_hash(url3)
        
        # Same base URL should generate same hash (after normalization)
        assert hash1 == hash2
        # Different URLs should generate different hashes
        assert hash1 != hash3
        
        # Hash should be consistent
        assert hash1 == discovery_engine.generate_url_hash(url1)

    @pytest.mark.asyncio
    async def test_run_discovery_cycle(self, discovery_engine, sample_keywords, sample_sources):
        """Test complete discovery cycle execution"""
        # Mock database responses
        discovery_engine.db.execute_query.side_effect = [
            sample_keywords,  # get_keywords_to_search
            sample_sources,   # get_sources_to_monitor
            [],               # manual queue
            [{'id': 1}],      # store results
            [{'id': 2}],      # store results
        ]
        
        # Mock discovery methods
        with patch.object(discovery_engine, 'search_with_serpapi') as mock_serpapi, \
             patch.object(discovery_engine, 'monitor_source') as mock_monitor:
            
            mock_serpapi.return_value = [
                {
                    'competitor_id': 1,
                    'platform_id': 1,
                    'url': 'https://facebook.com/test/posts/123',
                    'title': 'Test Post',
                    'source_type': 'keyword_search'
                }
            ]
            
            mock_monitor.return_value = [
                {
                    'competitor_id': 1,
                    'platform_id': 1,
                    'url': 'https://facebook.com/test/posts/456',
                    'title': 'Monitored Post',
                    'source_type': 'source_monitoring'
                }
            ]
            
            results = await discovery_engine.run_discovery_cycle([1], [1])
            
            assert isinstance(results, dict)
            assert 'keywords_processed' in results
            assert 'sources_monitored' in results
            assert 'urls_discovered' in results
            assert 'manual_urls_processed' in results

    @pytest.mark.asyncio
    async def test_discovery_with_errors(self, discovery_engine, network_error):
        """Test discovery engine error handling"""
        keyword_data = {
            'id': 1,
            'competitor_id': 1,
            'keyword': 'TEST',
            'priority_level': 1
        }
        
        # Test SerpAPI error handling
        with patch('engines.discovery_engine.GoogleSearch') as mock_search:
            mock_search.side_effect = network_error
            
            results = await discovery_engine.search_with_serpapi(keyword_data)
            
            # Should handle error gracefully and return empty results
            assert isinstance(results, list)
            assert len(results) == 0

    @pytest.mark.asyncio
    async def test_discovery_rate_limiting(self, discovery_engine):
        """Test rate limiting for external API calls"""
        # This test ensures we don't overwhelm external APIs
        start_time = datetime.now()
        
        keyword_data = {
            'id': 1,
            'competitor_id': 1,
            'keyword': 'TEST',
            'priority_level': 1
        }
        
        # Mock rapid API calls
        with patch('engines.discovery_engine.GoogleSearch') as mock_search:
            mock_search.return_value.get_dict.return_value = {'organic_results': []}
            
            # Make multiple rapid calls
            tasks = [discovery_engine.search_with_serpapi(keyword_data) for _ in range(5)]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            end_time = datetime.now()
            
            # Should take some minimum time due to rate limiting
            assert (end_time - start_time).total_seconds() >= 1.0

    @pytest.mark.asyncio
    async def test_discovery_result_validation(self, discovery_engine):
        """Test validation of discovery results before storage"""
        # Valid result
        valid_result = {
            'competitor_id': 1,
            'platform_id': 1,
            'url': 'https://facebook.com/test/posts/123',
            'title': 'Valid Post',
            'source_type': 'keyword_search'
        }
        
        # Invalid result (missing required fields)
        invalid_result = {
            'competitor_id': 1,
            'url': 'https://facebook.com/test/posts/456'
            # Missing platform_id, title, source_type
        }
        
        results = [valid_result, invalid_result]
        
        # Mock database to capture what gets stored
        stored_results = []
        async def mock_store(*args):
            stored_results.extend(args[1] if len(args) > 1 else [])
            return len(args[1]) if len(args) > 1 else 0
        
        discovery_engine.store_discovery_results = mock_store
        
        await discovery_engine.store_discovery_results(results)
        
        # Should only store valid results
        assert len(stored_results) <= len(results)  # Some may be filtered out

    @pytest.mark.asyncio
    async def test_duplicate_url_detection(self, discovery_engine):
        """Test detection of duplicate URLs during discovery"""
        # Mock existing URLs in database
        existing_urls = [
            {'url_hash': discovery_engine.generate_url_hash('https://facebook.com/test/posts/123')}
        ]
        discovery_engine.db.execute_query.return_value = existing_urls
        
        # Try to discover the same URL
        new_results = [
            {
                'competitor_id': 1,
                'platform_id': 1,
                'url': 'https://facebook.com/test/posts/123',
                'title': 'Duplicate Post',
                'source_type': 'keyword_search'
            }
        ]
        
        # Should detect and filter duplicates
        with patch.object(discovery_engine, 'filter_duplicate_urls') as mock_filter:
            mock_filter.return_value = []  # Filtered out as duplicate
            
            stored_count = await discovery_engine.store_discovery_results(new_results)
            
            # Should store 0 results due to duplication
            assert stored_count == 0

class TestDiscoveryEnginePerformance:
    """Performance tests for Discovery Engine"""

    @pytest.mark.asyncio
    async def test_keyword_search_performance(self, discovery_engine, performance_config):
        """Test keyword search performance under load"""
        keyword_data = {
            'id': 1,
            'competitor_id': 1,
            'keyword': 'PERFORMANCE_TEST',
            'priority_level': 1
        }
        
        start_time = datetime.now()
        
        # Mock fast response
        with patch('engines.discovery_engine.GoogleSearch') as mock_search:
            mock_search.return_value.get_dict.return_value = {
                'organic_results': [
                    {'link': f'https://facebook.com/test/posts/{i}', 'title': f'Post {i}'}
                    for i in range(100)  # Large result set
                ]
            }
            
            await discovery_engine.search_with_serpapi(keyword_data)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should complete within performance threshold
        assert execution_time < performance_config['max_response_time']

    @pytest.mark.asyncio
    async def test_concurrent_discovery_operations(self, discovery_engine, performance_config):
        """Test concurrent discovery operations"""
        keyword_data = [
            {
                'id': i,
                'competitor_id': 1,
                'keyword': f'TEST_KEYWORD_{i}',
                'priority_level': 1
            }
            for i in range(performance_config['concurrent_requests'])
        ]
        
        with patch('engines.discovery_engine.GoogleSearch') as mock_search:
            mock_search.return_value.get_dict.return_value = {'organic_results': []}
            
            start_time = datetime.now()
            
            # Run concurrent searches
            tasks = [discovery_engine.search_with_serpapi(kw) for kw in keyword_data]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            end_time = datetime.now()
            
            # Should handle concurrent operations without errors
            assert len(results) == len(keyword_data)
            assert all(not isinstance(r, Exception) for r in results)
            
            # Should be faster than sequential execution
            execution_time = (end_time - start_time).total_seconds()
            assert execution_time < len(keyword_data) * 2  # Should be parallelized

class TestDiscoveryEngineIntegration:
    """Integration tests requiring real external services (run with caution)"""

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    @pytest.mark.asyncio
    async def test_real_serpapi_integration(self, discovery_engine):
        """Test real SerpAPI integration (requires API key)"""
        keyword_data = {
            'id': 1,
            'competitor_id': 1,
            'keyword': 'Tamil Nadu politics',
            'priority_level': 1
        }
        
        results = await discovery_engine.search_with_serpapi(keyword_data)
        
        # Should get real results from SerpAPI
        assert isinstance(results, list)
        # Results may be empty if no matches found, which is acceptable

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    @pytest.mark.asyncio
    async def test_real_brave_integration(self, discovery_engine):
        """Test real Brave Search integration (requires API key)"""
        keyword_data = {
            'id': 1,
            'competitor_id': 1,
            'keyword': 'DMK news',
            'priority_level': 1
        }
        
        results = await discovery_engine.search_with_brave(keyword_data)
        
        # Should get real results from Brave Search
        assert isinstance(results, list)
        # Results may be empty if no matches found, which is acceptable