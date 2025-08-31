"""
Tests for Engagement Engine
Tests Stage 2 engagement metrics extraction functionality including Apify integration and content processing
"""

import os
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from engines.engagement_engine import EngagementEngine
from tests.conftest import (
    assert_engagement_result_structure,
    sample_discovery_results,
    sample_engagement_results
)

class TestEngagementEngine:
    """Test suite for Engagement Engine functionality"""

    @pytest.mark.asyncio
    async def test_engine_initialization(self, mock_db):
        """Test engagement engine initializes correctly"""
        engine = EngagementEngine(mock_db)
        await engine.initialize()
        
        assert engine.db == mock_db
        assert hasattr(engine, 'apify_field_mappings')
        assert hasattr(engine, 'platform_processors')
        assert 'facebook' in engine.apify_field_mappings
        assert 'twitter' in engine.apify_field_mappings

    @pytest.mark.asyncio
    async def test_get_pending_urls(self, engagement_engine, sample_discovery_results):
        """Test retrieval of URLs pending engagement processing"""
        engagement_engine.db.execute_query.return_value = sample_discovery_results
        
        urls = await engagement_engine.get_pending_urls([1, 2])
        
        assert isinstance(urls, list)
        assert len(urls) > 0
        assert all('url' in u for u in urls)
        assert all('competitor_id' in u for u in urls)

    @pytest.mark.asyncio
    @patch('apify_client.ApifyClient')
    async def test_process_with_apify(self, mock_apify_client_class, engagement_engine):
        """Test Apify integration for engagement scraping"""
        # Setup mock Apify client
        mock_client = MagicMock()
        mock_actor = MagicMock()
        mock_run = MagicMock()
        
        mock_apify_client_class.return_value = mock_client
        mock_client.actor.return_value = mock_actor
        mock_actor.call.return_value = mock_run
        mock_run.get_dataset_items.return_value = [
            {
                'url': 'https://facebook.com/test/posts/123',
                'text': 'Test post content',
                'likes': 150,
                'comments': 25,
                'shares': 10,
                'reactions': 200,
                'author': 'Test Author',
                'timestamp': '2024-01-01T00:00:00Z'
            }
        ]
        
        url_data = {
            'url': 'https://facebook.com/test/posts/123',
            'competitor_id': 1,
            'platform_id': 1
        }
        
        result = await engagement_engine.process_with_apify(url_data, 'facebook')
        
        assert isinstance(result, dict)
        assert result['success'] is True
        assert 'engagement_data' in result
        assert result['engagement_data']['total_engagement'] > 0

    @pytest.mark.asyncio
    @patch('playwright.async_api.async_playwright')
    async def test_process_with_browser_automation(self, mock_playwright, engagement_engine):
        """Test browser automation fallback for engagement scraping"""
        # Setup mock Playwright
        mock_playwright_instance = AsyncMock()
        mock_browser = AsyncMock()
        mock_page = AsyncMock()
        
        mock_playwright.return_value.__aenter__ = AsyncMock(return_value=mock_playwright_instance)
        mock_playwright.return_value.__aexit__ = AsyncMock()
        mock_playwright_instance.chromium.launch.return_value = mock_browser
        mock_browser.new_page.return_value = mock_page
        
        # Mock page content extraction
        mock_page.goto.return_value = None
        mock_page.wait_for_load_state.return_value = None
        mock_page.locator.return_value.inner_text.return_value = "100"  # Mock engagement numbers
        
        url_data = {
            'url': 'https://facebook.com/test/posts/123',
            'competitor_id': 1,
            'platform_id': 1,
            'title': 'Test Post'
        }
        
        result = await engagement_engine.process_with_browser_automation(url_data, 'facebook')
        
        assert isinstance(result, dict)
        assert 'total_engagement' in result

    @pytest.mark.asyncio
    async def test_extract_facebook_engagement(self, engagement_engine):
        """Test Facebook-specific engagement extraction"""
        raw_data = {
            'url': 'https://facebook.com/test/posts/123',
            'text': 'Test Facebook post content',
            'likes': 150,
            'comments': 25,
            'shares': 10,
            'reactions': 200,
            'author': 'Test Page',
            'timestamp': '2024-01-01T00:00:00Z'
        }
        
        result = engagement_engine.extract_facebook_engagement(raw_data)
        
        assert 'total_engagement' in result
        assert 'likes_count' in result
        assert 'comments_count' in result
        assert 'shares_count' in result
        assert result['total_engagement'] == 385  # 200 reactions + 25 comments + 10 shares + 150 likes

    @pytest.mark.asyncio
    async def test_extract_twitter_engagement(self, engagement_engine):
        """Test Twitter-specific engagement extraction"""
        raw_data = {
            'url': 'https://twitter.com/test/status/123',
            'text': 'Test Twitter post content',
            'likes': 75,
            'retweets': 15,
            'replies': 8,
            'author': 'TestUser',
            'timestamp': '2024-01-01T00:00:00Z'
        }
        
        result = engagement_engine.extract_twitter_engagement(raw_data)
        
        assert 'total_engagement' in result
        assert 'likes_count' in result
        assert 'retweets_count' in result
        assert 'replies_count' in result
        assert result['total_engagement'] == 98  # 75 + 15 + 8

    @pytest.mark.asyncio
    async def test_extract_youtube_engagement(self, engagement_engine):
        """Test YouTube-specific engagement extraction"""
        raw_data = {
            'url': 'https://youtube.com/watch?v=test123',
            'title': 'Test YouTube video',
            'likes': 500,
            'dislikes': 25,
            'views': 10000,
            'comments': 150,
            'author': 'Test Channel',
            'timestamp': '2024-01-01T00:00:00Z'
        }
        
        result = engagement_engine.extract_youtube_engagement(raw_data)
        
        assert 'total_engagement' in result
        assert 'likes_count' in result
        assert 'views_count' in result
        assert 'comments_count' in result
        assert result['total_engagement'] == 675  # 500 + 25 + 150

    @pytest.mark.asyncio
    async def test_calculate_engagement_metrics(self, engagement_engine):
        """Test engagement metrics calculation"""
        engagement_data = {
            'likes_count': 100,
            'comments_count': 20,
            'shares_count': 10,
            'total_engagement': 130,
            'follower_count': 5000  # Assumed follower count for rate calculation
        }
        
        metrics = engagement_engine.calculate_engagement_metrics(engagement_data)
        
        assert 'engagement_rate' in metrics
        assert 'engagement_per_follower' in metrics
        assert metrics['engagement_rate'] > 0
        assert metrics['engagement_rate'] <= 100  # Should be percentage

    @pytest.mark.asyncio
    async def test_process_single_url(self, engagement_engine, mock_apify_client):
        """Test processing a single URL for engagement metrics"""
        url = 'https://facebook.com/test/posts/123'
        competitor_id = 1
        platform_id = 1
        
        # Mock Apify processing
        with patch.object(engagement_engine, 'process_with_apify') as mock_apify:
            mock_apify.return_value = {
                'success': True,
                'engagement_data': {
                    'url': url,
                    'title': 'Test Post',
                    'content': 'Test content',
                    'likes_count': 100,
                    'comments_count': 20,
                    'shares_count': 5,
                    'total_engagement': 125
                }
            }
            
            result = await engagement_engine.process_single_url(url, competitor_id, platform_id)
            
            assert isinstance(result, dict)
            assert result['success'] is True
            assert 'engagement_data' in result

    @pytest.mark.asyncio
    async def test_process_urls_batch(self, engagement_engine, sample_discovery_results):
        """Test batch processing of multiple URLs"""
        # Mock successful processing for each URL
        with patch.object(engagement_engine, 'process_single_url') as mock_process:
            mock_process.return_value = {
                'success': True,
                'engagement_data': {
                    'total_engagement': 100,
                    'likes_count': 80,
                    'comments_count': 15,
                    'shares_count': 5
                }
            }
            
            results = await engagement_engine.process_urls_batch(sample_discovery_results[:2])
            
            assert isinstance(results, dict)
            assert 'urls_processed' in results
            assert 'successful' in results
            assert 'failed' in results
            assert results['successful'] > 0

    @pytest.mark.asyncio
    async def test_store_engagement_results(self, engagement_engine, sample_engagement_results):
        """Test storing engagement results to database"""
        # Mock successful database insertion
        engagement_engine.db.execute_query.return_value = [{'id': 1}, {'id': 2}]
        
        stored_count = await engagement_engine.store_engagement_results(sample_engagement_results)
        
        assert stored_count >= 0
        engagement_engine.db.execute_query.assert_called()

    @pytest.mark.asyncio
    async def test_update_stage_results_status(self, engagement_engine):
        """Test updating stage results status after processing"""
        stage_result_ids = [1, 2, 3]
        status = 'completed'
        
        # Mock database update
        engagement_engine.db.execute_query.return_value = []
        
        await engagement_engine.update_stage_results_status(stage_result_ids, status)
        
        engagement_engine.db.execute_query.assert_called()
        # Verify the update query was called with correct parameters
        call_args = engagement_engine.db.execute_query.call_args
        assert 'UPDATE stage_results' in call_args[0][0]

    @pytest.mark.asyncio
    async def test_content_extraction_and_cleaning(self, engagement_engine):
        """Test content extraction and cleaning functionality"""
        raw_content = """
        This is a test post with lots of extra whitespace   
        
        
        And some special characters: @#$%^&*()
        
        #hashtags #test #political
        
        https://example.com/link
        """
        
        cleaned_content = engagement_engine.clean_content(raw_content)
        
        assert len(cleaned_content) < len(raw_content)
        assert cleaned_content.strip() == cleaned_content
        assert 'test post' in cleaned_content.lower()

    @pytest.mark.asyncio
    async def test_detect_content_language(self, engagement_engine):
        """Test content language detection"""
        english_content = "This is an English political post about elections"
        tamil_content = "இது தமிழ் அரசியல் பதிவு தேர்தல் பற்றி"
        
        # Mock language detection (simplified)
        eng_lang = engagement_engine.detect_language(english_content)
        tamil_lang = engagement_engine.detect_language(tamil_content)
        
        # Basic language detection test
        assert isinstance(eng_lang, str)
        assert isinstance(tamil_lang, str)

    @pytest.mark.asyncio
    async def test_engagement_with_errors(self, engagement_engine, apify_error):
        """Test engagement engine error handling"""
        url_data = {
            'url': 'https://facebook.com/test/posts/123',
            'competitor_id': 1,
            'platform_id': 1
        }
        
        # Test Apify error handling
        with patch.object(engagement_engine, 'process_with_apify') as mock_apify:
            mock_apify.side_effect = apify_error
            
            # Should fallback to browser automation
            with patch.object(engagement_engine, 'process_with_browser_automation') as mock_browser:
                mock_browser.return_value = {'success': True, 'total_engagement': 0}
                
                result = await engagement_engine.process_single_url(
                    url_data['url'], 
                    url_data['competitor_id'], 
                    url_data['platform_id']
                )
                
                assert isinstance(result, dict)
                # Should have attempted browser automation fallback
                mock_browser.assert_called_once()

    @pytest.mark.asyncio
    async def test_engagement_rate_calculation_edge_cases(self, engagement_engine):
        """Test engagement rate calculation with edge cases"""
        # Zero engagement
        zero_engagement = {
            'likes_count': 0,
            'comments_count': 0,
            'shares_count': 0,
            'total_engagement': 0,
            'follower_count': 1000
        }
        
        metrics = engagement_engine.calculate_engagement_metrics(zero_engagement)
        assert metrics['engagement_rate'] == 0.0
        
        # Zero followers (avoid division by zero)
        zero_followers = {
            'likes_count': 100,
            'comments_count': 20,
            'shares_count': 10,
            'total_engagement': 130,
            'follower_count': 0
        }
        
        metrics = engagement_engine.calculate_engagement_metrics(zero_followers)
        assert metrics['engagement_rate'] >= 0  # Should handle gracefully

    @pytest.mark.asyncio
    async def test_platform_specific_processing(self, engagement_engine):
        """Test platform-specific processing logic"""
        facebook_url = 'https://facebook.com/test/posts/123'
        twitter_url = 'https://twitter.com/test/status/456'
        youtube_url = 'https://youtube.com/watch?v=789'
        
        # Test platform detection
        assert engagement_engine.detect_platform(facebook_url) == 'facebook'
        assert engagement_engine.detect_platform(twitter_url) == 'twitter'
        assert engagement_engine.detect_platform(youtube_url) == 'youtube'
        
        # Test platform-specific actor selection
        fb_actor = engagement_engine.get_apify_actor_for_platform('facebook')
        twitter_actor = engagement_engine.get_apify_actor_for_platform('twitter')
        youtube_actor = engagement_engine.get_apify_actor_for_platform('youtube')
        
        assert fb_actor != twitter_actor
        assert twitter_actor != youtube_actor

class TestEngagementEnginePerformance:
    """Performance tests for Engagement Engine"""

    @pytest.mark.asyncio
    async def test_batch_processing_performance(self, engagement_engine, performance_config):
        """Test batch processing performance"""
        # Create large batch of URLs
        urls = [
            {
                'id': i,
                'url': f'https://facebook.com/test/posts/{i}',
                'competitor_id': 1,
                'platform_id': 1
            }
            for i in range(50)  # Large batch
        ]
        
        # Mock fast processing
        with patch.object(engagement_engine, 'process_single_url') as mock_process:
            mock_process.return_value = {
                'success': True,
                'engagement_data': {'total_engagement': 100}
            }
            
            start_time = datetime.now()
            
            results = await engagement_engine.process_urls_batch(urls)
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Should complete within reasonable time
            assert execution_time < performance_config['max_response_time'] * 2
            assert results['urls_processed'] == len(urls)

    @pytest.mark.asyncio
    async def test_concurrent_engagement_processing(self, engagement_engine, performance_config):
        """Test concurrent engagement processing"""
        urls = [
            f'https://facebook.com/test/posts/{i}'
            for i in range(performance_config['concurrent_requests'])
        ]
        
        with patch.object(engagement_engine, 'process_single_url') as mock_process:
            mock_process.return_value = {'success': True, 'engagement_data': {}}
            
            start_time = datetime.now()
            
            # Process URLs concurrently
            tasks = [
                engagement_engine.process_single_url(url, 1, 1) 
                for url in urls
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            end_time = datetime.now()
            
            # Should handle concurrent processing
            assert len(results) == len(urls)
            assert all(not isinstance(r, Exception) for r in results)
            
            # Should be faster than sequential
            execution_time = (end_time - start_time).total_seconds()
            assert execution_time < len(urls) * 0.5  # Should be parallelized

    @pytest.mark.asyncio
    async def test_memory_usage_during_batch_processing(self, engagement_engine):
        """Test memory usage during large batch processing"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Process large batch
        large_batch = [
            {
                'id': i,
                'url': f'https://facebook.com/test/posts/{i}',
                'competitor_id': 1,
                'platform_id': 1
            }
            for i in range(1000)  # Large batch
        ]
        
        with patch.object(engagement_engine, 'process_single_url') as mock_process:
            mock_process.return_value = {
                'success': True,
                'engagement_data': {'total_engagement': 100}
            }
            
            await engagement_engine.process_urls_batch(large_batch)
            
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = final_memory - initial_memory
            
            # Should not use excessive memory
            assert memory_increase < 100  # Less than 100MB increase

class TestEngagementEngineIntegration:
    """Integration tests requiring real external services"""

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    @pytest.mark.asyncio
    async def test_real_apify_integration(self, engagement_engine):
        """Test real Apify integration (requires API key)"""
        url_data = {
            'url': 'https://facebook.com/narendramodi',  # Public page
            'competitor_id': 1,
            'platform_id': 1
        }
        
        # This will make a real Apify API call
        result = await engagement_engine.process_with_apify(url_data, 'facebook')
        
        # Should get real results from Apify
        assert isinstance(result, dict)
        # May succeed or fail based on API limits, both are acceptable

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    @pytest.mark.asyncio
    async def test_real_browser_automation(self, engagement_engine):
        """Test real browser automation (requires browser)"""
        url_data = {
            'url': 'https://facebook.com/narendramodi',
            'competitor_id': 1,
            'platform_id': 1,
            'title': 'Test Page'
        }
        
        # This will launch a real browser
        result = await engagement_engine.process_with_browser_automation(url_data, 'facebook')
        
        # Should get real results from browser
        assert isinstance(result, dict)
        # May succeed or fail based on page structure, both are acceptable