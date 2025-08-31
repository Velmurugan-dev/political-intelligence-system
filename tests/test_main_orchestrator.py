"""
Tests for Main Orchestrator
Tests the central coordination system that manages the entire political intelligence pipeline
"""

import os
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from main_orchestrator import PoliticalIntelligenceOrchestrator

class TestMainOrchestrator:
    """Test suite for Main Orchestrator functionality"""

    @pytest.mark.asyncio
    async def test_orchestrator_initialization(self, mock_db):
        """Test orchestrator initializes correctly"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        
        # Mock engine initialization
        with patch('main_orchestrator.DiscoveryEngine') as mock_discovery, \
             patch('main_orchestrator.EngagementEngine') as mock_engagement, \
             patch('main_orchestrator.DeduplicationEngine') as mock_dedup:
            
            mock_discovery_instance = AsyncMock()
            mock_engagement_instance = AsyncMock()
            mock_dedup_instance = AsyncMock()
            
            mock_discovery.return_value = mock_discovery_instance
            mock_engagement.return_value = mock_engagement_instance
            mock_dedup.return_value = mock_dedup_instance
            
            await orchestrator.initialize()
            
            assert orchestrator.discovery_engine == mock_discovery_instance
            assert orchestrator.engagement_engine == mock_engagement_instance
            assert orchestrator.dedup_engine == mock_dedup_instance

    @pytest.mark.asyncio
    async def test_run_discovery_cycle(self, orchestrator):
        """Test complete discovery cycle execution"""
        # Mock discovery engine
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value={
            'keywords_processed': 5,
            'sources_monitored': 3,
            'urls_discovered': 25,
            'manual_urls_processed': 2
        })
        
        # Mock deduplication
        orchestrator.dedup_engine.deduplicate_stage_results = AsyncMock(return_value={
            'duplicates_removed': 5,
            'unique_urls': 20
        })
        
        # Mock queue engagement tasks
        orchestrator._queue_engagement_tasks = AsyncMock(return_value=15)
        
        result = await orchestrator.run_discovery_cycle([1, 2], [1, 2])
        
        assert isinstance(result, dict)
        assert 'keywords_processed' in result
        assert 'urls_discovered' in result
        assert 'engagement_tasks_queued' in result
        assert result['engagement_tasks_queued'] == 15

    @pytest.mark.asyncio
    async def test_run_engagement_cycle(self, orchestrator):
        """Test complete engagement cycle execution"""
        # Mock pending URLs
        orchestrator._get_pending_engagement_urls = AsyncMock(return_value=[
            {'id': 1, 'url': 'https://facebook.com/test/posts/1', 'competitor_id': 1, 'platform_id': 1},
            {'id': 2, 'url': 'https://facebook.com/test/posts/2', 'competitor_id': 1, 'platform_id': 1}
        ])
        
        # Mock engagement processing
        orchestrator.engagement_engine.process_urls_batch = AsyncMock(return_value={
            'urls_processed': 2,
            'successful': 2,
            'failed': 0
        })
        
        # Mock content deduplication
        orchestrator.dedup_engine.deduplicate_final_results = AsyncMock(return_value={
            'content_duplicates_removed': 1,
            'unique_content': 1
        })
        
        result = await orchestrator.run_engagement_cycle([1], [1], max_urls=10)
        
        assert isinstance(result, dict)
        assert 'urls_processed' in result
        assert result['urls_processed'] == 2

    @pytest.mark.asyncio
    async def test_run_full_pipeline(self, orchestrator):
        """Test complete 2-stage pipeline execution"""
        # Mock discovery cycle
        orchestrator.run_discovery_cycle = AsyncMock(return_value={
            'urls_discovered': 20,
            'keywords_processed': 5
        })
        
        # Mock engagement cycle
        orchestrator.run_engagement_cycle = AsyncMock(return_value={
            'urls_processed': 15,
            'successful': 12,
            'failed': 3
        })
        
        result = await orchestrator.run_full_pipeline([1], [1])
        
        assert isinstance(result, dict)
        assert 'started_at' in result
        assert 'discovery' in result
        assert 'engagement' in result
        assert 'completed_at' in result
        assert result['success'] is True

    @pytest.mark.asyncio
    async def test_run_full_pipeline_discovery_only(self, orchestrator):
        """Test pipeline execution with discovery only"""
        # Mock discovery cycle
        orchestrator.run_discovery_cycle = AsyncMock(return_value={
            'urls_discovered': 20,
            'keywords_processed': 5
        })
        
        result = await orchestrator.run_full_pipeline([1], [1], discovery_only=True)
        
        assert isinstance(result, dict)
        assert 'discovery' in result
        assert 'engagement' not in result or result['engagement'] == {}
        assert result['success'] is True

    @pytest.mark.asyncio
    async def test_process_manual_urls(self, orchestrator):
        """Test manual URL processing"""
        # Mock manual queue
        manual_urls = [
            {
                'id': 1,
                'competitor_id': 1,
                'platform_id': 1,
                'url': 'https://facebook.com/manual/post/1',
                'priority_level': 1
            },
            {
                'id': 2,
                'competitor_id': 1,
                'platform_id': 1,
                'url': 'https://facebook.com/manual/post/2',
                'priority_level': 1
            }
        ]
        
        orchestrator.db.execute_query.side_effect = [
            manual_urls,  # Get manual URLs
            [],           # Update status (first URL)
            []            # Update status (second URL)
        ]
        
        # Mock engagement processing
        orchestrator.engagement_engine.process_single_url = AsyncMock(return_value={
            'success': True,
            'engagement_data': {'total_engagement': 100}
        })
        
        result = await orchestrator.process_manual_urls()
        
        assert isinstance(result, dict)
        assert 'urls_processed' in result
        assert 'successful' in result
        assert result['urls_processed'] == 2
        assert result['successful'] == 2

    @pytest.mark.asyncio
    async def test_run_monitoring_cycle(self, orchestrator):
        """Test monitoring cycle for scheduled sources"""
        # Mock monitoring schedules
        schedules = [
            {
                'id': 1,
                'source_id': 1,
                'source_name': 'Test Source',
                'competitor_name': 'ADMK',
                'platform_name': 'Facebook',
                'cron_expression': '0 */6 * * *'
            }
        ]
        
        orchestrator.db.execute_query.side_effect = [
            schedules,  # Get due schedules
            []          # Update next run time
        ]
        
        # Mock task queuing
        with patch('main_orchestrator.monitoring_task') as mock_task:
            mock_task.delay = MagicMock()
            
            result = await orchestrator.run_monitoring_cycle()
            
            assert isinstance(result, dict)
            assert 'schedules_processed' in result
            assert result['schedules_processed'] == 1
            mock_task.delay.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_analytics_summary(self, orchestrator):
        """Test analytics summary generation"""
        with patch('main_orchestrator.analytics_task') as mock_task:
            mock_task.delay = MagicMock()
            
            result = await orchestrator.generate_analytics_summary(days=7)
            
            assert isinstance(result, dict)
            assert result['success'] is True
            assert 'message' in result
            mock_task.delay.assert_called_once_with(days=7)

    @pytest.mark.asyncio
    async def test_get_system_status(self, orchestrator):
        """Test system status retrieval"""
        # Mock database queries for recent activity
        orchestrator.db.is_connected = AsyncMock(return_value=True)
        orchestrator.db.execute_query.side_effect = [
            [{'count': 50}],  # Discovery results from last 24h
            [{'count': 35}]   # Engagement results from last 24h
        ]
        
        # Mock Celery inspection
        with patch('main_orchestrator.celery_app') as mock_celery:
            mock_inspect = MagicMock()
            mock_inspect.active.return_value = {'worker1': [{'id': 'task1'}]}
            mock_celery.control.inspect.return_value = mock_inspect
            
            result = await orchestrator.get_system_status()
            
            assert isinstance(result, dict)
            assert 'timestamp' in result
            assert 'orchestrator_running' in result
            assert 'database_connected' in result
            assert 'components' in result
            assert 'queues' in result
            assert 'recent_activity' in result

    @pytest.mark.asyncio
    async def test_queue_engagement_tasks(self, orchestrator):
        """Test queuing of engagement tasks"""
        # Mock pending URLs
        pending_urls = [
            {'id': 1, 'competitor_id': 1, 'platform_id': 1, 'url': 'https://test1.com'},
            {'id': 2, 'competitor_id': 1, 'platform_id': 1, 'url': 'https://test2.com'}
        ]
        
        orchestrator.db.execute_query.return_value = pending_urls
        
        # Mock task queuing
        with patch('main_orchestrator.engagement_task') as mock_task:
            mock_task.delay = MagicMock()
            
            count = await orchestrator._queue_engagement_tasks([1], [1])
            
            assert count == 2
            assert mock_task.delay.call_count == 2

    @pytest.mark.asyncio
    async def test_get_pending_engagement_urls(self, orchestrator):
        """Test retrieval of URLs pending engagement processing"""
        pending_urls = [
            {
                'id': 1, 
                'competitor_id': 1, 
                'platform_id': 1, 
                'url': 'https://facebook.com/test/1',
                'source_type': 'keyword_search',
                'metadata': {}
            }
        ]
        
        orchestrator.db.execute_query.return_value = pending_urls
        
        urls = await orchestrator._get_pending_engagement_urls([1], [1], limit=10)
        
        assert isinstance(urls, list)
        assert len(urls) == 1
        assert urls[0]['url'] == 'https://facebook.com/test/1'

    @pytest.mark.asyncio
    async def test_error_handling_in_discovery(self, orchestrator):
        """Test error handling during discovery cycle"""
        # Mock discovery engine to raise an error
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(
            side_effect=Exception("Discovery engine error")
        )
        
        with pytest.raises(Exception) as exc_info:
            await orchestrator.run_discovery_cycle([1], [1])
        
        assert "Discovery engine error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_handling_in_engagement(self, orchestrator):
        """Test error handling during engagement cycle"""
        # Mock engagement engine to raise an error
        orchestrator._get_pending_engagement_urls = AsyncMock(
            side_effect=Exception("Engagement engine error")
        )
        
        with pytest.raises(Exception) as exc_info:
            await orchestrator.run_engagement_cycle([1], [1])
        
        assert "Engagement engine error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pipeline_with_partial_failures(self, orchestrator):
        """Test pipeline execution with partial failures"""
        # Mock discovery success but engagement failure
        orchestrator.run_discovery_cycle = AsyncMock(return_value={
            'urls_discovered': 20,
            'keywords_processed': 5
        })
        
        orchestrator.run_engagement_cycle = AsyncMock(
            side_effect=Exception("Engagement failed")
        )
        
        with pytest.raises(Exception):
            await orchestrator.run_full_pipeline([1], [1])

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, orchestrator):
        """Test handling of concurrent operations"""
        # Mock multiple simultaneous operations
        tasks = [
            orchestrator.run_discovery_cycle([1], [1]),
            orchestrator.process_manual_urls(),
            orchestrator.run_monitoring_cycle()
        ]
        
        # Mock all operations to succeed
        orchestrator.run_discovery_cycle = AsyncMock(return_value={'urls_discovered': 10})
        orchestrator.process_manual_urls = AsyncMock(return_value={'urls_processed': 5})
        orchestrator.run_monitoring_cycle = AsyncMock(return_value={'schedules_processed': 2})
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should complete without exceptions
        assert len(results) == 3
        assert all(not isinstance(r, Exception) for r in results)

    @pytest.mark.asyncio
    async def test_resource_cleanup_on_shutdown(self, orchestrator):
        """Test proper resource cleanup during shutdown"""
        # Mock database connection
        orchestrator.db.close = AsyncMock()
        
        await orchestrator.shutdown()
        
        assert orchestrator.is_running is False
        orchestrator.db.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_parameter_validation(self, orchestrator):
        """Test parameter validation in orchestrator methods"""
        # Test with invalid competitor IDs
        with patch.object(orchestrator, 'run_discovery_cycle', wraps=orchestrator.run_discovery_cycle):
            # Should handle None or empty lists gracefully
            result = await orchestrator.run_discovery_cycle(None, None)
            assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_logging_and_monitoring(self, orchestrator):
        """Test logging and monitoring functionality"""
        import logging
        
        with patch('main_orchestrator.logger') as mock_logger:
            # Mock discovery cycle
            orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value={
                'urls_discovered': 20
            })
            orchestrator.dedup_engine.deduplicate_stage_results = AsyncMock(return_value={})
            orchestrator._queue_engagement_tasks = AsyncMock(return_value=15)
            
            await orchestrator.run_discovery_cycle([1], [1])
            
            # Should log important events
            assert mock_logger.info.call_count > 0

class TestMainOrchestratorPerformance:
    """Performance tests for Main Orchestrator"""

    @pytest.mark.asyncio
    async def test_pipeline_performance_under_load(self, orchestrator, performance_config):
        """Test pipeline performance under high load"""
        # Mock engines to respond quickly
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value={
            'urls_discovered': 1000,
            'keywords_processed': 50
        })
        
        orchestrator.engagement_engine.process_urls_batch = AsyncMock(return_value={
            'urls_processed': 900,
            'successful': 850,
            'failed': 50
        })
        
        orchestrator.dedup_engine.deduplicate_stage_results = AsyncMock(return_value={})
        orchestrator.dedup_engine.deduplicate_final_results = AsyncMock(return_value={})
        orchestrator._queue_engagement_tasks = AsyncMock(return_value=900)
        orchestrator._get_pending_engagement_urls = AsyncMock(return_value=[
            {'id': i, 'url': f'https://test{i}.com', 'competitor_id': 1, 'platform_id': 1}
            for i in range(900)
        ])
        
        start_time = datetime.now()
        
        result = await orchestrator.run_full_pipeline([1, 2, 3], [1, 2, 3])
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should complete within performance threshold
        assert execution_time < performance_config['max_response_time'] * 3
        assert result['success'] is True

    @pytest.mark.asyncio
    async def test_concurrent_pipeline_executions(self, orchestrator, performance_config):
        """Test multiple concurrent pipeline executions"""
        # Mock engines for quick response
        orchestrator.run_discovery_cycle = AsyncMock(return_value={'urls_discovered': 10})
        orchestrator.run_engagement_cycle = AsyncMock(return_value={'urls_processed': 8})
        
        start_time = datetime.now()
        
        # Run multiple pipelines concurrently
        tasks = [
            orchestrator.run_full_pipeline([i], [1]) 
            for i in range(1, performance_config['concurrent_requests'] + 1)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should handle concurrent executions
        assert len(results) == performance_config['concurrent_requests']
        assert all(not isinstance(r, Exception) for r in results)
        
        # Should be faster than sequential execution
        assert execution_time < performance_config['concurrent_requests'] * 2

    @pytest.mark.asyncio
    async def test_memory_usage_during_large_pipeline(self, orchestrator):
        """Test memory usage during large pipeline execution"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Mock large-scale processing
        large_url_batch = [
            {'id': i, 'url': f'https://test{i}.com', 'competitor_id': 1, 'platform_id': 1}
            for i in range(5000)
        ]
        
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value={
            'urls_discovered': 5000
        })
        orchestrator._get_pending_engagement_urls = AsyncMock(return_value=large_url_batch)
        orchestrator.engagement_engine.process_urls_batch = AsyncMock(return_value={
            'urls_processed': 5000,
            'successful': 4500
        })
        orchestrator.dedup_engine.deduplicate_stage_results = AsyncMock(return_value={})
        orchestrator.dedup_engine.deduplicate_final_results = AsyncMock(return_value={})
        orchestrator._queue_engagement_tasks = AsyncMock(return_value=5000)
        
        await orchestrator.run_full_pipeline([1], [1])
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Should not use excessive memory
        assert memory_increase < 300  # Less than 300MB increase

class TestMainOrchestratorIntegration:
    """Integration tests requiring real components"""

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    @pytest.mark.asyncio
    async def test_full_integration_with_real_database(self, real_db):
        """Test full integration with real database"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = real_db
        
        try:
            await orchestrator.initialize()
            
            # Run a small-scale pipeline test
            result = await orchestrator.run_discovery_cycle([1], [1])
            
            assert isinstance(result, dict)
            # Results may vary based on actual data
            
        finally:
            await orchestrator.shutdown()

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    @pytest.mark.asyncio
    async def test_celery_integration(self, orchestrator):
        """Test integration with real Celery workers"""
        # This would require actual Celery workers to be running
        with patch('main_orchestrator.discovery_task') as mock_task:
            mock_task.delay = MagicMock(return_value=MagicMock(id='test-task-id'))
            
            count = await orchestrator._queue_engagement_tasks([1], [1])
            
            # Should queue tasks successfully
            assert count >= 0

class TestMainOrchestratorCLI:
    """Tests for CLI interface functionality"""

    @pytest.mark.asyncio
    async def test_cli_discovery_command(self, orchestrator):
        """Test CLI discovery command"""
        import sys
        from unittest.mock import patch
        
        # Mock command line arguments
        test_args = ['main_orchestrator.py', 'discovery', '--competitors', '1', '2']
        
        with patch.object(sys, 'argv', test_args), \
             patch('main_orchestrator.PoliticalIntelligenceOrchestrator') as mock_orch_class:
            
            mock_orch = AsyncMock()
            mock_orch_class.return_value = mock_orch
            mock_orch.initialize = AsyncMock()
            mock_orch.run_discovery_cycle = AsyncMock(return_value={'urls_discovered': 10})
            mock_orch.shutdown = AsyncMock()
            
            # Import and test CLI functionality would go here
            # This is a placeholder for CLI testing structure

    @pytest.mark.asyncio
    async def test_cli_pipeline_command(self, orchestrator):
        """Test CLI pipeline command"""
        import sys
        from unittest.mock import patch
        
        test_args = ['main_orchestrator.py', 'pipeline', '--competitors', '1', '--platforms', '1']
        
        with patch.object(sys, 'argv', test_args):
            # CLI pipeline test would go here
            pass

    @pytest.mark.asyncio
    async def test_cli_daemon_mode(self, orchestrator):
        """Test CLI daemon mode functionality"""
        # This would test the daemon loop functionality
        # Placeholder for daemon mode testing
        pass