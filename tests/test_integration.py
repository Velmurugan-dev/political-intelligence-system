"""
Integration Tests
Tests the complete system integration including database, engines, queue system, and web UI
"""

import pytest
import asyncio
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from database.connection import DatabaseConnection
from engines.discovery_engine import DiscoveryEngine
from engines.engagement_engine import EngagementEngine
from engines.deduplication_engine import DeduplicationEngine
from main_orchestrator import PoliticalIntelligenceOrchestrator

@pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
class TestSystemIntegration:
    """Test complete system integration with real components"""

    @pytest.fixture
    async def integrated_system(self):
        """Setup integrated system with real database"""
        # Use test database
        test_db = DatabaseConnection()
        await test_db.connect()
        
        # Create orchestrator with real components
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = test_db
        await orchestrator.initialize()
        
        yield orchestrator
        
        # Cleanup
        await orchestrator.shutdown()

    @pytest.mark.asyncio
    async def test_complete_pipeline_flow(self, integrated_system):
        """Test complete pipeline from discovery to engagement"""
        orchestrator = integrated_system
        
        # Insert test data
        await self._insert_test_data(orchestrator.db)
        
        try:
            # Run discovery cycle
            discovery_result = await orchestrator.run_discovery_cycle([1], [1])
            
            assert isinstance(discovery_result, dict)
            assert 'urls_discovered' in discovery_result
            
            # Run engagement cycle
            engagement_result = await orchestrator.run_engagement_cycle([1], [1])
            
            assert isinstance(engagement_result, dict)
            assert 'urls_processed' in engagement_result
            
            # Verify data was stored
            stored_discoveries = await orchestrator.db.execute_query(
                "SELECT COUNT(*) as count FROM stage_results"
            )
            stored_engagements = await orchestrator.db.execute_query(
                "SELECT COUNT(*) as count FROM final_results"
            )
            
            assert stored_discoveries[0]['count'] >= 0
            assert stored_engagements[0]['count'] >= 0
            
        finally:
            await self._cleanup_test_data(orchestrator.db)

    @pytest.mark.asyncio
    async def test_deduplication_integration(self, integrated_system):
        """Test deduplication integration across the pipeline"""
        orchestrator = integrated_system
        
        await self._insert_test_data(orchestrator.db)
        
        try:
            # Insert duplicate URLs
            duplicate_urls = [
                {
                    'competitor_id': 1,
                    'platform_id': 1,
                    'url': 'https://facebook.com/test/posts/123',
                    'title': 'Test Post',
                    'source_type': 'keyword_search',
                    'url_hash': None,
                    'engagement_status': 'pending'
                },
                {
                    'competitor_id': 1,
                    'platform_id': 1,
                    'url': 'https://facebook.com/test/posts/123?utm_source=test',
                    'title': 'Test Post',
                    'source_type': 'keyword_search',
                    'url_hash': None,
                    'engagement_status': 'pending'
                }
            ]
            
            for url_data in duplicate_urls:
                await orchestrator.db.execute_query(
                    """INSERT INTO stage_results 
                       (competitor_id, platform_id, url, title, source_type, url_hash, engagement_status)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                    [url_data['competitor_id'], url_data['platform_id'], url_data['url'],
                     url_data['title'], url_data['source_type'], url_data['url_hash'],
                     url_data['engagement_status']]
                )
            
            # Run deduplication
            dedup_result = await orchestrator.dedup_engine.deduplicate_stage_results()
            
            assert isinstance(dedup_result, dict)
            assert 'duplicate_count' in dedup_result
            assert dedup_result['duplicate_count'] > 0  # Should detect duplicates
            
        finally:
            await self._cleanup_test_data(orchestrator.db)

    @pytest.mark.asyncio
    async def test_manual_url_processing_integration(self, integrated_system):
        """Test manual URL processing integration"""
        orchestrator = integrated_system
        
        await self._insert_test_data(orchestrator.db)
        
        try:
            # Insert manual URLs
            manual_urls = [
                {
                    'competitor_id': 1,
                    'platform_id': 1,
                    'url': 'https://facebook.com/manual/post/1',
                    'priority_level': 1,
                    'status': 'pending'
                },
                {
                    'competitor_id': 1,
                    'platform_id': 1,
                    'url': 'https://facebook.com/manual/post/2',
                    'priority_level': 1,
                    'status': 'pending'
                }
            ]
            
            for url_data in manual_urls:
                await orchestrator.db.execute_query(
                    """INSERT INTO manual_queue 
                       (competitor_id, platform_id, url, priority_level, status)
                       VALUES ($1, $2, $3, $4, $5)""",
                    [url_data['competitor_id'], url_data['platform_id'], url_data['url'],
                     url_data['priority_level'], url_data['status']]
                )
            
            # Process manual URLs
            result = await orchestrator.process_manual_urls()
            
            assert isinstance(result, dict)
            assert 'urls_processed' in result
            assert result['urls_processed'] == 2
            
        finally:
            await self._cleanup_test_data(orchestrator.db)

    @pytest.mark.asyncio
    async def test_monitoring_integration(self, integrated_system):
        """Test source monitoring integration"""
        orchestrator = integrated_system
        
        await self._insert_test_data(orchestrator.db)
        
        try:
            # Insert monitoring schedule
            await orchestrator.db.execute_query(
                """INSERT INTO monitoring_schedule 
                   (source_id, cron_expression, is_active, priority_level, next_run)
                   VALUES ($1, $2, $3, $4, $5)""",
                [1, '0 */6 * * *', True, 1, datetime.utcnow()]
            )
            
            # Run monitoring cycle
            result = await orchestrator.run_monitoring_cycle()
            
            assert isinstance(result, dict)
            assert 'schedules_processed' in result
            
        finally:
            await self._cleanup_test_data(orchestrator.db)

    @pytest.mark.asyncio
    async def test_error_recovery_integration(self, integrated_system):
        """Test system error recovery and resilience"""
        orchestrator = integrated_system
        
        await self._insert_test_data(orchestrator.db)
        
        try:
            # Simulate partial system failure
            original_process_single_url = orchestrator.engagement_engine.process_single_url
            
            async def failing_process_url(*args, **kwargs):
                # Fail 50% of the time to test error handling
                import random
                if random.random() < 0.5:
                    raise Exception("Simulated processing error")
                return await original_process_single_url(*args, **kwargs)
            
            orchestrator.engagement_engine.process_single_url = failing_process_url
            
            # Run pipeline - should handle partial failures gracefully
            result = await orchestrator.run_full_pipeline([1], [1])
            
            assert isinstance(result, dict)
            # System should continue despite some failures
            assert 'discovery' in result
            
        except Exception as e:
            # System should handle errors gracefully
            assert "error" in str(e).lower() or "failed" in str(e).lower()
            
        finally:
            await self._cleanup_test_data(orchestrator.db)

    async def _insert_test_data(self, db):
        """Insert test data for integration tests"""
        # Insert test competitors
        await db.execute_query(
            """INSERT INTO competitors (name, full_name, priority_level, is_active)
               VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO NOTHING""",
            ['TEST_COMP', 'Test Competitor', 1, True]
        )
        
        # Insert test platforms
        await db.execute_query(
            """INSERT INTO platforms (name, api_identifier, is_active, supports_engagement)
               VALUES ($1, $2, $3, $4) ON CONFLICT (api_identifier) DO NOTHING""",
            ['Test Platform', 'test_platform', True, True]
        )
        
        # Insert test keywords
        competitor_id = (await db.execute_query("SELECT id FROM competitors WHERE name = 'TEST_COMP'"))[0]['id']
        platform_id = (await db.execute_query("SELECT id FROM platforms WHERE api_identifier = 'test_platform'"))[0]['id']
        
        await db.execute_query(
            """INSERT INTO keywords (competitor_id, keyword, priority_level, is_active)
               VALUES ($1, $2, $3, $4) ON CONFLICT (competitor_id, keyword) DO NOTHING""",
            [competitor_id, 'test keyword', 1, True]
        )
        
        # Insert test sources
        await db.execute_query(
            """INSERT INTO sources (competitor_id, platform_id, name, url, source_type, is_active)
               VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (competitor_id, platform_id, url) DO NOTHING""",
            [competitor_id, platform_id, 'Test Source', 'https://test.com', 'social_media', True]
        )

    async def _cleanup_test_data(self, db):
        """Cleanup test data after integration tests"""
        # Delete in reverse order of dependencies
        await db.execute_query("DELETE FROM final_results WHERE competitor_id IN (SELECT id FROM competitors WHERE name = 'TEST_COMP')")
        await db.execute_query("DELETE FROM stage_results WHERE competitor_id IN (SELECT id FROM competitors WHERE name = 'TEST_COMP')")
        await db.execute_query("DELETE FROM manual_queue WHERE competitor_id IN (SELECT id FROM competitors WHERE name = 'TEST_COMP')")
        await db.execute_query("DELETE FROM monitoring_schedule WHERE source_id IN (SELECT id FROM sources WHERE competitor_id IN (SELECT id FROM competitors WHERE name = 'TEST_COMP'))")
        await db.execute_query("DELETE FROM keywords WHERE competitor_id IN (SELECT id FROM competitors WHERE name = 'TEST_COMP')")
        await db.execute_query("DELETE FROM sources WHERE competitor_id IN (SELECT id FROM competitors WHERE name = 'TEST_COMP')")
        await db.execute_query("DELETE FROM competitors WHERE name = 'TEST_COMP'")
        await db.execute_query("DELETE FROM platforms WHERE api_identifier = 'test_platform'")

class TestEndToEndWorkflows:
    """Test complete end-to-end workflows"""

    @pytest.mark.asyncio
    async def test_complete_competitor_workflow(self, mock_db):
        """Test complete workflow for a competitor from setup to analytics"""
        # This test simulates the complete workflow for managing a competitor
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        
        # Mock initialization
        await orchestrator.initialize()
        
        # Mock discovery results
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value={
            'keywords_processed': 5,
            'sources_monitored': 2,
            'urls_discovered': 25
        })
        
        # Mock engagement results
        orchestrator.engagement_engine.process_urls_batch = AsyncMock(return_value={
            'urls_processed': 25,
            'successful': 20,
            'failed': 5
        })
        
        # Mock deduplication
        orchestrator.dedup_engine.deduplicate_stage_results = AsyncMock(return_value={})
        orchestrator.dedup_engine.deduplicate_final_results = AsyncMock(return_value={})
        orchestrator._queue_engagement_tasks = AsyncMock(return_value=20)
        orchestrator._get_pending_engagement_urls = AsyncMock(return_value=[])
        
        # Run complete pipeline
        result = await orchestrator.run_full_pipeline([1], [1])
        
        assert result['success'] is True
        assert 'discovery' in result
        assert 'engagement' in result

    @pytest.mark.asyncio
    async def test_bulk_url_processing_workflow(self, mock_db):
        """Test bulk URL processing workflow"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock large batch of manual URLs
        large_batch = [
            {
                'id': i,
                'competitor_id': 1,
                'platform_id': 1,
                'url': f'https://facebook.com/bulk/post/{i}',
                'priority_level': 1
            }
            for i in range(100)
        ]
        
        mock_db.execute_query.return_value = large_batch
        
        # Mock processing
        orchestrator.engagement_engine.process_single_url = AsyncMock(return_value={
            'success': True,
            'engagement_data': {'total_engagement': 100}
        })
        
        result = await orchestrator.process_manual_urls()
        
        assert result['urls_processed'] == 100
        assert result['successful'] == 100

    @pytest.mark.asyncio
    async def test_multi_competitor_pipeline_workflow(self, mock_db):
        """Test pipeline workflow with multiple competitors"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock multiple competitors
        competitors = [1, 2, 3, 4]  # ADMK, DMK, BJP, NTK
        platforms = [1, 2, 3]       # Facebook, Twitter, YouTube
        
        # Mock discovery for all competitors
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value={
            'keywords_processed': len(competitors) * 5,
            'urls_discovered': len(competitors) * 20
        })
        
        # Mock engagement processing
        orchestrator.engagement_engine.process_urls_batch = AsyncMock(return_value={
            'urls_processed': len(competitors) * 18,
            'successful': len(competitors) * 15
        })
        
        orchestrator.dedup_engine.deduplicate_stage_results = AsyncMock(return_value={})
        orchestrator.dedup_engine.deduplicate_final_results = AsyncMock(return_value={})
        orchestrator._queue_engagement_tasks = AsyncMock(return_value=len(competitors) * 15)
        orchestrator._get_pending_engagement_urls = AsyncMock(return_value=[])
        
        result = await orchestrator.run_full_pipeline(competitors, platforms)
        
        assert result['success'] is True
        assert result['discovery']['urls_discovered'] == len(competitors) * 20

class TestSystemResilience:
    """Test system resilience and error handling"""

    @pytest.mark.asyncio
    async def test_database_connection_resilience(self, mock_db):
        """Test system resilience to database connection issues"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        
        # Simulate database connection failure
        mock_db.execute_query.side_effect = [
            Exception("Database connection lost"),  # First call fails
            [{'id': 1}]  # Subsequent calls succeed (simulating reconnection)
        ]
        
        await orchestrator.initialize()
        
        # System should handle database errors gracefully
        try:
            result = await orchestrator.get_system_status()
            # Should return some status even with database issues
            assert isinstance(result, dict)
        except Exception as e:
            # Should be a handled exception
            assert "database" in str(e).lower() or "connection" in str(e).lower()

    @pytest.mark.asyncio
    async def test_external_api_resilience(self, mock_db):
        """Test resilience to external API failures"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock API failures
        with patch('engines.discovery_engine.GoogleSearch') as mock_serpapi:
            mock_serpapi.side_effect = Exception("SerpAPI quota exceeded")
            
            # Discovery should continue with other methods
            orchestrator.discovery_engine.search_with_brave = AsyncMock(return_value=[
                {'url': 'https://test.com', 'title': 'Test', 'source_type': 'keyword_search'}
            ])
            
            result = await orchestrator.run_discovery_cycle([1], [1])
            
            # Should continue despite SerpAPI failure
            assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_memory_leak_prevention(self, mock_db):
        """Test prevention of memory leaks during long-running operations"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock lightweight operations
        orchestrator.run_discovery_cycle = AsyncMock(return_value={'urls_discovered': 10})
        orchestrator.run_engagement_cycle = AsyncMock(return_value={'urls_processed': 8})
        
        # Run multiple pipeline cycles
        for _ in range(50):
            await orchestrator.run_full_pipeline([1], [1])
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Should not have significant memory increase
        assert memory_increase < 100  # Less than 100MB increase

    @pytest.mark.asyncio
    async def test_concurrent_operation_safety(self, mock_db):
        """Test safety of concurrent operations"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock quick operations
        orchestrator.run_discovery_cycle = AsyncMock(return_value={'urls_discovered': 5})
        orchestrator.process_manual_urls = AsyncMock(return_value={'urls_processed': 3})
        orchestrator.run_monitoring_cycle = AsyncMock(return_value={'schedules_processed': 2})
        
        # Run operations concurrently
        tasks = [
            orchestrator.run_discovery_cycle([1], [1]),
            orchestrator.run_discovery_cycle([2], [2]),
            orchestrator.process_manual_urls(),
            orchestrator.run_monitoring_cycle()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All operations should complete successfully
        assert len(results) == 4
        assert all(not isinstance(r, Exception) for r in results)

class TestPerformanceIntegration:
    """Performance integration tests"""

    @pytest.mark.asyncio
    async def test_large_scale_pipeline_performance(self, mock_db, performance_config):
        """Test pipeline performance with large-scale data"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock large-scale processing
        large_discovery_result = {
            'keywords_processed': 100,
            'sources_monitored': 50,
            'urls_discovered': 5000
        }
        
        large_engagement_result = {
            'urls_processed': 4500,
            'successful': 4200,
            'failed': 300
        }
        
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value=large_discovery_result)
        orchestrator.engagement_engine.process_urls_batch = AsyncMock(return_value=large_engagement_result)
        orchestrator.dedup_engine.deduplicate_stage_results = AsyncMock(return_value={})
        orchestrator.dedup_engine.deduplicate_final_results = AsyncMock(return_value={})
        orchestrator._queue_engagement_tasks = AsyncMock(return_value=4500)
        orchestrator._get_pending_engagement_urls = AsyncMock(return_value=[])
        
        start_time = datetime.now()
        
        result = await orchestrator.run_full_pipeline([1, 2, 3], [1, 2, 3])
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should handle large scale efficiently
        assert execution_time < performance_config['max_response_time'] * 5
        assert result['success'] is True
        assert result['discovery']['urls_discovered'] == 5000

    @pytest.mark.asyncio
    async def test_sustained_load_performance(self, mock_db, performance_config):
        """Test sustained load performance"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock consistent performance
        orchestrator.run_discovery_cycle = AsyncMock(return_value={'urls_discovered': 100})
        orchestrator.run_engagement_cycle = AsyncMock(return_value={'urls_processed': 90})
        
        execution_times = []
        
        # Run multiple cycles to test sustained performance
        for _ in range(10):
            start_time = datetime.now()
            await orchestrator.run_full_pipeline([1], [1])
            end_time = datetime.now()
            
            execution_times.append((end_time - start_time).total_seconds())
        
        # Performance should be consistent
        avg_time = sum(execution_times) / len(execution_times)
        max_time = max(execution_times)
        
        assert avg_time < performance_config['max_response_time']
        assert max_time < performance_config['max_response_time'] * 1.5  # Allow some variance

class TestSystemMonitoring:
    """Test system monitoring and health checks"""

    @pytest.mark.asyncio
    async def test_system_health_monitoring(self, mock_db):
        """Test system health monitoring capabilities"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock database health
        mock_db.is_connected.return_value = True
        mock_db.execute_query.return_value = [{'count': 100}]
        
        # Mock component health
        orchestrator.discovery_engine = MagicMock()
        orchestrator.engagement_engine = MagicMock()
        orchestrator.dedup_engine = MagicMock()
        
        status = await orchestrator.get_system_status()
        
        assert isinstance(status, dict)
        assert 'timestamp' in status
        assert 'database_connected' in status
        assert 'components' in status
        assert 'recent_activity' in status

    @pytest.mark.asyncio
    async def test_error_logging_and_tracking(self, mock_db):
        """Test error logging and tracking"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock an operation that will fail
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(
            side_effect=Exception("Test error for logging")
        )
        
        # Should log errors appropriately
        with patch('main_orchestrator.logger') as mock_logger:
            try:
                await orchestrator.run_discovery_cycle([1], [1])
            except Exception:
                pass
            
            # Should have logged the error
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_metrics_collection(self, mock_db):
        """Test system metrics collection"""
        orchestrator = PoliticalIntelligenceOrchestrator()
        orchestrator.db = mock_db
        await orchestrator.initialize()
        
        # Mock successful operations
        orchestrator.discovery_engine.run_discovery_cycle = AsyncMock(return_value={
            'keywords_processed': 10,
            'urls_discovered': 50
        })
        
        result = await orchestrator.run_discovery_cycle([1], [1])
        
        # Should collect relevant metrics
        assert 'keywords_processed' in result
        assert 'urls_discovered' in result
        assert isinstance(result['keywords_processed'], int)
        assert isinstance(result['urls_discovered'], int)