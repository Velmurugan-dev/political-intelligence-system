"""
Test configuration and fixtures for Political Intelligence System
"""

import pytest
import asyncio
import os
import sys
from typing import Generator, AsyncGenerator
from unittest.mock import MagicMock, AsyncMock

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database.connection import DatabaseConnection
from engines.discovery_engine import DiscoveryEngine
from engines.engagement_engine import EngagementEngine
from engines.deduplication_engine import DeduplicationEngine
from main_orchestrator import PoliticalIntelligenceOrchestrator

# Test database configuration
TEST_DATABASE_URL = os.getenv('TEST_DATABASE_URL', 'postgresql://test_user:test_pass@localhost/test_political_intelligence')

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def mock_db() -> AsyncMock:
    """Mock database connection for testing"""
    db = AsyncMock(spec=DatabaseConnection)
    db.is_connected.return_value = True
    db.execute_query.return_value = []
    return db

@pytest.fixture
async def real_db() -> AsyncGenerator[DatabaseConnection, None]:
    """Real database connection for integration tests (requires test database)"""
    if not os.getenv('RUN_INTEGRATION_TESTS'):
        pytest.skip("Integration tests require RUN_INTEGRATION_TESTS=1 environment variable")
    
    db = DatabaseConnection()
    try:
        await db.connect()
        yield db
    finally:
        await db.close()

@pytest.fixture
async def discovery_engine(mock_db) -> DiscoveryEngine:
    """Discovery engine instance with mocked database"""
    engine = DiscoveryEngine(mock_db)
    await engine.initialize()
    return engine

@pytest.fixture
async def engagement_engine(mock_db) -> EngagementEngine:
    """Engagement engine instance with mocked database"""
    engine = EngagementEngine(mock_db)
    await engine.initialize()
    return engine

@pytest.fixture
async def deduplication_engine(mock_db) -> DeduplicationEngine:
    """Deduplication engine instance with mocked database"""
    engine = DeduplicationEngine(mock_db)
    await engine.initialize()
    return engine

@pytest.fixture
async def orchestrator(mock_db) -> PoliticalIntelligenceOrchestrator:
    """Main orchestrator instance with mocked database"""
    orchestrator = PoliticalIntelligenceOrchestrator()
    orchestrator.db = mock_db
    await orchestrator.initialize()
    return orchestrator

@pytest.fixture
def sample_competitors():
    """Sample competitor data for testing"""
    return [
        {'id': 1, 'name': 'ADMK', 'full_name': 'All India Anna Dravida Munnetra Kazhagam', 'priority_level': 1, 'is_active': True},
        {'id': 2, 'name': 'DMK', 'full_name': 'Dravida Munnetra Kazhagam', 'priority_level': 1, 'is_active': True},
        {'id': 3, 'name': 'BJP', 'full_name': 'Bharatiya Janata Party', 'priority_level': 2, 'is_active': True},
    ]

@pytest.fixture
def sample_platforms():
    """Sample platform data for testing"""
    return [
        {'id': 1, 'name': 'Facebook', 'api_identifier': 'facebook', 'is_active': True, 'supports_engagement': True},
        {'id': 2, 'name': 'Twitter', 'api_identifier': 'twitter', 'is_active': True, 'supports_engagement': True},
        {'id': 3, 'name': 'YouTube', 'api_identifier': 'youtube', 'is_active': True, 'supports_engagement': True},
    ]

@pytest.fixture
def sample_keywords():
    """Sample keyword data for testing"""
    return [
        {'id': 1, 'competitor_id': 1, 'keyword': 'ADMK', 'priority_level': 1, 'is_active': True},
        {'id': 2, 'competitor_id': 1, 'keyword': 'அதிமுக', 'priority_level': 1, 'is_active': True},
        {'id': 3, 'competitor_id': 2, 'keyword': 'DMK', 'priority_level': 1, 'is_active': True},
        {'id': 4, 'competitor_id': 2, 'keyword': 'Stalin', 'priority_level': 2, 'is_active': True},
    ]

@pytest.fixture
def sample_sources():
    """Sample source data for testing"""
    return [
        {
            'id': 1, 
            'competitor_id': 1, 
            'platform_id': 1, 
            'name': 'ADMK Official Facebook', 
            'url': 'https://facebook.com/AIADMKOfficial', 
            'source_type': 'social_media',
            'is_active': True
        },
        {
            'id': 2, 
            'competitor_id': 2, 
            'platform_id': 2, 
            'name': 'Stalin Twitter', 
            'url': 'https://twitter.com/mkstalin', 
            'source_type': 'social_media',
            'is_active': True
        },
    ]

@pytest.fixture
def sample_discovery_results():
    """Sample discovery results data for testing"""
    return [
        {
            'id': 1,
            'competitor_id': 1,
            'platform_id': 1,
            'url': 'https://facebook.com/AIADMKOfficial/posts/123456',
            'title': 'ADMK Party Update',
            'source_type': 'keyword_search',
            'url_hash': 'abc123def456',
            'engagement_status': 'pending',
            'priority_level': 1,
            'metadata': {'search_query': 'ADMK', 'found_at': '2024-01-01T00:00:00Z'}
        },
        {
            'id': 2,
            'competitor_id': 2,
            'platform_id': 2,
            'url': 'https://twitter.com/mkstalin/status/123456789',
            'title': 'DMK Leadership Tweet',
            'source_type': 'source_monitoring',
            'url_hash': 'def456ghi789',
            'engagement_status': 'pending',
            'priority_level': 1,
            'metadata': {'source_id': 2, 'monitored_at': '2024-01-01T01:00:00Z'}
        }
    ]

@pytest.fixture
def sample_engagement_results():
    """Sample engagement results data for testing"""
    return [
        {
            'id': 1,
            'stage_result_id': 1,
            'competitor_id': 1,
            'platform_id': 1,
            'url': 'https://facebook.com/AIADMKOfficial/posts/123456',
            'title': 'ADMK Party Update',
            'content': 'Latest update from ADMK party leadership...',
            'author': 'ADMK Official',
            'published_at': '2024-01-01T00:00:00Z',
            'likes_count': 1500,
            'comments_count': 250,
            'shares_count': 100,
            'total_engagement': 1850,
            'engagement_rate': 12.5,
            'content_hash': 'content123hash456'
        }
    ]

@pytest.fixture
def mock_apify_client():
    """Mock Apify client for testing"""
    mock_client = MagicMock()
    mock_actor = MagicMock()
    mock_run = MagicMock()
    
    # Setup mock chain
    mock_client.actor.return_value = mock_actor
    mock_actor.call.return_value = mock_run
    mock_run.get_dataset_items.return_value = [
        {
            'url': 'https://facebook.com/test/posts/123',
            'text': 'Test post content',
            'likes': 100,
            'comments': 20,
            'shares': 5
        }
    ]
    
    return mock_client

@pytest.fixture
def mock_serpapi_client():
    """Mock SerpAPI client for testing"""
    mock_client = MagicMock()
    mock_client.get_dict.return_value = {
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
    return mock_client

@pytest.fixture
def mock_redis_client():
    """Mock Redis client for testing"""
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    mock_redis.set.return_value = True
    mock_redis.delete.return_value = 1
    mock_redis.exists.return_value = False
    return mock_redis

# Test data validation helpers
def assert_competitor_structure(competitor):
    """Assert that competitor data has required structure"""
    required_fields = ['id', 'name', 'full_name', 'priority_level', 'is_active']
    for field in required_fields:
        assert field in competitor

def assert_platform_structure(platform):
    """Assert that platform data has required structure"""
    required_fields = ['id', 'name', 'api_identifier', 'is_active', 'supports_engagement']
    for field in required_fields:
        assert field in platform

def assert_keyword_structure(keyword):
    """Assert that keyword data has required structure"""
    required_fields = ['id', 'competitor_id', 'keyword', 'priority_level', 'is_active']
    for field in required_fields:
        assert field in keyword

def assert_discovery_result_structure(result):
    """Assert that discovery result has required structure"""
    required_fields = ['id', 'competitor_id', 'platform_id', 'url', 'url_hash', 'engagement_status']
    for field in required_fields:
        assert field in result

def assert_engagement_result_structure(result):
    """Assert that engagement result has required structure"""
    required_fields = ['id', 'stage_result_id', 'competitor_id', 'platform_id', 'url', 'total_engagement']
    for field in required_fields:
        assert field in result

# Performance testing helpers
@pytest.fixture
def performance_config():
    """Configuration for performance tests"""
    return {
        'max_response_time': 5.0,  # seconds
        'max_memory_usage': 500,   # MB
        'concurrent_requests': 10,
        'test_duration': 30        # seconds
    }

# Error simulation helpers
@pytest.fixture
def network_error():
    """Simulate network errors"""
    from requests.exceptions import ConnectionError
    return ConnectionError("Simulated network error")

@pytest.fixture
def database_error():
    """Simulate database errors"""
    from asyncpg.exceptions import PostgresError
    return PostgresError("Simulated database error")

@pytest.fixture
def apify_error():
    """Simulate Apify API errors"""
    class ApifyError(Exception):
        pass
    return ApifyError("Simulated Apify API error")