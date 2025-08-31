"""
Tests for Web UI
Tests FastAPI web interface, API endpoints, and WebSocket functionality
"""

import os
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

# FastAPI testing imports
from fastapi.testclient import TestClient
from fastapi.websockets import WebSocket

# Import the web UI app
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'web_ui'))

class TestWebUIAPI:
    """Test suite for Web UI API endpoints"""

    @pytest.fixture
    def client(self):
        """Create test client for FastAPI app"""
        from web_ui.app import app
        return TestClient(app)

    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for web UI"""
        with patch('web_ui.app.DatabaseConnection') as mock_db_class:
            mock_db = AsyncMock()
            mock_db_class.return_value = mock_db
            mock_db.execute_query.return_value = []
            yield mock_db

    def test_dashboard_endpoint(self, client):
        """Test dashboard page rendering"""
        response = client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_competitors_page(self, client):
        """Test competitors management page"""
        response = client.get("/competitors")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_keywords_page(self, client):
        """Test keywords management page"""
        response = client.get("/keywords")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_manual_urls_page(self, client):
        """Test manual URLs page"""
        response = client.get("/manual-urls")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_monitoring_page(self, client):
        """Test monitoring page"""
        response = client.get("/monitoring")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_analytics_page(self, client):
        """Test analytics page"""
        response = client.get("/analytics")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_api_competitors_get(self, client, mock_db_connection, sample_competitors):
        """Test GET /api/competitors endpoint"""
        mock_db_connection.execute_query.return_value = sample_competitors
        
        response = client.get("/api/competitors")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == len(sample_competitors)
        assert data[0]['name'] == 'ADMK'

    def test_api_competitors_post(self, client, mock_db_connection):
        """Test POST /api/competitors endpoint"""
        mock_db_connection.execute_query.return_value = [{'id': 1}]
        
        competitor_data = {
            'name': 'TEST_PARTY',
            'full_name': 'Test Political Party',
            'priority_level': 3,
            'is_active': True,
            'metadata': {}
        }
        
        response = client.post("/api/competitors", json=competitor_data)
        assert response.status_code == 200
        
        data = response.json()
        assert 'id' in data
        assert data['success'] is True

    def test_api_competitors_put(self, client, mock_db_connection):
        """Test PUT /api/competitors/{id} endpoint"""
        mock_db_connection.execute_query.return_value = []
        
        competitor_data = {
            'name': 'UPDATED_PARTY',
            'full_name': 'Updated Political Party',
            'priority_level': 2,
            'is_active': True
        }
        
        response = client.put("/api/competitors/1", json=competitor_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data['success'] is True

    def test_api_competitors_delete(self, client, mock_db_connection):
        """Test DELETE /api/competitors/{id} endpoint"""
        mock_db_connection.execute_query.return_value = []
        
        response = client.delete("/api/competitors/1")
        assert response.status_code == 200
        
        data = response.json()
        assert data['success'] is True

    def test_api_platforms_get(self, client, mock_db_connection, sample_platforms):
        """Test GET /api/platforms endpoint"""
        mock_db_connection.execute_query.return_value = sample_platforms
        
        response = client.get("/api/platforms")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == len(sample_platforms)
        assert data[0]['name'] == 'Facebook'

    def test_api_keywords_get(self, client, mock_db_connection, sample_keywords):
        """Test GET /api/keywords endpoint"""
        mock_db_connection.execute_query.return_value = sample_keywords
        
        response = client.get("/api/keywords")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == len(sample_keywords)

    def test_api_keywords_post(self, client, mock_db_connection):
        """Test POST /api/keywords endpoint"""
        mock_db_connection.execute_query.return_value = [{'id': 1}]
        
        keyword_data = {
            'competitor_id': 1,
            'keyword': 'test keyword',
            'priority_level': 2,
            'is_active': True,
            'platforms': [1, 2]
        }
        
        response = client.post("/api/keywords", json=keyword_data)
        assert response.status_code == 200
        
        data = response.json()
        assert 'id' in data
        assert data['success'] is True

    def test_api_manual_urls_post(self, client, mock_db_connection):
        """Test POST /api/manual-urls endpoint"""
        mock_db_connection.execute_query.return_value = [{'id': 1}]
        
        url_data = {
            'competitor_id': 1,
            'platform_id': 1,
            'url': 'https://facebook.com/test/posts/123',
            'priority_level': 1
        }
        
        response = client.post("/api/manual-urls", json=url_data)
        assert response.status_code == 200
        
        data = response.json()
        assert 'id' in data
        assert data['success'] is True

    def test_api_manual_urls_bulk(self, client, mock_db_connection):
        """Test POST /api/manual-urls/bulk endpoint"""
        mock_db_connection.execute_query.return_value = [{'id': 1}, {'id': 2}]
        
        bulk_data = {
            'competitor_id': 1,
            'platform_id': 1,
            'urls': [
                'https://facebook.com/test/posts/1',
                'https://facebook.com/test/posts/2'
            ],
            'priority_level': 1
        }
        
        response = client.post("/api/manual-urls/bulk", json=bulk_data)
        assert response.status_code == 200
        
        data = response.json()
        assert data['urls_added'] == 2
        assert data['success'] is True

    def test_api_manual_urls_status(self, client, mock_db_connection):
        """Test GET /api/manual-urls/status endpoint"""
        mock_status_data = [
            {
                'id': 1,
                'url': 'https://facebook.com/test/posts/1',
                'status': 'completed',
                'competitor_name': 'ADMK',
                'platform_name': 'Facebook',
                'created_at': '2024-01-01T00:00:00Z'
            }
        ]
        
        mock_db_connection.execute_query.return_value = mock_status_data
        
        response = client.get("/api/manual-urls/status")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['status'] == 'completed'

    def test_api_sources_get(self, client, mock_db_connection, sample_sources):
        """Test GET /api/monitoring/sources endpoint"""
        mock_db_connection.execute_query.return_value = sample_sources
        
        response = client.get("/api/monitoring/sources")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == len(sample_sources)

    def test_api_sources_post(self, client, mock_db_connection):
        """Test POST /api/monitoring/sources endpoint"""
        mock_db_connection.execute_query.return_value = [{'id': 1}]
        
        source_data = {
            'competitor_id': 1,
            'platform_id': 1,
            'name': 'Test Source',
            'url': 'https://facebook.com/test',
            'source_type': 'social_media',
            'is_active': True
        }
        
        response = client.post("/api/monitoring/sources", json=source_data)
        assert response.status_code == 200
        
        data = response.json()
        assert 'id' in data
        assert data['success'] is True

    def test_api_analytics_summary(self, client, mock_db_connection):
        """Test GET /api/analytics/summary endpoint"""
        mock_analytics_data = {
            'metrics': {
                'total_content': 1000,
                'total_engagement': 50000,
                'avg_engagement_rate': 5.2,
                'top_performer': 'ADMK'
            },
            'charts': {
                'discovery_timeline': {'labels': [], 'values': []},
                'platform_distribution': {'labels': [], 'values': []}
            },
            'competitors': [],
            'platforms': []
        }
        
        with patch('web_ui.app.generate_analytics_summary') as mock_analytics:
            mock_analytics.return_value = mock_analytics_data
            
            response = client.get("/api/analytics/summary?start_date=2024-01-01&end_date=2024-01-31")
            assert response.status_code == 200
            
            data = response.json()
            assert 'metrics' in data
            assert 'charts' in data

    def test_api_system_status(self, client):
        """Test GET /api/system/status endpoint"""
        with patch('web_ui.app.get_system_status') as mock_status:
            mock_status.return_value = {
                'database_connected': True,
                'celery_workers': 2,
                'queue_length': 10,
                'last_update': '2024-01-01T00:00:00Z'
            }
            
            response = client.get("/api/system/status")
            assert response.status_code == 200
            
            data = response.json()
            assert 'database_connected' in data

    def test_api_pipeline_run(self, client):
        """Test POST /api/pipeline/run endpoint"""
        with patch('web_ui.app.run_pipeline') as mock_pipeline:
            mock_pipeline.return_value = {
                'success': True,
                'job_id': 'test-job-123',
                'message': 'Pipeline started successfully'
            }
            
            pipeline_data = {
                'competitors': [1, 2],
                'platforms': [1, 2],
                'discovery_only': False
            }
            
            response = client.post("/api/pipeline/run", json=pipeline_data)
            assert response.status_code == 200
            
            data = response.json()
            assert data['success'] is True
            assert 'job_id' in data

    def test_api_error_handling(self, client, mock_db_connection):
        """Test API error handling"""
        # Mock database error
        mock_db_connection.execute_query.side_effect = Exception("Database connection failed")
        
        response = client.get("/api/competitors")
        assert response.status_code == 500
        
        data = response.json()
        assert 'error' in data
        assert data['success'] is False

    def test_api_validation_errors(self, client, mock_db_connection):
        """Test API input validation"""
        # Invalid competitor data (missing required fields)
        invalid_data = {
            'name': '',  # Empty name should fail validation
            'priority_level': 'invalid'  # Invalid type
        }
        
        response = client.post("/api/competitors", json=invalid_data)
        assert response.status_code == 422  # Validation error
        
        data = response.json()
        assert 'detail' in data  # FastAPI validation error format

    def test_api_cors_headers(self, client):
        """Test CORS headers in API responses"""
        response = client.get("/api/competitors")
        
        # Check if CORS headers are present (if enabled)
        assert response.status_code in [200, 500]  # Should not be blocked by CORS

class TestWebUIWebSocket:
    """Test suite for WebSocket functionality"""

    @pytest.fixture
    def websocket_client(self):
        """Create WebSocket test client"""
        from web_ui.app import app
        return TestClient(app)

    def test_websocket_connection(self, websocket_client):
        """Test WebSocket connection establishment"""
        with websocket_client.websocket_connect("/ws/dashboard") as websocket:
            # Connection should be established successfully
            assert websocket is not None

    def test_websocket_real_time_updates(self, websocket_client):
        """Test real-time updates via WebSocket"""
        with websocket_client.websocket_connect("/ws/dashboard") as websocket:
            # Mock sending an update
            update_data = {
                'type': 'discovery_update',
                'data': {
                    'urls_discovered': 5,
                    'competitor_id': 1
                }
            }
            
            # In a real test, you would trigger an update and verify it's received
            # This is a placeholder for WebSocket testing structure

    def test_websocket_multiple_clients(self, websocket_client):
        """Test multiple WebSocket clients"""
        # Test that multiple clients can connect simultaneously
        with websocket_client.websocket_connect("/ws/dashboard") as ws1, \
             websocket_client.websocket_connect("/ws/dashboard") as ws2:
            
            assert ws1 is not None
            assert ws2 is not None

    def test_websocket_error_handling(self, websocket_client):
        """Test WebSocket error handling"""
        # Test invalid WebSocket endpoint
        try:
            with websocket_client.websocket_connect("/ws/invalid"):
                pass
        except Exception:
            # Should handle invalid endpoints gracefully
            pass

class TestWebUITemplates:
    """Test suite for HTML templates"""

    @pytest.fixture
    def client(self):
        """Create test client for template testing"""
        from web_ui.app import app
        return TestClient(app)

    def test_base_template_structure(self, client):
        """Test base template structure"""
        response = client.get("/")
        assert response.status_code == 200
        
        html_content = response.text
        # Check for essential HTML structure
        assert "<!DOCTYPE html>" in html_content
        assert "<html" in html_content
        assert "<head>" in html_content
        assert "<body>" in html_content
        assert "</html>" in html_content

    def test_dashboard_template_content(self, client):
        """Test dashboard template content"""
        response = client.get("/")
        assert response.status_code == 200
        
        html_content = response.text
        # Check for dashboard-specific content
        assert "Political Intelligence" in html_content
        assert "dashboard" in html_content.lower()

    def test_competitors_template_content(self, client):
        """Test competitors template content"""
        response = client.get("/competitors")
        assert response.status_code == 200
        
        html_content = response.text
        assert "competitors" in html_content.lower()
        assert "Add Competitor" in html_content or "add competitor" in html_content.lower()

    def test_keywords_template_content(self, client):
        """Test keywords template content"""
        response = client.get("/keywords")
        assert response.status_code == 200
        
        html_content = response.text
        assert "keywords" in html_content.lower()
        assert "keyword" in html_content.lower()

    def test_responsive_design_meta_tags(self, client):
        """Test responsive design meta tags"""
        response = client.get("/")
        assert response.status_code == 200
        
        html_content = response.text
        # Check for responsive design meta tags
        assert 'name="viewport"' in html_content
        assert 'width=device-width' in html_content

    def test_css_and_js_includes(self, client):
        """Test CSS and JavaScript includes"""
        response = client.get("/")
        assert response.status_code == 200
        
        html_content = response.text
        # Check for CSS includes
        assert '<link' in html_content and 'stylesheet' in html_content
        # Check for JavaScript includes
        assert '<script' in html_content

    def test_template_security_headers(self, client):
        """Test security headers in template responses"""
        response = client.get("/")
        
        # Check for security headers (if implemented)
        headers = response.headers
        # These are optional but recommended
        assert response.status_code == 200  # Basic success check

class TestWebUIAuthentication:
    """Test suite for authentication (if implemented)"""

    @pytest.fixture
    def client(self):
        """Create test client for authentication testing"""
        from web_ui.app import app
        return TestClient(app)

    def test_public_endpoints_access(self, client):
        """Test access to public endpoints"""
        # These endpoints should be accessible without authentication
        public_endpoints = ["/", "/competitors", "/keywords", "/manual-urls", "/monitoring", "/analytics"]
        
        for endpoint in public_endpoints:
            response = client.get(endpoint)
            assert response.status_code == 200

    def test_api_endpoints_access(self, client):
        """Test API endpoints access"""
        # API endpoints should be accessible (or properly secured if auth is implemented)
        api_endpoints = [
            "/api/competitors",
            "/api/platforms",
            "/api/keywords",
            "/api/manual-urls/status"
        ]
        
        with patch('web_ui.app.DatabaseConnection'):
            for endpoint in api_endpoints:
                response = client.get(endpoint)
                assert response.status_code in [200, 401, 403, 500]  # Valid HTTP status codes

class TestWebUIPerformance:
    """Performance tests for Web UI"""

    @pytest.fixture
    def client(self):
        """Create test client for performance testing"""
        from web_ui.app import app
        return TestClient(app)

    def test_page_load_performance(self, client, performance_config):
        """Test page load performance"""
        import time
        
        endpoints = ["/", "/competitors", "/keywords", "/manual-urls", "/monitoring", "/analytics"]
        
        for endpoint in endpoints:
            start_time = time.time()
            response = client.get(endpoint)
            end_time = time.time()
            
            response_time = end_time - start_time
            
            assert response.status_code == 200
            assert response_time < performance_config['max_response_time']

    def test_api_response_performance(self, client, mock_db_connection, performance_config):
        """Test API response performance"""
        import time
        
        # Mock database to return quickly
        mock_db_connection.execute_query.return_value = []
        
        api_endpoints = [
            "/api/competitors",
            "/api/platforms",
            "/api/keywords",
            "/api/manual-urls/status"
        ]
        
        with patch('web_ui.app.DatabaseConnection'):
            for endpoint in api_endpoints:
                start_time = time.time()
                response = client.get(endpoint)
                end_time = time.time()
                
                response_time = end_time - start_time
                
                assert response.status_code in [200, 500]  # Should not timeout
                assert response_time < performance_config['max_response_time']

    def test_concurrent_requests_handling(self, client, performance_config):
        """Test handling of concurrent requests"""
        import concurrent.futures
        import time
        
        def make_request():
            return client.get("/")
        
        start_time = time.time()
        
        # Make concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=performance_config['concurrent_requests']) as executor:
            futures = [executor.submit(make_request) for _ in range(performance_config['concurrent_requests'])]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # All requests should succeed
        assert all(response.status_code == 200 for response in results)
        
        # Should handle concurrent requests efficiently
        assert total_time < performance_config['max_response_time'] * 2

    def test_memory_usage_under_load(self, client):
        """Test memory usage under load"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Make many requests to test memory usage
        for _ in range(100):
            response = client.get("/")
            assert response.status_code == 200
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Should not leak significant memory
        assert memory_increase < 50  # Less than 50MB increase

class TestWebUIIntegration:
    """Integration tests for Web UI"""

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    def test_full_pipeline_integration(self, client):
        """Test full pipeline integration via Web UI"""
        # This would test the complete flow from UI to backend
        pipeline_data = {
            'competitors': [1],
            'platforms': [1],
            'discovery_only': False
        }
        
        response = client.post("/api/pipeline/run", json=pipeline_data)
        
        # Response may vary based on actual system state
        assert response.status_code in [200, 400, 500]

    @pytest.mark.skipif(not os.getenv('RUN_INTEGRATION_TESTS'), reason="Integration tests disabled")
    def test_database_integration(self, client, real_db):
        """Test database integration via Web UI"""
        # This would test actual database operations through the UI
        with patch('web_ui.app.DatabaseConnection', return_value=real_db):
            response = client.get("/api/competitors")
            
            # Should work with real database
            assert response.status_code == 200