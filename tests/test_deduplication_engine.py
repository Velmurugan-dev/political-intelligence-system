"""
Tests for Deduplication Engine
Tests URL normalization, content similarity detection, and multi-level deduplication
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from engines.deduplication_engine import DeduplicationEngine

class TestDeduplicationEngine:
    """Test suite for Deduplication Engine functionality"""

    @pytest.mark.asyncio
    async def test_engine_initialization(self, mock_db):
        """Test deduplication engine initializes correctly"""
        engine = DeduplicationEngine(mock_db)
        await engine.initialize()
        
        assert engine.db == mock_db
        assert hasattr(engine, 'url_normalizers')
        assert hasattr(engine, 'content_processors')
        assert hasattr(engine, 'similarity_threshold')

    @pytest.mark.asyncio
    async def test_url_normalization(self, deduplication_engine):
        """Test URL normalization for deduplication"""
        test_urls = [
            'https://facebook.com/test/posts/123?utm_source=twitter&utm_campaign=test',
            'http://facebook.com/test/posts/123',
            'https://facebook.com/test/posts/123/',
            'https://www.facebook.com/test/posts/123#comment-456',
            'https://m.facebook.com/test/posts/123'
        ]
        
        normalized_urls = [deduplication_engine.normalize_url(url) for url in test_urls]
        
        # All should normalize to the same base URL
        base_url = normalized_urls[0]
        assert all(url == base_url for url in normalized_urls)
        
        # Should remove tracking parameters, fragments, mobile prefixes
        assert 'utm_source' not in base_url
        assert 'utm_campaign' not in base_url
        assert '#comment' not in base_url
        assert base_url.startswith('https://')
        assert 'm.facebook.com' not in base_url

    @pytest.mark.asyncio
    async def test_url_hash_generation(self, deduplication_engine):
        """Test URL hash generation for indexing"""
        url1 = 'https://facebook.com/test/posts/123'
        url2 = 'https://facebook.com/test/posts/123?utm_source=test'
        url3 = 'https://facebook.com/test/posts/456'
        
        hash1 = deduplication_engine.generate_url_hash(url1)
        hash2 = deduplication_engine.generate_url_hash(url2)
        hash3 = deduplication_engine.generate_url_hash(url3)
        
        # Same normalized URLs should have same hash
        assert hash1 == hash2
        # Different URLs should have different hashes
        assert hash1 != hash3
        
        # Hash should be consistent and reasonable length
        assert len(hash1) > 10  # SHA-based hash should be long enough
        assert hash1 == deduplication_engine.generate_url_hash(url1)

    @pytest.mark.asyncio
    async def test_platform_specific_url_handling(self, deduplication_engine):
        """Test platform-specific URL normalization"""
        # Facebook URLs
        fb_urls = [
            'https://facebook.com/page/posts/123456',
            'https://www.facebook.com/page/posts/123456?fbclid=test',
            'https://m.facebook.com/page/posts/123456'
        ]
        
        fb_normalized = [deduplication_engine.normalize_url(url) for url in fb_urls]
        assert all(url == fb_normalized[0] for url in fb_normalized)
        
        # Twitter URLs
        twitter_urls = [
            'https://twitter.com/user/status/123456789',
            'https://x.com/user/status/123456789',
            'https://mobile.twitter.com/user/status/123456789?s=20'
        ]
        
        twitter_normalized = [deduplication_engine.normalize_url(url) for url in twitter_urls]
        assert all(url == twitter_normalized[0] for url in twitter_normalized)

    @pytest.mark.asyncio
    async def test_content_similarity_detection(self, deduplication_engine):
        """Test content similarity detection"""
        # Very similar content
        content1 = "Tamil Nadu Chief Minister announces new education policy for state schools"
        content2 = "TN CM announces new education policy for schools in the state"
        
        # Completely different content
        content3 = "Weather update: Heavy rains expected in Chennai tomorrow"
        
        similarity_1_2 = deduplication_engine.calculate_content_similarity(content1, content2)
        similarity_1_3 = deduplication_engine.calculate_content_similarity(content1, content3)
        
        # Similar content should have high similarity
        assert similarity_1_2 > 0.7  # Should be quite similar
        
        # Different content should have low similarity
        assert similarity_1_3 < 0.3  # Should be quite different

    @pytest.mark.asyncio
    async def test_trigram_similarity(self, deduplication_engine):
        """Test trigram-based text similarity"""
        text1 = "ADMK party announces candidate list for upcoming elections"
        text2 = "AIADMK announces candidates for the upcoming polls"
        text3 = "DMK releases manifesto for state assembly elections"
        
        # Using trigram similarity
        sim_1_2 = deduplication_engine.trigram_similarity(text1, text2)
        sim_1_3 = deduplication_engine.trigram_similarity(text1, text3)
        
        # Similar content should have higher similarity
        assert sim_1_2 > sim_1_3
        
        # Similarity should be between 0 and 1
        assert 0 <= sim_1_2 <= 1
        assert 0 <= sim_1_3 <= 1

    @pytest.mark.asyncio
    async def test_tamil_content_similarity(self, deduplication_engine):
        """Test similarity detection for Tamil content"""
        tamil_content1 = "தமிழ்நாடு முதலமைச்சர் புதிய கல்வி கொள்கையை அறிவித்தார்"
        tamil_content2 = "தமிழ்நாடு முதலமைச்சர் கல்வி கொள்கையை வெளியிட்டார்"
        tamil_content3 = "சென்னையில் நாளை கனமழை பெய்ய வாய்ப்பு"
        
        similarity_1_2 = deduplication_engine.calculate_content_similarity(tamil_content1, tamil_content2)
        similarity_1_3 = deduplication_engine.calculate_content_similarity(tamil_content1, tamil_content3)
        
        # Tamil content similarity should work
        assert similarity_1_2 > similarity_1_3
        assert similarity_1_2 > 0.5  # Should detect similarity

    @pytest.mark.asyncio
    async def test_metadata_similarity(self, deduplication_engine):
        """Test metadata-based similarity detection"""
        metadata1 = {
            'title': 'ADMK announces candidates',
            'author': 'ADMK Official',
            'published_date': '2024-01-01',
            'platform': 'facebook'
        }
        
        metadata2 = {
            'title': 'AIADMK announces candidate list',
            'author': 'ADMK Official',
            'published_date': '2024-01-01',
            'platform': 'facebook'
        }
        
        metadata3 = {
            'title': 'DMK manifesto released',
            'author': 'DMK Official',
            'published_date': '2024-01-02',
            'platform': 'twitter'
        }
        
        similarity_1_2 = deduplication_engine.calculate_metadata_similarity(metadata1, metadata2)
        similarity_1_3 = deduplication_engine.calculate_metadata_similarity(metadata1, metadata3)
        
        # Similar metadata should have higher score
        assert similarity_1_2 > similarity_1_3
        
        # Same author and date should increase similarity
        assert similarity_1_2 > 0.6

    @pytest.mark.asyncio
    async def test_duplicate_url_detection(self, deduplication_engine, sample_discovery_results):
        """Test detection of duplicate URLs in stage results"""
        # Mock existing URLs in database
        existing_hashes = [
            deduplication_engine.generate_url_hash(result['url']) 
            for result in sample_discovery_results
        ]
        
        deduplication_engine.db.execute_query.return_value = [
            {'url_hash': hash_val} for hash_val in existing_hashes
        ]
        
        # Try to add the same URLs again
        duplicates = await deduplication_engine.find_duplicate_urls(sample_discovery_results)
        
        assert len(duplicates) > 0
        # Should detect all as duplicates
        assert len(duplicates) == len(sample_discovery_results)

    @pytest.mark.asyncio
    async def test_duplicate_content_detection(self, deduplication_engine, sample_engagement_results):
        """Test detection of duplicate content in final results"""
        # Mock existing content in database
        existing_results = [
            {
                'content_hash': deduplication_engine.generate_content_hash(result['content']),
                'content': result['content']
            }
            for result in sample_engagement_results
        ]
        
        deduplication_engine.db.execute_query.return_value = existing_results
        
        # Try to add similar content
        duplicates = await deduplication_engine.find_duplicate_content(sample_engagement_results)
        
        assert isinstance(duplicates, list)
        # Should detect content similarity
        if duplicates:
            assert all('similarity_score' in dup for dup in duplicates)

    @pytest.mark.asyncio
    async def test_deduplicate_stage_results(self, deduplication_engine):
        """Test deduplication of stage results (URLs)"""
        # Mock database responses
        deduplication_engine.db.execute_query.side_effect = [
            # First call: get stage results
            [
                {
                    'id': 1, 'url': 'https://facebook.com/test/posts/123',
                    'url_hash': None, 'competitor_id': 1
                },
                {
                    'id': 2, 'url': 'https://facebook.com/test/posts/123?utm_source=test',
                    'url_hash': None, 'competitor_id': 1
                }
            ],
            # Second call: check existing hashes
            [],
            # Third call: update results
            []
        ]
        
        results = await deduplication_engine.deduplicate_stage_results()
        
        assert isinstance(results, dict)
        assert 'processed_count' in results
        assert 'duplicate_count' in results
        assert 'unique_count' in results

    @pytest.mark.asyncio
    async def test_deduplicate_final_results(self, deduplication_engine):
        """Test deduplication of final results (content)"""
        # Mock database responses
        deduplication_engine.db.execute_query.side_effect = [
            # First call: get final results
            [
                {
                    'id': 1, 'content': 'Test political content about ADMK',
                    'content_hash': None, 'competitor_id': 1, 'title': 'Test Post'
                },
                {
                    'id': 2, 'content': 'Similar test content about AIADMK',
                    'content_hash': None, 'competitor_id': 1, 'title': 'Similar Post'
                }
            ],
            # Second call: check existing content
            [],
            # Third call: update results
            []
        ]
        
        results = await deduplication_engine.deduplicate_final_results()
        
        assert isinstance(results, dict)
        assert 'processed_count' in results
        assert 'duplicate_count' in results
        assert 'similarity_matches' in results

    @pytest.mark.asyncio
    async def test_content_hash_generation(self, deduplication_engine):
        """Test content hash generation for deduplication"""
        content1 = "This is test political content about Tamil Nadu politics"
        content2 = "This is test political content about Tamil Nadu politics"  # Exact same
        content3 = "This is different content about weather updates"
        
        hash1 = deduplication_engine.generate_content_hash(content1)
        hash2 = deduplication_engine.generate_content_hash(content2)
        hash3 = deduplication_engine.generate_content_hash(content3)
        
        # Same content should generate same hash
        assert hash1 == hash2
        # Different content should generate different hash
        assert hash1 != hash3
        
        # Hash should be consistent
        assert hash1 == deduplication_engine.generate_content_hash(content1)

    @pytest.mark.asyncio
    async def test_fuzzy_matching(self, deduplication_engine):
        """Test fuzzy string matching for near-duplicates"""
        text1 = "ADMK party announces new policy"
        text2 = "AIADMK party announces new policy"  # Very similar
        text3 = "DMK criticizes government decision"  # Different
        
        ratio_1_2 = deduplication_engine.fuzzy_match(text1, text2)
        ratio_1_3 = deduplication_engine.fuzzy_match(text1, text3)
        
        # Similar texts should have high ratio
        assert ratio_1_2 > 80  # Using fuzzywuzzy scale (0-100)
        
        # Different texts should have lower ratio
        assert ratio_1_3 < 50

    @pytest.mark.asyncio
    async def test_batch_deduplication_performance(self, deduplication_engine):
        """Test performance of batch deduplication operations"""
        # Create large batch of similar URLs
        large_batch = [
            {
                'id': i,
                'url': f'https://facebook.com/test/posts/{i % 10}?param={i}',  # Many will be similar
                'url_hash': None
            }
            for i in range(100)
        ]
        
        start_time = datetime.now()
        
        # Mock database to return empty (no existing duplicates)
        deduplication_engine.db.execute_query.return_value = []
        
        # Process batch
        processed_urls = []
        for item in large_batch:
            normalized = deduplication_engine.normalize_url(item['url'])
            url_hash = deduplication_engine.generate_url_hash(normalized)
            processed_urls.append({'normalized_url': normalized, 'url_hash': url_hash})
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should complete reasonably quickly
        assert execution_time < 5.0  # Should process 100 URLs in under 5 seconds
        
        # Should detect duplicates within the batch
        unique_hashes = set(item['url_hash'] for item in processed_urls)
        assert len(unique_hashes) < len(large_batch)  # Should have duplicates

    @pytest.mark.asyncio
    async def test_similarity_caching(self, deduplication_engine):
        """Test caching of similarity calculations"""
        content1 = "Test content for similarity caching"
        content2 = "Similar test content for caching"
        
        # First calculation
        start_time = datetime.now()
        similarity1 = deduplication_engine.calculate_content_similarity(content1, content2)
        first_calc_time = (datetime.now() - start_time).total_seconds()
        
        # Second calculation (should use cache if implemented)
        start_time = datetime.now()
        similarity2 = deduplication_engine.calculate_content_similarity(content1, content2)
        second_calc_time = (datetime.now() - start_time).total_seconds()
        
        # Results should be the same
        assert similarity1 == similarity2
        
        # Second calculation might be faster if caching is implemented
        # (This is optional - caching may or may not be implemented)
        assert second_calc_time <= first_calc_time * 1.1  # Allow for some variance

    @pytest.mark.asyncio
    async def test_edge_case_handling(self, deduplication_engine):
        """Test handling of edge cases in deduplication"""
        # Empty content
        empty_similarity = deduplication_engine.calculate_content_similarity("", "")
        assert empty_similarity == 0.0
        
        # Very short content
        short_similarity = deduplication_engine.calculate_content_similarity("A", "B")
        assert 0 <= short_similarity <= 1
        
        # None values
        none_hash = deduplication_engine.generate_content_hash("")
        assert isinstance(none_hash, str)
        
        # Very long content
        long_content = "A" * 10000
        long_hash = deduplication_engine.generate_content_hash(long_content)
        assert isinstance(long_hash, str)
        assert len(long_hash) > 10

    @pytest.mark.asyncio
    async def test_multilingual_content_handling(self, deduplication_engine):
        """Test handling of multilingual content"""
        # Mixed English and Tamil
        mixed_content1 = "Tamil Nadu CM announces new policy - தமிழ்நாடு முதலமைச்சர் புதிய கொள்கை"
        mixed_content2 = "TN Chief Minister new policy announcement - தமிழ்நாடு முதலமைச்சர் கொள்கை அறிவிப்பு"
        
        similarity = deduplication_engine.calculate_content_similarity(mixed_content1, mixed_content2)
        
        # Should handle mixed content reasonably
        assert 0 <= similarity <= 1
        
        # Should detect some similarity despite language mixing
        assert similarity > 0.3  # Should have some similarity

    @pytest.mark.asyncio
    async def test_url_shortener_handling(self, deduplication_engine):
        """Test handling of URL shorteners and redirects"""
        original_url = "https://facebook.com/test/posts/123456"
        shortened_urls = [
            "https://bit.ly/xyz123",
            "https://tinyurl.com/abc456",
            "https://t.co/def789"
        ]
        
        # Mock URL expansion (in real implementation, this would make HTTP requests)
        def mock_expand_url(short_url):
            return original_url  # All expand to the same URL
        
        with patch.object(deduplication_engine, 'expand_shortened_url', side_effect=mock_expand_url):
            # All shortened URLs should normalize to the same URL
            expanded_urls = [deduplication_engine.expand_shortened_url(url) for url in shortened_urls]
            normalized_urls = [deduplication_engine.normalize_url(url) for url in expanded_urls]
            
            # All should be the same after expansion and normalization
            assert all(url == normalized_urls[0] for url in normalized_urls)

class TestDeduplicationEnginePerformance:
    """Performance tests for Deduplication Engine"""

    @pytest.mark.asyncio
    async def test_large_scale_url_deduplication(self, deduplication_engine, performance_config):
        """Test URL deduplication performance with large datasets"""
        # Create large dataset with many duplicates
        large_dataset = []
        base_urls = [f"https://facebook.com/page{i}/posts/" for i in range(10)]
        
        for i in range(1000):
            base_url = base_urls[i % len(base_urls)]
            url_with_params = f"{base_url}{i % 100}?utm_source=test&param={i}"
            large_dataset.append({
                'id': i,
                'url': url_with_params,
                'url_hash': None
            })
        
        start_time = datetime.now()
        
        # Process URLs for deduplication
        processed = []
        for item in large_dataset:
            normalized = deduplication_engine.normalize_url(item['url'])
            url_hash = deduplication_engine.generate_url_hash(normalized)
            processed.append(url_hash)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should complete within reasonable time
        assert execution_time < performance_config['max_response_time'] * 2
        
        # Should detect many duplicates
        unique_hashes = set(processed)
        duplicate_count = len(processed) - len(unique_hashes)
        assert duplicate_count > 500  # Should find many duplicates

    @pytest.mark.asyncio
    async def test_content_similarity_performance(self, deduplication_engine, performance_config):
        """Test content similarity calculation performance"""
        # Generate test content
        base_content = "Tamil Nadu political news about elections and candidates"
        content_variations = [
            base_content + f" Additional text {i}" + " " * (i * 10)
            for i in range(50)  # 50 variations with different lengths
        ]
        
        start_time = datetime.now()
        
        # Calculate similarities
        similarities = []
        for i in range(0, len(content_variations), 2):
            if i + 1 < len(content_variations):
                sim = deduplication_engine.calculate_content_similarity(
                    content_variations[i], 
                    content_variations[i + 1]
                )
                similarities.append(sim)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Should complete within reasonable time
        assert execution_time < performance_config['max_response_time']
        
        # All similarities should be valid
        assert all(0 <= sim <= 1 for sim in similarities)

    @pytest.mark.asyncio
    async def test_memory_usage_during_deduplication(self, deduplication_engine):
        """Test memory usage during large-scale deduplication"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Process large amount of data
        large_content = []
        for i in range(1000):
            content = f"Test content piece number {i} with some repeated text about politics and elections" * 5
            large_content.append(content)
        
        # Generate hashes for all content
        hashes = [deduplication_engine.generate_content_hash(content) for content in large_content]
        
        # Calculate some similarities
        similarities = []
        for i in range(0, min(100, len(large_content)), 2):
            if i + 1 < len(large_content):
                sim = deduplication_engine.calculate_content_similarity(
                    large_content[i],
                    large_content[i + 1]
                )
                similarities.append(sim)
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Should not use excessive memory
        assert memory_increase < 200  # Less than 200MB increase

class TestDeduplicationEngineErrorHandling:
    """Error handling tests for Deduplication Engine"""

    @pytest.mark.asyncio
    async def test_database_error_handling(self, deduplication_engine, database_error):
        """Test handling of database errors during deduplication"""
        # Mock database error
        deduplication_engine.db.execute_query.side_effect = database_error
        
        # Should handle database errors gracefully
        try:
            results = await deduplication_engine.deduplicate_stage_results()
            # Should return error information
            assert isinstance(results, dict)
            assert 'error' in results or 'success' in results
        except Exception as e:
            # Should be a handled exception, not a crash
            assert "database" in str(e).lower() or "connection" in str(e).lower()

    @pytest.mark.asyncio
    async def test_malformed_url_handling(self, deduplication_engine):
        """Test handling of malformed URLs"""
        malformed_urls = [
            "not_a_url",
            "http://",
            "ftp://invalid.url",
            "",
            None,
            "javascript:alert('test')",
            "data:text/html,<script>alert('test')</script>"
        ]
        
        for url in malformed_urls:
            try:
                # Should not crash on malformed URLs
                normalized = deduplication_engine.normalize_url(url) if url else ""
                url_hash = deduplication_engine.generate_url_hash(normalized)
                
                # Should produce some result (may be empty or default)
                assert isinstance(normalized, str)
                assert isinstance(url_hash, str)
            except Exception as e:
                # If it raises an exception, it should be a handled one
                assert "invalid" in str(e).lower() or "malformed" in str(e).lower()

    @pytest.mark.asyncio
    async def test_extreme_content_handling(self, deduplication_engine):
        """Test handling of extreme content cases"""
        # Very long content
        very_long_content = "A" * 100000  # 100k characters
        long_hash = deduplication_engine.generate_content_hash(very_long_content)
        assert isinstance(long_hash, str)
        
        # Content with special characters
        special_content = "!@#$%^&*(){}[]|\\:;\"'<>?,./-=_+"
        special_hash = deduplication_engine.generate_content_hash(special_content)
        assert isinstance(special_hash, str)
        
        # Unicode content
        unicode_content = "தமிழ் 中文 العربية русский 日本語 🎉🎊✨"
        unicode_hash = deduplication_engine.generate_content_hash(unicode_content)
        assert isinstance(unicode_hash, str)