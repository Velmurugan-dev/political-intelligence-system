#!/usr/bin/env python3
"""
Deduplication Engine
Multi-level deduplication for URLs and content with similarity detection
"""

import asyncio
import logging
import hashlib
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set, Any
from urllib.parse import urlparse, parse_qs, urlunparse
import difflib

import sys
sys.path.append('..')
from database import get_database

logger = logging.getLogger(__name__)

class DeduplicationEngine:
    """Multi-level deduplication system"""
    
    def __init__(self):
        self.db = None
        
        # Tracking parameters removal patterns
        self.tracking_params = {
            'youtube': ['t', 'feature', 'app', 'si'],
            'facebook': ['fbclid', 'ref', 'source', 'hash'],
            'instagram': ['igshid', 'img_index'],
            'twitter': ['s', 'ref_src', 'ref_url'],
            'reddit': ['utm_source', 'utm_medium', 'utm_campaign'],
            'common': ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content']
        }
        
        # Content similarity thresholds
        self.similarity_thresholds = {
            'url_similarity': 0.95,     # Very high threshold for URL similarity
            'title_similarity': 0.90,   # High threshold for title similarity
            'content_similarity': 0.85,  # Medium threshold for content similarity
            'metadata_similarity': 0.95  # High threshold for metadata (author+date)
        }
        
        self.stats = {
            'urls_processed': 0,
            'url_duplicates_found': 0,
            'content_duplicates_found': 0,
            'similarity_checks_performed': 0,
            'cleanup_operations': 0
        }
    
    async def initialize(self):
        """Initialize deduplication engine"""
        try:
            self.db = await get_database()
            logger.info("✅ Deduplication Engine initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Deduplication Engine initialization failed: {e}")
            return False
    
    def normalize_url(self, url: str, platform: str = '') -> str:
        """Normalize URL by removing tracking parameters and standardizing format"""
        
        try:
            parsed = urlparse(url)
            
            # Remove tracking parameters
            params_to_remove = set(self.tracking_params.get('common', []))
            if platform in self.tracking_params:
                params_to_remove.update(self.tracking_params[platform])
            
            # Parse query parameters
            query_params = parse_qs(parsed.query)
            cleaned_params = {k: v for k, v in query_params.items() if k not in params_to_remove}
            
            # Rebuild query string
            new_query = '&'.join([f"{k}={v[0]}" for k, v in cleaned_params.items()])
            
            # Rebuild URL
            cleaned_parsed = parsed._replace(query=new_query)
            normalized_url = urlunparse(cleaned_parsed)
            
            # Additional platform-specific normalization
            if platform == 'youtube':
                # Convert youtu.be to youtube.com/watch
                if 'youtu.be/' in normalized_url:
                    video_id = normalized_url.split('youtu.be/')[1].split('?')[0]
                    normalized_url = f"https://www.youtube.com/watch?v={video_id}"
                
                # Ensure consistent youtube.com domain
                normalized_url = normalized_url.replace('m.youtube.com', 'www.youtube.com')
            
            elif platform == 'facebook':
                # Standardize Facebook URLs
                normalized_url = normalized_url.replace('m.facebook.com', 'www.facebook.com')
                normalized_url = normalized_url.replace('web.facebook.com', 'www.facebook.com')
            
            elif platform == 'twitter':
                # Handle both twitter.com and x.com
                normalized_url = normalized_url.replace('twitter.com', 'x.com')
                normalized_url = normalized_url.replace('mobile.x.com', 'x.com')
            
            return normalized_url.lower().strip()
            
        except Exception as e:
            logger.warning(f"URL normalization failed for {url}: {e}")
            return url.lower().strip()
    
    def generate_url_hash(self, url: str, platform: str = '') -> str:
        """Generate hash for normalized URL"""
        normalized_url = self.normalize_url(url, platform)
        return hashlib.sha256(normalized_url.encode()).hexdigest()
    
    def generate_content_hash(self, title: str, content: str, author: str, published_at: datetime = None) -> str:
        """Generate hash for content similarity matching"""
        
        # Normalize text
        title_clean = re.sub(r'[^\w\s\u0B80-\u0BFF]', '', title or '').lower().strip()
        content_clean = re.sub(r'[^\w\s\u0B80-\u0BFF]', '', content or '').lower().strip()
        author_clean = (author or '').lower().strip()
        
        # Create content signature
        content_signature = f"{title_clean}|{content_clean[:500]}|{author_clean}"
        
        if published_at:
            # Add date for temporal grouping
            date_str = published_at.strftime('%Y-%m-%d')
            content_signature += f"|{date_str}"
        
        return hashlib.md5(content_signature.encode()).hexdigest()
    
    async def check_url_duplicate(self, url: str, competitor_id: int, platform_id: int, platform_name: str = '') -> Tuple[bool, Optional[int]]:
        """Check if URL is duplicate (hard deduplication)"""
        
        try:
            url_hash = self.generate_url_hash(url, platform_name)
            
            # Check for exact hash match
            existing_query = """
            SELECT stage_id, url FROM stage_results 
            WHERE competitor_id = $1 AND platform_id = $2 AND url_hash = $3
            LIMIT 1
            """
            
            existing = await self.db.execute_query(existing_query, (competitor_id, platform_id, url_hash))
            
            if existing:
                logger.debug(f"🔄 URL duplicate found: {url}")
                return True, existing[0]['stage_id']
            
            # Check for similar URLs (fuzzy matching)
            similar_stage_id = await self.check_url_similarity(url, competitor_id, platform_id)
            if similar_stage_id:
                return True, similar_stage_id
            
            return False, None
            
        except Exception as e:
            logger.error(f"URL duplicate check failed: {e}")
            return False, None
    
    async def check_url_similarity(self, url: str, competitor_id: int, platform_id: int) -> Optional[int]:
        """Check for similar URLs using fuzzy matching"""
        
        try:
            # Get recent URLs from same competitor and platform
            recent_urls_query = """
            SELECT stage_id, url FROM stage_results 
            WHERE competitor_id = $1 AND platform_id = $2 
            AND inserted_at >= NOW() - INTERVAL '7 days'
            ORDER BY inserted_at DESC
            LIMIT 100
            """
            
            recent_urls = await self.db.execute_query(recent_urls_query, (competitor_id, platform_id))
            
            normalized_new_url = self.normalize_url(url)
            
            for row in recent_urls:
                existing_url = row['url']
                normalized_existing = self.normalize_url(existing_url)
                
                # Calculate URL similarity
                similarity = difflib.SequenceMatcher(None, normalized_new_url, normalized_existing).ratio()
                
                if similarity >= self.similarity_thresholds['url_similarity']:
                    logger.debug(f"🔄 Similar URL found: {similarity:.2f} similarity")
                    return row['stage_id']
            
            return None
            
        except Exception as e:
            logger.error(f"URL similarity check failed: {e}")
            return None
    
    async def check_content_duplicate(self, title: str, content: str, author: str, 
                                    published_at: datetime, competitor_id: int, platform_id: int) -> Tuple[bool, Optional[int]]:
        """Check for content duplicates (soft deduplication)"""
        
        try:
            content_hash = self.generate_content_hash(title, content, author, published_at)
            
            # Check for exact content hash match
            existing_query = """
            SELECT fr.result_id, fr.title, fr.author 
            FROM final_results fr
            WHERE fr.competitor_id = $1 AND fr.platform_id = $2
            AND md5(CONCAT(COALESCE(fr.title, ''), '|', COALESCE(SUBSTRING(fr.content, 1, 500), ''), '|', COALESCE(fr.author, ''))) = $3
            AND DATE(fr.published_at) = DATE($4)
            LIMIT 1
            """
            
            existing = await self.db.execute_query(existing_query, (
                competitor_id, platform_id, content_hash, published_at
            ))
            
            if existing:
                logger.debug(f"🔄 Content duplicate found: {title}")
                return True, existing[0]['result_id']
            
            # Check for similar content using trigram similarity
            similar_result_id = await self.check_content_similarity(
                title, content, author, published_at, competitor_id, platform_id
            )
            
            if similar_result_id:
                return True, similar_result_id
            
            return False, None
            
        except Exception as e:
            logger.error(f"Content duplicate check failed: {e}")
            return False, None
    
    async def check_content_similarity(self, title: str, content: str, author: str,
                                     published_at: datetime, competitor_id: int, platform_id: int) -> Optional[int]:
        """Check for similar content using text similarity"""
        
        try:
            # Get recent content from same competitor and platform
            recent_content_query = """
            SELECT result_id, title, content, author, published_at
            FROM final_results 
            WHERE competitor_id = $1 AND platform_id = $2
            AND published_at >= $3 - INTERVAL '3 days'
            AND published_at <= $3 + INTERVAL '1 day'
            ORDER BY published_at DESC
            LIMIT 50
            """
            
            recent_content = await self.db.execute_query(recent_content_query, (
                competitor_id, platform_id, published_at
            ))
            
            # Check similarity with each existing content
            for row in recent_content:
                # Title similarity
                title_sim = self.calculate_text_similarity(title or '', row['title'] or '')
                
                # Content similarity (first 500 chars)
                content_sim = self.calculate_text_similarity(
                    (content or '')[:500], (row['content'] or '')[:500]
                )
                
                # Author + date similarity
                same_author = (author or '').lower() == (row['author'] or '').lower()
                same_day = published_at.date() == row['published_at'].date() if row['published_at'] else False
                
                # Combined similarity score
                if (title_sim >= self.similarity_thresholds['title_similarity'] and
                    content_sim >= self.similarity_thresholds['content_similarity'] and
                    same_author and same_day):
                    
                    logger.debug(f"🔄 Similar content found: title={title_sim:.2f}, content={content_sim:.2f}")
                    return row['result_id']
            
            return None
            
        except Exception as e:
            logger.error(f"Content similarity check failed: {e}")
            return None
    
    def calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity using multiple methods"""
        
        if not text1 or not text2:
            return 0.0
        
        # Normalize texts
        text1_clean = re.sub(r'[^\w\s\u0B80-\u0BFF]', ' ', text1.lower()).strip()
        text2_clean = re.sub(r'[^\w\s\u0B80-\u0BFF]', ' ', text2.lower()).strip()
        
        # Sequence similarity
        seq_similarity = difflib.SequenceMatcher(None, text1_clean, text2_clean).ratio()
        
        # Word-based similarity
        words1 = set(text1_clean.split())
        words2 = set(text2_clean.split())
        
        if not words1 or not words2:
            return seq_similarity
        
        word_similarity = len(words1.intersection(words2)) / len(words1.union(words2))
        
        # Combined similarity (weighted average)
        combined_similarity = (seq_similarity * 0.6) + (word_similarity * 0.4)
        
        return combined_similarity
    
    async def deduplicate_stage_results(self, competitor_ids: List[int] = None, platform_ids: List[int] = None) -> Dict[str, Any]:
        """Run deduplication on stage_results"""
        
        logger.info("🔄 Starting stage_results deduplication...")
        start_time = datetime.now()
        
        try:
            # Get potentially duplicate URLs
            duplicates_query = """
            SELECT sr1.stage_id, sr1.url, sr1.url_hash, sr1.competitor_id, sr1.platform_id,
                   sr1.title, sr1.author, sr1.published_at, sr1.inserted_at
            FROM stage_results sr1
            WHERE EXISTS (
                SELECT 1 FROM stage_results sr2 
                WHERE sr2.url_hash = sr1.url_hash 
                AND sr2.stage_id < sr1.stage_id
            )
            AND sr1.status != 'duplicate'
            """
            
            params = []
            if competitor_ids:
                query += f" AND sr1.competitor_id = ANY($1)"
                params.append(competitor_ids)
            
            if platform_ids:
                param_num = len(params) + 1
                query += f" AND sr1.platform_id = ANY(${param_num})"
                params.append(platform_ids)
            
            duplicates_query += " ORDER BY sr1.inserted_at DESC LIMIT 1000"
            
            potential_duplicates = await self.db.execute_query(duplicates_query, tuple(params) if params else ())
            
            duplicates_marked = 0
            
            for duplicate in potential_duplicates:
                try:
                    # Mark as duplicate
                    await self.db.execute_query(
                        "UPDATE stage_results SET status = 'duplicate' WHERE stage_id = $1",
                        (duplicate['stage_id'],)
                    )
                    duplicates_marked += 1
                    
                except Exception as e:
                    logger.error(f"Failed to mark duplicate: {e}")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Stage deduplication completed in {duration:.1f}s: {duplicates_marked} duplicates marked")
            
            self.stats['urls_processed'] += len(potential_duplicates)
            self.stats['url_duplicates_found'] += duplicates_marked
            
            return {
                'success': True,
                'duplicates_marked': duplicates_marked,
                'duration_seconds': duration,
                'stats': self.stats
            }
            
        except Exception as e:
            logger.error(f"Stage deduplication failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'stats': self.stats
            }
    
    async def deduplicate_final_results(self, competitor_ids: List[int] = None, platform_ids: List[int] = None) -> Dict[str, Any]:
        """Run content deduplication on final_results"""
        
        logger.info("🔄 Starting final_results content deduplication...")
        start_time = datetime.now()
        
        try:
            # Get recent results for similarity checking
            recent_results_query = """
            SELECT result_id, title, content, author, published_at, competitor_id, platform_id
            FROM final_results
            WHERE scraped_at >= NOW() - INTERVAL '24 hours'
            AND is_latest = TRUE
            """
            
            params = []
            if competitor_ids:
                recent_results_query += f" AND competitor_id = ANY($1)"
                params.append(competitor_ids)
            
            if platform_ids:
                param_num = len(params) + 1
                recent_results_query += f" AND platform_id = ANY(${param_num})"
                params.append(platform_ids)
            
            recent_results_query += " ORDER BY published_at DESC LIMIT 1000"
            
            recent_results = await self.db.execute_query(recent_results_query, tuple(params) if params else ())
            
            # Group by competitor and platform for efficiency
            grouped_results = {}
            for result in recent_results:
                key = (result['competitor_id'], result['platform_id'])
                if key not in grouped_results:
                    grouped_results[key] = []
                grouped_results[key].append(dict(result))
            
            content_duplicates_found = 0
            
            # Check for duplicates within each group
            for (competitor_id, platform_id), results in grouped_results.items():
                duplicates = await self.find_content_duplicates_in_group(results)
                
                for duplicate_id in duplicates:
                    try:
                        # Mark as not latest (soft deletion)
                        await self.db.execute_query(
                            "UPDATE final_results SET is_latest = FALSE WHERE result_id = $1",
                            (duplicate_id,)
                        )
                        content_duplicates_found += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to mark content duplicate: {e}")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Content deduplication completed in {duration:.1f}s: {content_duplicates_found} duplicates found")
            
            self.stats['content_duplicates_found'] += content_duplicates_found
            self.stats['similarity_checks_performed'] += sum(len(results) for results in grouped_results.values())
            
            return {
                'success': True,
                'content_duplicates_found': content_duplicates_found,
                'duration_seconds': duration,
                'stats': self.stats
            }
            
        except Exception as e:
            logger.error(f"Content deduplication failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'stats': self.stats
            }
    
    async def find_content_duplicates_in_group(self, results: List[Dict]) -> Set[int]:
        """Find content duplicates within a group of results"""
        
        duplicates = set()
        
        for i, result1 in enumerate(results):
            if result1['result_id'] in duplicates:
                continue
            
            for j, result2 in enumerate(results[i+1:], i+1):
                if result2['result_id'] in duplicates:
                    continue
                
                # Calculate similarity
                title_sim = self.calculate_text_similarity(
                    result1.get('title', ''), result2.get('title', '')
                )
                content_sim = self.calculate_text_similarity(
                    result1.get('content', ''), result2.get('content', '')
                )
                
                # Check author and date similarity
                same_author = (result1.get('author', '') or '').lower() == (result2.get('author', '') or '').lower()
                
                date_diff = 0
                if result1.get('published_at') and result2.get('published_at'):
                    date_diff = abs((result1['published_at'] - result2['published_at']).days)
                
                # Determine if duplicate
                is_duplicate = (
                    title_sim >= self.similarity_thresholds['title_similarity'] and
                    content_sim >= self.similarity_thresholds['content_similarity'] and
                    (same_author or date_diff <= 1)  # Same author OR published within 1 day
                )
                
                if is_duplicate:
                    # Keep the earlier result, mark later as duplicate
                    if result1.get('published_at', datetime.min) <= result2.get('published_at', datetime.min):
                        duplicates.add(result2['result_id'])
                    else:
                        duplicates.add(result1['result_id'])
                    
                    logger.debug(f"🔄 Content duplicate: title_sim={title_sim:.2f}, content_sim={content_sim:.2f}")
        
        return duplicates
    
    def calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity with Tamil language support"""
        
        if not text1 or not text2:
            return 0.0
        
        # Normalize texts (remove punctuation, lowercase)
        text1_clean = re.sub(r'[^\w\s\u0B80-\u0BFF]', ' ', text1.lower()).strip()
        text2_clean = re.sub(r'[^\w\s\u0B80-\u0BFF]', ' ', text2.lower()).strip()
        
        # Remove extra whitespace
        text1_clean = ' '.join(text1_clean.split())
        text2_clean = ' '.join(text2_clean.split())
        
        if not text1_clean or not text2_clean:
            return 0.0
        
        # Sequence matcher similarity
        seq_similarity = difflib.SequenceMatcher(None, text1_clean, text2_clean).ratio()
        
        # Word-based Jaccard similarity
        words1 = set(text1_clean.split())
        words2 = set(text2_clean.split())
        
        if words1 or words2:
            jaccard_similarity = len(words1.intersection(words2)) / len(words1.union(words2))
        else:
            jaccard_similarity = 0.0
        
        # N-gram similarity (for better handling of word order differences)
        ngram_similarity = self.calculate_ngram_similarity(text1_clean, text2_clean, n=3)
        
        # Weighted combination
        combined_similarity = (seq_similarity * 0.4) + (jaccard_similarity * 0.4) + (ngram_similarity * 0.2)
        
        return combined_similarity
    
    def calculate_ngram_similarity(self, text1: str, text2: str, n: int = 3) -> float:
        """Calculate n-gram similarity"""
        
        def get_ngrams(text: str, n: int) -> Set[str]:
            return set([text[i:i+n] for i in range(len(text) - n + 1)])
        
        if len(text1) < n or len(text2) < n:
            return 0.0
        
        ngrams1 = get_ngrams(text1, n)
        ngrams2 = get_ngrams(text2, n)
        
        if not ngrams1 or not ngrams2:
            return 0.0
        
        return len(ngrams1.intersection(ngrams2)) / len(ngrams1.union(ngrams2))
    
    async def cleanup_old_duplicates(self, days_old: int = 7) -> Dict[str, Any]:
        """Clean up old duplicate records"""
        
        logger.info(f"🧹 Cleaning up duplicates older than {days_old} days...")
        
        try:
            # Delete old duplicate stage_results
            stage_cleanup_query = """
            DELETE FROM stage_results 
            WHERE status = 'duplicate' 
            AND inserted_at < NOW() - INTERVAL '%s days'
            """ % days_old
            
            stage_result = await self.db.execute_query(stage_cleanup_query)
            
            # Clean up old non-latest final_results
            final_cleanup_query = """
            DELETE FROM final_results 
            WHERE is_latest = FALSE 
            AND scraped_at < NOW() - INTERVAL '%s days'
            """ % days_old
            
            final_result = await self.db.execute_query(final_cleanup_query)
            
            # Update stats
            self.stats['cleanup_operations'] += 1
            
            logger.info(f"✅ Cleanup completed: stage_results cleaned, final_results cleaned")
            
            return {
                'success': True,
                'stage_records_deleted': 0,  # asyncpg doesn't return rowcount easily
                'final_records_deleted': 0,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_duplicate_statistics(self) -> Dict[str, Any]:
        """Get deduplication statistics"""
        
        try:
            # URL duplicates by platform
            url_duplicates_query = """
            SELECT 
                p.name as platform,
                COUNT(CASE WHEN sr.status = 'duplicate' THEN 1 END) as url_duplicates,
                COUNT(*) as total_urls,
                ROUND(COUNT(CASE WHEN sr.status = 'duplicate' THEN 1 END)::decimal / COUNT(*) * 100, 2) as duplicate_rate
            FROM stage_results sr
            JOIN platforms p ON sr.platform_id = p.platform_id
            WHERE sr.inserted_at >= NOW() - INTERVAL '7 days'
            GROUP BY p.name
            ORDER BY duplicate_rate DESC
            """
            
            url_stats = await self.db.execute_query(url_duplicates_query)
            
            # Content duplicates by competitor
            content_duplicates_query = """
            SELECT 
                c.name as competitor,
                COUNT(CASE WHEN fr.is_latest = FALSE THEN 1 END) as content_duplicates,
                COUNT(*) as total_content,
                ROUND(COUNT(CASE WHEN fr.is_latest = FALSE THEN 1 END)::decimal / COUNT(*) * 100, 2) as duplicate_rate
            FROM final_results fr
            JOIN competitors c ON fr.competitor_id = c.competitor_id
            WHERE fr.scraped_at >= NOW() - INTERVAL '7 days'
            GROUP BY c.name
            ORDER BY duplicate_rate DESC
            """
            
            content_stats = await self.db.execute_query(content_duplicates_query)
            
            return {
                'url_duplicates_by_platform': [dict(row) for row in url_stats],
                'content_duplicates_by_competitor': [dict(row) for row in content_stats],
                'engine_stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get duplicate statistics: {e}")
            return {'error': str(e)}

# Global instance
deduplication_engine = None

async def get_deduplication_engine():
    """Get global deduplication engine instance"""
    global deduplication_engine
    if not deduplication_engine:
        deduplication_engine = DeduplicationEngine()
        await deduplication_engine.initialize()
    return deduplication_engine

# Utility functions for use in other modules
async def check_url_duplicate(url: str, competitor_id: int, platform_id: int, platform_name: str = '') -> Tuple[bool, Optional[int]]:
    """Check if URL is duplicate"""
    engine = await get_deduplication_engine()
    return await engine.check_url_duplicate(url, competitor_id, platform_id, platform_name)

async def check_content_duplicate(title: str, content: str, author: str, published_at: datetime, 
                                competitor_id: int, platform_id: int) -> Tuple[bool, Optional[int]]:
    """Check if content is duplicate"""
    engine = await get_deduplication_engine()
    return await engine.check_content_duplicate(title, content, author, published_at, competitor_id, platform_id)

if __name__ == "__main__":
    async def test_deduplication():
        engine = DeduplicationEngine()
        if await engine.initialize():
            
            # Test URL normalization
            test_urls = [
                "https://www.youtube.com/watch?v=abc123&t=30&feature=youtu.be",
                "https://youtu.be/abc123?t=30",
                "https://m.youtube.com/watch?v=abc123"
            ]
            
            for url in test_urls:
                normalized = engine.normalize_url(url, 'youtube')
                print(f"Original: {url}")
                print(f"Normalized: {normalized}")
                print(f"Hash: {engine.generate_url_hash(url, 'youtube')}")
                print("---")
            
            # Test text similarity
            text1 = "அதிமுக தலைவர் எடப்பாடி பழனிசாமி இன்று சென்னையில் கூட்டம்"
            text2 = "AIADMK leader Edappadi Palaniswami today meeting in Chennai"
            similarity = engine.calculate_text_similarity(text1, text2)
            print(f"Text similarity: {similarity:.2f}")
            
            # Run deduplication test
            result = await engine.deduplicate_stage_results()
            print(f"Deduplication test result: {result}")
    
    asyncio.run(test_deduplication())