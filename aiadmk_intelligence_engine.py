#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Main Orchestration Engine
Complete automated intelligence system for AIADMK political monitoring
"""

import os
import asyncio
import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

# Import all components
from config import get_config, validate_system_config
from database import get_database, close_database
from services.serpapi_service import get_serpapi_service, close_serpapi_service
from services.brave_search_service import get_brave_service, close_brave_service
from services.firecrawl_service import get_firecrawl_service, close_firecrawl_service
from services.apify_service import get_apify_service
from scrapers.facebook_scraper import get_facebook_scraper
from scrapers.youtube_scraper import get_youtube_scraper
from scrapers.instagram_scraper import get_instagram_scraper
from scrapers.twitter_scraper import get_twitter_scraper
from scrapers.reddit_scraper import get_reddit_scraper
from scrapers.tamil_news_processor import get_tamil_news_processor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/aiadmk_intelligence.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)

class AIADMKIntelligenceEngine:
    """Complete AIADMK Political Intelligence System"""
    
    def __init__(self):
        self.config = get_config()
        self.db = None
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=self.config.system['max_concurrent_scrapers'])
        
        # Initialize all scrapers
        self.scrapers = {
            'facebook': get_facebook_scraper(),
            'youtube': get_youtube_scraper(),
            'instagram': get_instagram_scraper(),
            'twitter': get_twitter_scraper(),
            'reddit': get_reddit_scraper(),
            'tamil_news': get_tamil_news_processor()
        }
        
        # Discovery services
        self.discovery_services = {
            'serpapi': None,
            'brave': None,
            'firecrawl': None
        }
        
        # System metrics
        self.metrics = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_run_time': None,
            'total_content_discovered': 0,
            'total_content_processed': 0,
            'platform_stats': {platform: {'runs': 0, 'content': 0, 'errors': 0} 
                             for platform in self.scrapers.keys()}
        }
        
        logger.info("🏛️ AIADMK Political Intelligence System initialized")
    
    async def initialize(self) -> bool:
        """Initialize all system components"""
        try:
            logger.info("🚀 Initializing AIADMK Intelligence System...")
            
            # Validate configuration
            if not validate_system_config():
                logger.error("❌ System configuration validation failed")
                return False
            
            # Initialize database
            self.db = await get_database()
            
            # Initialize discovery services
            self.discovery_services['serpapi'] = await get_serpapi_service()
            self.discovery_services['brave'] = await get_brave_service()
            self.discovery_services['firecrawl'] = await get_firecrawl_service()
            
            # Create logs directory
            os.makedirs('logs', exist_ok=True)
            
            logger.info("✅ AIADMK Intelligence System initialization completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}")
            return False
    
    async def run_content_discovery_cycle(self) -> Dict[str, Any]:
        """Run content discovery using SerpAPI and Brave Search"""
        logger.info("🔍 Starting content discovery cycle...")
        
        discovery_results = {
            'serpapi': {'success': False, 'urls_discovered': 0},
            'brave': {'success': False, 'urls_discovered': 0},
            'total_urls_discovered': 0,
            'errors': []
        }
        
        try:
            # Parallel content discovery
            serpapi_task = self.discovery_services['serpapi'].discover_aiadmk_content()
            brave_task = self.discovery_services['brave'].automated_aiadmk_monitoring()
            
            serpapi_result, brave_result = await asyncio.gather(
                serpapi_task, brave_task, return_exceptions=True
            )
            
            # Process SerpAPI results
            if not isinstance(serpapi_result, Exception) and serpapi_result.get('total_urls_found', 0) > 0:
                discovery_results['serpapi'] = {
                    'success': True,
                    'urls_discovered': serpapi_result['total_urls_found'],
                    'platform_breakdown': serpapi_result['platform_breakdown']
                }
                discovery_results['total_urls_discovered'] += serpapi_result['total_urls_found']
            else:
                error_msg = str(serpapi_result) if isinstance(serpapi_result, Exception) else "No URLs found"
                discovery_results['errors'].append(f"SerpAPI: {error_msg}")
            
            # Process Brave Search results
            if not isinstance(brave_result, Exception) and brave_result.get('new_urls_discovered', 0) > 0:
                discovery_results['brave'] = {
                    'success': True,
                    'urls_discovered': brave_result['new_urls_discovered'],
                    'platform_breakdown': brave_result['platform_breakdown']
                }
                discovery_results['total_urls_discovered'] += brave_result['new_urls_discovered']
            else:
                error_msg = str(brave_result) if isinstance(brave_result, Exception) else "No URLs found"
                discovery_results['errors'].append(f"Brave Search: {error_msg}")
            
            self.metrics['total_content_discovered'] += discovery_results['total_urls_discovered']
            
            logger.info(f"✅ Content discovery completed: {discovery_results['total_urls_discovered']} URLs discovered")
            return discovery_results
            
        except Exception as e:
            logger.error(f"Content discovery cycle failed: {e}")
            discovery_results['errors'].append(str(e))
            return discovery_results
    
    async def run_platform_scraping_cycle(self) -> Dict[str, Any]:
        """Run scraping cycle for all platforms"""
        logger.info("🔄 Starting platform scraping cycle...")
        
        scraping_results = {
            'total_platforms': len(self.scrapers),
            'successful_platforms': 0,
            'failed_platforms': 0,
            'platform_results': {},
            'total_content_processed': 0,
            'errors': []
        }
        
        # Create scraping tasks for all platforms
        scraping_tasks = {}
        for platform, scraper in self.scrapers.items():
            try:
                if hasattr(scraper, 'run_facebook_monitoring'):
                    scraping_tasks[platform] = scraper.run_facebook_monitoring()
                elif hasattr(scraper, 'run_youtube_monitoring'):
                    scraping_tasks[platform] = scraper.run_youtube_monitoring()
                elif hasattr(scraper, 'run_instagram_monitoring'):
                    scraping_tasks[platform] = scraper.run_instagram_monitoring()
                elif hasattr(scraper, 'run_twitter_monitoring'):
                    scraping_tasks[platform] = scraper.run_twitter_monitoring()
                elif hasattr(scraper, 'run_reddit_monitoring'):
                    scraping_tasks[platform] = scraper.run_reddit_monitoring()
                elif hasattr(scraper, 'run_tamil_news_monitoring'):
                    scraping_tasks[platform] = scraper.run_tamil_news_monitoring()
                else:
                    logger.warning(f"⚠️ No monitoring method found for {platform}")
                    continue
                
                logger.info(f"📱 Queued {platform} scraper")
                
            except Exception as e:
                logger.error(f"Failed to queue {platform} scraper: {e}")
                scraping_results['errors'].append(f"{platform}: {str(e)}")
        
        # Execute scraping tasks with limited concurrency
        semaphore = asyncio.Semaphore(self.config.system['max_concurrent_scrapers'])
        
        async def run_scraper(platform: str, task):
            async with semaphore:
                try:
                    logger.info(f"🚀 Starting {platform} scraper...")
                    result = await task
                    
                    self.metrics['platform_stats'][platform]['runs'] += 1
                    
                    if result.get('success'):
                        scraping_results['successful_platforms'] += 1
                        
                        # Extract content counts
                        content_count = 0
                        if 'scraping' in result:
                            content_count = result['scraping'].get('aiadmk_posts', 0) or result['scraping'].get('aiadmk_videos', 0) or result['scraping'].get('aiadmk_tweets', 0) or result['scraping'].get('aiadmk_articles', 0) or 0
                        elif 'processing' in result:
                            content_count = result['processing'].get('aiadmk_articles', 0)
                        
                        self.metrics['platform_stats'][platform]['content'] += content_count
                        scraping_results['total_content_processed'] += content_count
                        
                        logger.info(f"✅ {platform.upper()} scraping completed: {content_count} items processed")
                    else:
                        scraping_results['failed_platforms'] += 1
                        self.metrics['platform_stats'][platform]['errors'] += 1
                        error_msg = result.get('error', 'Unknown error')
                        scraping_results['errors'].append(f"{platform}: {error_msg}")
                        logger.error(f"❌ {platform.upper()} scraping failed: {error_msg}")
                    
                    scraping_results['platform_results'][platform] = result
                    return result
                    
                except Exception as e:
                    scraping_results['failed_platforms'] += 1
                    self.metrics['platform_stats'][platform]['errors'] += 1
                    error_msg = str(e)
                    scraping_results['errors'].append(f"{platform}: {error_msg}")
                    logger.error(f"❌ {platform.upper()} scraper exception: {error_msg}")
                    return {'success': False, 'error': error_msg}
        
        # Execute all scrapers
        if scraping_tasks:
            await asyncio.gather(*[
                run_scraper(platform, task) 
                for platform, task in scraping_tasks.items()
            ], return_exceptions=True)
        
        self.metrics['total_content_processed'] += scraping_results['total_content_processed']
        
        logger.info(f"✅ Platform scraping cycle completed: {scraping_results['successful_platforms']}/{scraping_results['total_platforms']} platforms successful, {scraping_results['total_content_processed']} items processed")
        return scraping_results
    
    async def run_intelligence_cycle(self) -> Dict[str, Any]:
        """Run complete intelligence gathering cycle"""
        cycle_start = datetime.now()
        self.metrics['total_runs'] += 1
        
        logger.info("🏛️ " + "="*60)
        logger.info("🏛️  AIADMK POLITICAL INTELLIGENCE CYCLE STARTED")
        logger.info("🏛️ " + "="*60)
        
        cycle_results = {
            'cycle_id': f"cycle_{int(cycle_start.timestamp())}",
            'start_time': cycle_start.isoformat(),
            'end_time': None,
            'duration_seconds': 0,
            'discovery': {},
            'scraping': {},
            'success': False,
            'total_content_discovered': 0,
            'total_content_processed': 0,
            'errors': []
        }
        
        try:
            # Phase 1: Content Discovery
            logger.info("📊 PHASE 1: Content Discovery")
            discovery_results = await self.run_content_discovery_cycle()
            cycle_results['discovery'] = discovery_results
            cycle_results['total_content_discovered'] = discovery_results['total_urls_discovered']
            
            # Small delay to allow URLs to be queued
            await asyncio.sleep(5)
            
            # Phase 2: Platform Scraping
            logger.info("📊 PHASE 2: Platform Scraping")
            scraping_results = await self.run_platform_scraping_cycle()
            cycle_results['scraping'] = scraping_results
            cycle_results['total_content_processed'] = scraping_results['total_content_processed']
            
            # Determine cycle success
            cycle_results['success'] = (
                scraping_results['successful_platforms'] > 0 or 
                discovery_results['total_urls_discovered'] > 0
            )
            
            if cycle_results['success']:
                self.metrics['successful_runs'] += 1
                logger.info("✅ Intelligence cycle completed successfully")
            else:
                self.metrics['failed_runs'] += 1
                logger.warning("⚠️ Intelligence cycle completed with no results")
            
            # Collect all errors
            cycle_results['errors'] = discovery_results['errors'] + scraping_results['errors']
            
        except Exception as e:
            self.metrics['failed_runs'] += 1
            error_msg = f"Intelligence cycle failed: {str(e)}"
            logger.error(f"❌ {error_msg}")
            cycle_results['errors'].append(error_msg)
            cycle_results['success'] = False
        
        # Finalize cycle metrics
        cycle_end = datetime.now()
        cycle_results['end_time'] = cycle_end.isoformat()
        cycle_results['duration_seconds'] = (cycle_end - cycle_start).total_seconds()
        self.metrics['last_run_time'] = cycle_end.isoformat()
        
        # Log summary
        logger.info("🏛️ " + "="*60)
        logger.info(f"🏛️  CYCLE SUMMARY - Duration: {cycle_results['duration_seconds']:.1f}s")
        logger.info(f"🏛️  Content Discovered: {cycle_results['total_content_discovered']}")
        logger.info(f"🏛️  Content Processed: {cycle_results['total_content_processed']}")
        logger.info(f"🏛️  Success: {'✅ YES' if cycle_results['success'] else '❌ NO'}")
        if cycle_results['errors']:
            logger.info(f"🏛️  Errors: {len(cycle_results['errors'])}")
        logger.info("🏛️ " + "="*60)
        
        return cycle_results
    
    async def run_continuous_monitoring(self, cycle_interval_minutes: int = 60):
        """Run continuous intelligence monitoring"""
        logger.info(f"🔄 Starting continuous monitoring (cycle every {cycle_interval_minutes} minutes)...")
        self.running = True
        
        cycle_count = 0
        
        try:
            while self.running:
                cycle_count += 1
                logger.info(f"🔄 Starting monitoring cycle #{cycle_count}")
                
                # Run intelligence cycle
                cycle_results = await self.run_intelligence_cycle()
                
                # Log cycle completion
                status = "✅ SUCCESS" if cycle_results['success'] else "❌ FAILED"
                logger.info(f"📊 Cycle #{cycle_count} completed: {status}")
                
                if not self.running:
                    break
                
                # Wait for next cycle
                logger.info(f"⏱️ Waiting {cycle_interval_minutes} minutes until next cycle...")
                await asyncio.sleep(cycle_interval_minutes * 60)
                
        except KeyboardInterrupt:
            logger.info("🛑 Monitoring stopped by user")
        except Exception as e:
            logger.error(f"❌ Continuous monitoring failed: {e}")
        finally:
            self.running = False
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        db_stats = await self.db.get_statistics() if self.db else {}
        
        return {
            'system_info': {
                'running': self.running,
                'uptime': self.metrics.get('last_run_time'),
                'total_runs': self.metrics['total_runs'],
                'success_rate': f"{(self.metrics['successful_runs'] / max(self.metrics['total_runs'], 1) * 100):.1f}%"
            },
            'content_stats': {
                'total_discovered': self.metrics['total_content_discovered'],
                'total_processed': self.metrics['total_content_processed'],
                'database_stats': db_stats
            },
            'platform_stats': self.metrics['platform_stats'],
            'configuration': {
                'enabled_platforms': self.config.get_enabled_platforms(),
                'max_concurrent_scrapers': self.config.system['max_concurrent_scrapers'],
                'batch_size': self.config.system['batch_size']
            }
        }
    
    async def shutdown(self):
        """Graceful system shutdown"""
        logger.info("🛑 Shutting down AIADMK Intelligence System...")
        self.running = False
        
        # Close all services
        try:
            await close_serpapi_service()
            await close_brave_service()
            await close_firecrawl_service()
            await close_database()
            
            self.executor.shutdown(wait=True)
            
            logger.info("✅ AIADMK Intelligence System shutdown completed")
        except Exception as e:
            logger.error(f"❌ Shutdown error: {e}")

# Global engine instance
intelligence_engine = None

def get_intelligence_engine():
    """Get global intelligence engine instance"""
    global intelligence_engine
    if not intelligence_engine:
        intelligence_engine = AIADMKIntelligenceEngine()
    return intelligence_engine

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    logger.info(f"🛑 Received signal {signum}, initiating shutdown...")
    if intelligence_engine:
        asyncio.create_task(intelligence_engine.shutdown())
    sys.exit(0)

# Main execution
async def main():
    """Main execution function"""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    engine = get_intelligence_engine()
    
    if await engine.initialize():
        try:
            # Run single cycle or continuous monitoring based on args
            if len(sys.argv) > 1 and sys.argv[1] == "--single-cycle":
                logger.info("🔄 Running single intelligence cycle...")
                result = await engine.run_intelligence_cycle()
                print(json.dumps(result, indent=2, ensure_ascii=False))
            else:
                # Run continuous monitoring (default)
                await engine.run_continuous_monitoring(cycle_interval_minutes=30)
        finally:
            await engine.shutdown()
    else:
        logger.error("❌ Failed to initialize AIADMK Intelligence System")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())