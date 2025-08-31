#!/usr/bin/env python3
"""
Main Orchestrator for Political Intelligence System
Coordinates discovery engine, engagement engine, queue system, and web UI
"""

import asyncio
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from database.connection import DatabaseConnection
from engines.discovery_engine import DiscoveryEngine
from engines.engagement_engine import EngagementEngine
from engines.deduplication_engine import DeduplicationEngine
from queue_system.celery_app import app as celery_app
from queue_system.tasks import discovery_task, engagement_task, monitoring_task, analytics_task

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PoliticalIntelligenceOrchestrator:
    """
    Main orchestrator that manages the entire political intelligence pipeline
    """
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.discovery_engine = None
        self.engagement_engine = None
        self.dedup_engine = None
        self.is_running = False
        
    async def initialize(self):
        """Initialize all components"""
        try:
            logger.info("Initializing Political Intelligence Orchestrator...")
            
            # Initialize database connection
            await self.db.connect()
            logger.info("Database connection established")
            
            # Initialize engines
            self.discovery_engine = DiscoveryEngine(self.db)
            await self.discovery_engine.initialize()
            logger.info("Discovery engine initialized")
            
            self.engagement_engine = EngagementEngine(self.db)
            await self.engagement_engine.initialize()
            logger.info("Engagement engine initialized")
            
            self.dedup_engine = DeduplicationEngine(self.db)
            await self.dedup_engine.initialize()
            logger.info("Deduplication engine initialized")
            
            logger.info("All components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize orchestrator: {e}")
            raise
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info("Shutting down orchestrator...")
        self.is_running = False
        
        if self.db:
            await self.db.close()
            logger.info("Database connection closed")
    
    async def run_discovery_cycle(
        self, 
        competitor_ids: Optional[List[int]] = None,
        platform_ids: Optional[List[int]] = None,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Run complete discovery cycle for specified competitors and platforms
        """
        logger.info(f"Starting discovery cycle for competitors: {competitor_ids}, platforms: {platform_ids}")
        
        try:
            # Run discovery engine
            discovery_results = await self.discovery_engine.run_discovery_cycle(
                competitor_ids=competitor_ids,
                platform_ids=platform_ids
            )
            
            # Apply deduplication to discovered URLs
            if discovery_results.get('urls_discovered', 0) > 0:
                dedup_results = await self.dedup_engine.deduplicate_stage_results()
                discovery_results['deduplication'] = dedup_results
            
            # Queue engagement tasks for discovered URLs
            queued_count = await self._queue_engagement_tasks(
                competitor_ids=competitor_ids,
                platform_ids=platform_ids
            )
            discovery_results['engagement_tasks_queued'] = queued_count
            
            logger.info(f"Discovery cycle completed: {discovery_results}")
            return discovery_results
            
        except Exception as e:
            logger.error(f"Discovery cycle failed: {e}")
            raise
    
    async def run_engagement_cycle(
        self,
        competitor_ids: Optional[List[int]] = None,
        platform_ids: Optional[List[int]] = None,
        max_urls: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run engagement processing cycle
        """
        logger.info(f"Starting engagement cycle for competitors: {competitor_ids}, platforms: {platform_ids}")
        
        try:
            # Get pending URLs from discovery results
            pending_urls = await self._get_pending_engagement_urls(
                competitor_ids=competitor_ids,
                platform_ids=platform_ids,
                limit=max_urls
            )
            
            if not pending_urls:
                logger.info("No pending URLs for engagement processing")
                return {'urls_processed': 0, 'success': True}
            
            # Process engagement in batches
            results = await self.engagement_engine.process_urls_batch(pending_urls)
            
            # Apply content deduplication to engagement results
            if results.get('urls_processed', 0) > 0:
                dedup_results = await self.dedup_engine.deduplicate_final_results()
                results['content_deduplication'] = dedup_results
            
            logger.info(f"Engagement cycle completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Engagement cycle failed: {e}")
            raise
    
    async def run_full_pipeline(
        self,
        competitor_ids: Optional[List[int]] = None,
        platform_ids: Optional[List[int]] = None,
        discovery_only: bool = False
    ) -> Dict[str, Any]:
        """
        Run complete 2-stage pipeline: Discovery → Engagement
        """
        logger.info("Starting full pipeline execution")
        
        try:
            pipeline_results = {
                'started_at': datetime.utcnow().isoformat(),
                'discovery': {},
                'engagement': {},
                'success': True
            }
            
            # Stage 1: Discovery
            discovery_results = await self.run_discovery_cycle(
                competitor_ids=competitor_ids,
                platform_ids=platform_ids
            )
            pipeline_results['discovery'] = discovery_results
            
            if discovery_only:
                logger.info("Pipeline completed (discovery only)")
                return pipeline_results
            
            # Wait a bit for discovery tasks to queue properly
            await asyncio.sleep(5)
            
            # Stage 2: Engagement
            engagement_results = await self.run_engagement_cycle(
                competitor_ids=competitor_ids,
                platform_ids=platform_ids
            )
            pipeline_results['engagement'] = engagement_results
            
            pipeline_results['completed_at'] = datetime.utcnow().isoformat()
            
            logger.info("Full pipeline completed successfully")
            return pipeline_results
            
        except Exception as e:
            logger.error(f"Full pipeline failed: {e}")
            pipeline_results['success'] = False
            pipeline_results['error'] = str(e)
            raise
    
    async def process_manual_urls(self) -> Dict[str, Any]:
        """
        Process URLs from manual queue
        """
        logger.info("Processing manual URLs")
        
        try:
            # Get manual URLs from queue
            query = """
            SELECT id, competitor_id, platform_id, url, priority_level
            FROM manual_queue 
            WHERE status = 'pending'
            ORDER BY priority_level, created_at
            LIMIT 100
            """
            manual_urls = await self.db.execute_query(query)
            
            if not manual_urls:
                return {'urls_processed': 0, 'message': 'No manual URLs pending'}
            
            results = {
                'urls_processed': 0,
                'successful': 0,
                'failed': 0,
                'errors': []
            }
            
            for url_record in manual_urls:
                try:
                    # Process URL directly through engagement engine
                    engagement_result = await self.engagement_engine.process_single_url(
                        url=url_record['url'],
                        competitor_id=url_record['competitor_id'],
                        platform_id=url_record['platform_id']
                    )
                    
                    if engagement_result['success']:
                        results['successful'] += 1
                        # Update manual queue status
                        await self.db.execute_query(
                            "UPDATE manual_queue SET status = 'completed', processed_at = NOW() WHERE id = $1",
                            [url_record['id']]
                        )
                    else:
                        results['failed'] += 1
                        results['errors'].append(f"URL {url_record['url']}: {engagement_result.get('error', 'Unknown error')}")
                        
                    results['urls_processed'] += 1
                    
                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append(f"URL {url_record['url']}: {str(e)}")
                    logger.error(f"Failed to process manual URL {url_record['url']}: {e}")
            
            logger.info(f"Manual URL processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Manual URL processing failed: {e}")
            raise
    
    async def run_monitoring_cycle(self) -> Dict[str, Any]:
        """
        Run monitoring cycle for scheduled sources
        """
        logger.info("Running monitoring cycle")
        
        try:
            # Get due monitoring schedules
            query = """
            SELECT ms.*, s.name as source_name, s.url as source_url,
                   c.name as competitor_name, p.name as platform_name
            FROM monitoring_schedule ms
            JOIN sources s ON ms.source_id = s.id
            JOIN competitors c ON s.competitor_id = c.id
            JOIN platforms p ON s.platform_id = p.id
            WHERE ms.is_active = true 
            AND ms.next_run <= NOW()
            ORDER BY ms.priority_level, ms.next_run
            """
            
            schedules = await self.db.execute_query(query)
            
            if not schedules:
                return {'schedules_processed': 0, 'message': 'No due monitoring schedules'}
            
            results = {
                'schedules_processed': 0,
                'successful': 0,
                'failed': 0,
                'urls_discovered': 0
            }
            
            for schedule in schedules:
                try:
                    # Queue monitoring task
                    monitoring_task.delay(
                        source_id=schedule['source_id'],
                        schedule_id=schedule['id']
                    )
                    
                    results['successful'] += 1
                    results['schedules_processed'] += 1
                    
                    # Update next run time
                    from croniter import croniter
                    cron = croniter(schedule['cron_expression'], datetime.utcnow())
                    next_run = cron.get_next(datetime)
                    
                    await self.db.execute_query(
                        "UPDATE monitoring_schedule SET next_run = $1, last_run = NOW() WHERE id = $2",
                        [next_run, schedule['id']]
                    )
                    
                except Exception as e:
                    results['failed'] += 1
                    logger.error(f"Failed to process monitoring schedule {schedule['id']}: {e}")
            
            logger.info(f"Monitoring cycle completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Monitoring cycle failed: {e}")
            raise
    
    async def generate_analytics_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        Generate analytics summary for the last N days
        """
        logger.info(f"Generating analytics summary for last {days} days")
        
        try:
            # Queue analytics task
            analytics_task.delay(days=days)
            
            return {
                'message': f'Analytics generation queued for last {days} days',
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Analytics generation failed: {e}")
            raise
    
    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get comprehensive system status
        """
        try:
            status = {
                'timestamp': datetime.utcnow().isoformat(),
                'orchestrator_running': self.is_running,
                'database_connected': self.db.is_connected() if self.db else False,
                'components': {},
                'queues': {},
                'recent_activity': {}
            }
            
            # Component status
            status['components'] = {
                'discovery_engine': self.discovery_engine is not None,
                'engagement_engine': self.engagement_engine is not None,
                'deduplication_engine': self.dedup_engine is not None
            }
            
            # Queue status (requires Celery inspection)
            try:
                inspect = celery_app.control.inspect()
                active_tasks = inspect.active()
                status['queues']['active_tasks'] = len(active_tasks or {})
            except:
                status['queues']['error'] = 'Unable to inspect queue status'
            
            # Recent activity from database
            if self.db and await self.db.is_connected():
                # Discovery results from last 24h
                discovery_count = await self.db.execute_query(
                    "SELECT COUNT(*) as count FROM stage_results WHERE created_at >= NOW() - INTERVAL '24 hours'"
                )
                status['recent_activity']['discovery_results_24h'] = discovery_count[0]['count'] if discovery_count else 0
                
                # Engagement results from last 24h
                engagement_count = await self.db.execute_query(
                    "SELECT COUNT(*) as count FROM final_results WHERE created_at >= NOW() - INTERVAL '24 hours'"
                )
                status['recent_activity']['engagement_results_24h'] = engagement_count[0]['count'] if engagement_count else 0
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e),
                'success': False
            }
    
    async def _queue_engagement_tasks(
        self, 
        competitor_ids: Optional[List[int]] = None,
        platform_ids: Optional[List[int]] = None
    ) -> int:
        """
        Queue engagement tasks for pending URLs
        """
        try:
            # Get pending URLs from stage_results
            where_conditions = ["engagement_status = 'pending'"]
            params = []
            
            if competitor_ids:
                where_conditions.append(f"competitor_id = ANY(${len(params) + 1})")
                params.append(competitor_ids)
            
            if platform_ids:
                where_conditions.append(f"platform_id = ANY(${len(params) + 1})")
                params.append(platform_ids)
            
            query = f"""
            SELECT id, competitor_id, platform_id, url
            FROM stage_results
            WHERE {' AND '.join(where_conditions)}
            ORDER BY priority_level, created_at
            LIMIT 1000
            """
            
            pending_urls = await self.db.execute_query(query, params)
            
            queued_count = 0
            for url_record in pending_urls:
                engagement_task.delay(
                    stage_result_id=url_record['id'],
                    url=url_record['url'],
                    competitor_id=url_record['competitor_id'],
                    platform_id=url_record['platform_id']
                )
                queued_count += 1
            
            return queued_count
            
        except Exception as e:
            logger.error(f"Failed to queue engagement tasks: {e}")
            return 0
    
    async def _get_pending_engagement_urls(
        self,
        competitor_ids: Optional[List[int]] = None,
        platform_ids: Optional[List[int]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get URLs pending engagement processing
        """
        try:
            where_conditions = ["engagement_status = 'pending'"]
            params = []
            
            if competitor_ids:
                where_conditions.append(f"competitor_id = ANY(${len(params) + 1})")
                params.append(competitor_ids)
            
            if platform_ids:
                where_conditions.append(f"platform_id = ANY(${len(params) + 1})")
                params.append(platform_ids)
            
            query = f"""
            SELECT id, competitor_id, platform_id, url, source_type, metadata
            FROM stage_results
            WHERE {' AND '.join(where_conditions)}
            ORDER BY priority_level, created_at
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            return await self.db.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Failed to get pending engagement URLs: {e}")
            return []

# CLI Interface
async def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Political Intelligence Orchestrator')
    parser.add_argument('command', choices=[
        'discovery', 'engagement', 'pipeline', 'manual', 'monitoring', 
        'analytics', 'status', 'daemon'
    ], help='Command to execute')
    
    parser.add_argument('--competitors', nargs='+', type=int, help='Competitor IDs to process')
    parser.add_argument('--platforms', nargs='+', type=int, help='Platform IDs to process')
    parser.add_argument('--discovery-only', action='store_true', help='Run discovery only')
    parser.add_argument('--max-urls', type=int, help='Maximum URLs to process in engagement')
    parser.add_argument('--days', type=int, default=7, help='Days for analytics (default: 7)')
    parser.add_argument('--interval', type=int, default=300, help='Daemon interval in seconds (default: 300)')
    
    args = parser.parse_args()
    
    # Initialize orchestrator
    orchestrator = PoliticalIntelligenceOrchestrator()
    
    try:
        await orchestrator.initialize()
        orchestrator.is_running = True
        
        if args.command == 'discovery':
            result = await orchestrator.run_discovery_cycle(
                competitor_ids=args.competitors,
                platform_ids=args.platforms
            )
            print(f"Discovery completed: {result}")
            
        elif args.command == 'engagement':
            result = await orchestrator.run_engagement_cycle(
                competitor_ids=args.competitors,
                platform_ids=args.platforms,
                max_urls=args.max_urls
            )
            print(f"Engagement completed: {result}")
            
        elif args.command == 'pipeline':
            result = await orchestrator.run_full_pipeline(
                competitor_ids=args.competitors,
                platform_ids=args.platforms,
                discovery_only=args.discovery_only
            )
            print(f"Pipeline completed: {result}")
            
        elif args.command == 'manual':
            result = await orchestrator.process_manual_urls()
            print(f"Manual processing completed: {result}")
            
        elif args.command == 'monitoring':
            result = await orchestrator.run_monitoring_cycle()
            print(f"Monitoring completed: {result}")
            
        elif args.command == 'analytics':
            result = await orchestrator.generate_analytics_summary(days=args.days)
            print(f"Analytics queued: {result}")
            
        elif args.command == 'status':
            result = await orchestrator.get_system_status()
            print(f"System status: {result}")
            
        elif args.command == 'daemon':
            print(f"Starting daemon mode (interval: {args.interval}s)")
            try:
                while orchestrator.is_running:
                    logger.info("Running scheduled pipeline cycle...")
                    
                    # Run full pipeline
                    await orchestrator.run_full_pipeline()
                    
                    # Process manual URLs
                    await orchestrator.process_manual_urls()
                    
                    # Run monitoring
                    await orchestrator.run_monitoring_cycle()
                    
                    # Wait for next cycle
                    await asyncio.sleep(args.interval)
                    
            except KeyboardInterrupt:
                logger.info("Daemon stopped by user")
    
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        raise
    
    finally:
        await orchestrator.shutdown()

if __name__ == '__main__':
    asyncio.run(main())