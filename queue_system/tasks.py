#!/usr/bin/env python3
"""
Celery Tasks for Political Intelligence System
Background tasks for discovery, engagement, monitoring, and analytics
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import json
import uuid

from celery import Task
from .celery_app import app

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseTask(Task):
    """Base task with database connection handling"""
    
    _db = None
    
    @property
    def db(self):
        if self._db is None:
            # Import here to avoid circular imports
            import sys
            sys.path.append('..')
            from database import get_database
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._db = loop.run_until_complete(get_database())
        
        return self._db

@app.task(bind=True, base=DatabaseTask, queue='discovery')
def discovery_task(self, competitor_ids: List[int] = None, platform_ids: List[int] = None):
    """Discovery task - Stage 1 of pipeline"""
    
    task_id = str(uuid.uuid4())
    
    try:
        # Create job record
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        job_data = {
            'job_id': task_id,
            'job_type': 'discovery',
            'status': 'running',
            'celery_task_id': self.request.id,
            'started_at': datetime.now(),
            'metadata': {
                'competitor_ids': competitor_ids,
                'platform_ids': platform_ids
            }
        }
        
        # Insert job record
        loop.run_until_complete(create_job_record(self.db, job_data))
        
        # Update progress
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100, 'status': 'Starting discovery...'})
        
        # Import and run discovery engine
        from engines.discovery_engine import run_discovery_cycle
        
        self.update_state(state='PROGRESS', meta={'current': 25, 'total': 100, 'status': 'Running discovery cycle...'})
        
        result = loop.run_until_complete(run_discovery_cycle(competitor_ids, platform_ids))
        
        if result['success']:
            # Update job as completed
            loop.run_until_complete(update_job_status(
                self.db, task_id, 'completed', 
                items_processed=result['stats'].get('total_urls_found', 0),
                items_successful=result['stats'].get('total_urls_added', 0)
            ))
            
            self.update_state(state='SUCCESS', meta={'current': 100, 'total': 100, 'status': 'Discovery completed', 'result': result})
            
            # Trigger engagement tasks for discovered URLs
            if result['stats'].get('total_urls_added', 0) > 0:
                engagement_task.delay(competitor_ids, platform_ids)
            
            return result
        else:
            # Update job as failed
            loop.run_until_complete(update_job_status(
                self.db, task_id, 'failed', error_message=result.get('error')
            ))
            
            self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'status': 'Discovery failed', 'error': result.get('error')})
            return result
    
    except Exception as e:
        logger.error(f"Discovery task failed: {e}")
        
        # Update job as failed
        try:
            loop.run_until_complete(update_job_status(
                self.db, task_id, 'failed', error_message=str(e)
            ))
        except:
            pass
        
        self.update_state(state='FAILURE', meta={'error': str(e)})
        raise

@app.task(bind=True, base=DatabaseTask, queue='engagement')
def engagement_task(self, competitor_ids: List[int] = None, platform_ids: List[int] = None, limit: int = 100):
    """Engagement task - Stage 2 of pipeline"""
    
    task_id = str(uuid.uuid4())
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create job record
        job_data = {
            'job_id': task_id,
            'job_type': 'engagement',
            'status': 'running',
            'celery_task_id': self.request.id,
            'started_at': datetime.now(),
            'metadata': {
                'competitor_ids': competitor_ids,
                'platform_ids': platform_ids,
                'limit': limit
            }
        }
        
        loop.run_until_complete(create_job_record(self.db, job_data))
        
        # Update progress
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100, 'status': 'Starting engagement extraction...'})
        
        # Import and run engagement engine
        from engines.engagement_engine import run_engagement_cycle
        
        self.update_state(state='PROGRESS', meta={'current': 50, 'total': 100, 'status': 'Processing URLs...'})
        
        result = loop.run_until_complete(run_engagement_cycle(competitor_ids, platform_ids, limit))
        
        if result['success']:
            # Update job as completed
            loop.run_until_complete(update_job_status(
                self.db, task_id, 'completed',
                items_processed=result['stats'].get('total_urls_processed', 0),
                items_successful=result['stats'].get('total_engagement_extracted', 0)
            ))
            
            self.update_state(state='SUCCESS', meta={'current': 100, 'total': 100, 'status': 'Engagement extraction completed', 'result': result})
            
            # Trigger analytics update
            analytics_task.delay(competitor_ids, platform_ids)
            
            return result
        else:
            # Update job as failed
            loop.run_until_complete(update_job_status(
                self.db, task_id, 'failed', error_message=result.get('error')
            ))
            
            self.update_state(state='FAILURE', meta={'error': result.get('error')})
            return result
    
    except Exception as e:
        logger.error(f"Engagement task failed: {e}")
        
        try:
            loop.run_until_complete(update_job_status(
                self.db, task_id, 'failed', error_message=str(e)
            ))
        except:
            pass
        
        self.update_state(state='FAILURE', meta={'error': str(e)})
        raise

@app.task(bind=True, base=DatabaseTask, queue='monitoring')
def monitoring_task(self, source_ids: List[int] = None):
    """Source monitoring task"""
    
    task_id = str(uuid.uuid4())
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create job record
        job_data = {
            'job_id': task_id,
            'job_type': 'monitoring',
            'status': 'running',
            'celery_task_id': self.request.id,
            'started_at': datetime.now(),
            'metadata': {'source_ids': source_ids}
        }
        
        loop.run_until_complete(create_job_record(self.db, job_data))
        
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100, 'status': 'Starting source monitoring...'})
        
        # Run source monitoring logic
        result = loop.run_until_complete(run_source_monitoring(source_ids))
        
        # Update job status
        loop.run_until_complete(update_job_status(
            self.db, task_id, 'completed' if result['success'] else 'failed',
            items_processed=result.get('sources_monitored', 0),
            error_message=result.get('error')
        ))
        
        self.update_state(state='SUCCESS' if result['success'] else 'FAILURE', meta={
            'current': 100, 'total': 100, 
            'status': 'Monitoring completed' if result['success'] else 'Monitoring failed',
            'result': result
        })
        
        return result
    
    except Exception as e:
        logger.error(f"Monitoring task failed: {e}")
        self.update_state(state='FAILURE', meta={'error': str(e)})
        raise

@app.task(bind=True, base=DatabaseTask, queue='analytics')
def analytics_task(self, competitor_ids: List[int] = None, platform_ids: List[int] = None):
    """Analytics calculation task"""
    
    task_id = str(uuid.uuid4())
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create job record
        job_data = {
            'job_id': task_id,
            'job_type': 'analytics',
            'status': 'running',
            'celery_task_id': self.request.id,
            'started_at': datetime.now(),
            'metadata': {
                'competitor_ids': competitor_ids,
                'platform_ids': platform_ids
            }
        }
        
        loop.run_until_complete(create_job_record(self.db, job_data))
        
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100, 'status': 'Calculating analytics...'})
        
        # Run analytics calculations
        result = loop.run_until_complete(calculate_analytics(competitor_ids, platform_ids))
        
        # Update job status
        loop.run_until_complete(update_job_status(
            self.db, task_id, 'completed' if result['success'] else 'failed',
            error_message=result.get('error')
        ))
        
        self.update_state(state='SUCCESS' if result['success'] else 'FAILURE', meta={
            'current': 100, 'total': 100,
            'status': 'Analytics completed' if result['success'] else 'Analytics failed',
            'result': result
        })
        
        return result
    
    except Exception as e:
        logger.error(f"Analytics task failed: {e}")
        self.update_state(state='FAILURE', meta={'error': str(e)})
        raise

@app.task(bind=True, base=DatabaseTask)
def scheduled_discovery_task(self):
    """Scheduled discovery task (runs every hour)"""
    return discovery_task.delay()

@app.task(bind=True, base=DatabaseTask)
def scheduled_engagement_task(self):
    """Scheduled engagement task (runs every 30 minutes)"""
    return engagement_task.delay(limit=50)

@app.task(bind=True, base=DatabaseTask)
def cleanup_old_data_task(self):
    """Cleanup old data task (runs daily)"""
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Clean up old job records (older than 7 days)
        cleanup_query = """
        DELETE FROM scraping_jobs 
        WHERE created_at < NOW() - INTERVAL '7 days' 
        AND status IN ('completed', 'failed')
        """
        
        loop.run_until_complete(self.db.execute_query(cleanup_query))
        
        # Clean up old logs (older than 30 days)
        logs_cleanup = """
        DELETE FROM system_logs 
        WHERE created_at < NOW() - INTERVAL '30 days'
        """
        
        loop.run_until_complete(self.db.execute_query(logs_cleanup))
        
        logger.info("✅ Cleanup task completed")
        return {"success": True, "message": "Cleanup completed"}
        
    except Exception as e:
        logger.error(f"Cleanup task failed: {e}")
        return {"success": False, "error": str(e)}

# Helper functions for database operations
async def create_job_record(db, job_data: Dict):
    """Create job record in database"""
    
    query = """
    INSERT INTO scraping_jobs (
        job_id, job_type, competitor_id, platform_id, status, 
        celery_task_id, started_at, metadata
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    """
    
    await db.execute_query(query, (
        job_data['job_id'],
        job_data['job_type'],
        job_data.get('competitor_id'),
        job_data.get('platform_id'),
        job_data['status'],
        job_data['celery_task_id'],
        job_data['started_at'],
        json.dumps(job_data['metadata'])
    ))

async def update_job_status(db, job_id: str, status: str, items_processed: int = 0, 
                          items_successful: int = 0, error_message: str = None):
    """Update job status in database"""
    
    query = """
    UPDATE scraping_jobs 
    SET status = $1, completed_at = NOW(), items_processed = $2, 
        items_successful = $3, error_message = $4
    WHERE job_id = $5
    """
    
    await db.execute_query(query, (
        status, items_processed, items_successful, error_message, job_id
    ))

async def run_source_monitoring(source_ids: List[int] = None) -> Dict[str, Any]:
    """Run source monitoring logic"""
    
    try:
        from engines.discovery_engine import get_discovery_engine
        
        engine = await get_discovery_engine()
        
        # Get sources to monitor
        if source_ids:
            sources_query = """
            SELECT s.*, c.name as competitor_name, p.name as platform_name
            FROM sources s
            JOIN competitors c ON s.competitor_id = c.competitor_id
            JOIN platforms p ON s.platform_id = p.platform_id
            WHERE s.source_id = ANY($1) AND s.is_active = TRUE
            """
            sources = await engine.db.execute_query(sources_query, (source_ids,))
        else:
            sources = await engine.get_sources_to_monitor()
        
        sources_monitored = 0
        content_found = 0
        
        for source in sources:
            try:
                # Monitor source
                source_dict = dict(source)
                content_results = await engine.monitor_source(source_dict)
                
                # Add found content to stage_results
                for result in content_results:
                    added = await engine.add_to_stage_results(result)
                    if added:
                        content_found += 1
                
                # Update source monitoring timestamp
                await engine.update_source_monitored(source_dict['source_id'], len(content_results))
                sources_monitored += 1
                
            except Exception as e:
                logger.error(f"Failed to monitor source {source['name']}: {e}")
        
        return {
            'success': True,
            'sources_monitored': sources_monitored,
            'content_found': content_found,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Source monitoring failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

async def calculate_analytics(competitor_ids: List[int] = None, platform_ids: List[int] = None) -> Dict[str, Any]:
    """Calculate analytics and update summary tables"""
    
    try:
        from database import get_database
        db = await get_database()
        
        # Calculate daily analytics summary
        today = datetime.now().date()
        
        summary_query = """
        INSERT INTO analytics_summary (
            competitor_id, platform_id, date, period_type,
            total_mentions, total_content, total_engagement,
            total_views, total_likes, total_shares, total_comments,
            avg_engagement_rate, viral_content_count, avg_sentiment
        )
        SELECT 
            fr.competitor_id, fr.platform_id, $1, 'daily',
            COUNT(*) as total_mentions,
            COUNT(*) as total_content,
            SUM(fr.views_count + fr.likes_count + fr.shares_count + fr.comments_count) as total_engagement,
            SUM(fr.views_count) as total_views,
            SUM(fr.likes_count) as total_likes,
            SUM(fr.shares_count + fr.retweets_count) as total_shares,
            SUM(fr.comments_count) as total_comments,
            AVG(fr.engagement_rate) as avg_engagement_rate,
            COUNT(CASE WHEN fr.viral_score > 5 THEN 1 END) as viral_content_count,
            AVG(fr.sentiment_score) as avg_sentiment
        FROM final_results fr
        WHERE DATE(fr.scraped_at) = $1
        AND ($2::int[] IS NULL OR fr.competitor_id = ANY($2))
        AND ($3::int[] IS NULL OR fr.platform_id = ANY($3))
        GROUP BY fr.competitor_id, fr.platform_id
        ON CONFLICT (competitor_id, platform_id, date, period_type) 
        DO UPDATE SET
            total_mentions = EXCLUDED.total_mentions,
            total_content = EXCLUDED.total_content,
            total_engagement = EXCLUDED.total_engagement,
            total_views = EXCLUDED.total_views,
            total_likes = EXCLUDED.total_likes,
            total_shares = EXCLUDED.total_shares,
            total_comments = EXCLUDED.total_comments,
            avg_engagement_rate = EXCLUDED.avg_engagement_rate,
            viral_content_count = EXCLUDED.viral_content_count,
            avg_sentiment = EXCLUDED.avg_sentiment
        """
        
        await db.execute_query(summary_query, (today, competitor_ids, platform_ids))
        
        # Calculate share of voice
        await calculate_share_of_voice(db, today, competitor_ids, platform_ids)
        
        return {
            'success': True,
            'date': today.isoformat(),
            'message': 'Analytics calculated successfully',
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Analytics calculation failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

async def calculate_share_of_voice(db, date, competitor_ids: List[int] = None, platform_ids: List[int] = None):
    """Calculate share of voice for competitors"""
    
    try:
        # Get total engagement per platform
        platform_totals_query = """
        SELECT platform_id, SUM(total_engagement) as platform_total
        FROM analytics_summary
        WHERE date = $1
        AND ($2::int[] IS NULL OR platform_id = ANY($2))
        GROUP BY platform_id
        """
        
        platform_totals = await db.execute_query(platform_totals_query, (date, platform_ids))
        
        # Update share of voice for each competitor
        for total_row in platform_totals:
            platform_id = total_row['platform_id']
            platform_total = total_row['platform_total']
            
            if platform_total > 0:
                sov_update_query = """
                UPDATE analytics_summary
                SET share_of_voice = (total_engagement::decimal / $1) * 100
                WHERE date = $2 AND platform_id = $3
                AND ($4::int[] IS NULL OR competitor_id = ANY($4))
                """
                
                await db.execute_query(sov_update_query, (
                    platform_total, date, platform_id, competitor_ids
                ))
        
        logger.info(f"✅ Share of voice calculated for {date}")
        
    except Exception as e:
        logger.error(f"Share of voice calculation failed: {e}")

# Periodic tasks
@app.task
def run_hourly_discovery():
    """Hourly discovery task"""
    return discovery_task.delay()

@app.task  
def run_hourly_engagement():
    """Hourly engagement task"""
    return engagement_task.delay(limit=100)

@app.task
def run_daily_analytics():
    """Daily analytics task"""
    return analytics_task.delay()

@app.task
def run_weekly_cleanup():
    """Weekly cleanup task"""
    return cleanup_old_data_task.delay()

# Task chaining helper
def start_full_pipeline(competitor_ids: List[int] = None, platform_ids: List[int] = None):
    """Start full discovery -> engagement -> analytics pipeline"""
    
    # Chain tasks: discovery -> engagement -> analytics
    from celery import chain
    
    pipeline = chain(
        discovery_task.s(competitor_ids, platform_ids),
        engagement_task.s(competitor_ids, platform_ids, 200),
        analytics_task.s(competitor_ids, platform_ids)
    )
    
    return pipeline.apply_async()

# Manual task triggers for web UI
@app.task
def trigger_competitor_discovery(competitor_id: int):
    """Trigger discovery for specific competitor"""
    return discovery_task.delay([competitor_id])

@app.task  
def trigger_platform_engagement(platform_id: int):
    """Trigger engagement extraction for specific platform"""
    return engagement_task.delay(None, [platform_id])

@app.task
def process_manual_url_queue():
    """Process all pending manual URLs"""
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        from engines.discovery_engine import get_discovery_engine
        engine = loop.run_until_complete(get_discovery_engine())
        
        # Process manual queue
        loop.run_until_complete(engine.process_manual_queue())
        
        return {"success": True, "message": "Manual queue processed"}
        
    except Exception as e:
        logger.error(f"Manual queue processing failed: {e}")
        return {"success": False, "error": str(e)}
    finally:
        if 'loop' in locals():
            loop.close()

if __name__ == "__main__":
    # Test task execution
    print("Testing Celery tasks...")
    
    # Test discovery task
    result = discovery_task.delay()
    print(f"Discovery task ID: {result.id}")
    
    # Test engagement task  
    result = engagement_task.delay(limit=5)
    print(f"Engagement task ID: {result.id}")