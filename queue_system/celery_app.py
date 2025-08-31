#!/usr/bin/env python3
"""
Celery Application Configuration
Queue system for political intelligence scraping pipeline
"""

import os
from celery import Celery
from kombu import Queue
from dotenv import load_dotenv

load_dotenv()

# Redis configuration
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Create Celery app
app = Celery('political_intelligence')

# Configuration
app.conf.update(
    # Broker settings
    broker_url=redis_url,
    result_backend=redis_url,
    
    # Task settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
    
    # Worker settings
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
    
    # Queue routing
    task_routes={
        'queue_system.tasks.discovery_task': {'queue': 'discovery'},
        'queue_system.tasks.engagement_task': {'queue': 'engagement'},
        'queue_system.tasks.monitoring_task': {'queue': 'monitoring'},
        'queue_system.tasks.analytics_task': {'queue': 'analytics'}
    },
    
    # Queue declarations
    task_default_queue='default',
    task_queues=(
        Queue('discovery', routing_key='discovery'),
        Queue('engagement', routing_key='engagement'),
        Queue('monitoring', routing_key='monitoring'),
        Queue('analytics', routing_key='analytics'),
        Queue('default', routing_key='default'),
    ),
    
    # Task execution settings
    task_soft_time_limit=600,  # 10 minutes
    task_time_limit=900,       # 15 minutes
    task_max_retries=3,
    task_default_retry_delay=60,  # 1 minute
    
    # Result backend settings
    result_expires=3600,  # 1 hour
    
    # Monitoring
    worker_send_task_events=True,
    task_send_sent_event=True,
)

# Auto-discover tasks
app.autodiscover_tasks(['queue_system'])

if __name__ == '__main__':
    app.start()