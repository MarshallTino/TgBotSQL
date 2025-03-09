"""
Celery configuration for the price tracker service.
"""

import os
import sys
from pathlib import Path
from celery import Celery

# Add the project root to Python's path
sys.path.append('/app')

# Import settings
from config.settings import CELERY_BROKER_URL, CELERY_RESULT_BACKEND

# Create Celery app
app = Celery('price_tracker')

# Configure Celery
app.conf.broker_url = CELERY_BROKER_URL
app.conf.result_backend = CELERY_RESULT_BACKEND
app.conf.task_serializer = 'json'
app.conf.accept_content = ['json']
app.conf.result_serializer = 'json'
app.conf.timezone = 'UTC'
app.conf.worker_max_tasks_per_child = 1000
app.conf.worker_prefetch_multiplier = 1  # One task per worker at a time

# Define scheduled tasks
app.conf.beat_schedule = {
    'update-token-prices-every-minute': {
        'task': 'scripts.price_tracker.tasks.update_all_token_prices',
        'schedule': 60.0,  # Run every 60 seconds
    },
    'process-mongodb-data-every-30-seconds': {
        'task': 'scripts.price_tracker.tasks.process_mongodb_data',
        'schedule': 30.0,   # Run every 30 seconds
    },
}

# Auto-discover tasks
app.autodiscover_tasks(['scripts.price_tracker'])
