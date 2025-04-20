import os
import logging
import json
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown

# Configure logger properly before using it
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Make sure we have at least a console handler
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

app = Celery('price_tracker')
app.conf.broker_url = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
app.conf.result_backend = os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')

# Configure Celery settings
app.conf.task_serializer = 'json'
app.conf.accept_content = ['json']
app.conf.result_serializer = 'json'
app.conf.worker_hijack_root_logger = False
app.conf.worker_log_color = True
app.conf.worker_log_format = "%(asctime)s - %(levelname)s: %(message)s"
app.conf.worker_task_log_format = "%(asctime)s - %(levelname)s: %(task_name)s - %(message)s"

# Setup database connection pooling when worker starts
@worker_process_init.connect
def init_worker_process(**kwargs):
    """Initialize database connections for each worker process"""
    from scripts.utils.db_postgres import init_connection_pool
    from scripts.utils.db_mongo import connect_mongodb
    
    # Initialize PostgreSQL connection pool
    success = init_connection_pool(min_conn=1, max_conn=5)
    if not success:
        logger.error("Failed to initialize PostgreSQL connection pool for worker")
    else:
        logger.info("PostgreSQL connection pool initialized successfully")
    
    # Test MongoDB connection
    mongo_client = connect_mongodb()
    if (mongo_client):
        logger.info("MongoDB connection test successful")
        mongo_client.close()
    else:
        logger.error("Failed to connect to MongoDB")

@worker_process_shutdown.connect
def cleanup_worker_process(**kwargs):
    """Clean up resources when worker shuts down"""
    from scripts.utils.db_postgres import connection_pool
    
    # Close all database connections
    if connection_pool:
        try:
            connection_pool.closeall()
            logger.info("Database connection pool closed")
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")
    else:
        logger.warning("No connection pool to close")

app.conf.beat_schedule = {
    # Update high-priority tokens (30s interval) frequently
    'update-token-prices-by-frequency': {
        'task': 'scripts.price_tracker.tasks.update_token_prices_by_frequency',
        'schedule': 15.0,  # Run every 15 seconds to catch 30s tokens
    },
    # Run the token classification hourly
    'classify-all-tokens': {
        'task': 'scripts.price_tracker.tasks.classify_all_tokens',
        'schedule': 3600.0,  # Every hour
    },
    'process-mongodb-every-minute': {
        'task': 'scripts.price_tracker.tasks.process_mongodb_data',
        'schedule': 45.0,
    },
    'log-minute-summary': {
        'task': 'scripts.price_tracker.tasks.log_minute_summary',
        'schedule': 60.0,
    },
    'check-database-health': {
        'task': 'scripts.price_tracker.tasks.check_database_health',
        'schedule': 300.0,  # Every 5 minutes
    },
    'analyze-recurring-failures': {
        'task': 'scripts.price_tracker.tasks.analyze_recurring_failures',
        'schedule': 300.0,  # Every 5 minutes
    },
    # Check for tokens that need deactivation due to consistent failures
    'process-failing-tokens': {
        'task': 'scripts.price_tracker.token_recovery.process_failing_tokens',
        'schedule': 900.0,  # Run every 15 minutes
        'kwargs': {'threshold': 10, 'limit': 50}
    },
    # Weekly task to check and revive inactive tokens
    'check-and-revive-inactive-tokens': {
        'task': 'scripts.price_tracker.tasks.check_and_revive_inactive_tokens',
        'schedule': 86400.0 * 7,  # Run once every 7 days
    },
    # Check inactive tokens more frequently but with a smaller batch
    'check-inactive-tokens': {
        'task': 'scripts.price_tracker.token_recovery.check_inactive_tokens_task',
        'schedule': 43200.0,  # Run twice daily
        'kwargs': {'limit': 25}
    },
    # Daily task to attempt recovery of consistently failing tokens
    'automatic-token-recovery': {
        'task': 'scripts.price_tracker.token_recovery.automatic_token_recovery',
        'schedule': 86400.0,  # Run daily
        'kwargs': {'min_failures': 5, 'max_tokens': 500}
    },
    # Synchronize in-memory failure tracking with database
    'sync-failure-tracking': {
        'task': 'scripts.price_tracker.token_recovery.sync_failure_tracking',
        'schedule': 1800.0,  # Run every 30 minutes
    }
}

# Make sure all task modules are imported
app.conf.imports = ['scripts.price_tracker.tasks', 'scripts.price_tracker.token_recovery']

# Make sure to properly import the tasks
app.autodiscover_tasks(['scripts.price_tracker'])

@app.task(name="price_tracker.test_celery_task")
def test_celery_task():
    """
    Simple test task to verify Celery is working properly.
    
    Returns:
        dict: Status message with timestamp
    """
    from datetime import datetime
    logger.info("Running test Celery task")
    return {
        "status": "success",
        "message": "Celery is working!",
        "timestamp": datetime.now().isoformat()
    }

if __name__ == '__main__':
    app.start()
