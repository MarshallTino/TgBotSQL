"""
Helper script to run celery worker and beat.
"""

import os
import sys
from pathlib import Path

# Add project root to Python's path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

if __name__ == "__main__":
    # Import the Celery app
    from scripts.price_tracker.celery_app import app
    
    # Initialize MongoDB
    from utils.db_mongo import initialize_mongodb
    initialize_mongodb()
    
    print("MongoDB initialized. Use these commands to run Celery:")
    print("\nStart worker:")
    print("celery -A scripts.price_tracker.celery_app worker --loglevel=info")
    print("\nStart beat scheduler:")
    print("celery -A scripts.price_tracker.celery_app beat --loglevel=info")
