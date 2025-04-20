"""Test script to verify Celery configuration in Docker"""
import os
import sys
from pathlib import Path

# Add project root to Python's path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

print("🔍 Docker Celery Test Script")
print("=========================")

# Print environment
print("\n📋 Environment variables:")
relevant_vars = ["REDIS_HOST", "PYTHONPATH", "CELERY_BROKER_URL"]
for var in relevant_vars:
    print(f"  {var}: {os.environ.get(var, 'NOT SET')}")

# Import Celery app
print("\n📋 Importing Celery app...")
try:
    from scripts.price_tracker.celery_app import app
    print("✅ Celery app imported successfully")
    print(f"  Broker URL: {app.conf.broker_url}")
    print(f"  Imports: {app.conf.imports}")
    print(f"  Tasks: {list(app.tasks.keys())}")
except Exception as e:
    print(f"❌ Error importing Celery app: {e}")

# Try importing tasks
print("\n📋 Importing tasks...")
try:
    from scripts.price_tracker.tasks import update_price_metrics
    print("✅ Tasks imported successfully")
except Exception as e:
    print(f"❌ Error importing tasks: {e}")

# Directly execute test task
print("\n📋 Running test task locally...")
try:
    from scripts.price_tracker.celery_app import test_celery_task
    result = test_celery_task()
    print(f"✅ Task ran successfully: {result}")
except Exception as e:
    print(f"❌ Error running task: {e}")

print("\n✅ Test script completed")
