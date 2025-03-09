import os
import logging
from pymongo import MongoClient

logger = logging.getLogger(__name__)

def get_mongodb_connection():
    """Connect to MongoDB with proper authentication."""
    try:
        username = os.getenv("MONGO_USERNAME", "bot")
        password = os.getenv("MONGO_PASSWORD", "bot1234")
        host = os.getenv("MONGO_HOST", "mongo")
        port = os.getenv("MONGO_PORT", "27017")
        
        # Create authenticated connection string
        conn_string = f"mongodb://{username}:{password}@{host}:{port}/admin"
        
        client = MongoClient(conn_string, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.server_info()
        logger.info(f"✅ Successfully connected to MongoDB at {host}:{port}")
        return client
    except Exception as e:
        logger.error(f"❌ Error connecting to MongoDB: {e}")
        raise

