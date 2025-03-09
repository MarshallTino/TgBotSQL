"""
MongoDB utility functions for the price tracker.
"""

import os
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Configure logging
logger = logging.getLogger(__name__)

def connect_mongodb():
    """Connect to MongoDB with proper authentication."""
    try:
        username = os.getenv("MONGO_USER", "bot")
        password = os.getenv("MONGO_PASSWORD", "bot1234")
        host = os.getenv("MONGO_HOST", "mongo")  # In Docker this will be "mongo", locally "localhost"
        port = os.getenv("MONGO_PORT", "27017")
        
        logger.info(f"Connecting to MongoDB at {host}:{port} with user {username}")
        
        # Build connection string - explicitly specify authSource=admin
        conn_string = f"mongodb://{username}:{password}@{host}:{port}/admin?authSource=admin"
        
        client = MongoClient(conn_string, serverSelectionTimeoutMS=5000)
        
        # Force a command to test the connection
        client.admin.command('ping')
        logger.info(f"✅ Successfully connected to MongoDB at {host}:{port}")
        return client
    except Exception as e:
        logger.error(f"❌ Error connecting to MongoDB: {e}")
        return None

# Add compatibility function
def get_mongo_client():
    """Alias for connect_mongodb()"""
    return connect_mongodb()

def get_collection(db_name=None, collection_name=None):
    """Get a specific MongoDB collection."""
    try:
        client = connect_mongodb()
        if not client:
            return None
        
        if not db_name:
            db_name = os.getenv("MONGO_DB", "tgbot_db")
            
        db = client[db_name]
        
        if not collection_name:
            collection_name = os.getenv("MONGO_COLLECTION_NAME", "dexscreener_data")
            
        return db[collection_name]
    except Exception as e:
        logger.error(f"❌ Error getting MongoDB collection: {e}")
        return None

def get_dexscreener_collection():
    """Get the collection for DexScreener data."""
    client = connect_mongodb()
    if not client:
        logger.error("Failed to connect to MongoDB")
        return None
    
    db_name = os.getenv("MONGO_DB", "tgbot_db")
    collection_name = os.getenv("MONGO_COLLECTION_NAME", "dexscreener_data")
    
    # Create the database and collection if they don't exist
    db = client[db_name]
    if collection_name not in db.list_collection_names():
        logger.info(f"Creating collection {collection_name} in {db_name}")
        db.create_collection(collection_name)
    
    return db[collection_name]

def initialize_mongodb():
    """Initialize MongoDB collections and indexes."""
    try:
        client = connect_mongodb()
        if not client:
            return False
            
        db_name = os.getenv("MONGO_DB", "tgbot_db")
        db = client[db_name]
        
        # Create or get collection
        dexscreener_collection = db["dexscreener_data"]
        
        # Create indexes
        dexscreener_collection.create_index([("token_address", 1)])
        dexscreener_collection.create_index([("pair_address", 1)])
        dexscreener_collection.create_index([("fetched_at", -1)])
        dexscreener_collection.create_index([("processed", 1)])
        
        logger.info("✅ MongoDB indexes created")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize MongoDB: {e}")
        return False
