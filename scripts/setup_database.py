import os
import sys
import logging

# Add parent directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils.db_postgres import connect_postgres
from scripts.utils.db_mongo import connect_mongodb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def setup_postgres():
    """Initialize PostgreSQL database tables"""
    try:
        conn = connect_postgres()
        if not conn:
            logger.error("Failed to connect to PostgreSQL")
            return False
            
        cursor = conn.cursor()
        
        # Create price_metrics table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_metrics (
            token_id BIGINT NOT NULL REFERENCES tokens(token_id),
            pair_address VARCHAR(66),
            timestamp TIMESTAMPTZ NOT NULL,
            price_native NUMERIC,
            price_usd NUMERIC,
            txns_buys INTEGER,
            txns_sells INTEGER,
            volume NUMERIC,
            liquidity_base NUMERIC,
            liquidity_quote NUMERIC,
            liquidity_usd NUMERIC,
            fdv NUMERIC,
            market_cap NUMERIC,
            mongo_id TEXT,
            PRIMARY KEY (token_id, timestamp)
        );
        """)
        
        # Try to convert to TimescaleDB hypertable
        try:
            cursor.execute("""
            SELECT create_hypertable('price_metrics', 'timestamp', if_not_exists => TRUE);
            
            CREATE INDEX IF NOT EXISTS idx_price_metrics_token_id ON price_metrics(token_id);
            CREATE INDEX IF NOT EXISTS idx_price_metrics_timestamp ON price_metrics(timestamp DESC);
            """)
            logger.info("✅ TimescaleDB hypertable created successfully")
        except Exception as e:
            logger.warning(f"⚠️ TimescaleDB hypertable creation failed (can be ignored if TimescaleDB not enabled): {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("PostgreSQL database setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to set up PostgreSQL database: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        return False

def test_mongo_connection():
    """Test MongoDB connection with authentication"""
    try:
        client = connect_mongodb()
        if not client:
            return False
            
        db_names = client.list_database_names()
        logger.info(f"MongoDB connection successful. Available databases: {db_names}")
        client.close()
        return True
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return False

if __name__ == "__main__":
    logger.info("Starting database setup process...")
    
    try:
        # Test MongoDB connection
        logger.info("Testing MongoDB connection...")
        if not test_mongo_connection():
            raise Exception("MongoDB connection failed")
        
        # Setup PostgreSQL database
        logger.info("Setting up PostgreSQL database...")
        if not setup_postgres():
            raise Exception("PostgreSQL setup failed")
            
        logger.info("✅ Database setup completed successfully")
    except Exception as e:
        logger.error(f"❌ Database setup failed: {e}")
        sys.exit(1)
