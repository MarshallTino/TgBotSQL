import logging
import psycopg2
from scripts.utils.db_postgres import connect_postgres

logger = logging.getLogger(__name__)

def initialize_database():
    """Initialize PostgreSQL database tables if they don't exist."""
    conn = connect_postgres()
    if not conn:
        logger.error("❌ Failed to connect to PostgreSQL")
        raise Exception("Could not connect to PostgreSQL database")
        
    try:
        cursor = conn.cursor()
        
        # Create price_metrics table if not exists
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
        logger.info("✅ PostgreSQL schema initialized successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Error initializing schema: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
import os
import psycopg2
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def get_postgres_connection():
    """
    Context manager for getting a PostgreSQL database connection
    """
    conn = None
    try:
        # Get PostgreSQL credentials from environment variables
        db_host = os.getenv('PG_HOST', 'postgres')
        db_port = os.getenv('PG_PORT', '5432')
        db_name = os.getenv('PG_DATABASE', 'crypto_db')
        db_user = os.getenv('PG_USER', 'bot')
        db_password = os.getenv('PG_PASSWORD', 'bot1234')
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        
        logger.info("✅ PostgreSQL connection established")
        yield conn
    except Exception as e:
        logger.error(f"❌ PostgreSQL connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def initialize_database():
    """
    Initialize the PostgreSQL database by creating necessary tables if they don't exist.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # Create tokens table if it doesn't exist
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    token_id SERIAL PRIMARY KEY,
                    token_address VARCHAR(255) NOT NULL,
                    chain_id VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50),
                    name VARCHAR(255),
                    last_price NUMERIC,
                    last_updated TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(token_address, chain_id)
                );
                """)
                
                # Create price_metrics table if it doesn't exist
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
                
                # Check if TimescaleDB extension is available and create hypertable
                try:
                    # Check if TimescaleDB extension is installed
                    cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'timescaledb';")
                    has_timescaledb = cursor.fetchone() is not None
                    
                    if has_timescaledb:
                        # Convert to TimescaleDB hypertable
                        cursor.execute("""
                        SELECT create_hypertable('price_metrics', 'timestamp', if_not_exists => TRUE);
                        """)
                        
                        # Create indices for common queries
                        cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_price_metrics_token_id ON price_metrics(token_id);
                        """)
                        cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_price_metrics_timestamp ON price_metrics(timestamp DESC);
                        """)
                        
                        logger.info("✅ TimescaleDB hypertable and indices created successfully")
                    else:
                        logger.warning("TimescaleDB extension not found. Tables created as regular PostgreSQL tables.")
                except Exception as e:
                    logger.warning(f"TimescaleDB setup error (non-fatal): {e}")
                
                conn.commit()
                logger.info("✅ Database tables initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize database tables: {e}")
        raise

def get_all_tracked_tokens(self=None):
    """
    Get all tracked tokens from the database.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT token_id, contract_address, blockchain, name
                FROM tokens
                """)
                
                columns = [desc[0] for desc in cursor.description]
                tokens = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                return tokens
    except psycopg2.Error as e:
        logger.warning(f"Database error in get_all_tracked_tokens: {e}")
        return []

def insert_price_metrics(price_data):
    """
    Insert price metrics into the price_metrics table.
    
    Args:
        price_data (dict): Dictionary containing price metrics data
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                INSERT INTO price_metrics 
                (token_id, pair_address, timestamp, price_native, price_usd, 
                txns_buys, txns_sells, volume, liquidity_base, liquidity_quote, 
                liquidity_usd, fdv, market_cap, mongo_id)
                VALUES 
                (%(token_id)s, %(pair_address)s, %(timestamp)s, %(price_native)s, %(price_usd)s,
                %(txns_buys)s, %(txns_sells)s, %(volume)s, %(liquidity_base)s, %(liquidity_quote)s,
                %(liquidity_usd)s, %(fdv)s, %(market_cap)s, %(mongo_id)s)
                ON CONFLICT (token_id, timestamp) DO UPDATE SET
                price_native = EXCLUDED.price_native,
                price_usd = EXCLUDED.price_usd,
                txns_buys = EXCLUDED.txns_buys,
                txns_sells = EXCLUDED.txns_sells,
                volume = EXCLUDED.volume,
                liquidity_base = EXCLUDED.liquidity_base,
                liquidity_quote = EXCLUDED.liquidity_quote,
                liquidity_usd = EXCLUDED.liquidity_usd,
                fdv = EXCLUDED.fdv,
                market_cap = EXCLUDED.market_cap
                """, price_data)
                
                conn.commit()
                logger.info(f"✅ Price metrics inserted for token_id {price_data.get('token_id')}")
                return True
    except Exception as e:
        logger.error(f"❌ Failed to insert price metrics: {e}")
        return False
