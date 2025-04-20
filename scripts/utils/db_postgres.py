import time
import os
import logging
import psycopg2
import random
import psycopg2.extras
from psycopg2 import pool
from datetime import datetime, timezone
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Define the global connection pool variable
connection_pool = None
pool_last_reset = None

def init_connection_pool(min_conn=5, max_conn=20):
    """Initialize a PostgreSQL connection pool with proper configuration"""
    global connection_pool, pool_last_reset
    
    # If pool already exists, just return
    if connection_pool is not None:
        return True
        
    try:
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", "5432")
        dbname = os.getenv("PG_DATABASE", "crypto_db")
        user = os.getenv("PG_USER", "bot")
        password = os.getenv("PG_PASSWORD", "bot1234")
        
        # Create a new connection pool with increased connection limits
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password,
            connect_timeout=5,
            # Add options to improve connection stability
            options="-c statement_timeout=30000"
        )
        
        # Record the time of pool creation
        pool_last_reset = time.time()
        
        logger.info(f"✅ Initialized PostgreSQL connection pool at {host}:{port} with {min_conn}-{max_conn} connections")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize connection pool: {e}")
        return False

def reset_connection_pool():
    """Reset the connection pool when it becomes exhausted or problematic"""
    global connection_pool, pool_last_reset
    
    # If pool was recently reset, don't do it again
    if pool_last_reset and time.time() - pool_last_reset < 10:
        logger.warning("⚠️ Not resetting pool - was reset too recently")
        return False
        
    try:
        # Close existing pool if it exists
        if connection_pool:
            logger.warning("🔄 Closing existing connection pool")
            connection_pool.closeall()
            
        # Set to None so init_connection_pool will create a new one
        connection_pool = None
        
        # Create a new pool with increased capacity
        success = init_connection_pool(min_conn=5, max_conn=20)
        if success:
            logger.info("✅ Connection pool has been reset")
        return success
    except Exception as e:
        logger.error(f"❌ Error resetting connection pool: {e}")
        return False

@contextmanager
def get_db_connection():
    """
    Context manager for database connections.
    Ensures connections are properly returned to pool.
    
    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Do database operations...
    """
    conn = None
    try:
        # Try to get connection from pool
        conn = get_connection()
        if conn is None:
            raise Exception("Failed to get database connection")
            
        # Provide connection to the caller
        yield conn
        
        # Commit if no errors occurred
        conn.commit()
        
    except Exception as e:
        # Rollback on error
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise
    finally:
        # Always return connection to pool
        if conn:
            release_connection(conn)

def get_connection():
    """Get a connection from the pool with retries and recovery"""
    global connection_pool
    
    # Initialize pool if needed
    if connection_pool is None:
        init_connection_pool()
        
    # Try to get a connection
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # Get connection from pool
            conn = connection_pool.getconn()
            
            # Verify connection works
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
            return conn
            
        except psycopg2.pool.PoolError as e:
            if "exhausted" in str(e).lower():
                logger.error(f"❌ Error borrowing connection from pool: {e}")
                
                # Reset pool on first exhaustion
                if attempt == 0:
                    reset_connection_pool()
                    
                # On subsequent attempts, wait briefly before retry
                time.sleep(0.5 * (attempt + 1))
            else:
                logger.error(f"❌ Pool error: {e}")
                # Try resetting pool for any pool error
                reset_connection_pool()
                time.sleep(0.5 * (attempt + 1))
                
        except Exception as e:
            logger.error(f"❌ Error getting connection: {e}")
            if "connection is closed" in str(e).lower():
                reset_connection_pool()
            
            time.sleep(0.5 * (attempt + 1))
                
    # If all attempts failed, try direct connection as fallback
    logger.warning("⚠️ All pool connection attempts failed, trying direct connection")
    return connect_postgres()

def release_connection(conn):
    """Safely return a connection to the pool"""
    global connection_pool
    
    if conn is None:
        return
        
    if connection_pool is None:
        try:
            conn.close()
        except:
            pass
        return
        
    try:
        # Make sure we can still put this connection back
        if not conn.closed:
            connection_pool.putconn(conn)
        else:
            # If connection is closed, just log it
            logger.warning("⚠️ Attempted to return closed connection to pool")
    except Exception as e:
        logger.error(f"❌ Error returning connection to pool: {e}")
        try:
            conn.close()
        except:
            pass

def connect_postgres():
    """Fallback direct connection (avoid using this directly)."""
    logger.warning("⚠️ Using direct connection instead of pool. This should be rare.")
    try:
        host = os.getenv("PG_HOST", "postgres")
        port = os.getenv("PG_PORT", "5432")
        dbname = os.getenv("PG_DATABASE", "crypto_db")
        user = os.getenv("PG_USER", "bot")
        password = os.getenv("PG_PASSWORD", "bot1234")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password,
            connect_timeout=10
        )
        logger.info(f"✅ Direct connection to PostgreSQL at {host}:{port}")
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        return None

def execute_query(query, params=None, fetch=False, fetch_one=False, use_pool=True):
    """
    Execute a SQL query with improved connection handling.
    
    Args:
        query (str): SQL query to execute
        params (tuple): Parameters for the query
        fetch (bool): If True, fetch all results
        fetch_one (bool): If True, fetch only first result
        use_pool (bool): Whether to use connection pooling
    
    Returns:
        Query results or success indicator
    """
    # Use context manager to ensure proper connection handling
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            if fetch_one:
                result = cursor.fetchone()
            elif fetch:
                result = cursor.fetchall()
            else:
                result = True
                
            cursor.close()
            return result
            
    except Exception as e:
        logger.error(f"❌ Error executing query: {e}\nQuery: {query}")
        return None

# Group-related functions
def insert_group(telegram_id, name):
    """Insert or retrieve a Telegram group by its ID."""
    query = """
        INSERT INTO telegram_groups (telegram_id, name)
        VALUES (%s, %s)
        ON CONFLICT (telegram_id) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING group_id
    """
    result = execute_query(query, (telegram_id, name), fetch=True)
    return result[0][0] if result and result[0] else None

def get_group_by_id(telegram_id):
    """Get a group by its Telegram ID."""
    query = "SELECT group_id, name FROM telegram_groups WHERE telegram_id = %s"
    result = execute_query(query, (telegram_id,), fetch=True)
    return result[0] if result else None

# Message-related functions
def insert_message(group_id, timestamp, text, sender_id, telegram_message_id=None, reply_to=None, token_id=None, is_call=False):
    """
    Insert a new message into the database with duplicate protection.
    Returns the message_id if inserted successfully, None if duplicate.
    """
    query = """
        INSERT INTO telegram_messages (
            group_id, message_timestamp, raw_text, sender_id, telegram_message_id,
            reply_to_message_id, token_id, is_call
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (group_id, telegram_message_id) 
        DO NOTHING
        RETURNING message_id
    """
    result = execute_query(query, (group_id, timestamp, text, sender_id, telegram_message_id, 
                                 reply_to, token_id, is_call), fetch=True)
    return result[0][0] if result and result[0] else None

def update_message(message_id, token_id=None, is_call=None):
    """Update an existing message."""
    updates = []
    params = []
    
    if token_id is not None:
        updates.append("token_id = %s")
        params.append(token_id)
    if is_call is not None:
        updates.append("is_call = %s")
        params.append(is_call)
        
    if not updates:
        return False
        
    params.append(message_id)
    query = f"UPDATE telegram_messages SET {', '.join(updates)} WHERE message_id = %s"
    return execute_query(query, tuple(params))

# Token-related functions
def insert_token(contract_address, blockchain="ethereum", name="Unknown", ticker="UNKNOWN", supply=0, call_price=0):
    """
    Insert or retrieve a token by contract_address and blockchain.
    Returns token_id.
    """
    query = """
        INSERT INTO tokens (
            name, ticker, blockchain, contract_address, supply, call_price
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (contract_address) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING token_id
    """
    result = execute_query(query, (name, ticker, blockchain, contract_address, supply, call_price), fetch=True)
    return result[0][0] if result and result[0] else None

def update_token_info(token_id, name=None, ticker=None, liquidity=None, price=None, 
                      dex=None, supply=None, age=None, group_name=None, 
                      dexscreener_url=None, additional_links=None):
    """Update token information in the database"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        updates = []
        params = []
        
        if name: updates.append("name = %s"); params.append(name)
        if ticker: updates.append("ticker = %s"); params.append(ticker)
        if liquidity is not None: updates.append("first_call_liquidity = %s"); params.append(liquidity)
        if price is not None: updates.append("call_price = %s"); params.append(price)
        if dex: updates.append("dex = %s"); params.append(dex)
        if supply is not None: updates.append("supply = %s"); params.append(supply)
        if age is not None: updates.append("token_age = %s"); params.append(age)
        if group_name: updates.append("group_call = COALESCE(group_call, %s)"); params.append(group_name)
        if dexscreener_url: updates.append("dexscreener_url = %s"); params.append(dexscreener_url)
        
        if not updates:
            return False
        
        params.append(token_id)
        query = f"UPDATE tokens SET {', '.join(updates)} WHERE token_id = %s"
        cursor.execute(query, tuple(params))
        
        conn.commit()
        cursor.close()
        release_connection(conn)
        return True
    except Exception as e:
        logger.error(f"❌ Error updating token info: {e}")
        return False
    
    
def update_token_best_pair(token_id, pair_address, connection=None):
    """
    Update the best_pair_address for a token if not already set.
    
    Args:
        token_id: ID of the token to update
        pair_address: The pair address to set
        connection: Optional database connection to use
    
    Returns:
        Boolean indicating success
    """
    # First check if token already has a best pair set
    check_query = "SELECT best_pair_address FROM tokens WHERE token_id = %s"
    result = execute_query(check_query, (token_id,), fetch=True, use_pool=False)
    
    if result and result[0][0]:
        # Best pair is already set
        return True
    
    # Update the best_pair_address
    update_query = """
        UPDATE tokens
        SET best_pair_address = %s
        WHERE token_id = %s
        AND (best_pair_address IS NULL OR best_pair_address != %s)
    """
    success = execute_query(update_query, (pair_address, token_id, pair_address), use_pool=False)
    
    if success:
        logger.info(f"✅ Updated best pair address for token {token_id} to {pair_address}")
    
    return success

def get_token_by_address(contract_address):
    """Get token by contract_address."""
    query = """
        SELECT token_id, name, ticker, blockchain, best_pair_address, dexscreener_url
        FROM tokens
        WHERE contract_address = %s
    """
    result = execute_query(query, (contract_address,), fetch=True)
    return result[0] if result else None

def get_all_tracked_tokens():
    """
    Get all tokens that need price tracking from PostgreSQL.
    Returns token_id, chain, address, and best_pair_address if available.
    """
    query = """
        SELECT 
            token_id, blockchain as chain, contract_address as address, best_pair_address
        FROM tokens 
        WHERE contract_address IS NOT NULL
    """
    result = execute_query(query, fetch=True)
    
    if result:
        logger.info(f"✅ Found {len(result)} tokens for price tracking")
        if len(result) > 2:
            logger.info(f"🪙 Sample tokens: {result[0]}, {result[1]}")
    else:
        logger.warning("⚠️ No tokens found for price tracking")
        
    return result

# Call-related functions
def insert_call(token_id, message_id, timestamp, price, note=None):
    """Insert a token call record."""
    query = """
        INSERT INTO token_calls (token_id, message_id, call_timestamp, call_price, note)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING call_id
    """
    result = execute_query(query, (token_id, message_id, timestamp, price, note), fetch=True)
    return result[0][0] if result and result[0] else None

def insert_price_metrics(token_id, pair_address, price_native=None, timestamp=None, 
                         volume_24h=None, liquidity_usd=None, price_change_24h=None,
                         txns_buys=None, txns_sells=None, price_usd=None, 
                         fdv=None, market_cap=None, mongo_id=None):
    """Insert price metrics into price_metrics table"""
    try:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
            
        query = """
            INSERT INTO price_metrics (
                token_id, pair_address, timestamp, price_native, price_usd,
                txns_buys, txns_sells, volume, liquidity_usd, mongo_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (token_id, timestamp) DO UPDATE
            SET 
                price_native = EXCLUDED.price_native,
                price_usd = EXCLUDED.price_usd,
                volume = EXCLUDED.volume,
                liquidity_usd = EXCLUDED.liquidity_usd,
                mongo_id = EXCLUDED.mongo_id
        """
        success = execute_query(query, (
            token_id, pair_address, timestamp, price_native, price_usd,
            txns_buys, txns_sells, volume_24h, liquidity_usd, mongo_id
        ))
        
        if success:
            logger.info(f"✅ Inserted price metrics for token {token_id}")
        
        return success
    except Exception as e:
        logger.error(f"Error inserting price metrics: {e}")
        return False

def insert_price_metrics_from_pair_data(token_id, pair, mongo_id=None):
    """
    Insert price metrics from a DexScreener pair data structure.
    Uses batch processing for better performance.
    """
    try:
        pair_address = pair.get('pairAddress', '')
        price_native = float(pair.get('priceNative', 0) or 0)
        price_usd = float(pair.get('priceUsd', 0) or 0)
        txns_buys = pair.get('txns', {}).get('h24', {}).get('buys', 0)
        txns_sells = pair.get('txns', {}).get('h24', {}).get('sells', 0)
        volume = pair.get('volume', {}).get('h24', 0)
        liquidity_base = pair.get('liquidity', {}).get('base', 0)
        liquidity_quote = pair.get('liquidity', {}).get('quote', 0)
        liquidity_usd = pair.get('liquidity', {}).get('usd', 0)
        fdv = pair.get('fdv', 0)
        market_cap = pair.get('marketCap', 0)
        
        data = [{
            'token_id': token_id,
            'pair_address': pair_address,
            'price_native': price_native,
            'price_usd': price_usd,
            'txns_buys': txns_buys,
            'txns_sells': txns_sells,
            'volume': volume,
            'liquidity_base': liquidity_base,
            'liquidity_quote': liquidity_quote,
            'liquidity_usd': liquidity_usd,
            'fdv': fdv,
            'market_cap': market_cap,
            'mongo_id': mongo_id
        }]
        
        conn = get_connection()
        if not conn:
            logger.error("❌ Failed to get database connection")
            return False
            
        try:
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO price_metrics (
                        token_id, pair_address, timestamp, price_native, price_usd,
                        txns_buys, txns_sells, volume, liquidity_base, liquidity_quote,
                        liquidity_usd, fdv, market_cap, mongo_id
                    )
                    VALUES (
                        %(token_id)s, %(pair_address)s, NOW(), %(price_native)s, %(price_usd)s,
                        %(txns_buys)s, %(txns_sells)s, %(volume)s, %(liquidity_base)s, 
                        %(liquidity_quote)s, %(liquidity_usd)s, %(fdv)s, %(market_cap)s, 
                        %(mongo_id)s
                    )
                """
                psycopg2.extras.execute_batch(cursor, query, data)
                
                # Removed incorrect update to tokens table that was causing errors
                # Store price data only in price_metrics table as designed
                
            conn.commit()
            
            # Update best pair address separately
            if pair_address:
                update_token_best_pair(token_id, pair_address)
                
            return True
        finally:
            release_connection(conn)
    except Exception as e:
        logger.error(f"❌ Error in insert_price_metrics_from_pair_data: {e}")
        return False

def get_latest_price_for_token(token_id):
    """Get the most recent price data for a token."""
    query = """
        SELECT price_usd, timestamp, liquidity_usd, volume, pair_address
        FROM price_metrics
        WHERE token_id = %s
        ORDER BY timestamp DESC
        LIMIT 1
    """
    result = execute_query(query, (token_id,), fetch=True)
    return result[0] if result else None

# Schema management functions
def ensure_best_pair_column():
    """Ensure best_pair_address column exists in tokens table."""
    try:
        query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'tokens' AND column_name = 'best_pair_address'
        """
        result = execute_query(query, fetch=True)
        
        if not result:
            # Column doesn't exist, add it
            alter_query = """
            ALTER TABLE tokens 
            ADD COLUMN best_pair_address VARCHAR(66)
            """
            execute_query(alter_query)
        return True
    except Exception as e:
        # Log error
        return False

def ensure_price_metrics_table():
    """Ensure the price_metrics table exists with all required columns."""
    check_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name = 'price_metrics'
    """
    result = execute_query(check_query, fetch=True)
    
    if not result:
        logger.info("Creating price_metrics table...")
        # This should normally be in setup_database.py, included here for reference
        create_query = """
            CREATE TABLE price_metrics (
                id SERIAL PRIMARY KEY,
                token_id BIGINT REFERENCES tokens(token_id),
                pair_address TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                price_native NUMERIC(30, 18),
                price_usd NUMERIC(30, 18) NOT NULL,
                txns_buys INTEGER,
                txns_sells INTEGER,
                volume NUMERIC(30, 2),
                liquidity_base NUMERIC(30, 18),
                liquidity_quote NUMERIC(30, 18),
                liquidity_usd NUMERIC(30, 2),
                fdv NUMERIC(30, 2),
                market_cap NUMERIC(30, 2),
                mongo_id TEXT
            )
        """
        execute_query(create_query)
        
        # Create TimescaleDB hypertable
        try:
            execute_query("SELECT create_hypertable('price_metrics', 'timestamp')")
            logger.info("✅ Created price_metrics hypertable")
        except Exception as e:
            logger.error(f"❌ Error creating hypertable: {e}")

# Best pair selection (from telegram_monitor.py and tasks.py)
def select_best_pair(pairs, stored_pair_address=None):
    """Select best trading pair based on liquidity or existing preference."""
    if not pairs:
        return None
        
    # If we only have one pair, use it
    if (len(pairs) == 1):
        return pairs[0]
        
    # If we have a stored pair, prioritize it
    if stored_pair_address:
        for pair in pairs:
            if pair.get('pairAddress', '').lower() == stored_pair_address.lower():
                return pair
                
    # Otherwise, find the pair with the best liquidity
    best_pair = None
    best_liquidity = 0
    
    for pair in pairs:
        try:
            liquidity_str = pair.get('liquidity', {}).get('usd', '0')
            if isinstance(liquidity_str, str):
                # Remove any $ or commas
                liquidity_str = liquidity_str.replace('$', '').replace(',', '')
            liquidity = float(liquidity_str)
            
            if liquidity > best_liquidity:
                best_liquidity = liquidity
                best_pair = pair
        except (ValueError, TypeError):
            continue
    
    return best_pair

from scripts.utils.db_postgres import execute_query
from scripts.price_tracker.tasks_logging import logger

def classify_all_tokens():
    """
    Classify all tokens based on liquidity and market cap, setting update intervals and activity status.
    Uses the latest price_metrics data or first_call_liquidity for new tokens.
    
    Tiers:
    - High (30s): Liquidity > $10k OR Market Cap > $50k
    - Medium (300s): Liquidity > $1k OR Market Cap > $5k
    - Low (3600s): Liquidity > $0 OR Market Cap > $0
    - Dead (86400s, inactive): Liquidity = 0 AND Market Cap = 0
    """
    try:
        # Step 1: Classify tokens with price_metrics
        query = """
        WITH latest_metrics AS (
            SELECT DISTINCT ON (token_id) 
                token_id, 
                liquidity_usd, 
                market_cap
            FROM price_metrics
            ORDER BY token_id, timestamp DESC
        )
        UPDATE tokens t
        SET 
            update_interval = CASE
                WHEN m.liquidity_usd > 10000 OR m.market_cap > 50000 THEN 30
                WHEN m.liquidity_usd > 1000 OR m.market_cap > 5000 THEN 300
                WHEN m.liquidity_usd > 0 OR m.market_cap > 0 THEN 3600
                ELSE 86400
            END,
            is_active = CASE
                WHEN m.liquidity_usd = 0 AND m.market_cap = 0 THEN FALSE
                ELSE TRUE
            END
        FROM latest_metrics m
        WHERE t.token_id = m.token_id
        """
        execute_query(query)

        # Step 2: Handle tokens without price_metrics using first_call_liquidity
        fallback_query = """
        UPDATE tokens t
        SET 
            update_interval = CASE
                WHEN first_call_liquidity > 10000 THEN 30
                WHEN first_call_liquidity > 1000 THEN 300
                WHEN first_call_liquidity > 0 THEN 3600
                ELSE 86400
            END,
            is_active = CASE
                WHEN first_call_liquidity > 0 THEN TRUE
                ELSE FALSE
            END
        WHERE NOT EXISTS (
            SELECT 1 FROM price_metrics p WHERE p.token_id = t.token_id
        )
        """
        execute_query(fallback_query)

        logger.info("Token classification completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Error in classify_all_tokens: {e}")
        return False

def update_token_failure_count(token_id, increment=True, reset=False, deactivate=False):
    """
    Update the failed_updates_count for a token with proper locking to prevent deadlocks.
    Also attempts to find a better pair before deactivation when failure count reaches threshold.
    
    Args:
        token_id (int): The ID of the token to update
        increment (bool): Whether to increment the counter (True) or not
        reset (bool): If True, reset the counter to 0
        deactivate (bool): If True, mark the token as inactive
        
    Returns:
        tuple: (success, current_failure_count, is_active)
    """
    try:
        conn = get_connection()
        if not conn:
            logger.error(f"❌ Failed to get connection for updating token failure count")
            return False, None, None
            
        with conn.cursor() as cursor:
            # Get token details with FOR UPDATE SKIP LOCKED to prevent deadlocks
            cursor.execute(
                """
                SELECT 
                    failed_updates_count, is_active, name, ticker, blockchain, contract_address, best_pair_address
                FROM tokens 
                WHERE token_id = %s 
                FOR UPDATE SKIP LOCKED
                """, 
                (token_id,)
            )
            
            result = cursor.fetchone()
            if not result:
                logger.warning(f"⚠️ Token {token_id} not found or locked by another process")
                release_connection(conn)
                return False, None, None
                
            current_count, is_active, name, ticker, blockchain, contract_address, best_pair_address = result
            
            # Determine the new count based on parameters
            if reset:
                new_count = 0
            elif increment:
                new_count = current_count + 1
            else:
                new_count = current_count
                
            # Default status is to maintain current active status
            new_active_status = is_active
            
            # Check if we've reached failure threshold (5) and need to try recovery or deactivation
            if new_count >= 5 and is_active and not deactivate:
                logger.warning(f"Token {name} ({ticker}) [ID: {token_id}] has {new_count} consecutive failures - attempting recovery")
                
                # Import here to avoid circular imports
                from scripts.utils.api_clients import get_pairs_data
                
                # Try to get new pairs data
                pairs = get_pairs_data(blockchain, [contract_address])
                
                if pairs and len(pairs) > 0:
                    # Find the best pair based on liquidity
                    best_pair = select_best_pair(pairs)
                    
                    if best_pair:
                        # Check if the pair has sufficient liquidity
                        liquidity_usd = 0
                        try:
                            liquidity_str = str(best_pair.get('liquidity', {}).get('usd', '0')).replace('$', '').replace(',', '')
                            liquidity_usd = float(liquidity_str)
                        except (ValueError, TypeError):
                            pass
                        
                        if liquidity_usd > 0:
                            # Update the best pair address
                            pair_address = best_pair.get('pairAddress')
                            if pair_address:
                                cursor.execute(
                                    """
                                    UPDATE tokens 
                                    SET best_pair_address = %s,
                                        failed_updates_count = 0
                                    WHERE token_id = %s
                                    """,
                                    (pair_address, token_id)
                                )
                                logger.info(f"✅ Recovery successful: Found new pair {pair_address} for {name} ({ticker}) [ID: {token_id}] with ${liquidity_usd:.2f} liquidity")
                                new_count = 0
                                conn.commit()
                                return True, 0, True
                
                # If we couldn't find a new pair or all pairs have no liquidity, deactivate the token
                if new_count >= 5:
                    logger.warning(f"❌ No valid pairs found for {name} ({ticker}) [ID: {token_id}] after {new_count} failures. Deactivating.")
                    new_active_status = False
                    deactivate = True
            
            # Handle explicit deactivation request
            if deactivate:
                new_active_status = False
            
            # Only update if something has changed
            if new_count != current_count or new_active_status != is_active:
                cursor.execute(
                    """
                    UPDATE tokens
                    SET 
                        failed_updates_count = %s,
                        is_active = %s,
                        last_updated_at = NOW()
                    WHERE token_id = %s
                    """,
                    (new_count, new_active_status, token_id)
                )
                
            conn.commit()
            logger.info(f"✅ Token {token_id} failure count {'reset' if reset else 'incremented'} to {new_count}, active: {new_active_status}")
            return True, new_count, new_active_status
            
    except Exception as e:
        logger.error(f"❌ Error updating token failure count: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False, None, None
    finally:
        if conn:
            release_connection(conn)

def _deactivate_token(token_id, reason="Excessive failures"):
    """Helper function to deactivate a token and log the reason"""
    try:
        query = """
        UPDATE tokens 
        SET is_active = FALSE,
            update_interval = 86400 -- Set to daily checks for potential revival
        WHERE token_id = %s
        RETURNING name, ticker, blockchain, contract_address
        """
        result = execute_query(query, (token_id,), fetch=True)
        
        if result and result[0]:
            name, ticker, blockchain, contract_address = result[0]
            logger.warning(f"🔴 Token DEACTIVATED: {name} ({ticker}) [ID: {token_id}] on {blockchain} - Reason: {reason}")
            logger.warning(f"🔴 Deactivated token address: {contract_address}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error deactivating token {token_id}: {e}")
        return False

def process_pair_data(token_id, token_address, blockchain, group_name, pair_data, message_id=None, timestamp=None):
    """
    Process pair data from DexScreener and update token information in the database.
    
    Args:
        token_id (int): The database ID of the token
        token_address (str): The contract address of the token
        blockchain (str): The blockchain name (ethereum, bsc, etc.)
        group_name (str): Name of Telegram group where token was found
        pair_data (dict): Pair data from DexScreener API
        message_id (int, optional): Message ID associated with this data
        timestamp (datetime, optional): Timestamp for the call data
        
    Returns:
        bool: True if operation was successful, False otherwise
    """
    try:
        logger.info(f"📊 Processing pair data for token ID {token_id}")
        
        # Extract basic info from pair
        liquidity_usd = 0
        price_usd = 0
        price_native = 0
        
        if pair_data.get('liquidity', {}).get('usd'):
            try:
                liquidity_str = str(pair_data['liquidity']['usd']).replace('$', '').replace(',', '')
                liquidity_usd = float(liquidity_str)
            except (ValueError, TypeError):
                pass
        
        if pair_data.get('priceUsd'):
            try:
                price_usd = float(pair_data['priceUsd'])
            except (ValueError, TypeError):
                pass
                
        if pair_data.get('priceNative'):
            try:
                price_native = float(pair_data['priceNative'])
            except (ValueError, TypeError):
                pass
        
        # Get token info
        token_name = pair_data.get('baseToken', {}).get('name', 'Unknown')
        token_symbol = pair_data.get('baseToken', {}).get('symbol', 'UNKNOWN')
        dex = pair_data.get('dexId', '')
        
        # Calculate supply if available
        supply = 0
        fdv = pair_data.get('fdv', 0)
        if fdv and price_usd > 0:
            try:
                supply = float(fdv) / price_usd
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # Build DexScreener URL
        dexscreener_url = f"https://dexscreener.com/{blockchain}/{pair_data.get('pairAddress', '')}"
        
        # Get pair age if available
        pair_age = None
        if pair_data.get('pairCreatedAt'):
            try:
                import time
                from datetime import datetime
                pair_created_timestamp = int(pair_data['pairCreatedAt']) / 1000  # Convert from milliseconds
                current_timestamp = time.time()
                pair_age = int((current_timestamp - pair_created_timestamp) / 3600)  # Age in hours
            except (ValueError, TypeError):
                pass
        
        # Insert price metrics from pair data
        insert_price_metrics_from_pair_data(token_id, pair_data)
        
        # Update token information with data from pair
        update_token_info(
            token_id=token_id,
            name=token_name,
            ticker=token_symbol,
            liquidity=liquidity_usd,
            price=price_usd,
            dex=dex,
            supply=supply,
            age=pair_age,
            group_name=group_name,
            dexscreener_url=dexscreener_url
        )
        
        # Update best pair address if needed
        update_token_best_pair(token_id, pair_data.get('pairAddress', ''))
        
        # Insert call record if message_id is provided
        if message_id and timestamp:
            insert_call(token_id, message_id, timestamp, price_usd)
            logger.info(f"📞 Call record inserted for token {token_id}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Error processing pair data: {e}")
        return False

def get_token_update_stats():
    """
    Get statistics about token update intervals and activity status.
    This helps verify that the classification function is working properly.
    
    Returns:
        dict: Statistics about token update intervals
    """
    try:
        query = """
        SELECT 
            update_interval, 
            COUNT(*) as token_count,
            COUNT(*) FILTER (WHERE is_active = TRUE) as active_count,
            COUNT(*) FILTER (WHERE is_active = FALSE) as inactive_count,
            MAX(last_updated_at) as latest_update,
            MIN(last_updated_at) as oldest_update
        FROM tokens 
        GROUP BY update_interval 
        ORDER BY update_interval ASC
        """
        
        result = execute_query(query, fetch=True)
        
        if not result:
            return {"error": "Failed to get token update statistics"}
            
        stats = {
            "intervals": [],
            "total_tokens": 0,
            "active_tokens": 0,
            "inactive_tokens": 0
        }
        
        for row in result:
            interval, count, active, inactive, latest, oldest = row
            stats["intervals"].append({
                "update_interval": interval,
                "token_count": count,
                "active_count": active,
                "inactive_count": inactive,
                "latest_update": str(latest) if latest else None,
                "oldest_update": str(oldest) if oldest else None,
            })
            stats["total_tokens"] += count
            stats["active_tokens"] += active
            stats["inactive_tokens"] += inactive
            
        # Get classification history
        query_history = """
        SELECT 
            DATE_TRUNC('hour', last_updated_at) as hour,
            COUNT(*) as updates
        FROM tokens
        WHERE last_updated_at IS NOT NULL
        GROUP BY DATE_TRUNC('hour', last_updated_at)
        ORDER BY hour DESC
        LIMIT 24
        """
        
        history_result = execute_query(query_history, fetch=True)
        if history_result:
            stats["update_history"] = [
                {"hour": str(row[0]), "updates": row[1]} for row in history_result
            ]
            
        return stats
    
    except Exception as e:
        logger.error(f"Error getting token update stats: {e}")
        return {"error": str(e)}



def get_failing_tokens(min_failures=3, max_failures=None):
    """
    Get tokens that are failing to update but still marked as active.
    
    Args:
        min_failures (int): Minimum number of consecutive failures
        max_failures (int, optional): Maximum number of failures to consider
        
    Returns:
        list: List of failing token records
    """
    where_clause = "failed_updates_count >= %s AND is_active = TRUE"
    params = [min_failures]
    
    if max_failures is not None:
        where_clause += " AND failed_updates_count <= %s"
        params.append(max_failures)
    
    query = f"""
        SELECT 
            token_id, name, ticker, blockchain, contract_address, 
            best_pair_address, failed_updates_count, last_updated_at
        FROM tokens
        WHERE {where_clause}
        ORDER BY failed_updates_count DESC
    """
    
    result = execute_query(query, tuple(params), fetch=True)
    return result if result else []

def reset_token_failures(token_id):
    """
    Reset the failure count for a token and ensure it's marked as active.
    
    Args:
        token_id (int): The token ID to reset
        
    Returns:
        bool: Success status
    """
    return update_token_failure_count(token_id, increment=False, reset=True, deactivate=False)[0]

def deactivate_token(token_id):
    """
    Mark a token as inactive when it has too many failures.
    
    Args:
        token_id (int): The token ID to deactivate
        
    Returns:
        bool: Success status
    """
    return update_token_failure_count(token_id, increment=False, reset=False, deactivate=True)[0]
