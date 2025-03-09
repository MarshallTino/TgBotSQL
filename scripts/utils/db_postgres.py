import os
import logging
import psycopg2
from datetime import datetime, timezone

from scripts.utils.api_clients import parse_float

logger = logging.getLogger(__name__)

def connect_postgres():
    """Connect to PostgreSQL using environment variables."""
    try:
        # Use variables that match .env
        host = os.getenv("PG_HOST", "postgres")
        port = os.getenv("PG_PORT", "5432")
        dbname = os.getenv("PG_DATABASE", "crypto_db")
        user = os.getenv("PG_USER", "bot")
        password = os.getenv("PG_PASSWORD", "bot1234")
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        
        logger.info(f"✅ Successfully connected to PostgreSQL at {host}:{port}")
        return conn
    except Exception as e:
        logger.error(f"❌ Error connecting to PostgreSQL: {e}")
        return None

def insert_group(telegram_id, name):
    """Inserta un grupo en la base de datos o retorna su ID si ya existe"""
    conn = connect_postgres()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO telegram_groups (telegram_id, name)
            VALUES (%s, %s)
            ON CONFLICT (telegram_id) DO UPDATE
            SET name = EXCLUDED.name
            RETURNING group_id
            """,
            (telegram_id, name)
        )
        group_id = cursor.fetchone()[0]
        conn.commit()
        return group_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def insert_message(group_id, timestamp, text, sender_id, reply_to=None, token_id=None, is_call=False):
    """Inserta un mensaje en la base de datos"""
    conn = connect_postgres()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO telegram_messages (
                group_id, message_timestamp, raw_text, sender_id, 
                reply_to_message_id, token_id, is_call
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING message_id
            """,
            (group_id, timestamp, text, sender_id, reply_to, token_id, is_call)
        )
        message_id = cursor.fetchone()[0]
        conn.commit()
        return message_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def insert_token(contract_address, blockchain="ethereum", name=None, ticker=None):
    """Inserta un token en la base de datos o retorna su ID si ya existe"""
    conn = connect_postgres()
    cursor = conn.cursor()
    try:
        # Primero buscar si el token ya existe
        cursor.execute(
            """
            SELECT token_id FROM tokens 
            WHERE contract_address = %s AND blockchain = %s
            """,
            (contract_address, blockchain)
        )
        existing = cursor.fetchone()
        
        if existing:
            return existing[0]
        
        # Si no existe, insertar nuevo token
        cursor.execute(
            """
            INSERT INTO tokens (
                name, ticker, blockchain, contract_address, 
                supply, call_price
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING token_id
            """,
            (
                name or "Unknown", 
                ticker or "UNKNOWN",
                blockchain,
                contract_address,
                0,  # Default supply
                0   # Default call_price
            )
        )
        token_id = cursor.fetchone()[0]
        conn.commit()
        return token_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def insert_call(token_id, message_id, timestamp, call_price=None):
    """Guarda un 'call' de un token en un mensaje"""
    conn = connect_postgres()
    cursor = conn.cursor()
    try:
        # Si no se proporciona precio, obtenerlo del token
        if call_price is None:
            cursor.execute(
                "SELECT call_price FROM tokens WHERE token_id = %s", 
                (token_id,)
            )
            token_data = cursor.fetchone()
            if token_data:
                call_price = token_data[0]
            else:
                call_price = 0
        
        # Verificar si ya existe un call para este token y mensaje
        cursor.execute(
            """
            SELECT call_id FROM token_calls 
            WHERE token_id = %s AND message_id = %s
            """, 
            (token_id, message_id)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Actualizar call existente
            call_id = existing[0]
            cursor.execute(
                """
                UPDATE token_calls SET 
                    call_timestamp = %s,
                    call_price = %s
                WHERE call_id = %s
                """, 
                (timestamp, call_price, call_id)
            )
        else:
            # Insertar nuevo call
            cursor.execute(
                """
                INSERT INTO token_calls (
                    token_id, call_timestamp, call_price, message_id
                ) VALUES (
                    %s, %s, %s, %s
                )
                RETURNING call_id
                """, 
                (token_id, timestamp, call_price, message_id)
            )
            call_id = cursor.fetchone()[0]
        
        # Actualizar el mensaje para marcar que contiene un call
        cursor.execute(
            """
            UPDATE telegram_messages 
            SET is_call = TRUE, token_id = %s
            WHERE message_id = %s
            """, 
            (token_id, message_id)
        )
        
        conn.commit()
        logger.info(f"✅ Call registrado con ID: {call_id} para token {token_id} y mensaje {message_id}")
        return call_id
    except Exception as e:
        logger.error(f"❌ Error en insert_call: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

def update_token_info(token_id, name=None, ticker=None, liquidity=None, price=None, 
                     dex=None, supply=None, age=None, group_name=None, dexscreener_url=None):
    """Actualiza la información de un token"""
    conn = connect_postgres()
    cursor = conn.cursor()
    try:
        # Construir la consulta dinámicamente basada en los parámetros proporcionados
        updates = []
        params = []
        
        if name:
            updates.append("name = %s")
            params.append(name)
        
        if ticker:
            updates.append("ticker = %s")
            params.append(ticker)
        
        if liquidity is not None:
            updates.append("first_call_liquidity = %s")
            params.append(liquidity)
        
        if price is not None:
            updates.append("call_price = %s")
            params.append(price)
        
        if dex:
            updates.append("dex = %s")
            params.append(dex)
        
        if supply is not None:
            updates.append("supply = %s")
            params.append(supply)
        
        if age is not None:
            updates.append("token_age = %s")
            params.append(age)
        
        if group_name:
            updates.append("group_call = COALESCE(group_call, %s) ")
            params.append(group_name)
            
        if dexscreener_url:
            updates.append("dexscreener_url = %s")
            params.append(dexscreener_url)
        
        if not updates:
            return False
        
        # Añadir token_id al final de los parámetros
        params.append(token_id)
        
        # Construir y ejecutar la consulta
        query = f"UPDATE tokens SET {', '.join(updates)} WHERE token_id = %s"
        cursor.execute(query, params)
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Error al actualizar token {token_id}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_all_tracked_tokens(connection=None):
    """Get all tokens from the tokens table for price tracking."""
    try:
        logger.info("🔍 Getting all tracked tokens from database")
        
        # Use connection if provided or create new one
        conn = connection
        if conn is None:
            logger.info("🔌 Creating new PostgreSQL connection")
            conn = connect_postgres()
            
        if not conn:
            logger.error("❌ Failed to connect to PostgreSQL")
            return []
            
        cursor = conn.cursor()
        
        # Debug: Check table structure
        try:
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='tokens'")
            columns = [col[0] for col in cursor.fetchall()]
            logger.info(f"📊 Tokens table columns: {columns}")
        except Exception as e:
            logger.error(f"❌ Error checking table structure: {e}")
        
        # Execute the query with more informative error handling
        try:
            query = """
                SELECT 
                    token_id, blockchain as chain, contract_address as address
                FROM tokens 
                WHERE contract_address IS NOT NULL
            """
            logger.info(f"🔍 Executing SQL: {query}")
            cursor.execute(query)
        except Exception as e:
            logger.error(f"❌ SQL query error: {e}")
            if connection is None and conn:
                conn.close()
            return []
        
        tokens = cursor.fetchall()
        cursor.close()
        
        if tokens:
            logger.info(f"✅ Found {len(tokens)} tokens for price tracking")
            # Log a sample of tokens
            for i, token in enumerate(tokens[:3]):
                logger.info(f"🪙 Sample token {i+1}: {token}")
        else:
            logger.warning("⚠️ No tokens found with contract addresses")
            
        return tokens
    except Exception as e:
        logger.error(f"❌ Database error in get_all_tracked_tokens: {e}")
        return []

def insert_price_metrics(connection, token_id, pair_data):
    """
    Insert price metrics into the price_metrics table.
    
    Args:
        connection: PostgreSQL connection
        token_id: ID from the tokens table
        pair_data: Dictionary with price data from DexScreener API
    """
    try:
        cursor = connection.cursor()
        
        # Extract values with fallbacks
        pair_address = pair_data.get('pairAddress')
        price_native = pair_data.get('priceNative')
        price_usd = pair_data.get('priceUsd')
        
        # Get transaction counts
        txns = pair_data.get('txns', {}).get('h24', {})
        buys = txns.get('buys', 0)
        sells = txns.get('sells', 0)
        volume = pair_data.get('volume', {}).get('h24')
        
        # Get liquidity data
        liquidity = pair_data.get('liquidity', {})
        liquidity_base = liquidity.get('base')
        liquidity_quote = liquidity.get('quote')
        liquidity_usd = liquidity.get('usd')
        
        # Get other metrics
        fdv = pair_data.get('fdv')
        market_cap = pair_data.get('marketCap')
        
        # MongoDB document ID reference
        mongo_id = str(pair_data.get('_id')) if hasattr(pair_data, 'get') and pair_data.get('_id') else None
        
        # Current timestamp
        timestamp = datetime.now(timezone.utc)
        
        cursor.execute("""
            INSERT INTO price_metrics 
            (token_id, pair_address, timestamp, price_native, price_usd, 
             txns_buys, txns_sells, volume, liquidity_base, liquidity_quote, 
             liquidity_usd, fdv, market_cap, mongo_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (token_id, timestamp) 
            DO UPDATE SET
                price_native = EXCLUDED.price_native,
                price_usd = EXCLUDED.price_usd,
                volume = EXCLUDED.volume,
                liquidity_base = EXCLUDED.liquidity_base,
                liquidity_quote = EXCLUDED.liquidity_quote,
                liquidity_usd = EXCLUDED.liquidity_usd,
                fdv = EXCLUDED.fdv,
                market_cap = EXCLUDED.market_cap
        """, (
            token_id, pair_address, timestamp, price_native, price_usd,
            buys, sells, volume, liquidity_base, liquidity_quote,
            liquidity_usd, fdv, market_cap, mongo_id
        ))
        
        connection.commit()
        
        # Also update the price in the tokens table
        if price_usd is not None:
            cursor.execute("""
                UPDATE tokens
                SET call_price = %s
                WHERE token_id = %s
            """, (price_usd, token_id))
            connection.commit()
            
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"❌ Error inserting price metrics: {e}")
        if connection:
            connection.rollback()
        return False

def insert_price_metrics(token_id, timestamp, pair, mongo_id=None):
    """
    Insert price metrics into the database from a DexScreener pair object
    """
    try:
        conn = connect_postgres()
        cursor = conn.cursor()
        
        # Extract metrics from pair
        price_native = parse_float(pair.get("priceNative"))
        price_usd = parse_float(pair.get("priceUsd"))
        liquidity_base = parse_float(pair.get("liquidity", {}).get("base"))
        liquidity_quote = parse_float(pair.get("liquidity", {}).get("quote"))
        fdv = parse_float(pair.get("fdv"))
        market_cap = parse_float(pair.get("marketCap"))
        pair_address = pair.get("pairAddress")
        
        # Get transaction counts for last minute
        txns = pair.get("txns", {}).get("m1", {})
        buys = int(txns.get("buys") or 0)
        sells = int(txns.get("sells") or 0)
        volume = parse_float(txns.get("volume"))
        
        # Insert into database
        cursor.execute("""
            INSERT INTO price_metrics 
            (token_id, pair_address, timestamp, price_native, price_usd, 
             txns_m1_buys, txns_m1_sells, volume_m1, 
             liquidity_base, liquidity_quote, fdv, market_cap)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            token_id, pair_address, timestamp, price_native, price_usd,
            buys, sells, volume,
            liquidity_base, liquidity_quote, fdv, market_cap
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error inserting price metrics: {e}")
        return False
