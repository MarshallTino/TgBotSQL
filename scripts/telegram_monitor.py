import asyncio
import random
import re
import sys
from pathlib import Path
import time
# Add the project root to Python's path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from telethon import TelegramClient, events
import os
import logging
import requests
from datetime import datetime

# Import from new config files
from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, SESSION_PATH, TELEGRAM_PHONE
from config.groups import TELEGRAM_GROUPS
from config.regex_patterns import CALL_PATTERN, RE_CA_BSC_ETH, RE_CA_SOL, DEX_LINK_REGEX, TINYASTRO_REGEX
from config.logging import configure_logging

# Import existing utils
from utils.api_clients import get_pairs_data, get_pair_by_address, parse_float
from utils.db_postgres import connect_postgres, insert_group, insert_message, insert_token, insert_call, update_token_info

# Setup logging
logger = configure_logging()

# Create client using imported settings
logger.info(f"🔧 Configurando TelegramClient con API_ID={TELEGRAM_API_ID}")
logger.info(f"📂 Usando ruta de sesión: {SESSION_PATH}")
client = TelegramClient(SESSION_PATH, TELEGRAM_API_ID, TELEGRAM_API_HASH)

# After creating the client, configure connection parameters
client = TelegramClient(SESSION_PATH, TELEGRAM_API_ID, TELEGRAM_API_HASH)

# Adjust connection parameters
client.flood_sleep_threshold = 60  # Higher threshold for flood wait errors
client.connection_retries = 10     # Number of retries per connection attempt
client.retry_delay = 1            # Initial delay between retries

# DexScreener API base URL
DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex"

# Use imported groups
groups = TELEGRAM_GROUPS

# Rest of your script remains mostly unchanged...
# Just remove any regex pattern definitions since they're now imported
processed_msg_ids = set()

def select_best_pair(pairs):
    """
    Select the best pair from a list of pairs for a token based on liquidity.
    Returns the selected pair object.
    """
    if not pairs:
        return None
    
    # Sort by liquidity (descending) and return highest
    sorted_pairs = sorted(
        pairs,
        key=lambda x: float(x.get('liquidity', {}).get('usd', 0) or 0),
        reverse=True
    )
    
    if sorted_pairs:
        return sorted_pairs[0]
    
    return None

def check_add_best_pair_column():
    """Ensure the best_pair_address column exists in tokens table."""
    try:
        conn = connect_postgres()
        if not conn:
            logger.error("Failed to connect to PostgreSQL")
            return False
            
        cursor = conn.cursor()
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tokens' AND column_name = 'best_pair_address';
        """)
        
        if not cursor.fetchone():
            logger.info("Adding best_pair_address column to tokens table")
            cursor.execute("""
                ALTER TABLE tokens ADD COLUMN best_pair_address character varying(66);
            """)
            conn.commit()
            logger.info("✅ Added best_pair_address column to tokens table")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error checking/adding column: {e}")
        return False

def update_token_best_pair(token_id, pair_address):
    """Update the best pair address for a token in the tokens table."""
    try:
        conn = connect_postgres()
        if not conn:
            return False
            
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tokens
            SET best_pair_address = %s
            WHERE token_id = %s
            AND (best_pair_address IS NULL OR best_pair_address != %s)
        """, (pair_address, token_id, pair_address))
        
        updated = cursor.rowcount > 0
        conn.commit()
        cursor.close()
        conn.close()
        
        if updated:
            logger.info(f"✅ Updated best pair address for token {token_id} to {pair_address}")
        
        return updated
    except Exception as e:
        logger.error(f"Error updating token best pair: {e}")
        return False

def insert_price_metrics(token_id, pair):
    """Insert initial price data for a token."""
    try:
        conn = connect_postgres()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        # Extract data from pair according to the table schema
        pair_address = pair.get('pairAddress', '')
        price_native = pair.get('priceNative', 0)
        price_usd = pair.get('priceUsd', 0)
        txns_buys = pair.get('txns', {}).get('h24', {}).get('buys', 0)
        txns_sells = pair.get('txns', {}).get('h24', {}).get('sells', 0)
        volume = pair.get('volume', {}).get('h24', 0)
        liquidity_base = pair.get('liquidity', {}).get('base', 0)
        liquidity_quote = pair.get('liquidity', {}).get('quote', 0)
        liquidity_usd = pair.get('liquidity', {}).get('usd', 0)
        fdv = pair.get('fdv', 0)
        market_cap = pair.get('marketCap', 0)
        
        # Insert price data
        cursor.execute("""
            INSERT INTO price_metrics
                (token_id, pair_address, timestamp, price_native, price_usd,
                txns_buys, txns_sells, volume, liquidity_base, liquidity_quote,
                liquidity_usd, fdv, market_cap)
            VALUES
                (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            token_id, pair_address, price_native, price_usd,
            txns_buys, txns_sells, volume, liquidity_base, liquidity_quote,
            liquidity_usd, fdv, market_cap
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"✅ Inserted initial price data for token {token_id}")
        return True
    except Exception as e:
        logger.error(f"Error inserting initial price data: {e}")
        return False

def process_pair_data(token_id, contract_address, blockchain, group_name, pair, message_id, timestamp):
    """Procesa los datos de un par de tokens y actualiza la base de datos"""
    try:
        # Calcular edad del token si es posible
        token_age = None
        if 'pairCreatedAt' in pair:
            creation_time = datetime.fromtimestamp(pair['pairCreatedAt'] / 1000)
            current_time = datetime.now()
            token_age = (current_time - creation_time).days
        
        # Obtener liquidez
        liquidity = None
        if 'liquidity' in pair and 'usd' in pair['liquidity']:
            liquidity = float(pair['liquidity']['usd'] or 0)
        
        # Obtener precio
        call_price = float(pair.get('priceUsd', 0) or 0)
        
        # Actualizar información del token
        success = update_token_info(
            token_id=token_id,
            name=pair.get('baseToken', {}).get('name', 'Unknown'),
            ticker=pair.get('baseToken', {}).get('symbol', 'UNKNOWN'),
            liquidity=liquidity,
            price=call_price,
            dex=pair.get('dexId', 'Unknown'),
            supply=float(pair.get('liquidity', {}).get('base', 0) or 0),
            age=token_age,
            group_name=group_name,  # This stores the group name as text in group_call column
            dexscreener_url=f"https://dexscreener.com/{blockchain}/{contract_address}"
        )
        
        # Log detailed token information
        logger.info(f"🔍 Token data: Name={pair.get('baseToken', {}).get('name', 'Unknown')}, "
                  f"Price=${call_price}, Liquidity=${liquidity or 'N/A'}, Age={token_age or 'N/A'}")
        
        if not success:
            logger.warning(f"⚠️ No se pudo actualizar el token {contract_address}")
            return False
        
        # Registrar el call
        try:
            call_id = insert_call(token_id, message_id, timestamp, call_price)
            if not call_id:
                logger.warning(f"⚠️ No se pudo registrar el call para el token {contract_address}")
                return False
        except Exception as e:
            logger.error(f"❌ Error específico al insertar call: {e}")
            return False
            
        # Save the best pair address if not already set
        pair_address = pair.get('pairAddress')
        if pair_address:
            update_token_best_pair(token_id, pair_address)
            
            # Also insert initial price metrics
            insert_price_metrics(token_id, pair)
        
        logger.info(f"✅ Token {contract_address} procesado exitosamente")
        return True
    except Exception as e:
        logger.error(f"❌ Error procesando datos del token {contract_address}: {e}")
        return False

@client.on(events.NewMessage(chats=list(groups.keys())))
async def handle_message(event):
    try:
        # Ensure the best_pair_address column exists
        check_add_best_pair_column()
        
        msg_id = event.message.id
        logger.info(f"📩 Nuevo mensaje ID: {msg_id} en chat: {event.chat_id}")
        if msg_id in processed_msg_ids:
            logger.info(f"🔄 Mensaje {msg_id} ya procesado, ignorando...")
            return
        processed_msg_ids.add(msg_id)

        chat_id = event.chat_id
        group_name = groups.get(chat_id, "Desconocido")
        text = event.message.message or ""
        timestamp = event.message.date
        sender_id = event.sender_id or 0

        # Detectar si es un "call" basado en el patrón
        is_call = bool(CALL_PATTERN.search(text))

        logger.info(f"📝 Procesando: {group_name} - {text[:50]}{'...' if len(text) > 50 else ''}")

        # Registrar grupo
        try:
            group_id = insert_group(chat_id, group_name)
            logger.info(f"✅ Grupo {group_name} registrado con ID: {group_id}")
        except Exception as e:
            logger.error(f"❌ Error al registrar grupo: {e}")
            return

        # Determinar si es una respuesta a otro mensaje
        reply_to = None
        if hasattr(event.message, 'reply_to') and event.message.reply_to:
            reply_to = event.message.reply_to.reply_to_msg_id

        # Guardar mensaje inicialmente (sin token_id aún)
        try:
            message_id = insert_message(
                group_id, 
                timestamp, 
                text, 
                sender_id,
                reply_to=reply_to,
                is_call=is_call
            )
            logger.info(f"💾 Mensaje guardado con ID: {message_id}")
        except Exception as e:
            logger.error(f"❌ Error al guardar mensaje: {e}")
            return

        # Variables para seguimiento de tokens detectados
        token_detected = False
        token_addresses = set()
        detected_token_id = None
        
        # 1. Buscar enlaces de DexScreener primero
        dex_matches = DEX_LINK_REGEX.findall(text)
        for chain, token in dex_matches:
            if token not in token_addresses:
                token_addresses.add(token)
                # Make sure to normalize chain names
                if chain.lower() == "eth":
                    chain = "ethereum"
                logger.info(f"🆕 Token detectado desde DexScreener: {token} ({chain})")
                try:
                    # Insertar token en la base de datos
                    token_id = insert_token(token, chain)
                    detected_token_id = token_id
                    
                    # Obtener datos adicionales desde DexScreener
                    pairs = get_pairs_data(chain, [token])
                    
                    if pairs and len(pairs) > 0:
                        # Select the best pair based on liquidity
                        best_pair = select_best_pair(pairs)
                        
                        # Procesar los datos del token con el mejor par
                        success = process_pair_data(
                            token_id, token, chain, group_name, best_pair, message_id, timestamp
                        )
                        token_detected = success or token_detected
                    else:
                        logger.warning(f"⚠️ No se encontraron pares para el token {token}")
                except Exception as e:
                    logger.error(f"❌ Error al procesar token {token} desde DexScreener: {e}")
        
        # Special handling for TinyAstro links (LP addresses)
        tinyastro_matches = TINYASTRO_REGEX.findall(text)
        for lp_address in tinyastro_matches:
            if lp_address not in token_addresses:
                token_addresses.add(lp_address)
                logger.info(f"🆕 LP detectado desde TinyAstro: {lp_address} (solana)")
                
                try:
                    # Insertar LP en la base de datos como token para rastrearlo
                    token_id = insert_token(lp_address, "solana")
                    detected_token_id = token_id
                    
                    # Intentar primero como par, luego como token si falla
                    pairs = get_pair_by_address("solana", lp_address)
                    
                    if pairs and len(pairs) > 0:
                        # Select the best pair (in this case, there should be only one since we're querying by pair address)
                        best_pair = pairs[0]
                        
                        # Extract the actual token address from the pair
                        token_address = best_pair.get('baseToken', {}).get('address')
                        if token_address:
                            logger.info(f"✅ Encontrado token {token_address} desde LP {lp_address}")
                        
                        # Process the pair data
                        success = process_pair_data(
                            token_id, lp_address, "solana", group_name, best_pair, message_id, timestamp
                        )
                        token_detected = success or token_detected
                    else:
                        logger.warning(f"⚠️ No se pudo obtener información para LP {lp_address}")
                except Exception as e:
                    logger.error(f"❌ Error al procesar LP {lp_address} desde TinyAstro: {e}")
        
        # First define chains to try for 0x addresses
        ETH_BSC_CHAINS = ["ethereum", "bsc"]  # Try Ethereum first, then BSC

        # Then modify the detection logic:
        # 2. Buscar direcciones de contrato en texto plano
        if not token_detected:
            # Check for 0x addresses (could be ETH or BSC)
            matches = RE_CA_BSC_ETH.findall(text)
            if matches:
                for token in matches:
                    if token in token_addresses:
                        continue
                        
                    token_addresses.add(token)
                    logger.info(f"🆕 Token detectado desde texto: {token} (eth/bsc)")
                    
                    # Try both ETH and BSC chains
                    for chain in ETH_BSC_CHAINS:
                        try:
                            logger.info(f"Intentando chain: {chain} para token {token}")
                            # Get data from DexScreener
                            pairs = get_pairs_data(chain, [token])
                            
                            if pairs and len(pairs) > 0:
                                logger.info(f"✅ Token encontrado en {chain}: {token}")
                                # Select the best pair based on liquidity
                                best_pair = select_best_pair(pairs)
                                
                                # Insert token with correct chain
                                token_id = insert_token(token, chain)
                                detected_token_id = token_id
                                
                                # Process pair data with correct chain
                                success = process_pair_data(
                                    token_id, token, chain, group_name, best_pair, message_id, timestamp
                                )
                                token_detected = success or token_detected
                                if success:
                                    break  # Found the correct chain, no need to try others
                        except Exception as e:
                            logger.error(f"❌ Error al procesar token {token} en {chain}: {e}")
        
        # Check for Solana addresses (remains unchanged)
        if not token_detected:
            try:
                matches = RE_CA_SOL.findall(text)
                for token in matches:
                    if token not in token_addresses:
                        token_addresses.add(token)
                        logger.info(f"🆕 Token detectado desde texto: {token} (solana)")
                        
                        try:
                            # Insertar token en la base de datos
                            token_id = insert_token(token, "solana")
                            detected_token_id = token_id
                            
                            # Obtener datos adicionales desde DexScreener
                            pairs = get_pairs_data("solana", [token])
                            
                            if pairs and len(pairs) > 0:
                                # Select the best pair based on liquidity
                                best_pair = select_best_pair(pairs)
                                
                                # Procesar los datos del token
                                success = process_pair_data(
                                    token_id, token, "solana", group_name, best_pair, message_id, timestamp
                                )
                                token_detected = success or token_detected
                                if success:
                                    break  # Éxito, no buscar más tokens
                            else:
                                logger.warning(f"⚠️ No se encontraron pares para el token {token}")
                        except Exception as e:
                            logger.error(f"❌ Error al procesar token {token} desde texto: {e}")
            except Exception as e:
                logger.error(f"❌ Error al procesar regex para solana: {e}")
        
        # 3. Asegurarnos de que el mensaje esté actualizado correctamente
        if token_detected and detected_token_id:
            # Verificar si el mensaje ya tiene la marca de call y el token_id
            conn = connect_postgres()
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT is_call, token_id FROM telegram_messages WHERE message_id = %s",
                    (message_id,)
                )
                msg_data = cursor.fetchone()
                
                if not msg_data or not msg_data[0] or msg_data[1] is None:
                    # Si algo no está actualizado, hacerlo manualmente
                    cursor.execute(
                        "UPDATE telegram_messages SET is_call = TRUE, token_id = %s WHERE message_id = %s",
                        (detected_token_id, message_id)
                    )
                    conn.commit()
                    logger.info(f"✅ Mensaje {message_id} actualizado como call con token {detected_token_id}")
            except Exception as e:
                logger.error(f"❌ Error al verificar/actualizar estado del mensaje: {e}")
                conn.rollback()
            finally:
                conn.close()

        # Final verification to ensure token information was saved
        if token_detected and detected_token_id:
            logger.info(f"✓ Verification: Token {detected_token_id} processed successfully")
            
            # Double check tokens table record
            conn = connect_postgres()
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT name, ticker, group_call, token_age, best_pair_address FROM tokens WHERE token_id = %s",
                    (detected_token_id,)
                )
                token_data = cursor.fetchone()
                if token_data:
                    logger.info(f"✓ Token record: Name={token_data[0]}, Ticker={token_data[1]}, "
                              f"Group={token_data[2]}, Age={token_data[3]}, Best Pair={token_data[4]}")
                else:
                    logger.warning("⚠️ Token record not found after processing")
            except Exception as e:
                logger.error(f"❌ Error during verification: {e}")
            finally:
                conn.close()

        # Informar resultado final
        if token_detected:
            logger.info(f"🎉 Call registrado exitosamente para mensaje {message_id}")
        else:
            logger.info(f"ℹ️ No se detectaron tokens/calls en este mensaje")
    except Exception as e:
        logger.error(f"❌ Error general al procesar mensaje: {e}")
        # Don't re-raise the exception - just log it

async def main():
    """Main function with improved connection handling"""
    # Asegurar que exista el directorio de sesión
    session_dir = os.path.dirname(SESSION_PATH)
    if not os.path.exists(session_dir):
        os.makedirs(session_dir, exist_ok=True)
        
    logger.info("🚀 Iniciando el bot de Telegram...")
    
    # Add retry logic
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            await client.start()
            if not await client.is_user_authorized():
                logger.warning("⚠️ Sesión no autorizada. Autenticación requerida.")
                phone = os.getenv("PHONE_NUMBER")
                if phone:
                    await client.send_code_request(phone)
                    code = input("Ingresa el código recibido: ")
                    await client.sign_in(phone, code)
                else:
                    logger.error("❌ PHONE_NUMBER no definido en .env. Autenticación manual requerida.")
                    return
                    
            logger.info("✅ Conexión a Telegram establecida")
            logger.info(f"🔍 Escuchando en {len(groups)} grupos: {list(groups.keys())}")
            
            # Reset retry count on successful connection
            retry_count = 0
            
            # Add a catch-all error handler
            @client.on(events.NewMessage)
            async def error_handler(event):
                try:
                    # Let regular handlers process first
                    await event.continue_propagation()
                except Exception as e:
                    logger.error(f"❌ Error no manejado en evento: {e}")
            
            await client.run_until_disconnected()
            
        except (ConnectionError, ServerError) as e:
            retry_count += 1
            wait_time = min(300, (2 ** retry_count) + random.randint(0, 10))  # Exponential backoff with jitter
            logger.warning(f"⚠️ Error de conexión: {e}. Reintento {retry_count}/{max_retries} en {wait_time} segundos...")
            time.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"❌ Error fatal: {e}")
            break
            
    logger.info("🛑 Bot detenido y desconectado")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot detenido por interrupción del usuario")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
    finally:
        logger.info("🔚 Fin de ejecución")
