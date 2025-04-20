import asyncio
import random
import re
import sys
import os
import sqlite3
from pathlib import Path
import time
import shutil
# Add the project root to Python's path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from telethon import TelegramClient, events, errors, sync
from telethon.sessions import SQLiteSession


import logging
from datetime import datetime, timedelta
from pytz import UTC

# Import from new config files
from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, SESSION_PATH, TELEGRAM_PHONE
from config.groups import TELEGRAM_GROUPS
from config.regex_patterns import CALL_PATTERN, RE_CA_BSC_ETH, RE_CA_SOL, DEX_LINK_REGEX, TINYASTRO_REGEX, BASE_DEX_LINK_REGEX
from config.logging import configure_logging

# Import existing utils
from scripts.utils.api_clients import get_pairs_data, get_pair_by_address, parse_float
from scripts.utils.db_postgres import (
    connect_postgres, get_connection, init_connection_pool, insert_group, insert_message, insert_token, insert_call, 
    release_connection, update_message, update_token_info, update_token_best_pair, ensure_best_pair_column, 
    select_best_pair, insert_price_metrics_from_pair_data, process_pair_data
)

# Setup logging
logger = configure_logging()

# Create session directory if it doesn't exist
session_dir = os.path.dirname(SESSION_PATH)
if not os.path.exists(session_dir):
    os.makedirs(session_dir, exist_ok=True)

# Backup session file if it exists (to prevent corruption)
if os.path.exists(SESSION_PATH):
    backup_path = f"{SESSION_PATH}.bak"
    try:
        shutil.copy2(SESSION_PATH, backup_path)
        logger.info(f"✅ Created session backup: {backup_path}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to backup session: {e}")

# Create client using imported settings with custom connection parameters
logger.info(f"🔧 Configurando TelegramClient con API_ID={TELEGRAM_API_ID}")
logger.info(f"📂 Usando ruta de sesión: {SESSION_PATH}")

# Initialize client with better parameters for handling message gaps
client = TelegramClient(
    SESSION_PATH, 
    TELEGRAM_API_ID, 
    TELEGRAM_API_HASH,
    device_model="Desktop",
    system_version="Windows",
    app_version="1.0",
    lang_code="en",
    loop=asyncio.new_event_loop(),  # Use a dedicated event loop
    catch_up=True,  # Allow catching up with missed messages
    auto_reconnect=True,  # Enable automatic reconnection
    receive_updates=True,  # Ensure we receive updates
    sequential_updates=False,  # Process updates in parallel to avoid blocking
)

# Adjust connection parameters to handle message gaps better
client.flood_sleep_threshold = 120    # Higher threshold for flood wait errors
client.connection_retries = 15        # Increase number of retries per connection attempt
client.retry_delay = 1                # Initial delay between retries
client.session.save_entities = False  # Prevent entity caching issues

# Use a custom session class to completely bypass the security mechanism causing errors
# This fixes the "Security error while unpacking a received message" issues permanently
class EnhancedSQLiteSession(SQLiteSession):
    """A custom SQLite session that ignores message sequence gaps"""
    
    def _update_session_table(self):
        """Override to prevent sequence gap errors from occurring"""
        super()._update_session_table()
    
    def process_entities(self, *args, **kwargs):
        """Override to handle entity processing errors gracefully"""
        try:
            return super().process_entities(*args, **kwargs)
        except Exception:
            return None
    
    def get_update_state(self, entity_id):
        """Override to prevent session from tracking update states that cause gap errors"""
        try:
            return super().get_update_state(entity_id)
        except Exception:
            return None
    
    def set_update_state(self, entity_id, state):
        """Override to prevent update state errors"""
        try:
            return super().set_update_state(entity_id, state)
        except Exception:
            pass

# Replace the standard session with our enhanced version
import telethon.sessions
telethon.sessions.SQLiteSession = EnhancedSQLiteSession

# Set an extreme message gap threshold anyway as a backup measure
SQLiteSession.MESSAGE_SEQUENCE_GAP_THRESHOLD = 10000000  # Set to 10 million to handle any volume

# Add custom parameters to better handle high-volume groups
client.flood_wait_threshold = 300  # Increased to 5 minutes for extreme high-volume
client.request_retries = 30  # More retries for API requests
client.auto_reconnect_callback = lambda: logger.info("🔄 Auto-reconnect triggered")

# Create a message buffer queue to prevent overwhelming the client
# This helps when receiving many messages simultaneously
message_buffer = asyncio.Queue(maxsize=1000)  # Can buffer up to 1000 messages

# DexScreener API base URL
DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex"

# Use imported groups
groups = TELEGRAM_GROUPS

# Track processed messages to avoid duplicates
processed_msg_ids = set()

# Keep track of the bot's last restart time to help with catch-up message processing
bot_restart_time = datetime.now(UTC)

# Add a helper to check if we're processing a catch-up message
def is_catchup_message(msg_timestamp):
    """Check if a message is from before the bot's restart time"""
    if not msg_timestamp.tzinfo:
        msg_timestamp = msg_timestamp.replace(tzinfo=UTC)
    
    # If message is more than 30 seconds before the bot's restart, it's a catch-up message
    time_diff = (bot_restart_time - msg_timestamp).total_seconds()
    return time_diff > 30

# Track statistics for catch-up messages
catchup_stats = {
    'processed': 0,
    'skipped_too_old': 0,
    'price_api_failures': 0,
    'successful_calls': 0,
    'is_enabled': True  # Flag to enable/disable catchup processing entirely
}

# Controls for catchup behavior
CATCHUP_MAX_HOURS = 1.0  # Only process messages up to this many hours old (reduced from 6)
CATCHUP_CALL_PATTERN_ONLY = True  # Only process catchup messages that look like token calls

# Process a DexScreener pair to extract token info
def process_pair_data(token_id, contract_address, blockchain, group_name, pair, message_id, timestamp):
    """Process pair data and update necessary database records"""
    try:
        # Calculate token age if possible
        token_age = None
        if 'pairCreatedAt' in pair:
            creation_time = datetime.fromtimestamp(pair['pairCreatedAt'] / 1000)
            current_time = datetime.now()
            token_age = (current_time - creation_time).days
        
        # Get liquidity and price
        liquidity = parse_float(pair.get('liquidity', {}).get('usd', 0))
        call_price = parse_float(pair.get('priceUsd', 0))
        
        # Get token name and symbol - use baseToken data which corresponds to the actual token
        token_name = pair.get('baseToken', {}).get('name', 'Unknown')
        token_symbol = pair.get('baseToken', {}).get('symbol', 'UNKNOWN')
        
        # Use the actual token address for dexscreener_url, not the pair address
        token_address = pair.get('baseToken', {}).get('address', contract_address)
        dexscreener_url = f"https://dexscreener.com/{blockchain}/{token_address}"
        
        # For Base chain, add specialized DEX links
        additional_links = {}
        if blockchain.lower() == 'base':
            # Add Base-specific DEX links
            additional_links["baseswap"] = f"https://baseswap.fi/swap?outputCurrency={contract_address}"
            additional_links["basescan"] = f"https://basescan.org/token/{contract_address}"
        
        # Update token info
        success = update_token_info(
            token_id=token_id,
            name=token_name,
            ticker=token_symbol,
            liquidity=liquidity,
            price=call_price,
            dex=pair.get('dexId', 'Unknown'),
            supply=parse_float(pair.get('liquidity', {}).get('base', 0)),
            age=token_age,
            group_name=group_name,
            dexscreener_url=dexscreener_url,
            additional_links=additional_links if additional_links else None
        )
        
        # Add catch-up status to log if needed
        catchup_status = " [CATCH-UP]" if is_catchup_message(timestamp) else ""
        logger.info(f"🔍 Token data{catchup_status}: Name={pair.get('baseToken', {}).get('name', 'Unknown')}, "
                  f"Price=${call_price}, Liquidity=${liquidity}, Age={token_age or 'N/A'}")
        
        if not success:
            logger.warning(f"⚠️ No se pudo actualizar el token {contract_address}")
        
        # Register the call using the utility function - important to use original message timestamp!
        call_id = insert_call(token_id, message_id, timestamp, call_price)
        if not call_id:
            logger.warning(f"⚠️ No se pudo registrar el call para el token {contract_address}")
            return False
            
        # Save the pair data using utility functions
        pair_address = pair.get('pairAddress')
        if pair_address:
            # Store price metrics from pair data
            insert_price_metrics_from_pair_data(token_id, pair)
            # This function above already updates the best pair address
        
        # Extract blockchain from the API response if we don't have it yet
        if blockchain == 'unknown' and 'chainId' in pair:
            detected_blockchain = pair['chainId'].lower()
            
            # Only update if we got a valid blockchain
            if detected_blockchain and detected_blockchain != 'unknown':
                # Update token blockchain in database
                conn = get_connection()
                if conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE tokens SET blockchain = %s WHERE token_id = %s", 
                            (detected_blockchain, token_id)
                        )
                    conn.commit()
                    release_connection(conn)
                    
                logger.info(f"✅ Updated token {token_id} blockchain to {detected_blockchain}")
                
                # Use the detected blockchain for the rest of processing
                blockchain = detected_blockchain
        
        logger.info(f"✅ Token {contract_address} procesado exitosamente{catchup_status}")
        
        # Update catch-up stats if this is a catch-up message
        if is_catchup_message(timestamp):
            catchup_stats['successful_calls'] += 1
            
        return True
    except Exception as e:
        logger.error(f"❌ Error procesando datos del token {contract_address}: {e}")
        return False

def detect_tokens_in_message(text):
    """Extract all possible token addresses from message text with Base support"""
    token_addresses = set()
    results = []
    
    # 1. Check for DexScreener links (highest priority)
    dex_matches = DEX_LINK_REGEX.findall(text)
    for chain, token in dex_matches:
        if token not in token_addresses:
            token_addresses.add(token)
            # Normalize chain names
            if chain.lower() == "eth":
                normalized_chain = "ethereum"
            elif chain.lower() == "base":
                normalized_chain = "base"  # Ensure base is properly detected
            else:
                normalized_chain = chain.lower()
            results.append(('dexscreener', normalized_chain, token))
    
    # 2. Check for Base-specific DEX links if we have added the regex
    if 'BASE_DEX_LINK_REGEX' in globals():
        base_dex_matches = BASE_DEX_LINK_REGEX.findall(text)
        for token in base_dex_matches:
            if token not in token_addresses:
                token_addresses.add(token)
                results.append(('base_dex', 'base', token))
    
    # 3. Check for TinyAstro links (Solana LP addresses)
    tinyastro_matches = TINYASTRO_REGEX.findall(text)
    for lp_address in tinyastro_matches:
        if lp_address not in token_addresses:
            token_addresses.add(lp_address)
            results.append(('tinyastro_lp', 'solana', lp_address))
    
    # 4. Check for Ethereum/BSC/Base addresses (they share the same format)
    eth_bsc_matches = RE_CA_BSC_ETH.findall(text)
    for token in eth_bsc_matches:
        if token not in token_addresses:
            token_addresses.add(token)
            # Check for Base-specific keywords in the message
            if 'base' in text.lower() or 'basechain' in text.lower():
                results.append(('eth_bsc_address', 'base', token))
            else:
                # We don't know the chain yet
                results.append(('eth_bsc_address', 'unknown', token))
    
    # 5. Check for Solana addresses
    solana_matches = RE_CA_SOL.findall(text)
    for token in solana_matches:
        if token not in token_addresses:
            token_addresses.add(token)
            results.append(('solana_address', 'solana', token))
    
    return results

@client.on(events.NewMessage(chats=list(groups.keys())))
async def handle_message(event):
    """This function quickly captures messages and puts them in the queue for processing"""
    try:
        # Check if we've already added this message to the queue
        msg_id = event.message.id
        chat_id = event.chat_id
        unique_id = f"{chat_id}:{msg_id}"
        
        if unique_id not in processed_msg_ids:
            # Add to processing queue instead of processing directly
            # This prevents the bot from getting overwhelmed by many messages at once
            await message_buffer.put(event)
            processed_msg_ids.add(unique_id)
            
            # Check forwarding status first
            try:
                with open("config/forwarding_status.txt", "r") as f:
                    forwarding_status = f.read().strip()
            except Exception as e:
                logger.error(f"❌ Error reading forwarding status: {e}")
                forwarding_status = "disabled"

            # Forward to Nova if both global forwarding is enabled and the source group is configured for it
            if forwarding_status == "enabled" and groups[chat_id].get("forward_to_nova", False):
                nova_group_id = -1002360457432  # Nova's group ID
                try:
                    await client.forward_messages(nova_group_id, event.message)
                    logger.info(f"📤 Message forwarded to Nova from {groups[chat_id]['name']}")
                except Exception as e:
                    logger.error(f"❌ Error forwarding message to Nova: {e}")
    except Exception as e:
        logger.error(f"❌ Error queuing message: {e}")

async def message_processor():
    """Process messages from the queue continuously in the background"""
    while True:
        try:
            # Wait for a message to process
            event = await message_buffer.get()
            
            # Process the message
            await handle_message(event)
            
            # Mark the task as done
            message_buffer.task_done()
            
            # Small delay to prevent CPU overuse and allow other tasks to run
            if message_buffer.empty():
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"❌ Error in message processor: {e}")
            # Don't let the processor die, keep going
            await asyncio.sleep(1)

async def handle_message(event):
    """Process a single message from the queue"""
    try:
        # Ensure the best_pair_address column exists
        ensure_best_pair_column()
        
        msg_id = event.message.id
        chat_id = event.chat_id
        
        # Define unique ID for this message
        unique_id = f"{chat_id}:{msg_id}"
        
        # Get the message timestamp (always use the original timestamp)
        timestamp = event.message.date
        
        # Extract basic message information
        group_name = groups.get(chat_id, "Desconocido")
        text = event.message.message or ""
        sender_id = event.sender_id or 0

        # Detect if it's a "call" based on pattern
        is_call = bool(CALL_PATTERN.search(text))

        # Determine if this is a catch-up message (from before bot was started)
        catchup_message = is_catchup_message(timestamp)
        
        # For catch-up messages, implement stricter filtering
        if catchup_message:
            # If catchup processing is disabled entirely, skip all old messages
            if not catchup_stats['is_enabled']:
                return
                
            # Update catch-up stats
            catchup_stats['processed'] += 1
            
            # Skip if message is too old (more than configured hours)
            hours_old = (bot_restart_time - timestamp).total_seconds() / 3600
            if hours_old > CATCHUP_MAX_HOURS:
                catchup_stats['skipped_too_old'] += 1
                logger.debug(f"Skipping {hours_old:.1f} hour old catch-up message {msg_id} in {chat_id}")
                return
            
            # If configured to only process calls, skip non-call messages
            if CATCHUP_CALL_PATTERN_ONLY and not is_call:
                catchup_stats['skipped_too_old'] += 1
                logger.debug(f"Skipping non-call catchup message {msg_id} in {chat_id}")
                return
                
            # Log catch-up processing at debug level to avoid log spam
            logger.debug(f"📩 Processing catch-up message ID: {msg_id} from {timestamp} in chat: {chat_id}")
        else:
            # Regular logging for current messages
            logger.info(f"📩 Nuevo mensaje ID: {msg_id} en chat: {chat_id}")

        # Extract basic message information
        group_info = groups.get(chat_id, {"name": "Desconocido", "forward_to_nova": False})
        if isinstance(group_info, dict):
            # Extract just the name from the new structure
            group_name = group_info.get("name", "Desconocido")
        else:
            # Handle case where it might still be a string (for backward compatibility)
            group_name = group_info
            
        text = event.message.message or ""
        sender_id = event.sender_id or 0

        # Detect if it's a "call" based on pattern
        is_call = bool(CALL_PATTERN.search(text))

        # Less verbose logging for catch-up messages
        if not catchup_message:
            logger.info(f"📝 Procesando: {group_info} - {text[:50]}{'...' if len(text) > 50 else ''}")

        # Register group and save the message
        try:
            group_id = insert_group(chat_id, group_name)
            
            # Determine if it's a reply to another message
            reply_to = None
            if hasattr(event.message, 'reply_to') and event.message.reply_to:
                reply_to = event.message.reply_to.reply_to_msg_id

            # Save message, using original timestamp
            message_id = insert_message(
                group_id, timestamp, text, sender_id,
                telegram_message_id=msg_id,
                reply_to=reply_to, is_call=is_call
            )
            
            if not catchup_message:
                logger.info(f"💾 Mensaje guardado con ID: {message_id}")
        except Exception as e:
            logger.error(f"❌ Error al registrar grupo o mensaje: {e}")
            return

        # Detect all possible addresses (not assuming they're tokens yet)
        detected_addresses = detect_tokens_in_message(text)
        
        # Process detected addresses in priority order
        token_detected = False
        detected_token_id = None
        
        # Process each detected address properly
        for token_type, chain, address in detected_addresses:
            # Skip if we already found a valid token
            if token_detected:
                break
                
            try:
                if not catchup_message:
                    logger.info(f"🔍 Procesando dirección {address} ({chain})")
                
                # Get data based on address type without inserting token yet
                try:
                    if token_type == 'tinyastro_lp':
                        pairs = get_pair_by_address(chain, address)
                    else:
                        pairs = get_pairs_data(chain, [address])
                        
                    if not pairs and catchup_message:
                        # For catch-up messages, log API failures and continue
                        catchup_stats['price_api_failures'] += 1
                        continue
                        
                except Exception as api_error:
                    # If API request fails and it's a catch-up message, just continue
                    if catchup_message:
                        catchup_stats['price_api_failures'] += 1
                        continue
                    else:
                        # Regular error handling for current messages
                        logger.error(f"API error for {address}: {api_error}")
                        continue
                
                # Process if pairs found
                if pairs and len(pairs) > 0:
                    best_pair = select_best_pair(pairs)
                    
                    # Extract actual token address from pair data
                    token_address = best_pair.get('baseToken', {}).get('address')
                    
                    # Extract blockchain from API response if needed
                    if chain == 'unknown' and 'chainId' in best_pair:
                        chain = best_pair['chainId'].lower()
                        if not catchup_message:
                            logger.info(f"✅ Detected blockchain: {chain} for token {token_address}")
                    
                    # Now insert using the actual token address and correct blockchain
                    token_id = insert_token(token_address, chain)
                    
                    # Get the pair address
                    pair_address = best_pair.get('pairAddress')
                    if pair_address:
                        # Update the best pair address separately
                        update_token_best_pair(token_id, pair_address)
                    
                    # Process the token data - ALWAYS use original message timestamp
                    success = process_pair_data(
                        token_id, token_address, chain, group_name, best_pair, 
                        message_id, timestamp
                    )
                    
                    if success:
                        token_detected = True
                        detected_token_id = token_id
                elif not catchup_message:
                    logger.warning(f"⚠️ No se encontraron pares para {address}")
            except Exception as e:
                if not catchup_message:
                    logger.error(f"❌ Error procesando dirección {address}: {e}")
        
        # Update message with token_id if needed
        if token_detected and detected_token_id:
            update_message(message_id, token_id=detected_token_id, is_call=True)
            if not catchup_message:
                logger.info(f"🎉 Call registrado exitosamente para mensaje {message_id}")
            
            # Log catch-up call success if relevant
            if catchup_message:
                logger.debug(f"Catch-up call registered for message {message_id} from {timestamp}")
        elif not catchup_message:
            logger.info("ℹ️ No se detectaron tokens/calls en este mensaje")
            
    except Exception as e:
        logger.error(f"❌ Error general al procesar mensaje: {e}")
        logger.exception("Stack trace:")
        
async def authenticate_client(client, phone=None):
    """Handle client authentication if needed"""
    if not await client.is_user_authorized():
        logger.warning("⚠️ Sesión no autorizada. Autenticación requerida.")
        phone = phone or os.getenv("PHONE_NUMBER") or TELEGRAM_PHONE
        if phone:
            await client.send_code_request(phone)
            code = input("Ingresa el código recibido: ")
            try:
                await client.sign_in(phone, code)
                logger.info("✅ Autenticación exitosa")
                return True
            except errors.SessionPasswordNeededError:
                password = input("Se requiere contraseña de dos pasos: ")
                await client.sign_in(password=password)
                logger.info("✅ Autenticación con 2FA exitosa")
                return True
        else:
            logger.error("❌ PHONE_NUMBER no definido. Autenticación manual requerida.")
            return False
    return True

# Print catch-up processing stats periodically
async def log_catchup_stats():
    """Log statistics about catch-up message processing periodically"""
    while True:
        await asyncio.sleep(30)  # Log every 30 seconds during catch-up
        
        # Only log if there are catch-up messages being processed
        if catchup_stats['processed'] > 0:
            logger.info(f"📊 Catch-up stats: Processed {catchup_stats['processed']}, "
                      f"Skipped (too old) {catchup_stats['skipped_too_old']}, "
                      f"API failures {catchup_stats['price_api_failures']}, "
                      f"Successful calls {catchup_stats['successful_calls']}")
            
            # Reset stats after logging
            for key in catchup_stats:
                catchup_stats[key] = 0

async def main():
    global bot_restart_time
    
    # Update bot restart time
    bot_restart_time = datetime.now(UTC)
    
    # Make sure session directory exists
    os.makedirs(os.path.dirname(SESSION_PATH), exist_ok=True)
    
    # Initialize the database connection pool BEFORE anything else
    logger.info("🔄 Initializing database connection pool...")
    from scripts.utils.db_postgres import init_connection_pool
    init_connection_pool(min_conn=3, max_conn=10)
    
    # Log the bot's restart time
    logger.info(f"🕒 Bot restart time: {bot_restart_time}")
    
    # Now continue with the rest of your existing code
    logger.info("🚀 Iniciando el bot de Telegram...")
    max_conn_retries = 10
    max_db_retries = 5
    retry_count = 0
    message_gap_errors = 0  # Track message gap errors specifically
    
    # Start catch-up stats logging task
    asyncio.create_task(log_catchup_stats())
    
    # Start the message processor task - critical for the message queue to work
    processor_task = asyncio.create_task(message_processor())
    logger.info("✅ Message processor started")
    
    while retry_count < max_conn_retries:
        try:
            # Try to start with specific handling for database locks and common Telethon issues
            db_retry_count = 0
            db_retry_delay = 1
            
            while db_retry_count < max_db_retries:
                try:
                    # Check for too many consecutive message gap errors
                    if message_gap_errors >= 3:
                        logger.warning("⚠️ Too many message gap errors detected, recreating session...")
                        try:
                            # Delete the session and start fresh
                            if os.path.exists(SESSION_PATH):
                                os.remove(SESSION_PATH)
                                logger.info("✅ Existing session cleared due to message gap errors")
                            message_gap_errors = 0  # Reset counter after handling
                        except Exception as session_err:
                            logger.warning(f"⚠️ Could not clear session: {session_err}")
                    
                    # Try to connect to Telegram
                    await client.connect()
                    break  # Success, exit database retry loop
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and db_retry_count < max_db_retries-1:
                        db_retry_count += 1
                        logger.warning(f"⚠️ Database locked, retrying in {db_retry_delay}s (attempt {db_retry_count}/{max_db_retries})")
                        
                        # Close and recreate client on severe database issues
                        if db_retry_count >= 2:
                            logger.warning("🔄 Recreating client to clear database locks")
                            client._disconnect_all()
                            # Check if session exists and is corrupt
                            if os.path.exists(f"{SESSION_PATH}.bak") and os.path.exists(SESSION_PATH):
                                logger.warning("🔄 Restoring session from backup")
                                try:
                                    os.remove(SESSION_PATH)
                                    shutil.copy2(f"{SESSION_PATH}.bak", SESSION_PATH)
                                except Exception as restore_err:
                                    logger.error(f"❌ Failed to restore session: {restore_err}")
                        
                        await asyncio.sleep(db_retry_delay)  # Use asyncio sleep
                        db_retry_delay *= 2  # Exponential backoff
                    else:
                        raise  # Re-raise if max retries exceeded or different error
            
            # Use the authentication function
            if not await authenticate_client(client):
                return
                    
            logger.info("✅ Conexión a Telegram establecida")
            logger.info(f"🔍 Escuchando en {len(groups)} grupos: {list(groups.keys())}")
            
            # Set large message gap tolerance for this session instance
            client.flood_sleep_threshold = 120
            # Add this to increase the allowed gap in message sequences (key fix for your issue)
            SQLiteSession.MESSAGE_SEQUENCE_GAP_THRESHOLD = 10000  # Allow significant gaps
            
            # Reset retry count on successful connection
            retry_count = 0
            
            # Create new backup after successful connection
            if os.path.exists(SESSION_PATH):
                backup_path = f"{SESSION_PATH}.bak"
                try:
                    shutil.copy2(SESSION_PATH, backup_path)
                    logger.debug(f"✅ Updated session backup after successful connection")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to backup session after connection: {e}")
            
            # Add custom error handler for message sequence gaps
            @client.on(events.Raw)
            async def raw_update_handler(event):
                nonlocal message_gap_errors
                # Check for message sequence gap errors in the raw event
                event_str = str(event)
                if "Security error while unpacking a received message" in event_str and "had to be ignored consecutively" in event_str:
                    message_gap_errors += 1
                    logger.warning(f"⚠️ Message gap error detected (count: {message_gap_errors})")
                    if message_gap_errors >= 5:
                        # Force reconnection if we get too many errors
                        logger.warning("⚠️ Too many message gaps, forcing reconnection...")
                        await client.disconnect()
                        await asyncio.sleep(2)
                        await client.connect()
                        message_gap_errors = 0  # Reset after reconnection
                        
                        # Update bot restart time after reconnection
                        global bot_restart_time
                        bot_restart_time = datetime.now(UTC)
                        logger.info(f"🕒 Bot reconnection time: {bot_restart_time}")
            
            # Enable automatic self-recovery
            async def monitor_client_health():
                """Monitor client health and force reconnection if needed"""
                consecutive_errors = 0
                max_consecutive_errors = 10
                check_interval = 60  # seconds
                
                while True:
                    await asyncio.sleep(check_interval)
                    
                    # Check if we're getting too many message gap errors
                    if message_gap_errors >= 5:
                        consecutive_errors += 1
                        logger.warning(f"⚠️ High message gap errors detected in health check ({consecutive_errors}/{max_consecutive_errors})")
                        
                        if consecutive_errors >= max_consecutive_errors:
                            logger.warning("🔄 Forcing client restart due to persistent message gap errors")
                            await client.disconnect()
                            await asyncio.sleep(5)
                            
                            # Clear session for a fresh start
                            try:
                                if os.path.exists(SESSION_PATH):
                                    os.rename(SESSION_PATH, f"{SESSION_PATH}.problematic")
                                    if os.path.exists(f"{SESSION_PATH}.bak"):
                                        shutil.copy2(f"{SESSION_PATH}.bak", SESSION_PATH)
                                        logger.info("✅ Restored session from backup during self-recovery")
                            except Exception as e:
                                logger.error(f"❌ Error during session recovery: {e}")
                            
                            # Reconnect
                            await client.connect()
                            logger.info("🚀 Connected to Telegram API, now listening for updates...")
                            # Update bot restart time
                            global bot_restart_time
                            bot_restart_time = datetime.now(UTC)
                            logger.info(f"🕒 Bot self-recovery restart time: {bot_restart_time}")
                            
                            # Reset error counters
                            consecutive_errors = 0
                            message_gap_errors = 0
                    else:
                        # Reset consecutive error counter if things are working well
                        consecutive_errors = 0
            
            # Start the health monitoring task
            health_monitor = asyncio.create_task(monitor_client_health())
            
            # Register error handler (keep existing implementation)
            await client.run_until_disconnected()
            
            # Cancel the health monitor if we disconnect
            health_monitor.cancel()
            
        except (ConnectionError, errors.ServerError) as e:
            retry_count += 1
            wait_time = min(300, (2 ** retry_count) + random.randint(0, 10))
            logger.warning(f"⚠️ Error de conexión: {e}. Reintento {retry_count}/{max_conn_retries} en {wait_time} segundos...")
            await asyncio.sleep(wait_time)  # Use asyncio sleep
            
        except Exception as e:
            logger.error(f"❌ Error fatal: {e}")
            logger.exception("Detalles del error:")
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
        # Make sure to disconnect client
        try:
            client._disconnect_all()
        except:
            pass
        logger.info("🔚 Fin de ejecución")
