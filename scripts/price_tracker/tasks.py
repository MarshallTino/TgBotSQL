import os
import logging
import requests
from datetime import datetime
from celery import Celery
from scripts.utils.db_mongo import connect_mongodb
from scripts.utils.db_postgres import connect_postgres

# Set up logger
logger = logging.getLogger(__name__)

# DexScreener API base URL
DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex"

# Create Celery app
app = Celery("price_tracker")

def get_all_tracked_tokens():
    """
    Get all tokens that need price tracking from PostgreSQL.
    Returns token_id, chain, address, and best_pair_address if available.
    """
    try:
        conn = connect_postgres()
        if not conn:
            logger.error("Failed to connect to PostgreSQL")
            return None
            
        cursor = conn.cursor()
        # Get token data including best_pair_address if available
        cursor.execute("""
                SELECT 
                    token_id, blockchain as chain, contract_address as address, best_pair_address
                FROM tokens 
                WHERE contract_address IS NOT NULL
            """)
        
        tokens = cursor.fetchall()
        
        if tokens:
            logger.info(f"✅ Found {len(tokens)} tokens for price tracking")
            if len(tokens) > 2:
                logger.info(f"🪙 Sample tokens: {tokens[0]}, {tokens[1]}, {tokens[2]}")
        
        cursor.close()
        conn.close()
        return tokens
    except Exception as e:
        logger.error(f"Error fetching tracked tokens: {str(e)}")
        return None

def update_token_best_pair(conn, token_id, pair_address):
    """Update the best pair address for a token in the tokens table."""
    try:
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
        
        if updated:
            logger.info(f"✅ Updated best pair address for token {token_id} to {pair_address}")
        
        return updated
    except Exception as e:
        logger.error(f"Error updating token best pair: {str(e)}")
        conn.rollback()
        return False

def select_best_pair(pairs, token_id=None, stored_pair_address=None):
    """
    Select the best pair from a list of pairs for a token.
    
    Priority order:
    1. Use the stored pair address from tokens table if available and still exists
    2. Choose the pair with highest liquidity
    
    Returns the selected pair object.
    """
    if not pairs:
        return None
    
    # Check if we have a stored pair address and if it's in the new data
    if stored_pair_address:
        for pair in pairs:
            if pair.get('pairAddress', '').lower() == stored_pair_address.lower():
                logger.info(f"Using stored pair {stored_pair_address} for token_id {token_id}")
                return pair
    
    # Otherwise sort by liquidity (descending) and return highest
    sorted_pairs = sorted(
        pairs,
        key=lambda x: float(x.get('liquidity', {}).get('usd', 0) or 0),
        reverse=True
    )
    
    if sorted_pairs:
        logger.info(f"Selected new best pair {sorted_pairs[0].get('pairAddress')} for token_id {token_id}")
        return sorted_pairs[0]
    
    return None

def insert_price_metrics(conn, token_id, pair, mongo_id=None):
    """Insert price metrics into PostgreSQL for a given token and pair."""
    if not conn or not pair:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Extract data from pair according to the DB schema
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
        
        # Insert the data
        cursor.execute("""
            INSERT INTO price_metrics
                (token_id, pair_address, timestamp, price_native, price_usd,
                txns_buys, txns_sells, volume, liquidity_base, liquidity_quote,
                liquidity_usd, fdv, market_cap, mongo_id)
            VALUES
                (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            token_id, pair_address, price_native, price_usd,
            txns_buys, txns_sells, volume, liquidity_base, liquidity_quote,
            liquidity_usd, fdv, market_cap, mongo_id
        ))
        
        conn.commit()
        cursor.close()
        
        # Also update the best pair address in tokens table if needed
        update_token_best_pair(conn, token_id, pair_address)
        
        return True
    except Exception as e:
        logger.error(f"Error inserting price metrics for token {token_id}: {str(e)}")
        conn.rollback()
        return False

@app.task
def update_all_token_prices():
    """
    Task to fetch token prices from DexScreener API and store in MongoDB/PostgreSQL.
    Uses the best_pair_address from tokens table for consistency.
    """
    try:
        # Get all tracked tokens WITH their best pair addresses
        conn = connect_postgres()
        if not conn:
            logger.error("Failed to connect to PostgreSQL")
            return "Failed to connect to PostgreSQL"
            
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                token_id, blockchain as chain, contract_address as address, best_pair_address
            FROM tokens 
            WHERE contract_address IS NOT NULL
        """)
        
        tokens = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not tokens:
            return "No tokens found for price updates"
        
        logger.info(f"Found {len(tokens)} tokens to update prices")
        
        # Process tokens in batches of 30 (DexScreener's API limit)
        batch_size = 30
        token_batches = []
        
        for i in range(0, len(tokens), batch_size):
            batch = tokens[i:i+batch_size]
            token_batches.append(batch)
        
        # Create mapping of token addresses to token_ids and best pair addresses
        token_info_map = {}
        for token in tokens:
            if token[2]:  # contract_address
                # Store token_id and best_pair_address
                token_info_map[token[2].lower()] = {
                    'token_id': token[0],
                    'best_pair_address': token[3] if len(token) > 3 else None
                }
        
        # Enhanced token_id_map that includes best_pair_address
        token_info_map = {}
        for token in tokens:
            if token[2]:  # token[2] is contract_address
                # Map structure: address -> (token_id, best_pair_address)
                token_info_map[token[2].lower()] = {
                    'token_id': token[0],
                    'best_pair_address': token[3] if len(token) > 3 else None
                }
        
        # Process each batch
        documents_stored = 0
        for batch in token_batches:
            # Extract addresses for this batch
            addresses = [token[2] for token in batch if token[2]]
            
            # Make the API request for this batch
            url = f"{DEXSCREENER_BASE_URL}/tokens/{','.join(addresses)}"
            logger.info(f"==> DexScreener tokens batch request: {url}")
            
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Store raw response in MongoDB with enhanced token info
                mongo_client = connect_mongodb()
                if mongo_client:
                    collection = mongo_client[os.getenv('MONGO_DB', 'tgbot_db')]['dexscreener_data']
                    mongo_doc = {
                        "raw_data": data,
                        "fetched_at": datetime.now(),
                        "token_addresses": addresses,
                        "token_info_map": token_info_map,  # Store the enhanced mapping
                        "processed": False
                    }
                    result = collection.insert_one(mongo_doc)
                    documents_stored += 1
                    logger.info(f"✅ Stored data in MongoDB with ID: {result.inserted_id}")
                    mongo_client.close()
                
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                continue
        
        # Trigger the processing task asynchronously
        process_mongodb_data.delay()
        
        return f"Fetched and stored data for {documents_stored} batches in MongoDB"
    except Exception as e:
        logger.error(f"Error in update_all_token_prices: {str(e)}")
        return f"Error: {str(e)}"

@app.task(bind=True, max_retries=3)
def process_mongodb_data(self):
    """
    Task 2: Process unprocessed data from MongoDB and store in PostgreSQL.
    This task performs the actual data transformation and storage.
    """
    try:
        logger.info("🔍 Starting process_mongodb_data task")
        
        # Connect to MongoDB
        mongo_client = connect_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            return "Failed to connect to MongoDB"
            
        # Get the collection
        db_name = os.getenv('MONGO_DB', 'tgbot_db')
        collection_name = os.getenv('MONGO_COLLECTION_NAME', 'dexscreener_data')
        db = mongo_client[db_name]
        collection = db[collection_name]
        
        # Check unprocessed documents
        total_count = collection.count_documents({})
        unprocessed_count = collection.count_documents({'processed': False})
        
        logger.info(f"📊 Total documents in collection: {total_count}")
        logger.info(f"📊 Unprocessed documents: {unprocessed_count}")
        
        # If no unprocessed documents, just exit
        if unprocessed_count == 0:
            return "No documents to process"
        
        # Find unprocessed documents (limit to 10 at a time for processing chunks)
        unprocessed = collection.find({'processed': False}).limit(10)
        
        tokens_processed = 0
        docs_processed = 0
        processed_token_ids = set()  # Track which token IDs were processed
        
        for doc in unprocessed:
            doc_id = doc.get('_id')
            logger.info(f"📄 Processing document ID: {doc_id}")
            
            # Skip documents with no raw_data
            if 'raw_data' not in doc or 'pairs' not in doc['raw_data'] or not doc['raw_data']['pairs']:
                logger.warning(f"Document {doc_id} has no valid pairs data, marking as processed")
                collection.update_one({'_id': doc_id}, {'$set': {'processed': True}})
                docs_processed += 1
                continue
                
            pairs = doc['raw_data']['pairs']
            
            # Get token info map - either from document or rebuild it
            token_info_map = doc.get('token_info_map', {})
            if not token_info_map:
                # Rebuild mapping from token addresses - this is a fallback
                conn = connect_postgres()
                if conn:
                    cursor = conn.cursor()
                    token_info_map = {}
                    
                    # Extract unique base token addresses from pairs
                    base_addresses = set()
                    for pair in pairs:
                        if 'baseToken' in pair and 'address' in pair['baseToken']:
                            base_addresses.add(pair['baseToken']['address'].lower())
                    
                    # Look up token IDs and best pairs for these addresses
                    for address in base_addresses:
                        cursor.execute("""
                            SELECT token_id, best_pair_address 
                            FROM tokens 
                            WHERE LOWER(contract_address) = %s
                        """, (address,))
                        
                        result = cursor.fetchone()
                        if result:
                            token_info_map[address] = {
                                'token_id': result[0],
                                'best_pair_address': result[1]
                            }
                    
                    cursor.close()
                    conn.close()
            
            # Group pairs by token address
            pairs_by_token = {}
            for pair in pairs:
                if 'baseToken' in pair and 'address' in pair['baseToken']:
                    base_address = pair['baseToken']['address'].lower()
                    if base_address not in pairs_by_token:
                        pairs_by_token[base_address] = []
                    pairs_by_token[base_address].append(pair)
            
            # Now process each token's pairs
            conn = connect_postgres()
            if not conn:
                logger.error("Failed to connect to PostgreSQL")
                continue
                
            for token_address, token_pairs in pairs_by_token.items():
                # Skip if we don't have a token_id for this address
                if token_address not in token_info_map:
                    continue
                
                token_info = token_info_map[token_address]
                token_id = token_info['token_id']
                best_pair_address = token_info.get('best_pair_address')
                
                # Select the best pair for this token (using stored pair address if available)
                best_pair = select_best_pair(token_pairs, token_id, best_pair_address)
                
                if best_pair:
                    try:
                        # Insert price data
                        if insert_price_metrics(conn, token_id, best_pair, str(doc_id)):
                            tokens_processed += 1
                            processed_token_ids.add(token_id)
                    except Exception as e:
                        logger.error(f"Error processing token {token_id}: {str(e)}")
                        # Continue to next token even if this one fails
            
            conn.close()
            
            # Mark document as processed
            collection.update_one({'_id': doc_id}, {'$set': {'processed': True}})
            logger.info(f"✅ Marked document {doc_id} as processed")
            docs_processed += 1
        
        mongo_client.close()
        
        # Log summary of which tokens were processed
        logger.info(f"Processed tokens: {sorted(list(processed_token_ids))}")
        
        # If there are more unprocessed documents, trigger another task
        if unprocessed_count > 10:
            process_mongodb_data.delay()
        
        return f"Processed {tokens_processed} tokens from {docs_processed} MongoDB documents"
    except Exception as e:
        logger.error(f"Error in process_mongodb_data: {str(e)}")
        self.retry(countdown=30, exc=e)

@app.task
def process_token_batch(token_batch):
    """
    Process a batch of tokens directly, bypassing MongoDB storage.
    This is for immediate price updates for specific tokens.
    
    token_batch: List of token_id values to update
    """
    try:
        # Connect to PostgreSQL
        conn = connect_postgres()
        if not conn:
            logger.error("Failed to connect to PostgreSQL")
            return "Failed to connect to PostgreSQL"
        
        # Get token data for this batch
        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(token_batch))
        cursor.execute(f"""
            SELECT token_id, blockchain, contract_address, best_pair_address
            FROM tokens
            WHERE token_id IN ({placeholders})
            AND contract_address IS NOT NULL
        """, token_batch)
        
        tokens = cursor.fetchall()
        cursor.close()
        
        if not tokens:
            return f"No valid tokens found in batch"
        
        # Group tokens by batches of 30 for DexScreener API
        addresses = [token[2] for token in tokens if token[2]]
        
        # Create token info mapping
        token_info_map = {}
        for token in tokens:
            if token[2]:  # token[2] is contract_address
                token_info_map[token[2].lower()] = {
                    'token_id': token[0],
                    'best_pair_address': token[3] if len(token) > 3 else None
                }
        
        # Make batches of 30 addresses
        address_batches = []
        for i in range(0, len(addresses), 30):
            address_batches.append(addresses[i:i+30])
        
        # Process each batch
        processed_tokens = 0
        for address_batch in address_batches:
            url = f"{DEXSCREENER_BASE_URL}/tokens/{','.join(address_batch)}"
            logger.info(f"==> DexScreener direct batch request: {url}")
            
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if 'pairs' not in data or not data['pairs']:
                    continue
                
                pairs = data['pairs']
                
                # Group pairs by token address
                pairs_by_token = {}
                for pair in pairs:
                    if 'baseToken' in pair and 'address' in pair['baseToken']:
                        base_address = pair['baseToken']['address'].lower()
                        if base_address not in pairs_by_token:
                            pairs_by_token[base_address] = []
                        pairs_by_token[base_address].append(pair)
                
                # Process each token's pairs
                for token_address, token_pairs in pairs_by_token.items():
                    # Skip if we don't have token info for this address
                    if token_address not in token_info_map:
                        continue
                    
                    token_info = token_info_map[token_address]
                    token_id = token_info['token_id']
                    best_pair_address = token_info.get('best_pair_address')
                    
                    # Select the best pair for this token
                    best_pair = select_best_pair(token_pairs, token_id, best_pair_address)
                    
                    if best_pair:
                        # Insert price data
                        if insert_price_metrics(conn, token_id, best_pair):
                            processed_tokens += 1
                
            except Exception as e:
                logger.error(f"Error processing direct batch: {e}")
                continue
        
        conn.close()
        return f"Directly processed {processed_tokens} tokens"
    except Exception as e:
        logger.error(f"Error in process_token_batch: {e}")
        return f"Error: {str(e)}"

@app.task
def update_token_batch(token_ids):
    """
    Update prices for a specific batch of tokens by ID.
    This is useful for immediate price updates for specific tokens.
    """
    return process_token_batch(token_ids)
