import os
import logging
import requests
import time
from datetime import datetime, timezone
from bson.objectid import ObjectId
from celery.schedules import crontab
from scripts.utils.db_mongo import connect_mongodb
from scripts.utils.db_postgres import (
    get_all_tracked_tokens,
    get_db_connection,
    get_connection,
    insert_price_metrics_from_pair_data,
    release_connection,
    update_token_best_pair,
    get_token_by_address,
    update_token_failure_count,
    select_best_pair
)
from scripts.price_tracker.tasks_logging import (
    log_batch_summary, start_new_cycle, track_token_failure,
    track_api_call, print_box, stats, persistent_failures, failure_counts
)
from scripts.price_tracker.celery_app import app
from collections import defaultdict
import hashlib
import random

logger = logging.getLogger(__name__)
DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex"

_batch_counters = {}

def generate_batch_id(blockchain=None, size=None):
    """Generate a unique batch ID."""
    timestamp = datetime.now().strftime('%H%M%S')
    key = f"{blockchain or 'multi'}"
    if key not in _batch_counters:
        _batch_counters[key] = 0
    _batch_counters[key] += 1
    random_part = hashlib.md5(f"{random.randint(1000,9999)}".encode()).hexdigest()[:3]
    return f"B{timestamp}-{key[:3]}{_batch_counters[key]:02d}-{size or '0'}-{random_part}"

@app.task
def process_token_batch(batch_size_or_ids=30):
    """Process a batch of tokens with improved connection handling"""
    batch_start = time.time()
    
    # Generate batch ID
    blockchain = None
    if isinstance(batch_size_or_ids, list) and batch_size_or_ids:
        size = len(batch_size_or_ids)
    else:
        size = batch_size_or_ids
    batch_id = generate_batch_id(blockchain, size)
    
    success_tokens = []
    failures = defaultdict(list)
    
    try:
        # Use the context manager for connection handling
        with get_db_connection() as conn:
            tokens = []
            if isinstance(batch_size_or_ids, list):
                token_ids_str = ','.join(str(id) for id in batch_size_or_ids)
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT token_id, blockchain, contract_address, best_pair_address 
                        FROM tokens 
                        WHERE token_id IN (%s) AND best_pair_address IS NOT NULL
                    """ % token_ids_str)  # Safe since we're joining integers
                    tokens = cursor.fetchall()
            else:
                tokens = get_all_tracked_tokens()[:batch_size_or_ids]
            
            if not tokens:
                logger.info("No tokens found for processing")
                return "No tokens found for processing"
            
            total_tokens = len(tokens)
            tokens_by_blockchain = defaultdict(list)
            for token_id, blockchain, contract_address, pair_address in tokens:
                if not pair_address:
                    failures["No pair address"].append(token_id)
                    track_token_failure(token_id, contract_address, blockchain, "No pair address")
                    # Ensure failure count is updated in database
                    update_token_failure_count(token_id)
                    continue
                tokens_by_blockchain[blockchain.lower()].append({
                    'token_id': token_id,
                    'contract_address': contract_address,
                    'pair_address': pair_address
                })
        
        # Process tokens by blockchain
        all_pairs_data = []
        batch_size = 30
        for blockchain, blockchain_tokens in tokens_by_blockchain.items():
            for i in range(0, len(blockchain_tokens), batch_size):
                batch = blockchain_tokens[i:i+batch_size]
                pair_addresses = [token['pair_address'] for token in batch]
                url = f"{DEXSCREENER_BASE_URL}/pairs/{blockchain}/{','.join(pair_addresses)}"
                
                try:
                    response = requests.get(url, timeout=15)
                    track_api_call(response.status_code == 200)
                    if response.status_code == 200:
                        data = response.json()
                        if 'pairs' in data and data['pairs']:
                            pairs_by_address = {pair.get('pairAddress', '').lower(): pair for pair in data['pairs']}
                            for token in batch:
                                pair_address = token['pair_address'].lower()
                                token_id = token['token_id']
                                if pair_address in pairs_by_address:
                                    pair = pairs_by_address[pair_address]
                                    # Use the centralized function instead of the local one
                                    insert_price_metrics_from_pair_data(token_id, pair)
                                    success_tokens.append(token_id)
                                    
                                    # Reset failure count on success
                                    with get_db_connection() as conn:
                                        with conn.cursor() as cursor:
                                            cursor.execute("""
                                                UPDATE tokens 
                                                SET failed_updates_count = 0
                                                WHERE token_id = %s
                                            """, (token_id,))
                                else:
                                    error_msg = "No matching pair in data"
                                    track_token_failure(token['token_id'], token['contract_address'], blockchain, error_msg)
                                    failures[error_msg].append(token['token_id'])
                                    # Ensure failure count is updated in database
                                    update_token_failure_count(token['token_id'])
                        else:
                            error_msg = "Empty pairs data" 
                            for token in batch:
                                track_token_failure(token['token_id'], token['contract_address'], blockchain, error_msg)
                                failures[error_msg].append(token['token_id'])
                                # Ensure failure count is updated in database
                                update_token_failure_count(token['token_id'])
                    else:
                        error_msg = f"API error {response.status_code}"
                        for token in batch:
                            track_token_failure(token['token_id'], token['contract_address'], blockchain, error_msg)
                            failures[error_msg].append(token['token_id'])
                            # Ensure failure count is updated in database
                            update_token_failure_count(token['token_id'])
                except Exception as e:
                    logger.error("Batch API error: %s", str(e))
                    error_msg = str(e)[:30]
                    for token in batch:
                        track_token_failure(token['token_id'], token['contract_address'], blockchain, error_msg)
                        failures[f"Exception {error_msg}"].append(token['token_id'])
                        # Ensure failure count is updated in database
                        update_token_failure_count(token['token_id'])
        
        # Store data in MongoDB if needed
        if all_pairs_data:
            mongo_client = connect_mongodb()
            if (mongo_client):
                try:
                    db_name = os.getenv('MONGO_DB', 'tgbot_db')
                    collection_name = os.getenv('MONGO_COLLECTION', 'dexscreener_data')
                    collection = mongo_client[db_name][collection_name]
                    
                    result = collection.insert_many(all_pairs_data)
                    doc_ids = result.inserted_ids
                    
                    for i in range(0, len(doc_ids), 25):
                        process_mongodb_data.delay([str(doc_id) for doc_id in doc_ids[i:i+25]])
                finally:
                    mongo_client.close()
        
        # Update our stats using the tracked failures
        for error_type, failed_token_ids in failures.items():
            for token_id in failed_token_ids:
                failure_counts[token_id] = failure_counts.get(token_id, 0) + 1
                
                # Add to persistent failures with timestamp
                persistent_failures[token_id] = {
                    'count': failure_counts[token_id],
                    'last_error': error_type,
                    'last_seen': datetime.now()
                }
                
                # Only log individual failures if they've failed multiple times
                if failure_counts[token_id] >= 3:
                    logger.warning("Persistent token failure: ID %s (%d failures)", token_id, failure_counts[token_id])
        
        # Calculate duration and stats
        duration = max(0.01, time.time() - batch_start)  # Prevent negative values
        failure_count = sum(len(tokens) for tokens in failures.values())
        
        # Update global stats
        stats['tokens']['succeeded'] += len(success_tokens)
        stats['tokens']['failed'] += failure_count
        
        # Create summary for logging
        summary = [
            "Batch %s" % batch_id,
            "Processed: %d tokens" % total_tokens,
            "Succeeded: %d" % len(success_tokens),
            "Failed: %d" % failure_count,
            "Time: %.2fs" % duration,
        ]
        
        if failures:
            summary.append("")
            summary.append("Errors:")
            for error_type, failed_ids in failures.items():
                summary.append("  • %s: %d tokens" % (error_type, len(failed_ids)))
        
        print_box("Batch Summary", summary, icon="📦")
        
        return "Batch %s: %d/%d success" % (batch_id, len(success_tokens), total_tokens)
        
    except Exception as e:
        logger.error("Error in batch %s: %s", batch_id, str(e), exc_info=True)
        return "Error in batch %s: %s" % (batch_id, str(e))

@app.task
def update_all_token_prices():
    """
    Fetch token info from PostgreSQL, request DexScreener data for each batch,
    store it in MongoDB as a buffer, and then schedule processing task.
    """
    logger.info("🔄 Starting update_all_token_prices task")
    start_time = time.time()
    
    # Step 1: Fetch tokens from PostgreSQL
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT token_id, blockchain, contract_address, best_pair_address
                    FROM tokens
                    WHERE best_pair_address IS NOT NULL 
                    ORDER BY token_id
                """)
                tokens = cursor.fetchall()
    except Exception as e:
        logger.error("Error querying tokens from PostgreSQL: %s", str(e))
        return "Error: %s" % str(e)
    
    if not tokens:
        logger.warning("No tokens found for price tracking")
        return "No tokens found for price tracking"
    
    # Start new cycle with proper token count
    from scripts.price_tracker.tasks_logging import start_new_cycle
    start_new_cycle(len(tokens))
    
    # Step 2: Request data from DexScreener and store in MongoDB
    mongo_client = connect_mongodb()
    if not mongo_client:
        logger.error("Failed to connect to MongoDB")
        return "MongoDB connection error"
    
    batch_size = 30
    tokens_by_chain = defaultdict(list)
    for t in tokens:
        token_id, blockchain, contract_address, pair_address = t
        tokens_by_chain[blockchain.lower()].append({
            'token_id': token_id,
            'contract_address': contract_address,
            'pair_address': pair_address
        })
    
    total_batches = 0
    raw_data_docs = []
    
    try:
        for blockchain, chain_tokens in tokens_by_chain.items():
            for i in range(0, len(chain_tokens), batch_size):
                batch = chain_tokens[i:i+batch_size]
                pair_addresses = [tok['pair_address'] for tok in batch]
                url = f"{DEXSCREENER_BASE_URL}/pairs/{blockchain}/{','.join(pair_addresses)}"
                
                try:
                    response = requests.get(url, timeout=15)
                    from scripts.price_tracker.tasks_logging import track_api_call
                    track_api_call(response.status_code == 200, blockchain)
                    
                    raw_doc = {
                        "created_at": datetime.utcnow(),
                        "blockchain": blockchain,
                        "tokens": batch, 
                        "processed": False,
                        "raw_data": {}
                    }
                    
                    if response.status_code == 200:
                        data = response.json()
                        raw_doc["raw_data"] = data
                    else:
                        # Track failures for each token in this batch
                        from scripts.price_tracker.tasks_logging import track_token_failure
                        error_msg = f"API error {response.status_code}"
                        for tok in batch:
                            track_token_failure(tok['token_id'], tok['contract_address'], blockchain, error_msg)
                            # Update failure count in database
                            update_token_failure_count(tok['token_id'])
                    
                    raw_data_docs.append(raw_doc)
                    total_batches += 1
                except Exception as inner_ex:
                    # Track failures for entire batch if request error
                    from scripts.price_tracker.tasks_logging import track_token_failure
                    error_msg = str(inner_ex)[:50]
                    for tok in batch:
                        track_token_failure(tok['token_id'], tok['contract_address'], blockchain, error_msg)
                        # Update failure count in database
                        update_token_failure_count(tok['token_id'])
                        
    finally:
        if raw_data_docs:
            # Insert all raw data in one go
            try:
                db_name = os.getenv('MONGO_DB', 'tgbot_db')
                collection_name = os.getenv('MONGO_COLLECTION', 'dexscreener_data')
                collection = mongo_client[db_name][collection_name]
                
                result = collection.insert_many(raw_data_docs)
                inserted_ids = result.inserted_ids
                # Schedule the process_mongodb_data task for these docs
                process_mongodb_data.delay([str(_id) for _id in inserted_ids])
            except Exception as e:
                logger.error("Failed to insert raw data to MongoDB: %s", str(e))
        
        # Always close the client
        mongo_client.close()
    
    duration = time.time() - start_time
    from scripts.price_tracker.tasks_logging import print_box
    summary = [
        "Price Update Summary",
        "Total tokens: %d" % len(tokens),
        "Total batches created: %d" % total_batches,
        "Cycle duration: %.2fs" % duration
    ]
    print_box("Update Initiated", summary, icon="🔄")
    
    return "Queued %d tokens in %d batches" % (len(tokens), total_batches)

@app.task
def process_mongodb_data(doc_ids=None):
    """Process MongoDB documents with proper error handling and stat tracking"""
    from scripts.price_tracker.tasks_logging import track_token_success, track_token_failure
    
    start_time = datetime.now()
    docs_processed = 0
    tokens_succeeded = 0
    tokens_failed = 0
    errors = defaultdict(int)
    
    try:
        # Connect to MongoDB (silently)
        mongo_client = connect_mongodb()
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            return "Failed to connect to MongoDB"
        
        try:
            # Process docs using context manager for safe connection handling
            with get_db_connection() as conn:
                db_name = os.getenv('MONGO_DB', 'tgbot_db')
                collection_name = os.getenv('MONGO_COLLECTION', 'dexscreener_data')
                collection = mongo_client[db_name][collection_name]
                
                # Build query to find unprocessed documents
                query = {'processed': False}
                if doc_ids:
                    if isinstance(doc_ids, list):
                        query['_id'] = {'$in': [ObjectId(doc_id) for doc_id in doc_ids]}
                    else:
                        query['_id'] = ObjectId(doc_ids)
                
                unprocessed = collection.find(query).limit(100)
                
                # Process each document
                for doc in unprocessed:
                    docs_processed += 1
                    
                    if 'tokens' not in doc or 'raw_data' not in doc:
                        continue
                    
                    # Store the MongoDB document ID as a string for PostgreSQL
                    mongo_doc_id = str(doc['_id'])
                    
                    # Process tokens from document
                    for token_data in doc['tokens']:
                        token_id = token_data.get('token_id')
                        contract_address = token_data.get('contract_address', 'unknown')
                        blockchain = doc.get('blockchain', 'unknown')
                        
                        try:
                            # Look for matching pair in raw data
                            if 'pairs' in doc['raw_data'] and doc['raw_data']['pairs']:
                                pair_address = token_data.get('pair_address', '').lower()
                                matching_pair = None
                                
                                for pair in doc['raw_data']['pairs']:
                                    if pair.get('pairAddress', '').lower() == pair_address:
                                        matching_pair = pair
                                        break
                                
                                # Process if matching pair found
                                if matching_pair:
                                    # Pass the MongoDB document ID to the insert function
                                    insert_price_metrics_from_pair_data(token_id, matching_pair, mongo_doc_id)
                                    
                                    # Track success - properly update cycle stats
                                    tokens_succeeded += 1
                                    track_token_success(token_id)
                                    
                                    # Reset failure count on success
                                    with conn.cursor() as cursor:
                                        cursor.execute("""
                                            UPDATE tokens 
                                            SET failed_updates_count = 0
                                            WHERE token_id = %s
                                        """, (token_id,))
                                else:
                                    # Track failure - no matching pair
                                    tokens_failed += 1
                                    error_msg = "No matching pair in data"
                                    track_token_failure(token_id, contract_address, blockchain, error_msg)
                                    
                                    # Update failure count in database
                                    update_token_failure_count(token_id)
                            else:
                                # Track failure - no pairs data
                                tokens_failed += 1
                                error_msg = "No pairs data available"
                                track_token_failure(token_id, contract_address, blockchain, error_msg)
                                
                                # Update failure count in database
                                update_token_failure_count(token_id)
                        except Exception as e:
                            # Track specific errors for each token
                            tokens_failed += 1
                            errors[str(e)[:50]] += 1
                            track_token_failure(token_id, contract_address, blockchain, str(e)[:50])
                            
                            # Update failure count in database
                            update_token_failure_count(token_id)
                    
                    # Mark document as processed
                    collection.update_one({'_id': doc['_id']}, {'$set': {'processed': True}})
                
        finally:
            # Always close MongoDB connection
            mongo_client.close()
        
        # Log processing summary
        duration = max(0.01, (datetime.now() - start_time).total_seconds())  # Prevent negative values
        logger.debug(f"Processed {docs_processed} MongoDB docs: {tokens_succeeded} tokens updated, {tokens_failed} failed")
        
        # Show MongoDB processing box
        if docs_processed > 0:
            summary = [
                f"Processed {docs_processed} docs",
                f"Updated tokens: {tokens_succeeded}",
                f"Failed tokens: {tokens_failed}",
                f"Time: {duration:.2f}s",
            ]
            
            if tokens_failed > 0 and errors:
                error_summary = sorted(errors.items(), key=lambda x: x[1], reverse=True)
                for err, count in error_summary[:3]:
                    summary.append(f"  • {err}: {count} tokens")
            
            print_box("MongoDB Processing", summary, icon="📊")
        
        return f"Processed {docs_processed} docs: {tokens_succeeded} updated, {tokens_failed} failed"
    
    except Exception as e:
        logger.error(f"Error in MongoDB processing: {str(e)}", exc_info=True)
        return f"Error: {str(e)}"

@app.task
def check_database_health():
    """Verify database connections are healthy and reset if needed"""
    from scripts.utils.db_postgres import get_db_connection, reset_connection_pool
    
    try:
        # Test if we can get a working connection
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM tokens")
                count = cursor.fetchone()[0]
                
        logger.info("✅ Database health check passed - %d tokens in database", count)
        return True
        
    except Exception as e:
        logger.error("❌ Database health check failed: %s", str(e))
        
        # Force reset connection pool
        reset_connection_pool()
        return False
    
@app.task
def check_and_finalize_cycles():
    """
    Check if any cycles have been running too long and need to be closed.
    This ensures cycles don't stay open forever if something goes wrong.
    """
    from scripts.price_tracker.tasks_logging import cycle_stats, end_current_cycle
    
    current = cycle_stats['current']
    
    # If a cycle has been running for over 45 seconds, it's probably stuck
    if current['tokens_total'] > 0:
        cycle_duration = (datetime.now() - current['start_time']).total_seconds()
        if cycle_duration > 45:
            logger.warning("Found cycle #%d running for %.1fs - forcing completion", cycle_stats['cycle_count'], cycle_duration)
            end_current_cycle()
            return "Forced cycle completion"
    return "No stuck cycles found"

@app.task
def log_minute_summary():
    """Generate minute summary logs."""
    from scripts.price_tracker.tasks_logging import log_minute_summary as perform_summary
    return perform_summary()

@app.task
def analyze_recurring_failures():
    """Analyze tokens that consistently fail and show all failures."""
    from scripts.price_tracker.tasks_logging import analyze_recurring_failures as perform_analysis
    result = perform_analysis()
    # Always return a success result
    return result

@app.task
def classify_all_tokens():
    """Classify all tokens by relevance and update their update intervals."""
    logger.info("Starting token classification")
    
    try:
        from scripts.utils.db_postgres import classify_token_relevance
        result = classify_token_relevance()
        
        if result:
            return "Successfully classified all tokens"
        else:
            return "Error classifying tokens"
    except Exception as e:
        logger.error(f"Error in classify_all_tokens: {e}")
        return f"Error: {str(e)}"

@app.task
def update_token_prices_by_frequency():
    """
    Update token prices based on their update frequency.
    Selects tokens that are due for an update.
    Processes them in batches by blockchain.
    """
    logger.info("🔄 Starting frequency-based token price updates")
    start_time = time.time()
    
    try:
        # Select tokens due for an update
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT token_id, blockchain, contract_address, best_pair_address
                    FROM tokens
                    WHERE is_active = TRUE
                    AND (last_updated_at IS NULL OR 
                         EXTRACT(EPOCH FROM (NOW() - last_updated_at)) >= update_interval)
                    ORDER BY 
                        CASE 
                            WHEN update_interval <= 30 THEN 1  -- High priority
                            WHEN update_interval <= 300 THEN 2 -- Medium priority
                            ELSE 3                             -- Low priority
                        END,
                        last_updated_at ASC NULLS FIRST
                    LIMIT 150  -- Maximum tokens to process in one run
                """)
                tokens = cursor.fetchall()
    
        if not tokens:
            logger.debug("No tokens due for update")
            return "No tokens due for update"
        
        # Group by blockchain and track tokens to update last_updated_at
        tokens_by_chain = defaultdict(list)
        token_ids_to_update = []
        
        for token_id, blockchain, contract_address, pair_address in tokens:
            if not pair_address:
                # Skip tokens without a pair address
                continue
                
            token_ids_to_update.append(token_id)
            tokens_by_chain[blockchain.lower()].append({
                'token_id': token_id,
                'contract_address': contract_address,
                'pair_address': pair_address
            })
        
        # Start a cycle for tracking
        from scripts.price_tracker.tasks_logging import start_new_cycle
        start_new_cycle(len(token_ids_to_update))
        
        # Process tokens by blockchain in batches of 30
        batch_size = 30
        total_batches = 0
        mongo_client = connect_mongodb()
        
        if not mongo_client:
            logger.error("Failed to connect to MongoDB")
            return "MongoDB connection error"
        
        try:
            for blockchain, chain_tokens in tokens_by_chain.items():
                # Process each blockchain's tokens in batches
                for i in range(0, len(chain_tokens), batch_size):
                    batch = chain_tokens[i:i+batch_size]
                    pair_addresses = [tok['pair_address'] for tok in batch]
                    
                    # Make the API request
                    url = f"{DEXSCREENER_BASE_URL}/pairs/{blockchain}/{','.join(pair_addresses)}"
                    
                    try:
                        response = requests.get(url, timeout=15)
                        from scripts.price_tracker.tasks_logging import track_api_call
                        track_api_call(response.status_code == 200, blockchain)
                        
                        # Create MongoDB document
                        raw_doc = {
                            "created_at": datetime.now(),
                            "blockchain": blockchain,
                            "tokens": batch,
                            "processed": False,
                            "raw_data": {}
                        }
                        
                        if response.status_code == 200:
                            data = response.json()
                            raw_doc["raw_data"] = data
                            # Store in MongoDB
                            db_name = os.getenv('MONGO_DB', 'tgbot_db')
                            collection_name = os.getenv('MONGO_COLLECTION', 'dexscreener_data')
                            collection = mongo_client[db_name][collection_name]
                            result = collection.insert_one(raw_doc)
                            
                            # Schedule processing
                            process_mongodb_data.delay(str(result.inserted_id))
                            total_batches += 1
                            
                            # Reset failed_updates_count for successful tokens
                            with get_db_connection() as conn:
                                with conn.cursor() as cursor:
                                    # Update all tokens in this batch
                                    token_ids = [t['token_id'] for t in batch]
                                    cursor.execute("""
                                        UPDATE tokens 
                                        SET failed_updates_count = 0,
                                            is_active = TRUE
                                        WHERE token_id = ANY(%s)
                                    """, (token_ids,))
                        else:
                            # Handle API error - track failures for each token
                            for token in batch:
                                track_token_failure(token['token_id'], token['contract_address'], blockchain, 
                                                  f"API error {response.status_code}")
                                # Explicitly update failure count in database
                                update_token_failure_count(token['token_id'])
                    
                    except Exception as e:
                        logger.error(f"Error in batch API request: {str(e)}")
                        # Track failures for each token
                        for token in batch:
                            track_token_failure(token['token_id'], token['contract_address'], blockchain, str(e)[:50])
                            # Explicitly update failure count in database
                            update_token_failure_count(token['token_id'])
        
        finally:
            # Update last_updated_at for all processed tokens
            if token_ids_to_update:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            UPDATE tokens 
                            SET last_updated_at = NOW()
                            WHERE token_id = ANY(%s)
                        """, (token_ids_to_update,))
            
            # Close MongoDB connection
            if mongo_client:
                mongo_client.close()
        
        # Log summary
        duration = time.time() - start_time
        from scripts.price_tracker.tasks_logging import print_box
        summary = [
            "Frequency-Based Update Summary",
            f"Total tokens: {len(token_ids_to_update)}",
            f"Total batches created: {total_batches}",
            f"Duration: {duration:.2f}s"
        ]
        print_box("Update Complete", summary, icon="🔄")
        
        return f"Updated {len(token_ids_to_update)} tokens in {total_batches} batches"
    
    except Exception as e:
        logger.error(f"Error in update_token_prices_by_frequency: {e}")
        return f"Error: {str(e)}"

@app.task
def check_and_revive_inactive_tokens():
    """
    Weekly task to check if inactive tokens have recovered.
    Queries DexScreener API for all inactive tokens and reactivates them
    if they now have sufficient liquidity or market cap.
    """
    logger.info("Starting weekly check of inactive tokens")
    
    try:
        # Step 1: Get all inactive tokens
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        token_id, name, ticker, blockchain, contract_address, 
                        best_pair_address, failed_updates_count
                    FROM tokens
                    WHERE is_active = FALSE
                    ORDER BY last_updated_at ASC NULLS FIRST
                    LIMIT 300  -- Process in batches to prevent overload
                """)
                inactive_tokens = cursor.fetchall()
        
        if not inactive_tokens:
            logger.info("No inactive tokens found to check")
            return "No inactive tokens to check"
        
        logger.info(f"Found {len(inactive_tokens)} inactive tokens to check")
        
        # Track stats
        revived_tokens = 0
        tokens_checked = 0
        revival_results = []
        
        # Process tokens in batches by blockchain
        tokens_by_chain = defaultdict(list)
        for token in inactive_tokens:
            token_id, name, ticker, blockchain, contract_address, pair_address, failures = token
            if not contract_address:
                continue
                
            tokens_by_chain[blockchain.lower()].append({
                'token_id': token_id,
                'name': name,
                'ticker': ticker,
                'contract_address': contract_address,
                'pair_address': pair_address
            })
        
        # Check each blockchain's tokens
        for blockchain, chain_tokens in tokens_by_chain.items():
            # Process tokens in smaller batches of 20 to prevent API rate limits
            for i in range(0, len(chain_tokens), 20):
                batch = chain_tokens[i:i+20]
                tokens_checked += len(batch)
                
                # Get contract addresses for this batch
                addresses = [t['contract_address'] for t in batch]
                
                try:
                    # Query DexScreener API for token data
                    url = f"{DEXSCREENER_BASE_URL}/tokens/{blockchain}/{','.join(addresses)}"
                    response = requests.get(url, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Extract and group pairs by token address
                        pairs_by_token = {}
                        if 'pairs' in data and data['pairs']:
                            for pair in data['pairs']:
                                base_address = pair.get('baseToken', {}).get('address', '').lower()
                                if (base_address):
                                    if (base_address) not in pairs_by_token:
                                        pairs_by_token[base_address] = []
                                    pairs_by_token[base_address].append(pair)
                        
                        # Check each token
                        for token in batch:
                            token_address = token['contract_address'].lower()
                            token_id = token['token_id']
                            
                            # Skip if no pairs found
                            if token_address not in pairs_by_token:
                                continue
                                
                            # Find best pair by liquidity
                            token_pairs = pairs_by_token[token_address]
                            best_pair = select_best_pair(token_pairs)
                            
                            if not best_pair:
                                continue
                            
                            # Check if token should be revived based on liquidity and market cap
                            try:
                                liquidity_str = str(best_pair.get('liquidity', {}).get('usd', '0')).replace('$', '').replace(',', '')
                                liquidity = float(liquidity_str)
                                
                                market_cap_str = str(best_pair.get('marketCap', '0')).replace('$', '').replace(',', '')
                                market_cap = float(market_cap_str) if market_cap_str else 0
                                
                                # Token revival criteria: has good liquidity OR market cap
                                if liquidity > 1000 or market_cap > 5000:
                                    # Token has recovered! Update it and reactivate
                                    pair_address = best_pair.get('pairAddress')
                                    
                                    # Save new price metrics
                                    insert_price_metrics_from_pair_data(token_id, best_pair)
                                    
                                    # Update token status
                                    with get_db_connection() as conn:
                                        with conn.cursor() as cursor:
                                            cursor.execute("""
                                                UPDATE tokens
                                                SET is_active = TRUE,
                                                    update_interval = CASE 
                                                        WHEN %s > 10000 OR %s > 50000 THEN 30
                                                        WHEN %s > 1000 OR %s > 5000 THEN 300
                                                        ELSE 3600 
                                                    END,
                                                    best_pair_address = %s,
                                                    failed_updates_count = 0,
                                                    last_updated_at = NOW()
                                                WHERE token_id = %s
                                            """, (liquidity, market_cap, liquidity, market_cap, pair_address, token_id))
                                            
                                    # Update tracking stats
                                    revived_tokens += 1
                                    
                                    # Log revival
                                    revival_detail = f"{token['name']} ({token['ticker']}) - Liquidity: ${liquidity:.2f}, Market Cap: ${market_cap:.2f}"
                                    revival_results.append(revival_detail)
                                    logger.info(f"✅ Revived token {token_id}: {revival_detail}")
                            except (ValueError, TypeError) as e:
                                logger.debug(f"Error processing token {token_id} revival check: {str(e)}")
                                continue
                except Exception as e:
                    logger.error(f"Error checking blockchain {blockchain} tokens: {str(e)}")
                
                # Slight delay to prevent API rate limiting
                time.sleep(1)
        
        # Log summary stats
        summary = [
            f"Inactive tokens checked: {tokens_checked}",
            f"Tokens revived: {revived_tokens}"
        ]
        
        if revived_tokens > 0:
            summary.append("")
            summary.append("Revived tokens:")
            # Show first 10 revived tokens in the log
            for i, result in enumerate(revival_results[:10]):
                summary.append(f"  • {result}")
            
            if len(revival_results) > 10:
                summary.append(f"  • ...and {len(revival_results) - 10} more")
        
        print_box("Token Revival Check", summary, icon="🔄")
        
        return f"Checked {tokens_checked} inactive tokens, revived {revived_tokens}"
            
    except Exception as e:
        logger.error(f"Error in check_and_revive_inactive_tokens: {str(e)}")
        return f"Error: {str(e)}"
