"""
Token recovery utilities to handle problematic tokens that fail to update.
Provides tools to identify, diagnose, and fix tokens with persistent update failures.
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from scripts.utils.db_postgres import (
    get_connection, execute_query, release_connection,
    update_token_best_pair, select_best_pair
)
from scripts.utils.api_clients import get_pairs_data, get_pair_by_address
from scripts.price_tracker.celery_app import app
from scripts.price_tracker.tasks_logging import persistent_failures, failure_counts

# Configure logging
logger = logging.getLogger(__name__)

def get_failing_tokens(min_failures=5, blockchain=None, limit=50, include_inactive=False):
    """
    Get tokens with persistent failures.
    
    Args:
        min_failures (int): Minimum number of failures to include
        blockchain (str): Optional blockchain filter
        limit (int): Maximum number of tokens to return
        include_inactive (bool): Whether to include inactive tokens
        
    Returns:
        list: List of failing tokens with details
    """
    query = """
        SELECT 
            t.token_id, 
            t.name, 
            t.ticker, 
            t.blockchain, 
            t.contract_address,
            t.best_pair_address,
            t.failed_updates_count,
            t.last_updated_at,
            t.is_active
        FROM tokens t
        WHERE t.failed_updates_count >= %s
        {}
        {}
        ORDER BY t.failed_updates_count DESC
        LIMIT %s
    """
    
    params = [min_failures]
    
    # Add blockchain filter if provided
    if blockchain:
        query = query.format("AND t.blockchain = %s", "")
        params.append(blockchain)
    else:
        query = query.format("", "")
    
    # Add active filter if needed
    if not include_inactive:
        if blockchain:
            query = query.format("AND t.blockchain = %s", "AND t.is_active = TRUE")
        else:
            query = query.format("", "AND t.is_active = TRUE")
    
    params.append(limit)
    
    result = execute_query(query, tuple(params), fetch=True)
    
    if not result:
        return []
    
    # Convert to list of dicts for easier processing
    tokens = []
    for row in result:
        tokens.append({
            'token_id': row[0],
            'name': row[1],
            'ticker': row[2],
            'blockchain': row[3],
            'contract_address': row[4],
            'best_pair_address': row[5],
            'failed_updates_count': row[6],
            'last_updated_at': row[7],
            'is_active': row[8]
        })
    
    return tokens

def analyze_failures_by_blockchain():
    """
    Analyze failure patterns by blockchain to identify systematic issues.
    
    Returns:
        dict: Blockchain-specific failure statistics
    """
    query = """
        SELECT 
            blockchain, 
            COUNT(*) as total_tokens,
            COUNT(*) FILTER (WHERE failed_updates_count > 0) as failing_tokens,
            AVG(failed_updates_count) FILTER (WHERE failed_updates_count > 0) as avg_failures,
            MAX(failed_updates_count) as max_failures
        FROM tokens
        GROUP BY blockchain
        ORDER BY failing_tokens DESC
    """
    
    result = execute_query(query, fetch=True)
    
    if not result:
        return {}
    
    blockchain_stats = {}
    for row in result:
        blockchain = row[0]
        blockchain_stats[blockchain] = {
            'total_tokens': row[1],
            'failing_tokens': row[2],
            'avg_failures': round(row[3], 1) if row[3] else 0,
            'max_failures': row[4],
            'failure_rate': round((row[2] / row[1]) * 100, 1) if row[1] > 0 else 0
        }
    
    return blockchain_stats

def recover_token(token_id, force_lookup=False):
    """
    Attempt to recover a failing token by finding alternative trading pairs.
    
    Args:
        token_id (int): ID of the token to recover
        force_lookup (bool): If True, force a new API lookup even for tokens with low failure counts
        
    Returns:
        dict: Result of the recovery attempt
    """
    # Get token details
    query = """
        SELECT 
            token_id, 
            name, 
            ticker, 
            blockchain, 
            contract_address,
            best_pair_address,
            failed_updates_count,
            is_active
        FROM tokens
        WHERE token_id = %s
    """
    
    result = execute_query(query, (token_id,), fetch=True)
    
    if not result:
        return {
            'success': False,
            'message': f'Token with ID {token_id} not found'
        }
    
    token_id, name, ticker, blockchain, contract_address, best_pair_address, failed_count, is_active = result[0]
    
    # Only attempt recovery if token has enough failures or force_lookup is True
    if failed_count < 3 and not force_lookup:
        return {
            'success': False,
            'message': f'Token {name} only has {failed_count} failures, recovery not needed'
        }
    
    # Log recovery attempt
    logger.info(f"🔄 Attempting to recover token {name} ({ticker}) [ID: {token_id}] with {failed_count} failures")
    
    # Different recovery strategies based on blockchain
    pairs = []
    
    # Try to get data using contract address
    pairs = get_pairs_data(blockchain, [contract_address])
    
    # For some blockchains, we might need additional strategies
    if not pairs and blockchain.lower() == 'solana':
        # For Solana, we sometimes have LP addresses instead of token addresses
        lp_address = best_pair_address or contract_address
        pairs = get_pair_by_address(blockchain, lp_address)
    
    # If we found pairs, select the best one
    if pairs and len(pairs) > 0:
        best_pair = select_best_pair(pairs)
        
        if best_pair:
            # Check if pair has liquidity
            liquidity_usd = 0
            try:
                liquidity_str = str(best_pair.get('liquidity', {}).get('usd', '0')).replace('$', '').replace(',', '')
                liquidity_usd = float(liquidity_str)
            except (ValueError, TypeError):
                pass
            
            if liquidity_usd > 0:
                # Found a valid pair with liquidity
                pair_address = best_pair.get('pairAddress')
                
                # Connect to the database with explicit transaction handling
                conn = get_connection()
                if not conn:
                    return {
                        'success': False, 
                        'message': 'Failed to connect to database'
                    }
                
                try:
                    cursor = conn.cursor()
                    
                    # Update token with new pair and reset failure counter
                    cursor.execute("""
                        UPDATE tokens 
                        SET best_pair_address = %s,
                            failed_updates_count = 0,
                            is_active = TRUE,
                            last_updated_at = NOW()
                        WHERE token_id = %s
                    """, (pair_address, token_id))
                    
                    conn.commit()
                    
                    # Also clear in-memory failure tracking
                    if token_id in failure_counts:
                        del failure_counts[token_id]
                    if token_id in persistent_failures:
                        del persistent_failures[token_id]
                    
                    # Return success result with the recovered pair
                    return {
                        'success': True,
                        'message': f'Successfully recovered token {name} with new pair {pair_address[:8]}...',
                        'pair_address': pair_address,
                        'liquidity': liquidity_usd,
                        'dex': best_pair.get('dexId', 'unknown')
                    }
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error during token recovery database update: {e}")
                    return {
                        'success': False,
                        'message': f'Database error during recovery: {str(e)}'
                    }
                finally:
                    release_connection(conn)
    
    # If we reach here, no valid pairs were found
    logger.warning(f"❌ Failed to recover token {name} ({ticker}) [ID: {token_id}] - No valid pairs found")
    
    try:
        # If the token has had many failures, mark it as inactive
        if failed_count > 10:
            deactivate_query = """
                UPDATE tokens 
                SET is_active = FALSE,
                    update_interval = 86400  # Set to daily checks for potential revival
                WHERE token_id = %s AND failed_updates_count > 10
                RETURNING name, ticker
            """
            result = execute_query(deactivate_query, (token_id,), fetch=True)
            
            if result and result[0]:
                token_name, token_ticker = result[0]
                logger.warning(f"🔴 Token DEACTIVATED: {token_name} ({token_ticker}) [ID: {token_id}] - No valid pairs after {failed_count} failures")
            
            return {
                'success': False,
                'message': f'Marked token {name} as inactive after {failed_count} failures with no valid pairs',
                'action_taken': 'deactivated'
            }
    except Exception as e:
        logger.error(f"Error deactivating token: {e}")
    
    return {
        'success': False,
        'message': f'No valid pairs found for token {name} ({ticker})'
    }

def bulk_recover_tokens(min_failures=10, blockchain=None, limit=20):
    """
    Attempt to recover multiple failing tokens.
    
    Args:
        min_failures (int): Minimum number of failures to attempt recovery
        blockchain (str): Optional blockchain filter
        limit (int): Maximum number of tokens to process
        
    Returns:
        dict: Summary of the recovery attempt
    """
    # Get failing tokens
    tokens = get_failing_tokens(min_failures, blockchain, limit)
    
    if not tokens:
        return {
            'success': True,
            'message': f'No tokens with {min_failures}+ failures found',
            'recovered': 0,
            'failed': 0
        }
    
    # Track results
    results = {
        'total': len(tokens),
        'recovered': 0,
        'failed': 0,
        'tokens': []
    }
    
    # Process each token
    for token in tokens:
        start_time = time.time()
        recovery_result = recover_token(token['token_id'])
        elapsed_time = time.time() - start_time
        
        token_result = {
            'token_id': token['token_id'],
            'name': token['name'],
            'ticker': token['ticker'],
            'blockchain': token['blockchain'],
            'failures': token['failed_updates_count'],
            'success': recovery_result['success'],
            'message': recovery_result['message'],
            'time_taken': round(elapsed_time, 2)
        }
        
        if recovery_result['success']:
            results['recovered'] += 1
        else:
            results['failed'] += 1
        
        results['tokens'].append(token_result)
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    # Calculate success rate
    results['success_rate'] = round((results['recovered'] / results['total']) * 100, 1) if results['total'] > 0 else 0
    
    return results

def reset_failures_for_token(token_id):
    """
    Reset the failure counter for a specific token.
    
    Args:
        token_id (int): ID of the token to reset
        
    Returns:
        bool: True if operation was successful, False otherwise
    """
    try:
        query = """
        UPDATE tokens 
        SET failed_updates_count = 0
        WHERE token_id = %s
        RETURNING name, ticker
        """
        
        result = execute_query(query, (token_id,), fetch=True)
        
        if not result:
            logger.warning(f"Token with ID {token_id} not found")
            return False
        
        name, ticker = result[0]
        logger.info(f"✅ Reset failure counter for token {name} ({ticker}) [ID: {token_id}]")
        return True
        
    except Exception as e:
        logger.error(f"Error resetting failure counter: {e}")
        return False

def reset_all_failures(blockchain=None):
    """
    Reset failure counters for all tokens or tokens on a specific blockchain.
    
    Args:
        blockchain (str): Optional blockchain filter
        
    Returns:
        int: Number of tokens reset
    """
    try:
        query = """
        UPDATE tokens 
        SET failed_updates_count = 0
        {}
        RETURNING COUNT(*)
        """
        
        if blockchain:
            query = query.format("WHERE blockchain = %s")
            result = execute_query(query, (blockchain,), fetch=True)
        else:
            query = query.format("")
            result = execute_query(query, fetch=True)
        
        reset_count = result[0][0] if result else 0
        
        if blockchain:
            logger.info(f"✅ Reset failure counters for {reset_count} tokens on {blockchain}")
        else:
            logger.info(f"✅ Reset failure counters for all {reset_count} tokens")
            
        return reset_count
        
    except Exception as e:
        logger.error(f"Error resetting all failure counters: {e}")
        return 0

def diagnose_token(token_id):
    """
    Perform detailed diagnostics on a specific token to identify update issues.
    
    Args:
        token_id (int): ID of the token to diagnose
        
    Returns:
        dict: Diagnostic information about the token
    """
    # Get comprehensive token information
    query = """
    SELECT 
        t.token_id, t.name, t.ticker, t.blockchain, t.contract_address,
        t.best_pair_address, t.dex, t.failed_updates_count, t.is_active,
        t.last_updated_at, t.update_interval,
        (SELECT COUNT(*) FROM price_metrics pm WHERE pm.token_id = t.token_id) as metrics_count,
        (SELECT MAX(pm.timestamp) FROM price_metrics pm WHERE pm.token_id = t.token_id) as last_price_time,
        (SELECT pm.liquidity_usd FROM price_metrics pm 
         WHERE pm.token_id = t.token_id 
         ORDER BY pm.timestamp DESC LIMIT 1) as last_liquidity,
        (SELECT pm.price_usd FROM price_metrics pm 
         WHERE pm.token_id = t.token_id 
         ORDER BY pm.timestamp DESC LIMIT 1) as last_price
    FROM tokens t
    WHERE t.token_id = %s
    """
    
    result = execute_query(query, (token_id,), fetch=True)
    
    if not result:
        return {
            'success': False,
            'message': f'Token with ID {token_id} not found'
        }
    
    row = result[0]
    
    token_info = {
        'token_id': row[0],
        'name': row[1],
        'ticker': row[2],
        'blockchain': row[3],
        'contract_address': row[4],
        'best_pair_address': row[5],
        'dex': row[6],
        'failed_updates_count': row[7],
        'is_active': row[8],
        'last_updated_at': row[9],
        'update_interval': row[10],
        'price_metrics_count': row[11],
        'last_price_time': row[12],
        'last_liquidity': row[13],
        'last_price': row[14]
    }
    
    # Check for API data availability
    api_data = None
    pairs = []
    
    try:
        pairs = get_pairs_data(token_info['blockchain'], [token_info['contract_address']])
        if pairs:
            api_data = {
                'pairs_found': len(pairs),
                'best_pair': None
            }
            
            best_pair = select_best_pair(pairs)
            if best_pair:
                api_data['best_pair'] = {
                    'pair_address': best_pair.get('pairAddress', 'unknown'),
                    'dex': best_pair.get('dexId', 'unknown'),
                    'liquidity_usd': best_pair.get('liquidity', {}).get('usd', 0),
                    'price_usd': best_pair.get('priceUsd', 0),
                    'txns_24h': {
                        'buys': best_pair.get('txns', {}).get('h24', {}).get('buys', 0),
                        'sells': best_pair.get('txns', {}).get('h24', {}).get('sells', 0)
                    }
                }
    except Exception as e:
        api_data = {
            'error': str(e),
            'pairs_found': 0
        }
    
    # Add diagnostic information
    diagnostics = {
        'token_exists': True,
        'metrics_exist': token_info['price_metrics_count'] > 0,
        'active_status': token_info['is_active'],
        'days_since_update': (datetime.now(timezone.utc) - token_info['last_updated_at']).days if token_info['last_updated_at'] else None,
        'has_best_pair': token_info['best_pair_address'] is not None,
        'api_available': api_data is not None and api_data.get('pairs_found', 0) > 0,
        'suggested_action': None
    }
    
    # Determine suggested action based on diagnostics
    if not diagnostics['api_available']:
        diagnostics['suggested_action'] = 'DEACTIVATE'
    elif diagnostics['api_available'] and not diagnostics['active_status']:
        diagnostics['suggested_action'] = 'REACTIVATE'
    elif token_info['failed_updates_count'] > 5:
        if api_data and api_data.get('best_pair'):
            diagnostics['suggested_action'] = 'RESET_AND_UPDATE_PAIR'
        else:
            diagnostics['suggested_action'] = 'RESET_FAILURES'
    else:
        diagnostics['suggested_action'] = 'NO_ACTION'
    
    # Return comprehensive report
    return {
        'success': True,
        'token_info': token_info,
        'api_data': api_data,
        'diagnostics': diagnostics
    }

def reactivate_token(token_id):
    """
    Reactivate a previously deactivated token if it's now showing activity.
    
    Args:
        token_id (int): ID of the token to reactivate
        
    Returns:
        dict: Result of the reactivation attempt
    """
    # First diagnose the token
    diagnosis = diagnose_token(token_id)
    
    if not diagnosis['success']:
        return {
            'success': False,
            'message': f"Couldn't diagnose token: {diagnosis['message']}"
        }
    
    # Check if token is inactive but has API data available
    if (not diagnosis['diagnostics']['active_status'] and 
            diagnosis['diagnostics']['api_available']):
        
        # Get the best pair from API
        best_pair = None
        pair_address = None
        liquidity = 0
        
        if diagnosis['api_data'] and diagnosis['api_data'].get('best_pair'):
            best_pair = diagnosis['api_data']['best_pair']
            pair_address = best_pair['pair_address']
            liquidity = best_pair['liquidity_usd']
        
        # Only reactivate if there's liquidity
        if liquidity > 0:
            try:
                query = """
                UPDATE tokens
                SET is_active = TRUE,
                    failed_updates_count = 0,
                    best_pair_address = COALESCE(%s, best_pair_address),
                    last_updated_at = NOW(),
                    update_interval = CASE
                        WHEN %s > 10000 THEN 300
                        WHEN %s > 1000 THEN 1800
                        ELSE 3600
                    END
                WHERE token_id = %s
                RETURNING name, ticker
                """
                
                result = execute_query(query, (pair_address, liquidity, liquidity, token_id), fetch=True)
                
                if result:
                    name, ticker = result[0]
                    logger.info(f"✅ Reactivated token {name} ({ticker}) [ID: {token_id}] with ${liquidity} liquidity")
                    
                    return {
                        'success': True,
                        'message': f"Successfully reactivated token {name} with ${liquidity} liquidity",
                        'liquidity': liquidity,
                        'pair_address': pair_address
                    }
            except Exception as e:
                logger.error(f"Error reactivating token {token_id}: {e}")
                return {
                    'success': False,
                    'message': f"Error during reactivation: {str(e)}"
                }
    
    return {
        'success': False,
        'message': f"Token not eligible for reactivation: active={diagnosis['diagnostics']['active_status']}, api_available={diagnosis['diagnostics']['api_available']}"
    }

def check_for_inactive_tokens_with_activity(limit=50):
    """
    Find inactive tokens that might have regained activity.
    
    Args:
        limit (int): Maximum number of tokens to check
        
    Returns:
        list: Tokens that could potentially be reactivated
    """
    # Get inactive tokens ordered by most recently updated first
    query = """
    SELECT 
        token_id, name, ticker, blockchain, contract_address, 
        best_pair_address, last_updated_at
    FROM tokens
    WHERE is_active = FALSE
    ORDER BY last_updated_at DESC NULLS LAST
    LIMIT %s
    """
    
    result = execute_query(query, (limit,), fetch=True)
    
    if not result:
        return []
    
    # Results data structure
    recoverable = []
    
    # Check each token with the API
    for row in result:
        token_id, name, ticker, blockchain, address, pair_address, last_updated = row
        
        try:
            # Try to get pairs data
            pairs = get_pairs_data(blockchain, [address])
            
            if pairs and len(pairs) > 0:
                # Find best pair
                best_pair = select_best_pair(pairs)
                
                if best_pair:
                    # Check liquidity
                    liquidity_usd = 0
                    try:
                        liquidity_str = str(best_pair.get('liquidity', {}).get('usd', '0')).replace('$', '').replace(',', '')
                        liquidity_usd = float(liquidity_str)
                    except (ValueError, TypeError):
                        pass
                    
                    if liquidity_usd > 0:
                        # Token has activity and liquidity, add to recoverable
                        recoverable.append({
                            'token_id': token_id,
                            'name': name,
                            'ticker': ticker,
                            'blockchain': blockchain,
                            'liquidity': liquidity_usd,
                            'pair_address': best_pair.get('pairAddress'),
                            'dex': best_pair.get('dexId', 'unknown')
                        })
                        logger.info(f"Found inactive token with activity: {name} ({ticker}) - ${liquidity_usd} liquidity")
                        
                        # Don't spam the API too much
                        time.sleep(0.2)
        except Exception as e:
            logger.warning(f"Error checking token {token_id} ({name}): {e}")
            time.sleep(0.5)  # Longer delay after error
    
    return recoverable

@app.task(name="token_recovery.automatic_token_recovery")
def automatic_token_recovery(min_failures=5, max_tokens=20):
    """
    Celery task to automatically recover failing tokens.
    
    Args:
        min_failures (int): Minimum number of failures to attempt recovery
        max_tokens (int): Maximum number of tokens to process
        
    Returns:
        dict: Summary of the recovery operations
    """
    start_time = time.time()
    
    try:
        # Get blockchain statistics to prioritize
        blockchain_stats = analyze_failures_by_blockchain()
        
        # Prioritize blockchains with highest failure rates
        priority_chains = sorted(
            blockchain_stats.items(),
            key=lambda x: x[1]['failing_tokens'],
            reverse=True
        )
        
        logger.info(f"Starting automatic token recovery for tokens with {min_failures}+ failures")
        logger.info(f"Priority chains: {', '.join([c[0] for c in priority_chains[:3] if c[1]['failing_tokens'] > 0])}")
        
        # Results structure
        results = {
            'tokens_checked': 0,
            'tokens_recovered': 0,
            'tokens_deactivated': 0,
            'blockchain_results': {},
            'execution_time': 0
        }
        
        # Process each blockchain separately
        tokens_per_chain = max(3, max_tokens // len(priority_chains)) if priority_chains else max_tokens
        
        for blockchain, stats in priority_chains:
            # Skip if no failing tokens
            if stats['failing_tokens'] == 0:
                continue
                
            logger.info(f"Processing {blockchain} with {stats['failing_tokens']} failing tokens")
            
            # Get failing tokens for this blockchain
            tokens = get_failing_tokens(
                min_failures=min_failures,
                blockchain=blockchain,
                limit=tokens_per_chain,
                include_inactive=False  # Only active tokens
            )
            
            if not tokens:
                continue
                
            # Track results for this blockchain
            chain_results = {
                'tokens_checked': len(tokens),
                'recovered': 0,
                'failed': 0,
                'deactivated': 0
            }
            
            # Process each token
            for token in tokens:
                # Attempt recovery
                recovery_result = recover_token(token['token_id'])
                results['tokens_checked'] += 1
                
                if recovery_result.get('success', False):
                    chain_results['recovered'] += 1
                    results['tokens_recovered'] += 1
                elif recovery_result.get('action_taken') == 'deactivated':
                    chain_results['deactivated'] += 1
                    results['tokens_deactivated'] += 1
                else:
                    chain_results['failed'] += 1
                
                # Small delay
                time.sleep(0.2)
            
            # Store results for this blockchain
            results['blockchain_results'][blockchain] = chain_results
            
            # Log summary
            logger.info(f"{blockchain} recovery summary: {chain_results['recovered']}/{chain_results['tokens_checked']} tokens recovered, {chain_results['deactivated']} deactivated")
        
        # Overall execution time
        results['execution_time'] = round(time.time() - start_time, 2)
        
        logger.info(f"Automatic token recovery completed in {results['execution_time']}s: {results['tokens_recovered']}/{results['tokens_checked']} tokens recovered")
        return results
        
    except Exception as e:
        logger.error(f"Error in automatic token recovery: {e}")
        return {
            'error': str(e),
            'execution_time': round(time.time() - start_time, 2)
        }

@app.task(name="token_recovery.check_inactive_tokens")
def check_inactive_tokens_task(limit=50):
    """
    Celery task to check for inactive tokens that show signs of activity.
    
    Args:
        limit (int): Maximum number of tokens to check
        
    Returns:
        dict: Summary of the reactivation operations
    """
    start_time = time.time()
    
    try:
        logger.info(f"Checking up to {limit} inactive tokens for signs of activity")
        
        # Find inactive tokens with activity
        potentially_active = check_for_inactive_tokens_with_activity(limit)
        
        if not potentially_active:
            logger.info("No inactive tokens with activity found")
            return {
                'tokens_checked': limit,
                'tokens_reactivated': 0,
                'execution_time': round(time.time() - start_time, 2)
            }
            
        logger.info(f"Found {len(potentially_active)} inactive tokens showing activity")
        
        # Track results
        results = {
            'tokens_checked': limit,
            'tokens_reactivated': 0,
            'reactivated_tokens': [],
            'execution_time': 0
        }
        
        # Reactivate each token
        for token in potentially_active:
            reactivation_result = reactivate_token(token['token_id'])
            
            if reactivation_result.get('success', False):
                results['tokens_reactivated'] += 1
                results['reactivated_tokens'].append({
                    'token_id': token['token_id'],
                    'name': token['name'],
                    'ticker': token['ticker'],
                    'blockchain': token['blockchain'],
                    'liquidity': token.get('liquidity', 0)
                })
            
            # Small delay
            time.sleep(0.2)
        
        # Overall execution time
        results['execution_time'] = round(time.time() - start_time, 2)
        
        logger.info(f"Token reactivation completed: {results['tokens_reactivated']}/{len(potentially_active)} tokens reactivated")
        return results
    
    except Exception as e:
        logger.error(f"Error in check_inactive_tokens_task: {e}")
        return {
            'error': str(e),
            'execution_time': round(time.time() - start_time, 2)
        }

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "diagnose" and len(sys.argv) > 2:
            token_id = int(sys.argv[2])
            result = diagnose_token(token_id)
            print(f"Diagnostic results for token {token_id}:")
            print(result)
            
        elif command == "recover" and len(sys.argv) > 2:
            token_id = int(sys.argv[2])
            result = recover_token(token_id, force_lookup=True)
            print(f"Recovery result for token {token_id}:")
            print(result)
            
        elif command == "analyze":
            result = analyze_failures_by_blockchain()
            print("Failure analysis by blockchain:")
            for blockchain, stats in result.items():
                print(f"- {blockchain}: {stats['failing_tokens']}/{stats['total_tokens']} ({stats['failure_rate']}%) failing")
                
        elif command == "bulk-recover":
            min_failures = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            blockchain = sys.argv[3] if len(sys.argv) > 3 else None
            result = bulk_recover_tokens(min_failures, blockchain)
            print(f"Bulk recovery results: {result['recovered']}/{result['total']} tokens recovered")
            
        elif command == "reactivate" and len(sys.argv) > 2:
            token_id = int(sys.argv[2])
            result = reactivate_token(token_id)
            print(f"Reactivation result for token {token_id}:")
            print(result)
            
        elif command == "check-inactive":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
            result = check_for_inactive_tokens_with_activity(limit)
            print(f"Found {len(result)} inactive tokens with activity")
    else:
        print("Usage:")
        print("  python token_recovery.py diagnose <token_id>")
        print("  python token_recovery.py recover <token_id>")
        print("  python token_recovery.py analyze")
        print("  python token_recovery.py bulk-recover [min_failures] [blockchain]")
        print("  python token_recovery.py reactivate <token_id>")
        print("  python token_recovery.py check-inactive [limit]")
