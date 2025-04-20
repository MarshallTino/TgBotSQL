#!/usr/bin/env python3
"""
Token Failure Monitor

This script helps monitor and verify how the system handles token update failures,
particularly checking if tokens are properly marked inactive after reaching
the failure threshold.

Usage:
    python monitor_token_failures.py --action verify
    python monitor_token_failures.py --action test --token_id 123
    python monitor_token_failures.py --action reset --token_id 123
"""

import argparse
import json
import time
import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from tabulate import tabulate

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Constants
FAILURE_THRESHOLD = 5  # Number of failures before a token is marked as inactive

def get_db_connection():
    """Create a direct connection to PostgreSQL"""
    # Always use localhost since we're connecting from outside Docker
    host = 'localhost'
    port = '5432'
    database = 'crypto_db'
    user = 'bot'
    password = 'bot1234'
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    conn.autocommit = True
    return conn

def verify_failure_handling():
    """Verify how the system handles token update failures"""
    conn = None
    
    try:
        conn = get_db_connection()
        
        # Get tokens with failed updates to see their current status
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Query tokens with failed updates
            cur.execute("""
                SELECT 
                    token_id, 
                    name, 
                    ticker, 
                    blockchain, 
                    contract_address,
                    failed_updates_count,
                    is_active,
                    last_updated_at
                FROM tokens
                WHERE failed_updates_count > 0
                ORDER BY failed_updates_count DESC, last_updated_at ASC
                LIMIT 20
            """)
            tokens = cur.fetchall()
            
            if not tokens:
                print("No tokens with update failures found.")
                return
            
            print(f"Found {len(tokens)} tokens with update failures:")
            print(tabulate([dict(t) for t in tokens], headers='keys', tablefmt='psql'))
            
            # Verify if tokens with ≥ FAILURE_THRESHOLD failures are inactive
            failure_policy_works = True
            for token in tokens:
                if token['failed_updates_count'] >= FAILURE_THRESHOLD and token['is_active']:
                    print(f"❌ POLICY ERROR: Token {token['token_id']} ({token['name']}) has "
                          f"{token['failed_updates_count']} failures but is still marked as active!")
                    failure_policy_works = False
            
            if failure_policy_works:
                print(f"✅ All tokens with {FAILURE_THRESHOLD}+ failures are correctly marked as inactive")
            
            # Check for tokens that should be active but aren't
            cur.execute("""
                SELECT 
                    token_id, 
                    name, 
                    failed_updates_count,
                    is_active
                FROM tokens
                WHERE failed_updates_count < %s AND is_active = false
                LIMIT 10
            """, (FAILURE_THRESHOLD,))
            
            incorrect_inactive = cur.fetchall()
            if incorrect_inactive:
                print(f"\n⚠️ Found {len(incorrect_inactive)} tokens that are inactive but have fewer than {FAILURE_THRESHOLD} failures:")
                print(tabulate([dict(t) for t in incorrect_inactive], headers='keys', tablefmt='psql'))
                print("Note: These might be manually deactivated tokens or have other deactivation reasons.")
    
    except Exception as e:
        print(f"Error verifying failure handling: {e}")
    
    finally:
        if conn:
            conn.close()

def get_token_details(token_id):
    """Get current details for a specific token"""
    conn = None
    
    try:
        conn = get_db_connection()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    token_id, 
                    name, 
                    ticker, 
                    blockchain, 
                    contract_address,
                    update_interval,
                    failed_updates_count,
                    is_active,
                    last_updated_at,
                    dexscreener_url,
                    best_pair_address
                FROM tokens
                WHERE token_id = %s
            """, (token_id,))
            
            token = cur.fetchone()
            
            if not token:
                print(f"Token with ID {token_id} not found.")
                return None
            
            return token
    
    except Exception as e:
        print(f"Error getting token details: {e}")
        return None
    
    finally:
        if conn:
            conn.close()

def reset_token_failures(token_id):
    """Reset the failure count for a specific token"""
    conn = None
    
    try:
        conn = get_db_connection()
        
        # First get current token state
        token_before = get_token_details(token_id)
        if not token_before:
            return
            
        print(f"Current token state:")
        print(json.dumps(dict(token_before), default=str, indent=2))
        
        # Reset failure count and reactivate token
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE tokens 
                SET failed_updates_count = 0, is_active = true
                WHERE token_id = %s
                RETURNING token_id
            """, (token_id,))
            
            if cur.rowcount == 0:
                print(f"Failed to reset token {token_id}.")
                return
        
        # Get updated token state
        token_after = get_token_details(token_id)
        print(f"\nToken reset successfully:")
        print(json.dumps(dict(token_after), default=str, indent=2))
        
    except Exception as e:
        print(f"Error resetting token failures: {e}")
    
    finally:
        if conn:
            conn.close()

def test_failure_increment(token_id, monitor_count=10):
    """Monitor a token for failure count increments by simulating updates and watching the database"""
    token = get_token_details(token_id)
    if not token:
        return
    
    print(f"Starting monitoring for token {token_id} ({token['name']}):")
    print(json.dumps(dict(token), default=str, indent=2))
    
    initial_failures = token['failed_updates_count']
    initial_active = token['is_active']
    
    print(f"\nCurrent state: {initial_failures} failures, active={initial_active}")
    print(f"Will monitor for {monitor_count} checks, watching for failure increments...")
    print(f"Note: This assumes update tasks are running in the background.")
    
    for i in range(monitor_count):
        time.sleep(30)  # Wait for 30 seconds between checks
        
        token = get_token_details(token_id)
        if not token:
            print("Error: Token no longer found in database!")
            return
            
        new_failures = token['failed_updates_count']
        new_active = token['is_active']
        
        status_changed = (new_failures != initial_failures) or (new_active != initial_active)
        
        print(f"Check {i+1}/{monitor_count}: failures={new_failures} (was {initial_failures}), "
              f"active={new_active} (was {initial_active})")
        
        if status_changed:
            print("✅ Status changed! Current token state:")
            print(json.dumps(dict(token), default=str, indent=2))
            
            # Check if token got deactivated
            if initial_active and not new_active:
                print(f"✅ CONFIRMED: Token was deactivated after {new_failures} failures")
                break
                
            initial_failures = new_failures
            initial_active = new_active
    
    print("\nMonitoring complete.")
    
    # Final verification of failure policy
    if token['failed_updates_count'] >= FAILURE_THRESHOLD:
        if not token['is_active']:
            print(f"✅ Token properly marked as inactive after {token['failed_updates_count']} failures")
        else:
            print(f"❌ POLICY ERROR: Token has {token['failed_updates_count']} failures "
                  f"but is still active! Expected to be inactive.")

def main():
    parser = argparse.ArgumentParser(description='Monitor token update failures')
    parser.add_argument('--action', choices=['verify', 'test', 'reset'], required=True,
                       help='Action to perform: verify system behavior, test with a specific token, or reset a token')
    parser.add_argument('--token_id', type=int, help='Token ID for test or reset actions')
    parser.add_argument('--monitor_count', type=int, default=10, 
                       help='Number of monitoring checks for test action (default: 10)')
    
    args = parser.parse_args()
    
    if args.action == 'verify':
        verify_failure_handling()
    elif args.action == 'test':
        if not args.token_id:
            print("Error: token_id is required for test action")
            return
        test_failure_increment(args.token_id, args.monitor_count)
    elif args.action == 'reset':
        if not args.token_id:
            print("Error: token_id is required for reset action")
            return
        reset_token_failures(args.token_id)

if __name__ == '__main__':
    main()
