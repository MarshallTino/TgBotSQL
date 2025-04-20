#!/usr/bin/env python3
"""
TgBot Database Client Utility

A comprehensive tool for accessing PostgreSQL and MongoDB databases
from outside Docker containers.

Usage:
    # PostgreSQL queries
    python dbclient.py pg "SELECT * FROM tokens LIMIT 5"
    python dbclient.py pg "SELECT count(*) FROM price_metrics WHERE mongo_id IS NOT NULL"
    
    # MongoDB queries
    python dbclient.py mongo dexscreener_data '{"processed": true}' --limit 5
    
    # Run with --help for more options
"""

import os
import sys
import logging
import json
import argparse
from datetime import datetime, date
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from tabulate import tabulate

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default connection settings when running outside Docker
DEFAULT_POSTGRES_CONFIG = {
    'host': 'localhost',  # Use localhost for host machine access
    'port': 5432,
    'database': 'crypto_db',
    'user': 'bot',
    'password': 'bot1234'
}

DEFAULT_MONGO_CONFIG = {
    'host': 'localhost',  # Use localhost for host machine access
    'port': 27017,
    'username': 'bot',
    'password': 'bot1234',
    'database': 'tgbot_db',
    'auth_source': 'admin'
}

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle PostgreSQL and MongoDB specific types"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, '__str__'):
            return str(obj)
        return super().default(obj)

def get_pg_connection():
    """Create a direct connection to PostgreSQL for local script usage"""
    try:
        # Always use localhost when running outside Docker
        host = 'localhost'  # Force localhost for external access
        port = os.environ.get('PG_PORT', DEFAULT_POSTGRES_CONFIG['port'])
        database = os.environ.get('PG_DATABASE', DEFAULT_POSTGRES_CONFIG['database'])
        user = os.environ.get('PG_USER', DEFAULT_POSTGRES_CONFIG['user'])
        password = os.environ.get('PG_PASSWORD', DEFAULT_POSTGRES_CONFIG['password'])
        
        logger.info(f"Connecting to PostgreSQL at {host}:{port}/{database}")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

def get_mongo_connection():
    """Create a direct connection to MongoDB for local script usage"""
    try:
        # Always use localhost when running outside Docker
        host = 'localhost'  # Force localhost for external access
        port = int(os.environ.get('MONGO_PORT', DEFAULT_MONGO_CONFIG['port']))
        username = os.environ.get('MONGO_USER', DEFAULT_MONGO_CONFIG['username'])
        password = os.environ.get('MONGO_PASSWORD', DEFAULT_MONGO_CONFIG['password'])
        database = os.environ.get('MONGO_DB', DEFAULT_MONGO_CONFIG['database'])
        auth_source = os.environ.get('MONGO_AUTH_SOURCE', DEFAULT_MONGO_CONFIG['auth_source'])
        
        logger.info(f"Connecting to MongoDB at {host}:{port}/{database}")
        
        # Construct connection URI with proper authentication
        uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource={auth_source}"
        
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        
        # Verify connection
        client.admin.command('ping')
        logger.info("✅ MongoDB connection successful")
        return client
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        return None

def pg_query(query, params=None, format_output='table', dict_cursor=True):
    """
    Execute a PostgreSQL query and return formatted results
    
    Args:
        query (str): SQL query to execute
        params (tuple|dict): Query parameters
        format_output (str): Output format (table, json, csv, raw)
        dict_cursor (bool): Use dictionary cursor if True
    
    Returns:
        str: Formatted query results
    """
    conn = None
    
    try:
        conn = get_pg_connection()
        if not conn:
            return "Failed to connect to PostgreSQL"
        
        cursor_factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cursor:
            cursor.execute(query, params)
            
            if cursor.description is None:  # No results expected (for INSERT, UPDATE, DELETE)
                return f"Query executed successfully. Rows affected: {cursor.rowcount}"
            
            rows = cursor.fetchall()
            if not rows:
                return "No results found"
                
            row_count = len(rows)
            
            # Format results based on selected output format
            if format_output == 'json':
                if dict_cursor:
                    return json.dumps(rows, cls=CustomJSONEncoder, indent=2)
                else:
                    column_names = [desc[0] for desc in cursor.description]
                    result = [dict(zip(column_names, row)) for row in rows]
                    return json.dumps(result, cls=CustomJSONEncoder, indent=2)
            elif format_output == 'csv':
                if dict_cursor:
                    headers = rows[0].keys()
                    csv_rows = [','.join([str(row[h]) for h in headers]) for row in rows]
                    return f"{row_count} rows returned\n" + ','.join(headers) + '\n' + '\n'.join(csv_rows)
                else:
                    column_names = [desc[0] for desc in cursor.description]
                    csv_rows = [','.join([str(col) for col in row]) for row in rows]
                    return f"{row_count} rows returned\n" + ','.join(column_names) + '\n' + '\n'.join(csv_rows)
            elif format_output == 'raw':
                return rows
            else:  # table format
                if dict_cursor:
                    result = tabulate([dict(row) for row in rows], headers='keys', tablefmt='psql')
                else:
                    column_names = [desc[0] for desc in cursor.description]
                    result = tabulate(rows, headers=column_names, tablefmt='psql')
                return f"{row_count} rows returned\n{result}"
    
    except Exception as e:
        return f"Error executing query: {e}"
    
    finally:
        if conn:
            conn.close()

def mongo_query(collection_name, query=None, projection=None, limit=100, format_output='table'):
    """
    Execute a MongoDB query and return formatted results
    
    Args:
        collection_name (str): Name of MongoDB collection to query
        query (dict): MongoDB query document
        projection (dict): Fields to include or exclude
        limit (int): Maximum number of documents to return
        format_output (str): Output format (table, json, csv, raw)
    
    Returns:
        str: Formatted query results
    """
    client = None
    
    try:
        client = get_mongo_connection()
        if not client:
            return "Failed to connect to MongoDB"
            
        db_name = os.environ.get('MONGO_DB', DEFAULT_MONGO_CONFIG['database'])
        db = client[db_name]
        collection = db[collection_name]
        
        # Set default query and projection if None
        if query is None:
            query = {}
        
        # Execute query
        cursor = collection.find(query, projection).limit(limit)
        documents = list(cursor)
        
        if not documents:
            return f"No documents found in collection '{collection_name}' matching query {query}"
            
        row_count = len(documents)
        
        # Format results based on selected output format
        if format_output == 'json':
            return json.dumps(documents, cls=CustomJSONEncoder, indent=2)
        elif format_output == 'csv':
            # Get all unique fields across all documents
            all_fields = set()
            for doc in documents:
                all_fields.update(doc.keys())
            headers = sorted(all_fields)
            
            # Create CSV
            csv_rows = [','.join([str(doc.get(field, '')) for field in headers]) for doc in documents]
            return f"{row_count} documents returned\n" + ','.join(headers) + '\n' + '\n'.join(csv_rows)
        elif format_output == 'raw':
            return documents
        else:  # table format
            # Convert MongoDB documents to tabular format
            # For MongoDB documents, we need to handle nested structures
            flat_docs = []
            for doc in documents:
                # Convert ObjectId to string for better display
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                flat_docs.append(doc)
                
            return f"{row_count} documents returned\n" + tabulate(flat_docs, headers='keys', tablefmt='psql')
    
    except Exception as e:
        return f"Error executing MongoDB query: {e}"
    
    finally:
        if client:
            client.close()

def show_examples():
    """Display usage examples"""
    examples = [
        "\n=== PostgreSQL Examples ===",
        "# Get token counts",
        "python dbclient.py pg \"SELECT COUNT(*) FROM tokens\"",
        "",
        "# Find inactive tokens",
        "python dbclient.py pg \"SELECT token_id, name, blockchain FROM tokens WHERE is_active = false LIMIT 10\"",
        "",
        "# Check tokens with MongoDB IDs",
        "python dbclient.py pg \"SELECT COUNT(*) FROM price_metrics WHERE mongo_id IS NOT NULL\"",
        "",
        "# Find tokens with high failure counts",
        "python dbclient.py pg \"SELECT token_id, name, failed_updates_count FROM tokens WHERE failed_updates_count > 3 ORDER BY failed_updates_count DESC LIMIT 10\"",
        "",
        "\n=== MongoDB Examples ===",
        "# List recent documents",
        "python dbclient.py mongo dexscreener_data --limit 5",
        "",
        "# Find unprocessed documents",
        "python dbclient.py mongo dexscreener_data '{\"processed\": false}' --limit 5",
        "",
        "# Get document by ID",
        "python dbclient.py mongo dexscreener_data '{\"_id\": {\"$oid\": \"67f3f44680862324ada25ce6\"}}' --format json",
    ]
    return "\n".join(examples)

def main():
    """Command-line interface for database queries"""
    parser = argparse.ArgumentParser(description="TgBot Database Client - Execute queries against PostgreSQL or MongoDB")
    
    # Database type and query arguments
    parser.add_argument('db', choices=['pg', 'mongo', 'postgres', 'mongodb'], 
                        help="Database type (pg/postgres or mongo/mongodb)")
    
    # The second argument is either collection name (MongoDB) or SQL query (PostgreSQL)
    parser.add_argument('query_or_collection', nargs='?', 
                        help="SQL query (for PostgreSQL) or collection name (for MongoDB)")
    
    # MongoDB-specific arguments
    parser.add_argument('filter', nargs='?', help="MongoDB filter query as a JSON string")
    
    # Common options
    parser.add_argument('--format', '-f', choices=['table', 'json', 'csv', 'raw'], default='table',
                      help="Output format (default: table)")
    parser.add_argument('--limit', '-l', type=int, default=100, help="Limit number of results")
    parser.add_argument('--examples', action='store_true', help="Show usage examples")
    
    args = parser.parse_args()
    
    # Show examples if requested
    if args.examples:
        print(show_examples())
        return
    
    # Make database type case-insensitive
    db_type = args.db.lower()
    
    # Normalize database type
    if db_type in ('pg', 'postgres'):
        db_type = 'pg'
    elif db_type in ('mongo', 'mongodb'):
        db_type = 'mongo'
    
    # Require query/collection
    if not args.query_or_collection:
        if db_type == 'pg':
            print("Error: SQL query is required for PostgreSQL")
        else:
            print("Error: Collection name is required for MongoDB")
        parser.print_help()
        return
    
    if db_type == 'pg':
        result = pg_query(args.query_or_collection, format_output=args.format)
        print(result)
    elif db_type == 'mongo':
        collection_name = args.query_or_collection
        
        query = {}
        if args.filter:
            try:
                query = json.loads(args.filter)
            except json.JSONDecodeError:
                print("Error: Invalid JSON filter format")
                return
                
        result = mongo_query(collection_name, query, limit=args.limit, format_output=args.format)
        print(result)

if __name__ == '__main__':
    main()
