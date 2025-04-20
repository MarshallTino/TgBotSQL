"""
Tests for database operations with PostgreSQL.

This test suite verifies that database operations work correctly,
including connection handling, data insertion, and querying.
"""
import sys
import os
import unittest
import unittest.mock
from pathlib import Path
from unittest.mock import patch, MagicMock, call

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.utils.db_postgres import (
    execute_query, get_connection, release_connection,
    insert_token, update_token_info, update_token_best_pair,
    insert_call, update_token_failure_count
)

class TestDatabaseOperations(unittest.TestCase):
    """Test database operations with mocked connections."""
    
    @patch('scripts.utils.db_postgres.get_connection')
    def test_execute_query(self, mock_get_conn):
        """Test execute_query function."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        # Define test query and params
        test_query = "SELECT * FROM tokens WHERE token_id = %s"
        test_params = (123,)
        
        # Set return value for fetchall
        mock_cursor.fetchall.return_value = [("Test Token", "TEST", "ethereum")]
        
        # Call function with fetch=True
        result = execute_query(test_query, test_params, fetch=True)
        
        # Verify query execution
        mock_cursor.execute.assert_called_with(test_query, test_params)
        self.assertEqual(result, [("Test Token", "TEST", "ethereum")], "Incorrect query result")
        
        # Test without fetch
        result = execute_query(test_query, test_params, fetch=False)
        self.assertTrue(result, "Should return True for successful execution")
        
        # Test error handling
        mock_cursor.execute.side_effect = Exception("Database error")
        result = execute_query(test_query, test_params, fetch=True)
        self.assertIsNone(result, "Should return None on error")
    
    @patch('scripts.utils.db_postgres.execute_query')
    def test_insert_token(self, mock_execute):
        """Test insert_token function."""
        # Setup mocked return for execute_query
        mock_execute.return_value = [(123,)]  # token_id
        
        # Call insert_token
        token_id = insert_token("0xtoken1", "ethereum")
        
        # Verify correct query and parameters
        self.assertEqual(token_id, 123, "Should return token_id")
        mock_execute.assert_called_once()
        # Verify the correct INSERT or SELECT query was used
        query_arg = mock_execute.call_args[0][0]
        self.assertIn("INSERT INTO tokens", query_arg, "Should use INSERT query")
        self.assertIn("ON CONFLICT", query_arg, "Should use ON CONFLICT for upsert")
    
    @patch('scripts.utils.db_postgres.get_connection')
    def test_update_token_info(self, mock_get_conn):
        """Test update_token_info function."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        # Call function
        result = update_token_info(
            token_id=123,
            name="Updated Token",
            ticker="UPTK",
            liquidity=100000,
            price=1.23,
            dex="uniswap"
        )
        
        # Verify function execution
        self.assertTrue(result, "Should return True for successful update")
        mock_cursor.execute.assert_called_once()
        # Check that UPDATE query was used
        query_arg = mock_cursor.execute.call_args[0][0]
        self.assertIn("UPDATE tokens SET", query_arg, "Should use UPDATE query")
        
        # Test error handling
        mock_cursor.execute.side_effect = Exception("Database error")
        result = update_token_info(token_id=123, name="Test")
        self.assertFalse(result, "Should return False on error")
    
    @patch('scripts.utils.db_postgres.get_connection')
    def test_update_token_failure_count(self, mock_get_conn):
        """Test update_token_failure_count function."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        # Mock cursor.fetchone to return current count and status
        mock_cursor.fetchone.return_value = (3, True, "Test Token", "TEST", "ethereum", "0xtoken1", "0xpair1")
        
        # Test increment=True (default)
        success, new_count, is_active = update_token_failure_count(123)
        
        # Verify results
        self.assertTrue(success, "Should return success=True")
        self.assertEqual(new_count, 4, "Should increment count by 1")
        self.assertTrue(is_active, "Token should still be active")
        
        # Test increment to deactivation threshold
        mock_cursor.fetchone.return_value = (4, True, "Test Token", "TEST", "ethereum", "0xtoken1", "0xpair1")
        success, new_count, is_active = update_token_failure_count(123)
        
        # For count=5, it should try recovery before potentially deactivating
        # Verify that it called execute at least twice (select and update)
        self.assertTrue(mock_cursor.execute.call_count >= 2)
        
        # Test reset=True
        mock_cursor.fetchone.return_value = (5, False, "Test Token", "TEST", "ethereum", "0xtoken1", "0xpair1")
        success, new_count, is_active = update_token_failure_count(123, increment=False, reset=True)
        
        self.assertEqual(new_count, 0, "Should reset count to 0")
        
        # Test error handling
        mock_cursor.execute.side_effect = Exception("Database error")
        success, new_count, is_active = update_token_failure_count(123)
        self.assertFalse(success, "Should return False on error")
        
    @patch('scripts.utils.db_postgres.execute_query')
    def test_insert_call(self, mock_execute):
        """Test insert_call function."""
        # Setup mock to return call_id
        mock_execute.return_value = [(456,)]
        
        # Call function
        call_id = insert_call(token_id=123, message_id=789, timestamp="2025-04-10 12:00:00", price=1.23)
        
        # Verify function call
        self.assertEqual(call_id, 456, "Should return call_id from query")
        mock_execute.assert_called_once()
        query_arg = mock_execute.call_args[0][0]
        self.assertIn("INSERT INTO token_calls", query_arg, "Should use INSERT INTO token_calls query")
        
        # Test error case
        mock_execute.return_value = None
        call_id = insert_call(token_id=123, message_id=789, timestamp="2025-04-10 12:00:00", price=1.23)
        self.assertIsNone(call_id, "Should return None on error")

if __name__ == "__main__":
    unittest.main()
