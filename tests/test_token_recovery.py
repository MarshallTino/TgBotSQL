"""
Tests for token recovery functionality.

This test suite verifies the token recovery system works correctly,
handling failed tokens and attempting to recover them.
"""
import sys
import os
import unittest
import unittest.mock
from pathlib import Path
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.price_tracker.token_recovery import (
    recover_token, get_failing_tokens, reset_failures_for_token,
    bulk_recover_tokens, reactivate_token
)

class TestTokenRecovery(unittest.TestCase):
    """Test token recovery system functionality."""
    
    @patch('scripts.price_tracker.token_recovery.execute_query')
    def test_get_failing_tokens(self, mock_execute):
        """Test get_failing_tokens function."""
        # Setup mock response from database
        mock_execute.return_value = [
            (1, "Token1", "TKN1", "ethereum", "0xtoken1", "0xpair1", 5, datetime(2025, 4, 10), True),
            (2, "Token2", "TKN2", "solana", "soltoken2", "solpair2", 10, datetime(2025, 4, 9), True)
        ]
        
        # Call function
        tokens = get_failing_tokens(min_failures=5, blockchain=None, limit=50)
        
        # Verify results
        self.assertEqual(len(tokens), 2, "Should return 2 failing tokens")
        self.assertEqual(tokens[0]["token_id"], 1, "First token should have ID 1")
        self.assertEqual(tokens[0]["blockchain"], "ethereum", "First token should be on ethereum")
        self.assertEqual(tokens[0]["failed_updates_count"], 5, "First token should have 5 failures")
        
        # Test with blockchain filter
        tokens = get_failing_tokens(min_failures=5, blockchain="solana", limit=50)
        mock_execute.assert_called()  # Verify a new query was executed
        
        # Test inactive token filtering
        tokens = get_failing_tokens(min_failures=5, include_inactive=True)
        self.assertTrue(mock_execute.call_count >= 3, "Should call execute_query at least 3 times")
    
    @patch('scripts.price_tracker.token_recovery.execute_query')
    @patch('scripts.price_tracker.token_recovery.get_pairs_data')
    def test_recover_token(self, mock_get_pairs, mock_execute):
        """Test recover_token function."""
        # Setup mocks
        # First mock the token info retrieval
        mock_execute.side_effect = [
            # First call - get token info
            [(1, "TestToken", "TTK", "ethereum", "0xtoken1", "0xoldpair", 5, datetime(2025, 4, 10), True)],
            # Second call - update token with new pair
            True
        ]
        
        # Mock pair data with a valid pair
        mock_get_pairs.return_value = [{
            "pairAddress": "0xnewpair",
            "baseToken": {"address": "0xtoken1"},
            "liquidity": {"usd": "$50000"},
            "dexId": "uniswap"
        }]
        
        # Call recover_token
        result = recover_token(1)
        
        # Verify results
        self.assertTrue(result["success"], "Recovery should be successful")
        self.assertEqual(result["pair_address"], "0xnewpair", "Should return new pair address")
        self.assertIn("Successfully recovered token", result["message"], "Message should indicate success")
        
        # Test case where no valid pairs are found
        mock_get_pairs.return_value = []
        mock_execute.side_effect = [
            [(1, "TestToken", "TTK", "ethereum", "0xtoken1", "0xoldpair", 15, datetime(2025, 4, 10), True)],
            True  # For deactivation query
        ]
        
        result = recover_token(1)
        self.assertFalse(result["success"], "Recovery should fail with no pairs")
        self.assertIn("action_taken", result, "Should indicate action taken")
        self.assertEqual(result["action_taken"], "deactivated", "Token should be deactivated")
    
    @patch('scripts.price_tracker.token_recovery.execute_query')
    def test_reset_failures_for_token(self, mock_execute):
        """Test reset_failures_for_token function."""
        # Setup mock to return token name and ticker
        mock_execute.return_value = [("TestToken", "TTK")]
        
        # Call function
        result = reset_failures_for_token(1)
        
        # Verify results
        self.assertTrue(result, "Should return True for successful reset")
        mock_execute.assert_called_once()
        query_arg = mock_execute.call_args[0][0]
        self.assertIn("UPDATE tokens SET failed_updates_count = 0", query_arg, "Should reset failure count")
        
        # Test error case
        mock_execute.return_value = []
        result = reset_failures_for_token(999)  # Non-existent token
        self.assertFalse(result, "Should return False for non-existent token")
    
    @patch('scripts.price_tracker.token_recovery.get_failing_tokens')
    @patch('scripts.price_tracker.token_recovery.recover_token')
    @patch('scripts.price_tracker.token_recovery.time.sleep')
    def test_bulk_recover_tokens(self, mock_sleep, mock_recover, mock_get_tokens):
        """Test bulk_recover_tokens function."""
        # Setup mocks
        mock_get_tokens.return_value = [
            {
                "token_id": 1,
                "name": "Token1",
                "ticker": "TKN1",
                "blockchain": "ethereum",
                "failed_updates_count": 5
            },
            {
                "token_id": 2,
                "name": "Token2",
                "ticker": "TKN2",
                "blockchain": "solana",
                "failed_updates_count": 10
            }
        ]
        
        # Mock recovery results
        mock_recover.side_effect = [
            {"success": True, "message": "Recovered Token1"},
            {"success": False, "message": "Failed to recover Token2"}
        ]
        
        # Call bulk recovery
        result = bulk_recover_tokens(min_failures=5, blockchain=None, limit=10)
        
        # Verify results
        self.assertEqual(result["total"], 2, "Should process 2 tokens")
        self.assertEqual(result["recovered"], 1, "Should recover 1 token")
        self.assertEqual(result["failed"], 1, "Should fail to recover 1 token")
        self.assertEqual(len(result["tokens"]), 2, "Should have details for 2 tokens")
        
        # Verify mock calls
        mock_recover.assert_has_calls([call(1), call(2)])
        mock_sleep.assert_called()  # Verify sleep was called to avoid API rate limits
    
    @patch('scripts.price_tracker.token_recovery.execute_query')
    def test_reactivate_token(self, mock_execute):
        """Test reactivate_token function."""
        # Setup mock for token info and update
        mock_execute.side_effect = [
            [(1, "TestToken", "TTK", "ethereum", "0xtoken1", "0xpair", 5, datetime(2025, 4, 10), False)],
            True  # For update query
        ]
        
        # Call reactivate
        result = reactivate_token(1)
        
        # Verify results
        self.assertTrue(result["success"], "Reactivation should succeed")
        self.assertIn("Successfully reactivated", result["message"], "Message should indicate success")
        
        # Verify execution
        self.assertEqual(mock_execute.call_count, 2, "Should call execute_query twice")
        update_query = mock_execute.call_args_list[1][0][0]
        self.assertIn("SET is_active = TRUE, failed_updates_count = 0", update_query, "Should reset failures and activate")
        
        # Test reactivating already active token
        mock_execute.side_effect = [
            [(1, "TestToken", "TTK", "ethereum", "0xtoken1", "0xpair", 0, datetime(2025, 4, 10), True)],
        ]
        
        result = reactivate_token(1)
        self.assertFalse(result["success"], "Should not reactivate already active token")
        self.assertIn("already active", result["message"].lower(), "Message should indicate already active")

if __name__ == "__main__":
    unittest.main()
