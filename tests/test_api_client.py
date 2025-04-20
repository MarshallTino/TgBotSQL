"""
Tests for API client functionality.

This test suite verifies that the system correctly interacts with
external APIs like DexScreener to fetch token information.
"""
import sys
import os
import unittest
import unittest.mock
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.utils.api_clients import (
    get_pairs_data, get_pair_by_address, 
    select_best_pair, parse_float
)

class TestAPIClient(unittest.TestCase):
    """Test cases for API client functionality."""
    
    def test_parse_float(self):
        """Test parse_float function handles different formats correctly."""
        # Test normal numbers
        self.assertEqual(parse_float(10.5), 10.5)
        self.assertEqual(parse_float("15.75"), 15.75)
        
        # Test currency notation
        self.assertEqual(parse_float("$1,234.56"), 1234.56)
        self.assertEqual(parse_float("$0.00012345"), 0.00012345)
        
        # Test None and invalid inputs
        self.assertEqual(parse_float(None), 0)
        self.assertEqual(parse_float("Not a number"), 0)
        self.assertEqual(parse_float(""), 0)
    
    @patch('scripts.utils.api_clients.requests.get')
    def test_get_pairs_data(self, mock_get):
        """Test get_pairs_data function with mocked API response."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pairs": [
                {
                    "chainId": "ethereum",
                    "dexId": "uniswap",
                    "pairAddress": "0xpair1",
                    "baseToken": {
                        "address": "0xtoken1",
                        "name": "Test Token",
                        "symbol": "TEST"
                    },
                    "priceUsd": "1.23",
                    "liquidity": {"usd": "$500000"}
                }
            ]
        }
        mock_get.return_value = mock_response
        
        # Call function
        result = get_pairs_data("ethereum", ["0xtoken1"])
        
        # Verify results
        self.assertTrue(len(result) > 0, "Should return at least one pair")
        self.assertEqual(result[0]["pairAddress"], "0xpair1", "Incorrect pair address")
        self.assertEqual(result[0]["baseToken"]["address"], "0xtoken1", "Incorrect token address")
        
        # Verify API was called with correct parameters
        mock_get.assert_called_once()
        url_arg = mock_get.call_args[0][0]
        self.assertIn("ethereum", url_arg, "Blockchain not in URL")
        self.assertIn("0xtoken1", url_arg, "Token address not in URL")
    
    @patch('scripts.utils.api_clients.requests.get')
    def test_get_pairs_data_error_handling(self, mock_get):
        """Test get_pairs_data error handling."""
        # Test API error
        mock_get.return_value = MagicMock(status_code=500)
        result = get_pairs_data("ethereum", ["0xtoken1"])
        self.assertEqual(result, [], "Should return empty list on API error")
        
        # Test timeout error
        mock_get.side_effect = Exception("Timeout")
        result = get_pairs_data("ethereum", ["0xtoken1"])
        self.assertEqual(result, [], "Should return empty list on exception")
    
    @patch('scripts.utils.api_clients.requests.get')
    def test_get_pair_by_address(self, mock_get):
        """Test get_pair_by_address function."""
        # Setup mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"pair": {
            "pairAddress": "0xpair1",
            "baseToken": {"address": "0xtoken1"},
            "liquidity": {"usd": "$100000"}
        }}
        mock_get.return_value = mock_response
        
        # Call function
        result = get_pair_by_address("ethereum", "0xpair1")
        
        # Verify results
        self.assertTrue(len(result) > 0, "Should return at least one pair")
        self.assertEqual(result[0]["pairAddress"], "0xpair1", "Incorrect pair address")
    
    def test_select_best_pair(self):
        """Test select_best_pair selects pair with highest liquidity."""
        # Define test pairs
        pairs = [
            {
                "pairAddress": "0xpair1",
                "liquidity": {"usd": "$100000"}
            },
            {
                "pairAddress": "0xpair2",
                "liquidity": {"usd": "$500000"}  # Highest liquidity
            },
            {
                "pairAddress": "0xpair3",
                "liquidity": {"usd": "$50000"}
            }
        ]
        
        # Call function
        best_pair = select_best_pair(pairs)
        
        # Verify results
        self.assertEqual(best_pair["pairAddress"], "0xpair2", "Did not select pair with highest liquidity")
    
    def test_select_best_pair_empty(self):
        """Test select_best_pair with empty list."""
        self.assertIsNone(select_best_pair([]), "Should return None for empty list")
    
    def test_select_best_pair_invalid_liquidity(self):
        """Test select_best_pair with invalid liquidity values."""
        pairs = [
            {
                "pairAddress": "0xpair1",
                "liquidity": {"usd": "invalid"}
            },
            {
                "pairAddress": "0xpair2",
                "liquidity": {"usd": "$500"}  # Should be selected
            }
        ]
        
        best_pair = select_best_pair(pairs)
        self.assertEqual(best_pair["pairAddress"], "0xpair2", "Failed to handle invalid liquidity")

if __name__ == "__main__":
    unittest.main()
