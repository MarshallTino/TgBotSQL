"""
Tests for token detection functionality from telegram messages.

This test suite verifies that the system correctly detects
different types of cryptocurrency token addresses in messages.
"""
import sys
import os
import unittest
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.telegram_monitor import detect_tokens_in_message
from config.regex_patterns import CALL_PATTERN

class TestTokenDetection(unittest.TestCase):
    """Test cases for token detection functionality."""
    
    def test_ethereum_address_detection(self):
        """Test detection of Ethereum/BSC addresses."""
        # Test standard ETH address
        message = "Check out this token: 0x1234567890abcdef1234567890abcdef12345678"
        results = detect_tokens_in_message(message)
        
        self.assertTrue(len(results) > 0, "Failed to detect Ethereum address")
        
        found = False
        for token_type, chain, address in results:
            if address == "0x1234567890abcdef1234567890abcdef12345678":
                found = True
                self.assertEqual(token_type, "eth_bsc_address", "Incorrect token type for ETH address")
                self.assertEqual(chain, "unknown", "Chain should be 'unknown' for plain ETH address")
        
        self.assertTrue(found, "Specific ETH address not found in results")
    
    def test_base_address_detection(self):
        """Test detection of Base chain addresses."""
        # Test ETH address with Base context
        message = "New Base gem: 0xabcdef1234567890abcdef1234567890abcdef12 #BASE"
        results = detect_tokens_in_message(message)
        
        self.assertTrue(len(results) > 0, "Failed to detect Base address")
        
        found = False
        for token_type, chain, address in results:
            if address == "0xabcdef1234567890abcdef1234567890abcdef12":
                found = True
                self.assertEqual(token_type, "eth_bsc_address", "Incorrect token type for Base address")
                self.assertEqual(chain, "base", "Chain should be 'base' for Base-context ETH address")
        
        self.assertTrue(found, "Base address not found in results")
    
    def test_solana_address_detection(self):
        """Test detection of Solana addresses."""
        # Test standard Solana address
        message = "Solana token to watch: SoLaNaAddRe55ExampLeADDRE55S1111111111111111"
        results = detect_tokens_in_message(message)
        
        self.assertTrue(len(results) > 0, "Failed to detect Solana address")
        
        found = False
        for token_type, chain, address in results:
            if address == "SoLaNaAddRe55ExampLeADDRE55S1111111111111111":
                found = True
                self.assertEqual(token_type, "solana_address", "Incorrect token type for Solana address")
                self.assertEqual(chain, "solana", "Chain should be 'solana' for Solana address")
        
        self.assertTrue(found, "Solana address not found in results")
    
    def test_dexscreener_link_detection(self):
        """Test detection of DexScreener links."""
        # Test DexScreener Ethereum link
        message = "Check chart: https://dexscreener.com/ethereum/0x1234567890abcdef1234567890abcdef12345678"
        results = detect_tokens_in_message(message)
        
        self.assertTrue(len(results) > 0, "Failed to detect DexScreener link")
        
        found = False
        for token_type, chain, address in results:
            if address == "0x1234567890abcdef1234567890abcdef12345678":
                found = True
                self.assertEqual(token_type, "dexscreener", "Incorrect token type for DexScreener link")
                self.assertEqual(chain, "ethereum", "Chain should be 'ethereum' for Ethereum DexScreener link")
        
        self.assertTrue(found, "DexScreener token address not found in results")
    
    def test_call_pattern_detection(self):
        """Test detection of call patterns in messages."""
        # Test call pattern
        call_messages = [
            "🚨 CALL 🚨 0x1234567890abcdef1234567890abcdef12345678",
            "NEW CALL: SoLaNaAddRe55ExampLeADDRE55S1111111111111111",
            "FRESH CALL ⚡️ https://dexscreener.com/ethereum/0x1234567890abcdef1234567890abcdef12345678"
        ]
        
        for message in call_messages:
            self.assertTrue(bool(CALL_PATTERN.search(message)), 
                           f"Failed to detect call pattern in: {message}")
    
    def test_multiple_addresses_detection(self):
        """Test detection of multiple addresses in a single message."""
        message = ("Check these tokens: "
                  "0x1234567890abcdef1234567890abcdef12345678 "
                  "SoLaNaAddRe55ExampLeADDRE55S1111111111111111 "
                  "https://dexscreener.com/ethereum/0xabcdef1234567890abcdef1234567890abcdef12")
        
        results = detect_tokens_in_message(message)
        self.assertEqual(len(results), 3, "Should detect exactly 3 token addresses")
        
        # Check that all addresses are detected
        addresses = [address for _, _, address in results]
        self.assertIn("0x1234567890abcdef1234567890abcdef12345678", addresses)
        self.assertIn("SoLaNaAddRe55ExampLeADDRE55S1111111111111111", addresses)
        self.assertIn("0xabcdef1234567890abcdef1234567890abcdef12", addresses)

if __name__ == "__main__":
    unittest.main()
