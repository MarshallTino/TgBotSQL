"""
Regular expressions used in the application
"""
import re

# Regex patterns for token and call detection
CALL_PATTERN = re.compile(r"🎲\s*New\s*Gamble\s*Call", re.IGNORECASE)
RE_CA_BSC_ETH = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
RE_CA_SOL = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
DEX_LINK_REGEX = re.compile(r"https?://dexscreener\.com/(solana|bsc|ethereum)/([^/\s\?]+)")
TINYASTRO_REGEX = re.compile(r"https?://photon-sol\.tinyastro\.io/\w+/lp/([1-9A-HJ-NP-Za-km-z]{32,44})")
