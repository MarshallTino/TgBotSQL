"""
Regular expressions used in the application
"""
import re

# Regex patterns for token and call detection
CALL_PATTERN = re.compile(r"🎲\s*New\s*Gamble\s*Call", re.IGNORECASE)
RE_CA_BSC_ETH = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
RE_CA_SOL = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
DEX_LINK_REGEX = re.compile(r'https?://(?:www\.)?dexscreener\.com/([^/]+)/([^/\s]+)', re.IGNORECASE)
TINYASTRO_REGEX = re.compile(r"https?://photon-sol\.tinyastro\.io/\w+/lp/([1-9A-HJ-NP-Za-km-z]{32,44})")

# Add Base-specific patterns if needed
BASE_DEX_LINK_REGEX = re.compile(r'https?://(?:www\.)?(?:baseswap\.fi|alienswap\.xyz|aerodrome\.finance)/(?:swap|info|pair)/([a-zA-Z0-9]{42})', re.IGNORECASE)

# Add a regex to match Base mentions
BASE_MENTION_REGEX = re.compile(r'\b(?:base|basechain|on\s+base)\b', re.IGNORECASE)

# Add Base patterns to existing regexes if needed
# Since Base uses the same address format as Ethereum (0x...), RE_CA_BSC_ETH should already catch it
