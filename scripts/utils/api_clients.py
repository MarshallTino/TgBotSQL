import requests
import logging
from config.settings import DEXSCREENER_API_TIMEOUT

logger = logging.getLogger(__name__)

def get_pairs_data(chain, token_addresses):
    """Get data for token(s) from DexScreener API"""
    if not token_addresses:
        return []
    
    # Use the correct endpoint for tokens
    joined = ",".join(token_addresses)
    url = f"https://api.dexscreener.com/latest/dex/tokens/{joined}"
    logger.info(f"==> DexScreener tokens request: {url}")
    
    try:
        # Use timeout from settings
        r = requests.get(url, timeout=DEXSCREENER_API_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        
        if "pairs" in data and data["pairs"] is not None and len(data["pairs"]) > 0:
            return data["pairs"]
        
        # If no results from token endpoint, try the pair endpoint
        logger.info(f"No token data found, trying pair endpoint...")
        return get_pair_by_address(chain, token_addresses[0])
        
    except Exception as e:
        logger.error(f"⚠️ Error get_pairs_data => {e}")
        return []

def get_pair_by_address(chain, pair_address):
    """Get data for a specific pair from DexScreener API"""
    # Map chain names if needed
    chain_map = {
        "eth": "ethereum",
        "solana": "solana",
        "bsc": "bsc",
        "ethereum": "ethereum"
    }
    
    dex_chain = chain_map.get(chain.lower(), chain.lower())
    
    url = f"https://api.dexscreener.com/latest/dex/pairs/{dex_chain}/{pair_address}"
    logger.info(f"==> DexScreener pairs request: {url}")
    
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        if "pair" in data and data["pair"] is not None:
            # Return as list for consistency
            return [data["pair"]]
        
        return []
    except Exception as e:
        logger.error(f"⚠️ Error get_pair_by_address => {e}")
        return []

def parse_float(value, default=None):
    """
    Safely parse float values from various sources.
    Handles None, empty strings, and non-numeric strings.
    """
    if value is None:
        return default
    
    if isinstance(value, (float, int)):
        return float(value)
    
    try:
        # Remove common currency formatting
        if isinstance(value, str):
            value = value.replace(',', '')
            value = value.replace('$', '')
            value = value.strip()
            if value == '':
                return default
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to float")
        return default

