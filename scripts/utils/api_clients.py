import requests
import logging
from config.settings import DEXSCREENER_API_TIMEOUT

logger = logging.getLogger(__name__)

def get_pairs_data(chain, token_addresses):
    """Get data for token(s) from DexScreener API with Base support"""
    if not token_addresses:
        return []
    
    # Normalize chain names
    normalized_chain = chain.lower()
    if normalized_chain == "eth":
        normalized_chain = "ethereum"
    
    # Handle unknown chain by trying main chains
    if normalized_chain == "unknown":
        # Try ethereum first
        eth_pairs = get_pairs_data("ethereum", token_addresses)
        if eth_pairs:
            return eth_pairs
        
        # Then try BSC
        bsc_pairs = get_pairs_data("bsc", token_addresses)
        if bsc_pairs:
            return bsc_pairs
        
        # Finally try Base
        base_pairs = get_pairs_data("base", token_addresses)
        if base_pairs:
            return base_pairs
        
        return []
    
    # Use the correct endpoint for tokens (same for ETH/BSC/Base)
    # For Ethereum/BSC/Base tokens, don't include chain in URL
    joined = ",".join(token_addresses)
    url = f"https://api.dexscreener.com/latest/dex/tokens/{joined}"
    logger.info(f"==> DexScreener tokens request: {url}")
    
    try:
        # Use timeout from settings
        r = requests.get(url, timeout=DEXSCREENER_API_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        
        if "pairs" in data and data["pairs"] is not None and len(data["pairs"]) > 0:
            # For Base tokens, make sure to filter by Base chain
            if normalized_chain == "base":
                base_pairs = [p for p in data["pairs"] if p.get("chainId", "").lower() == "base"]
                if base_pairs:
                    return base_pairs
            
            # For other chains, just return all pairs (filtering done elsewhere)
            return data["pairs"]
        
        # If no results from token endpoint, try the pair endpoint
        logger.info(f"No token data found, trying pair endpoint...")
        return get_pair_by_address(normalized_chain, token_addresses[0])
        
    except Exception as e:
        logger.error(f"⚠️ Error get_pairs_data => {e}")
        return []

def get_pair_by_address(chain, pair_address):
    """Get data for a specific pair from DexScreener API with improved Base support"""
    # Map chain names if needed
    chain_map = {
        "eth": "ethereum",
        "solana": "solana",
        "bsc": "bsc",
        "ethereum": "ethereum",
        "base": "base",
        "unknown": "address"  # Special case: use 'address' endpoint for unknown chain
    }
    
    dex_chain = chain_map.get(chain.lower(), chain.lower())
    
    # Handle the "unknown" chain case - try a generic approach first
    if dex_chain == "address":
        url = f"https://api.dexscreener.com/latest/dex/pairs/address/{pair_address}"
    else:
        url = f"https://api.dexscreener.com/latest/dex/pairs/{dex_chain}/{pair_address}"
    
    logger.info(f"==> DexScreener pairs request: {url}")
    
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        if "pairs" in data and data["pairs"] is not None and len(data["pairs"]) > 0:
            return data["pairs"]
        
        # If the generic approach failed for unknown chain, try specific chains
        if dex_chain == "address" and not data.get("pairs"):
            # Try ethereum
            eth_pairs = get_pair_by_address("ethereum", pair_address)
            if eth_pairs:
                return eth_pairs
            
            # Try bsc
            bsc_pairs = get_pair_by_address("bsc", pair_address)
            if bsc_pairs:
                return bsc_pairs
            
            # Try base
            base_pairs = get_pair_by_address("base", pair_address)
            if base_pairs:
                return base_pairs
        
        return []
    except Exception as e:
        logger.error(f"⚠️ Error get_pair_by_address => {e}")
        return []

def parse_float(value, default=None):
    """Safely parse float values from various sources."""
    # Existing implementation unchanged
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
