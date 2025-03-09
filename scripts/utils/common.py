import logging

logger = logging.getLogger(__name__)

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
