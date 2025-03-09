"""
Logging configuration
"""
import logging
import sys
from pathlib import Path

# Create logs directory if it doesn't exist
logs_dir = Path(__file__).resolve().parent.parent / "logs"
logs_dir.mkdir(exist_ok=True)

def configure_logging():
    """Configure and return a logger"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(logs_dir / 'telegram_bot.log')
        ]
    )
    return logging.getLogger(__name__)
