"""
Configuration file for application settings
"""
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Application root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Telegram API settings
TELEGRAM_API_ID = int(os.getenv("API_ID", "28644650"))
TELEGRAM_API_HASH = os.getenv("API_HASH", "e963f9b807bcf9d665b1d20de66f7c69")
TELEGRAM_PHONE = os.getenv("PHONE_NUMBER")
SESSION_PATH = os.getenv("SESSION_PATH", "./session/session_name")

# Database settings
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "timescaledb"),
    "port": os.getenv("PG_PORT", "5432"),
    "user": os.getenv("PG_USER", "bot"),
    "password": os.getenv("PG_PASSWORD", "bot1234"),
    "database": os.getenv("PG_DATABASE", "crypto_db")
}

# API Settings
DEXSCREENER_API_TIMEOUT = 15  # seconds


# Add these settings to the existing file

# MongoDB settings
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'tgbot_db')

# Celery settings
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')
