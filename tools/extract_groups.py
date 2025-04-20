#!/usr/bin/env python3
"""
Telegram Groups & Channels ID Extractor
Extracts basic group and channel information to add to your configuration file.
"""

import os
import sys
import asyncio
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat

# Add the project root to the path (TgBot directory)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../'))
sys.path.insert(0, ROOT_DIR)

# Try to import from the correct location
try:
    from config.settings import TELEGRAM_API_ID, TELEGRAM_API_HASH, SESSION_PATH
except ImportError:
    try:
        # Second attempt with env variables
        import dotenv
        dotenv.load_dotenv(os.path.join(ROOT_DIR, '.env'))
        TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID'))
        TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
        SESSION_PATH = os.getenv('SESSION_PATH', os.path.join(ROOT_DIR, 'tg_session'))
    except (ImportError, TypeError, ValueError):
        print("Could not import API credentials from config or environment")
        TELEGRAM_API_ID = int(input("Enter your Telegram API ID: "))
        TELEGRAM_API_HASH = input("Enter your Telegram API hash: ")
        SESSION_PATH = os.path.join(ROOT_DIR, "tg_session")

# Use SESSION_PATH if available, otherwise default to tg_session in the project root
SESSION_FILE = SESSION_PATH if 'SESSION_PATH' in locals() else os.path.join(ROOT_DIR, "tg_session")
print(f"Using session file: {SESSION_FILE}")

async def extract_telegram_entities():
    """Extract both groups and channels with their IDs"""
    # Initialize the client with your existing session
    client = TelegramClient(SESSION_FILE, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start()
    
    print("Connected to Telegram! Fetching dialogs...")
    
    # Get all dialogs
    dialogs = await client.get_dialogs()
    
    # Extract all group-like entities (groups and supergroups)
    groups = []
    channels = []
    
    for dialog in dialogs:
        entity = dialog.entity
        
        # Process based on entity type
        if isinstance(entity, Channel):
            if entity.broadcast:
                # This is a channel
                channels.append({
                    'id': entity.id,
                    'title': getattr(entity, 'title', 'No title'),
                    'username': getattr(entity, 'username', None)
                })
            else:
                # This is a supergroup
                groups.append({
                    'id': entity.id,
                    'title': getattr(entity, 'title', 'No title'),
                    'is_supergroup': True,
                    'username': getattr(entity, 'username', None)
                })
        elif isinstance(entity, Chat):
            # This is a regular group
            groups.append({
                'id': entity.id,
                'title': getattr(entity, 'title', 'No title'),
                'is_supergroup': False,
                'username': None  # Regular groups don't have usernames
            })
    
    # Print groups in an easily copyable format
    print("\n--- GROUPS DATA FOR CONFIGURATION ---")
    print("# Group Name : Group ID")
    for group in sorted(groups, key=lambda x: x['title']):
        group_type = "Supergroup" if group['is_supergroup'] else "Group"
        username = f" (@{group['username']})" if group['username'] else ""
        print(f"# {group['title']}{username} ({group_type})")
        print(f"{group['id']},  # {group['title']}")
    
    # Print channels in an easily copyable format
    print("\n--- CHANNELS DATA FOR CONFIGURATION ---")
    print("# Channel Name : Channel ID")
    for channel in sorted(channels, key=lambda x: x['title']):
        username = f" (@{channel['username']})" if channel['username'] else ""
        print(f"# {channel['title']}{username} (Channel)")
        print(f"{channel['id']},  # {channel['title']}")
    
    # Save to a file for easier copying
    output_file = os.path.join(ROOT_DIR, "my_telegram_entities.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("# TELEGRAM GROUPS AND CHANNELS\n\n")
        
        f.write("# === GROUPS ===\n")
        for group in sorted(groups, key=lambda x: x['title']):
            group_type = "Supergroup" if group['is_supergroup'] else "Group"
            username = f" (@{group['username']})" if group['username'] else ""
            f.write(f"# {group['title']}{username} ({group_type})\n")
            f.write(f"{group['id']},  # {group['title']}\n\n")
        
        f.write("\n# === CHANNELS ===\n")
        for channel in sorted(channels, key=lambda x: x['title']):
            username = f" (@{channel['username']})" if channel['username'] else ""
            f.write(f"# {channel['title']}{username} (Channel)\n")
            f.write(f"{channel['id']},  # {channel['title']}\n\n")
    
    print(f"\nAll entities data saved to: {output_file}")
    
    # Show counts
    print(f"\nTotal groups found: {len(groups)}")
    print(f"Total channels found: {len(channels)}")
    
    await client.disconnect()
    print("\nDisconnected from Telegram")

if __name__ == "__main__":
    asyncio.run(extract_telegram_entities())
