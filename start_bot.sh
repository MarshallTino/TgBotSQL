#!/bin/bash

# Store PIDs for cleanup
BOT_PID=""

# Function to kill all processes on exit
cleanup() {
    echo "Stopping all processes..."
    
    # Kill the Telegram bot if running
    if [ -n "$BOT_PID" ]; then
        echo "Stopping Telegram bot (PID: $BOT_PID)..."
        kill -TERM $BOT_PID 2>/dev/null || true
    fi
    
    # Stop Celery workers via docker-compose
    echo "Stopping Celery workers..."
    docker-compose stop celery_worker celery_beat
    
    echo "All processes stopped."
    exit 0
}

# Set up signal handling - when Ctrl+C is pressed, run cleanup
trap cleanup SIGINT SIGTERM

# Ensure services are running
echo "Verificando que los servicios estén funcionando..."
if ! docker ps | grep -q postgres || ! docker ps | grep -q mongo || ! docker ps | grep -q redis; then
  echo "Iniciando servicios necesarios..."
  docker-compose up -d postgres pgadmin mongo redis
  
  # Give MongoDB time to initialize
  echo "Esperando 5 segundos para que MongoDB se inicialice..."
  sleep 5
fi

# Check if Celery workers are running
if ! docker ps | grep -q celery_worker; then
  echo "Iniciando servicios de Celery..."
  docker-compose up -d celery_worker celery_beat
fi

# Activate virtual environment
echo "Activando el entorno virtual tg_env..."
source tg_env/bin/activate

# Set environment variables for local development
# IMPORTANT: These must match docker-compose.yml settings
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=crypto_db
export PG_USER=bot
export PG_PASSWORD=bot1234

export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_USER=bot  # Must match MONGO_INITDB_ROOT_USERNAME in docker-compose
export MONGO_PASSWORD=bot1234  # Must match MONGO_INITDB_ROOT_PASSWORD in docker-compose
export MONGO_AUTH_SOURCE=admin
export MONGO_DB=tgbot_db

export REDIS_HOST=localhost
export REDIS_PORT=6379

# Initialize database
echo "Setting up database..."
python scripts/setup_database.py

# Start the Telegram bot
echo "Iniciando el bot de Telegram localmente..."
python scripts/telegram_monitor.py & 
BOT_PID=$!
echo "Telegram bot started with PID: $BOT_PID"

# Wait for the bot to finish (or until interrupted)
wait $BOT_PID

# If wait is interrupted, cleanup will be called by the trap
