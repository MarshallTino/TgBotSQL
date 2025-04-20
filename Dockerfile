FROM python:3.10-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set Python path and environment variables for database connections
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PG_CONNECT_TIMEOUT=10
ENV PG_KEEPALIVES=1
ENV PG_KEEPALIVES_IDLE=30
ENV PG_KEEPALIVES_INTERVAL=10
ENV PG_KEEPALIVES_COUNT=3

# Default command
CMD ["python", "scripts/app.py"]
