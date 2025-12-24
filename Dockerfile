# Meshtastic Discord Bridge Bot
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY bot.py .
COPY database.py .
COPY config.py .
COPY fix_database.py .
COPY maintain_db.py .

# Create directory for persistent data
RUN mkdir -p /data

# Set environment variable for database location
ENV DATABASE_PATH=/data/meshtastic.db

# Health check - verify the bot process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD pgrep -f "python bot.py" || exit 1

# Run the bot
CMD ["python", "-u", "bot.py"]
