FROM python:3.12-slim

# Install compilation tools required for python crypto/c-extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install headless Reticulum dependencies
RUN pip install --no-cache-dir rns msgpack

# Copy core engine source files
COPY speakeasy_db.py fed_engine.py speakeasy_daemon.py speakeasy_config.json signing.py ./

# Create persistent state directory for identities and SQLite databases
RUN mkdir -p /root/.reti_speakeasy

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "speakeasy_daemon.py", "speakeasy_config.json"]
