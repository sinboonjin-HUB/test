# Railway-friendly Python image
FROM python:3.12-slim

# Install system deps (optional but good for TLS/CA & locales)
RUN apt-get update && apt-get install -y --no-install-recommends     ca-certificates tzdata &&     rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /app

# Ensure runtime data dir exists (mounted as persistent volume in Railway)
RUN mkdir -p /data

# Expose (not used for polling, but harmless if you later switch to webhooks)
EXPOSE 8080

# Start the bot (polling)
CMD ["python", "app.py"]
