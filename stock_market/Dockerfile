FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    netcat-traditional \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /app/dags /app/logs /app/plugins /app/config

# Copy requirements first for better caching
COPY stock_market/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create a non-root user
RUN useradd -m -d /app airflow && \
    chown -R airflow:airflow /app

# Copy the entrypoint script first
COPY stock_market/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    chown airflow:airflow /entrypoint.sh

# Copy the application code
COPY stock_market /app
COPY common /app/common

USER airflow

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose ports
EXPOSE 8080

# Use entrypoint script
ENTRYPOINT ["/entrypoint.sh"]
