FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/dags /app/logs /app/plugins

# Set environment variables
ENV AIRFLOW_HOME=/app
ENV PYTHONPATH=/app

# Initialize Airflow DB and create admin user
RUN airflow db init && \
    airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin

# Expose ports
EXPOSE 8080

# Start Airflow webserver
CMD ["airflow", "webserver"]
