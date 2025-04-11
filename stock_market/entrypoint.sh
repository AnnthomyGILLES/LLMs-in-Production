#!/bin/bash

# Initialize Airflow DB
airflow db init

# Create an admin user if not already created
if ! airflow users list | grep -q admin; then
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

# Start Airflow webserver
exec airflow webserver
