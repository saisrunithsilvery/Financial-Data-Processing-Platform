#!/bin/bash

# cleanup_airflow.sh
echo "Stopping Airflow processes..."
pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true

echo "Removing Airflow configuration..."
rm -f ~/airflow/airflow.cfg

echo "Cleaning up Airflow database..."
rm -f ~/airflow/airflow.db

echo "Cleanup complete. You can now run ./airflow.sh again."