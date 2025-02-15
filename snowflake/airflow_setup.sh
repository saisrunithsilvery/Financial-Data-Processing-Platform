#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_message() {
    echo -e "${2}${1}${NC}"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if Python is installed
if ! command_exists python3; then
    print_message "Python3 is not installed. Please install Python first." "$RED"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    print_message "Creating virtual environment..." "$YELLOW"
    python3 -m venv venv
fi

# Activate virtual environment
print_message "Activating virtual environment..." "$YELLOW"
source venv/bin/activate

# Install requirements
print_message "Installing requirements..." "$YELLOW"
pip install -r requirements.txt

# Export Airflow home
export AIRFLOW_HOME=$(pwd)/airflow
print_message "Setting AIRFLOW_HOME to: $AIRFLOW_HOME" "$GREEN"

# Initialize Airflow database
print_message "Initializing Airflow database..." "$YELLOW"
airflow db init

# Create Airflow user if it doesn't exist
if ! airflow users list | grep -q "admin"; then
    print_message "Creating Airflow admin user..." "$YELLOW"
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

# Function to prompt for Snowflake details
get_snowflake_details() {
    read -p "Enter Snowflake account (e.g., xy12345.us-east-1): " rob31655.us-east-1
    read -p "Enter Snowflake username: "  vishal_prasanna
    read -s -p "Enter Snowflake password: " snowflake_password
    echo
    read -p "Enter Snowflake database: " snowflake_database
    read -p "Enter Snowflake warehouse: " snowflake_warehouse
    read -p "Enter Snowflake schema: " snowflake_schema
    read -p "Enter Snowflake role: " snowflake_role
}

# Get Snowflake connection details
print_message "\nPlease provide your Snowflake connection details:" "$YELLOW"
get_snowflake_details

# Create Snowflake connection in Airflow
print_message "\nCreating Snowflake connection in Airflow..." "$YELLOW"
airflow connections add 'snowflake_default' \
    --conn-type 'snowflake' \
    --conn-login "$snowflake_username" \
    --conn-password "$snowflake_password" \
    --conn-host "$snowflake_account.snowflakecomputing.com" \
    --conn-schema "$snowflake_schema" \
    --conn-extra "{\"database\":\"$snowflake_database\",\"warehouse\":\"$snowflake_warehouse\",\"role\":\"$snowflake_role\",\"account\":\"$snowflake_account\"}"

# Test the connection
print_message "\nTesting Snowflake connection..." "$YELLOW"
if airflow connections test snowflake_default; then
    print_message "Snowflake connection test successful!" "$GREEN"
else
    print_message "Snowflake connection test failed. Please check your credentials." "$RED"
fi

# Create necessary directories
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/plugins

# Start Airflow scheduler in background
print_message "\nStarting Airflow scheduler..." "$YELLOW"
airflow scheduler -D

# Start Airflow webserver in background
print_message "Starting Airflow webserver..." "$YELLOW"
airflow webserver -D -p 8080

print_message "\nSetup complete! You can access the Airflow web interface at http://localhost:8080" "$GREEN"
print_message "Username: admin" "$GREEN"
print_message "Password: admin" "$GREEN"

# Add instructions for stopping services
print_message "\nTo stop Airflow services, run:" "$YELLOW"
print_message "pkill -f airflow" "$NC"