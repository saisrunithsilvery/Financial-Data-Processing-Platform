#!/bin/bash

# airflow.sh

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Error handling
set -e
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
trap 'echo -e "${RED}\"${last_command}\" command failed with exit code $?.${NC}"' EXIT

# Function to check command success
check_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Success${NC}"
    else
        echo -e "${RED}✗ Failed${NC}"
        exit 1
    fi
}

# Check if port 8080 is available
if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null ; then
    echo -e "${RED}Port 8080 is already in use. Please free up the port and try again.${NC}"
    exit 1
fi

echo -e "${GREEN}Starting Airflow Setup...${NC}"

# Install Airflow with constraints
echo -e "${YELLOW}Installing Apache Airflow...${NC}"
AIRFLOW_VERSION=2.9.0
PYTHON_VERSION="3.11"  # Explicitly set Python version
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# First uninstall existing airflow and providers
echo -e "${YELLOW}Removing existing Airflow installations...${NC}"
pip uninstall -y apache-airflow apache-airflow-providers-* || true

# Install Airflow with correct version
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
check_success

# Set Airflow home directory
echo -e "${YELLOW}Setting up Airflow home directory...${NC}"
export AIRFLOW_HOME=~/airflow
grep -q "AIRFLOW_HOME" ~/.bashrc || echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc
check_success

# Initialize the database
echo -e "${YELLOW}Initializing Airflow database...${NC}"
airflow db init
check_success

# Create admin user
echo -e "${YELLOW}Creating admin user...${NC}"
airflow users create \
    --username admin \
    --firstname Vishal \
    --lastname Prasanna \
    --role Admin \
    --email vishalprasanna11@gmail.com \
    --password root@123
check_success

# Create necessary directories
echo -e "${YELLOW}Creating project directories...${NC}"
mkdir -p ~/airflow/dags
mkdir -p ~/airflow/logs
mkdir -p ~/airflow/plugins
check_success

# Create a requirements.txt file with compatible versions
echo -e "${YELLOW}Creating requirements.txt...${NC}"
cat > ~/airflow/requirements.txt << EOL
apache-airflow==${AIRFLOW_VERSION}
apache-airflow-providers-amazon>=9.0.0
apache-airflow-providers-snowflake>=5.0.0
apache-airflow-client>=2.9.0  # Changed from airflow-client
boto3
fastapi
uvicorn
python-multipart
python-jose[cryptography]
passlib[bcrypt]
EOL

echo -e "${YELLOW}Installing additional requirements...${NC}"
pip install -r ~/airflow/requirements.txt
check_success

# Create a script to start Airflow services
echo -e "${YELLOW}Creating start script...${NC}"
cat > ~/start_airflow.sh << 'EOL'
#!/bin/bash

# Kill existing Airflow processes
echo "Stopping any existing Airflow processes..."
pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true
sleep 2

# Start webserver
echo "Starting Airflow webserver..."
airflow webserver --port 8080 --daemon

# Wait for webserver to start
echo "Waiting for webserver to start..."
for i in {1..30}; do
    if curl -s http://localhost:8080 > /dev/null; then
        break
    fi
    sleep 1
done

# Start scheduler
echo "Starting Airflow scheduler..."
airflow scheduler
EOL

# Make the start script executable
chmod +x ~/start_airflow.sh
check_success

# Create a test DAG
echo -e "${YELLOW}Creating test DAG...${NC}"
cat > ~/airflow/dags/test_dag.py << 'EOL'
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,
    tags=['test'],
)

def test_function():
    print("DAG is working!")
    return "Success"

test_task = PythonOperator(
    task_id='test_task',
    python_callable=test_function,
    dag=dag,
)
EOL
check_success

# Copy the finance pipeline DAG if it exists
if [ -f "s3_processing_dag.py" ]; then
    echo -e "${YELLOW}Copying finance pipeline DAG...${NC}"
    cp s3_processing_dag.py ~/airflow/dags/
    check_success
fi

# Configure Airflow for API access
echo -e "${YELLOW}Configuring Airflow API...${NC}"
cat >> ~/airflow/airflow.cfg << EOL

[api]
auth_backends = airflow.api.auth.backend.basic_auth
EOL
check_success

echo -e "${GREEN}Setup is complete! Follow these steps:${NC}"
echo -e "1. Update AWS credentials in ~/.bashrc"
echo -e "2. Run: source ~/.bashrc"
echo -e "3. Start Airflow: ~/start_airflow.sh"
echo -e "4. Visit http://localhost:8080 in your browser"
echo -e "5. Login with username: admin, password: root@123"

# Remove error handling trap
trap - EXIT