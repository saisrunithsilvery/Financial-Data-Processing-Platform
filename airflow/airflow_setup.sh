#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Error handling
set -euo pipefail
trap 'echo -e "${RED}Error occurred at line $LINENO${NC}"' ERR

# Project Configuration
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AIRFLOW_HOME=~/airflow
BACKEND_DIR="${PROJECT_ROOT}/backend"

# Function to check command success
check_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Success${NC}"
    else
        echo -e "${RED}✗ Failed${NC}"
        exit 1
    fi
}

# Kill existing processes
kill_existing_processes() {
    echo -e "${YELLOW}Killing existing Airflow processes...${NC}"
    pkill -f "airflow webserver" || true
    pkill -f "airflow scheduler" || true
    
    # Check and kill processes on port 8080
    lsof -ti:8080 | xargs kill -9 || true
    sleep 2
}

# Prepare Python environment
prepare_python_env() {
    echo -e "${YELLOW}Preparing Python environment...${NC}"
    
    # Check if virtual environment exists
    if [ ! -d "${BACKEND_DIR}/venv" ]; then
        echo -e "${YELLOW}Creating virtual environment...${NC}"
        python3 -m venv "${BACKEND_DIR}/venv"
    fi

    # Activate virtual environment
    source "${BACKEND_DIR}/venv/bin/activate"

    # Upgrade pip and install wheel
    pip install --upgrade pip wheel
    check_success
}

# Install Airflow and dependencies
install_airflow_deps() {
    echo -e "${YELLOW}Installing Apache Airflow and dependencies...${NC}"
    
    # Detect Python version
    PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    AIRFLOW_VERSION=2.9.0
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

    # Uninstall existing Airflow
    pip freeze | grep "apache-airflow" | xargs pip uninstall -y || true

    # Install Airflow with constraints
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    
    # Install additional providers and dependencies
    pip install \
        apache-airflow-providers-amazon \
        apache-airflow-providers-snowflake \
        fastapi \
        snowflake-connector-python \
        boto3 \
        python-multipart \
        setuptools
    
    check_success
}

# Setup Airflow configuration
setup_airflow_config() {
    echo -e "${YELLOW}Setting up Airflow configuration...${NC}"
    
    # Create Airflow home directory
    mkdir -p "$AIRFLOW_HOME"/{dags,logs,plugins}
    
    # Generate secret key
    SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    
    # Create airflow.cfg
    cat > "$AIRFLOW_HOME/airflow.cfg" << EOL
[core]
dags_folder = $AIRFLOW_HOME/dags
executor = SequentialExecutor
load_examples = False
pythonpath = $PROJECT_ROOT:$AIRFLOW_HOME

[database]
sql_alchemy_conn = sqlite:////$AIRFLOW_HOME/airflow.db

[logging]
base_log_folder = $AIRFLOW_HOME/logs

[webserver]
base_url = http://localhost:8080
web_server_host = 0.0.0.0
web_server_port = 8080
secret_key = $SECRET_KEY
expose_config = False
auth_backends = airflow.api.auth.backend.basic_auth

[api]
auth_backends = airflow.api.auth.backend.basic_auth
enable_experimental_api = False

[scheduler]
min_file_process_interval = 30
dag_dir_list_interval = 60
catchup_by_default = False

[security]
session_expiration_days = 14
EOL

    # Initialize Airflow database
    airflow db init
    check_success
}

# Create admin user
create_admin_user() {
    echo -e "${YELLOW}Creating admin user...${NC}"
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    check_success
}

# Copy DAG files
copy_dag_files() {
    echo -e "${YELLOW}Copying DAG files and backend...${NC}"
    
    # Copy all DAG files from project to Airflow DAGs directory
    cp "${PROJECT_ROOT}"/dags/*.py "$AIRFLOW_HOME/dags/"
    mkdir -p "$AIRFLOW_HOME/dags/backend/controllers"
    mkdir -p "$AIRFLOW_HOME/dags/backend/models"
    mkdir -p "$AIRFLOW_HOME/dags/backend/utils"
    mkdir -p "$AIRFLOW_HOME/dags/snowflake"

    # Copy backend directory to Airflow home
    cp ../backend/controllers/query_controller.py "$AIRFLOW_HOME/dags/backend/controllers/"
    cp ../backend/models/query_model.py "$AIRFLOW_HOME/dags/backend/models/"
    cp ../snowflake/run_pipeline.py "$AIRFLOW_HOME/dags/snowflake/"
    cp ./dags/s3_processing_dag.py "$AIRFLOW_HOME/dags/"    
    
    # Set PYTHONPATH to include the project root
    echo -e "${YELLOW}Setting PYTHONPATH...${NC}"
    export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
    
    check_success
}

# Create start script
create_start_script() {
    echo -e "${YELLOW}Creating Airflow start script...${NC}"
    cat > "$AIRFLOW_HOME/start_airflow.sh" << 'EOL'
#!/bin/bash
set -e

# Activate virtual environment
source "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../backend/venv/bin/activate"

# Set PYTHONPATH to include the project root
export PYTHONPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../:${PYTHONPATH:-}"

# Kill existing processes
pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true
sleep 2

# Start webserver
echo "Starting Airflow webserver..."
airflow webserver --port 8080 --daemon

# Wait for webserver to start
echo "Waiting for webserver to start..."
timeout 30 bash -c 'until curl -s http://localhost:8080 > /dev/null; do sleep 1; done'

# Start scheduler
echo "Starting Airflow scheduler..."
airflow scheduler
EOL

    chmod +x "$AIRFLOW_HOME/start_airflow.sh"
    check_success
}

# Main setup function

    kill_existing_processes
    prepare_python_env
    install_airflow_deps
    setup_airflow_config
    create_admin_user
    copy_dag_files
    create_start_script

    echo -e "${GREEN}Airflow setup complete!${NC}"
    echo -e "1. Start Airflow: ${AIRFLOW_HOME}/start_airflow.sh"
    echo -e "2. Visit http://localhost:8080"

