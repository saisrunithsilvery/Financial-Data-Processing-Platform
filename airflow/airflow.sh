#!/bin/bash

# setup_airflow.sh

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Airflow Setup...${NC}"

# Install Airflow
echo -e "${YELLOW}Installing Apache Airflow...${NC}"
pip install apache-airflow

# Set Airflow home directory
echo -e "${YELLOW}Setting up Airflow home directory...${NC}"
export AIRFLOW_HOME=~/airflow
echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc

# Initialize the database
echo -e "${YELLOW}Initializing Airflow database...${NC}"
airflow db init

# Create admin user
echo -e "${YELLOW}Creating admin user...${NC}"
airflow users create \
    --username admin \
    --firstname Vishal \
    --lastname Prasanna \
    --role Admin \
    --email vishalprasanna11@gmail.com\
    --password root@123

# Create necessary directories
echo -e "${YELLOW}Creating project directories...${NC}"
mkdir -p ~/airflow/dags
mkdir -p ~/airflow/logs
mkdir -p ~/airflow/plugins

# Set AWS credentials (update these with your credentials)
echo -e "${YELLOW}Setting up AWS credentials...${NC}"
#echo "export AWS_ACCESS_KEY_ID='your_access_key'" >> ~/.bashrc
#echo "export AWS_SECRET_ACCESS_KEY='your_secret_key'" >> ~/.bashrc
#echo "export AWS_REGION='us-east-1'" >> ~/.bashrc

# Create a script to start Airflow services
echo -e "${YELLOW}Creating start script...${NC}"
cat > ~/start_airflow.sh << 'EOL'
#!/bin/bash

# Start webserver
echo "Starting Airflow webserver..."
airflow webserver --port 8080 &

# Wait a few seconds
sleep 5

# Start scheduler
echo "Starting Airflow scheduler..."
airflow scheduler
EOL

# Make the start script executable
chmod +x ~/start_airflow.sh

echo -e "${GREEN}Setup completed!${NC}"
echo -e "${GREEN}To start Airflow, run: ~/start_airflow.sh${NC}"
echo -e "${GREEN}Access the Airflow UI at: http://localhost:8080${NC}"
echo -e "${GREEN}Login with username: admin, password: admin${NC}"

# Create a requirements.txt file
echo -e "${YELLOW}Creating requirements.txt...${NC}"
cat > ~/airflow/requirements.txt << 'EOL'
apache-airflow
boto3
fastapi
uvicorn
python-multipart
EOL

echo -e "${YELLOW}Installing additional requirements...${NC}"
pip install -r ~/airflow/requirements.txt

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
    schedule_interval=None,
)

def test_function():
    print("DAG is working!")

test_task = PythonOperator(
    task_id='test_task',
    python_callable=test_function,
    dag=dag,
)
EOL

echo -e "${GREEN}Setup is complete! Follow these steps:${NC}"
echo -e "1. Update AWS credentials in ~/.bashrc"
echo -e "2. Run: source ~/.bashrc"
echo -e "3. Start Airflow: ~/start_airflow.sh"
echo -e "4. Visit http://localhost:8080 in your browser"