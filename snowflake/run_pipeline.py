import os
import yaml
import logging
import snowflake.connector
from pathlib import Path
import argparse
from time import sleep

class PipelineRunner:
    def __init__(self, config_path='config.yaml'):
        """Initialize pipeline with configuration"""
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.conn = None
        self.cursor = None
        self._connect_snowflake()
        
    def _load_config(self, config_path):
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"Error loading config file: {str(e)}")

    def _setup_logging(self):
        """Configure logging based on config settings"""
        log_config = self.config['logging']
        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format=log_config['format'],
            filename=log_config.get('file'),
            filemode='a'
        )
        self.logger = logging.getLogger(__name__)

    def _connect_snowflake(self):
        """Establish connection to Snowflake"""
        try:
            sf_config = self.config['snowflake']
            # Initial connection without database/schema
            self.conn = snowflake.connector.connect(
                account=sf_config['account'],
                user=sf_config['user'],
                password=sf_config['password'],
                warehouse=sf_config['warehouse'],
                role=sf_config.get('role', 'ACCOUNTADMIN')
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Successfully connected to Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    def _setup_storage_integration(self):
        """Set up storage integration and grant necessary privileges"""
        try:
            integration_name = self.config['s3_stage']['storage_integration']
            s3_config = self.config.get('s3_config', {})
            
            # Try to describe the integration to check if it exists
            try:
                self.cursor.execute(f"DESC STORAGE INTEGRATION {integration_name}")
                self.logger.info(f"Storage integration {integration_name} already exists")
            except snowflake.connector.errors.ProgrammingError:
                # If integration doesn't exist, create it
                create_integration_sql = f"""
                CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
                    TYPE = EXTERNAL_STAGE
                    STORAGE_PROVIDER = 'S3'
                    ENABLED = TRUE
                    STORAGE_AWS_ROLE_ARN = '{s3_config.get('aws_role_arn', 'arn:aws:iam::891377329304:role/snowflake_access_role')}'
                    STORAGE_ALLOWED_LOCATIONS = ('{self.config['s3_stage']['url']}');
                """
                self.cursor.execute(create_integration_sql)
                self.logger.info(f"Created storage integration: {integration_name}")
            
            # Grant integration usage - use ACCOUNTADMIN to ensure permissions
            self.cursor.execute(f"GRANT USAGE ON INTEGRATION {integration_name} TO ROLE ACCOUNTADMIN")
            
        except Exception as e:
            self.logger.error(f"Storage integration setup failed: {str(e)}")
            raise

    def _setup_file_format(self):
        """Set up file format for data loading"""
        try:
            create_file_format_sql = """
            CREATE OR REPLACE FILE FORMAT TXT_FILE_FORMAT
                TYPE = CSV
                FIELD_DELIMITER = '\t'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                TRIM_SPACE = TRUE
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
            """
            self.cursor.execute(create_file_format_sql)
            self.logger.info("File format TXT_FILE_FORMAT created successfully")
        except Exception as e:
            self.logger.error(f"File format setup failed: {str(e)}")
            raise
    
    def _setup_database(self):
        """Set up database and schema"""
        try:
            sf_config = self.config['snowflake']
            
            # Create database if not exists
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {sf_config['database']}")
            self.cursor.execute(f"USE DATABASE {sf_config['database']}")
            
            # Create schema if not exists
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {sf_config['schema']}")
            self.cursor.execute(f"USE SCHEMA {sf_config['schema']}")
            
            # Set warehouse
            self.cursor.execute(f"USE WAREHOUSE {sf_config['warehouse']}")
            
            # We'll simplify privilege management
            # Typically, this is handled by a Snowflake admin before running the pipeline
            self.logger.info(f"Database setup completed: {sf_config['database']}.{sf_config['schema']}")
        except Exception as e:
            self.logger.error(f"Database setup failed: {str(e)}")
            raise

    def _setup_stage(self):
        """Set up S3 stage"""
        try:
            stage_config = self.config['s3_stage']
            create_stage_sql = f"""
            CREATE OR REPLACE STAGE {stage_config['name']}
                URL = '{stage_config['url']}'
                STORAGE_INTEGRATION = {stage_config['storage_integration']}
                FILE_FORMAT = TXT_FILE_FORMAT;
            """
            self.cursor.execute(create_stage_sql)
            
            # Simplified stage usage grant
            self.cursor.execute(f"GRANT USAGE ON STAGE {stage_config['name']} TO ROLE ACCOUNTADMIN")
            
            self.logger.info(f"Stage created: {stage_config['name']}")
        except Exception as e:
            self.logger.error(f"Stage setup failed: {str(e)}")
            raise

    def execute_sql_file(self, file_path, retries=None):
        """Execute SQL from file with retry logic"""
        # Use retry settings from config if not specified
        if retries is None:
            retries = self.config['error_handling'].get('max_retries', 3)
        
        retry_count = 0
        while retry_count < retries:
            try:
                with open(file_path, 'r') as f:
                    sql = f.read()
                    self.logger.info(f"Executing SQL from {file_path}")
                    
                    # Execute each statement separately
                    for statement in sql.split(';'):
                        if statement.strip():
                            self.cursor.execute(statement.strip())
                    return True
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Error executing {file_path}: {str(e)}")
                if retry_count < retries:
                    sleep(self.config['error_handling'].get('retry_delay', 60))
                else:
                    if self.config['error_handling'].get('stop_on_error', True):
                        raise
                    return False

    def run_table_creation(self):
        """Create database tables"""
        tables_dir = Path(self.config['file_paths']['tables_dir'])
        self.logger.info("Starting table creation...")
        
        if not tables_dir.exists():
            raise Exception(f"Tables directory not found: {tables_dir}")
        
        for sql_file in sorted(tables_dir.glob('*.sql')):
            if not self.execute_sql_file(sql_file):
                raise Exception(f"Failed to create tables from {sql_file}")

    def run_data_loading(self):
        """Load data into tables"""
        load_dir = Path(self.config['file_paths']['load_dir'])
        self.logger.info("Starting data loading...")
        
        if not load_dir.exists():
            raise Exception(f"Load directory not found: {load_dir}")
        
        for sql_file in sorted(load_dir.glob('*.sql')):
            if not self.execute_sql_file(sql_file):
                raise Exception(f"Failed to load data from {sql_file}")

    def validate_loaded_data(self):
        """Validate loaded data"""
        try:
            tables = ['SUB', 'TAG', 'NUM', 'PRE']
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                self.logger.info(f"Table {table} contains {count} rows")
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            raise

    def run_pipeline(self):
        """Execute the complete pipeline"""
        try:
            self.logger.info(f"Starting pipeline in {self.config['environment']} environment")
            
            # Step 1: Setup storage integration
            self._setup_storage_integration()
            
            # Step 2: Setup database and schema
            self._setup_database()
            
            # Step 3: Setup file format
            self._setup_file_format()
            
            # Step 4: Setup stage
            self._setup_stage()
            
            # Step 5: Create tables
            self.run_table_creation()
            
            # Step 6: Load data
            self.run_data_loading()
            
            # Step 7: Validate data
            self.validate_loaded_data()
            
            self.logger.info("Pipeline completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return False
            
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
            self.logger.info("Cleaned up Snowflake connection")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Run Snowflake data pipeline')
    parser.add_argument('--config', default='config.yaml', 
                      help='Path to configuration file')
    parser.add_argument('--environment', default='development',
                      choices=['development', 'staging', 'production'],
                      help='Environment to run in')
    
    args = parser.parse_args()
    
    try:
        runner = PipelineRunner(args.config)
        success = runner.run_pipeline()
        exit(0 if success else 1)
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()