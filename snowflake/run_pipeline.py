import os
import yaml
import logging
import snowflake.connector
from pathlib import Path
import argparse
from time import sleep

class PipelineRunner:
    def __init__(self, config_path='config.yaml', year=None, quarter=None):
        """Initialize pipeline with configuration and time parameters
        
        Args:
            config_path (str): Path to configuration file
            year (int): Year to process data for
            quarter (int): Quarter to process data for (1-4)
        """
        self.year = year
        self.quarter = quarter
        self.validate_time_params()
        self.period_folder = f"{self.year}q{self.quarter}"
        
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.conn = None
        self.cursor = None
        self._connect_snowflake()

    def validate_time_params(self):
        """Validate year and quarter parameters"""
        if self.year is None or self.quarter is None:
            raise ValueError("Both year and quarter must be specified")
        
        try:
            self.year = int(self.year)
            self.quarter = int(self.quarter)
        except ValueError:
            raise ValueError("Year and quarter must be valid integers")
            
        if self.year < 1900 or self.year > 2100:
            raise ValueError("Year must be between 1900 and 2100")
            
        if self.quarter not in [1, 2, 3, 4]:
            raise ValueError("Quarter must be between 1 and 4")

    def run_data_loading(self):
        """Load data into tables for specified year and quarter"""
        load_dir = Path(self.config['file_paths']['load_dir'])
        self.logger.info(f"Starting data loading for period: {self.period_folder}...")
        
        if not load_dir.exists():
            raise Exception(f"Load directory not found: {load_dir}")
        
        # Get AWS credentials from config
        aws_key_id = self.config['s3_stage']['aws_access_key']
        aws_secret_key = self.config['s3_stage']['aws_secret_key']
        
        for sql_file in sorted(load_dir.glob('*.sql')):
            try:
                with open(sql_file, 'r') as f:
                    sql = f.read()
                    self.logger.info(f"Executing SQL from {sql_file} for period {self.period_folder}")
                    
                    # Execute each statement separately
                    for statement in sql.split(';'):
                        if statement.strip():
                            # Replace AWS credentials and period parameters in the SQL
                            statement = statement.replace(':aws_key_id', f"'{aws_key_id}'")
                            statement = statement.replace(':aws_secret_key', f"'{aws_secret_key}'")
                            statement = statement.replace(':period_folder', f"'{self.period_folder}'")
                            statement = statement.replace(':year', str(self.year))
                            statement = statement.replace(':quarter', str(self.quarter))
                            self.cursor.execute(statement.strip())
                
            except Exception as e:
                self.logger.error(f"Failed to load data from {sql_file}: {str(e)}")
                raise

    def validate_loaded_data(self):
        """Validate loaded data for the specified year and quarter"""
        try:
            tables = ['SUB', 'TAG', 'NUM', 'PRE']
            
            for table in tables:
                # Count total rows
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_count = self.cursor.fetchone()[0]
                
                # Count rows for the specific period
                self.cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM {table} 
                    WHERE period = '{self.period_folder}'
                """)
                period_count = self.cursor.fetchone()[0]
                
                self.logger.info(f"Table {table}:")
                self.logger.info(f"  - Total rows: {total_count}")
                self.logger.info(f"  - Rows for {self.period_folder}: {period_count}")
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Run Snowflake data pipeline')
    parser.add_argument('--config', default='config.yaml', 
                      help='Path to configuration file')
    parser.add_argument('--environment', default='development',
                      choices=['development', 'staging', 'production'],
                      help='Environment to run in')
    parser.add_argument('--year', type=int, required=True,
                      help='Year to process data for')
    parser.add_argument('--quarter', type=int, choices=[1, 2, 3, 4], required=True,
                      help='Quarter to process data for (1-4)')
    
    args = parser.parse_args()
    
    try:
        runner = PipelineRunner(args.config, args.year, args.quarter)
        success = runner.run_pipeline()
        exit(0 if success else 1)
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()