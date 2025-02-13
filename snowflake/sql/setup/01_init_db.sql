-- 1. Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::891377329304:role/snowflake_access_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://damgassign02/unziped_folder/2018q4/');

-- 2. Grant integration usage to role
GRANT USAGE ON INTEGRATION s3_int TO ROLE accountadmin;

-- 3. Create database and schema
CREATE DATABASE IF NOT EXISTS damg_sec_db;
USE DATABASE damg_sec_db;
CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

-- 4. Grant necessary privileges
GRANT CREATE TABLE ON DATABASE damg_sec_db TO ROLE accountadmin;
GRANT USAGE ON SCHEMA public TO ROLE accountadmin;

-- 5. Create file format
CREATE OR REPLACE FILE FORMAT TXT_FILE_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = '\t'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE;

-- 6. Create stage with explicit permissions
CREATE OR REPLACE STAGE s3_stage
  URL = 's3://damgassign02/unziped_folder/2018q4/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = TXT_FILE_FORMAT;

-- 7. Grant stage usage
GRANT USAGE ON STAGE s3_stage TO ROLE accountadmin;

-- 8. Test access
LIST @s3_stage;