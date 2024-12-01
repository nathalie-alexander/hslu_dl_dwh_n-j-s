"""
Configuration file for the production environment

This file contains all naming conventions and configurations for the production environment.
"""

import logging

logger = logging.getLogger(__name__)


def set_aws_data():
    global AWS_ACCESS_KEY_ID
    global AWS_SECRET_ACCESS_KEY
    global AWS_SESSION_TOKEN
    global AWS_TOKEN_STRING

    for line in AWS_TOKEN_STRING.split("\n")[2:-1]:
        key, value = line.split("=", 1)
        if key == "aws_access_key_id":
            AWS_ACCESS_KEY_ID = value
        elif key == "aws_secret_access_key":
            AWS_SECRET_ACCESS_KEY = value
        elif key == "aws_session_token":
            AWS_SESSION_TOKEN = value

    logger.info("Set AWS credentials:")
    logger.info("AWS_ACCESS KEY ID: \n", AWS_ACCESS_KEY_ID)
    logger.info("AWS_SECRET_ACCESS_KEY: \n", AWS_SECRET_ACCESS_KEY)
    logger.info("AWS_SESSION_TOKEN: \n", AWS_SESSION_TOKEN)


# Database
DB_HOST_ENDPOINT = 'dwl2024db2.cjuim2ggwm61.us-east-1.rds.amazonaws.com'
DB_NAME = 'dwl2024db2'
DB_PORT = 5432
DB_USER = 'postgres'
DB_PASSWORD = 'INSERT PW'

# file paths
BASE_DATA_PATH = 'data'
WEATHER_PREFIX = 'weather_data'
VEHICLES_PREFIX = 'vehicles_data'
DEMOGRAPHICS_PREFIX = 'demographics_data'
DUMMY_PREFIX = 'dummy_data'

# RDS table names
TABLE_NAME_WEATHER_RAW = 'weather_raw'
TABLE_NAME_VECHICLES_RAW = 'vehicles_raw'
TABLE_DEMOGRAPHICS_RAW = 'demographics_raw'
TABLE_NAME_DUMMY = 'dummy_table'

# S3 bucket names
S3_BUCKET_NAME = 'dwl2024-airflow-bucket'

# AWS CLI information --> these change with every instance!
# but usually only necessary when running local code and not for lambdas
AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None
AWS_SESSION_TOKEN = None
AWS_REGION = 'us-east-1'
AWS_TOKEN_STRING = \
"""
[default]
aws_access_key_id=ASIAJQJZJZJZJZJZJZJZ
aws_secret_access_key=SECRET KEY
aws_session_token=SESSION TOKEN
"""

# Set AWS credentials from copied AWS CLI information for fewer copy pastes :
set_aws_data()
logger.info("CONSTANTS loaded.")
