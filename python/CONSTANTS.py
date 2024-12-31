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

# ========== PASSWORD SECTION ==========

# Database
DB_HOST_ENDPOINT = 'rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com'
DB_NAME = 'rawdatadb'
DB_PORT = 5432
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

# AWS CLI information --> these change with every instance!
# but usually only necessary when running local code and not for lambdas
AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None
AWS_SESSION_TOKEN = None
AWS_REGION = 'us-east-1'
AWS_TOKEN_STRING = \
"""
[default]
aws_access_key_id=ASIAWIQOUAXGNESJAWHS
aws_secret_access_key=6d8r3eWw5Ay3qV6b573gmzyIiLYG80BTsi40h1sp
aws_session_token=IQoJb3JpZ2luX2VjEGAaCXVzLXdlc3QtMiJGMEQCIE9mj0W3+uBQcbBX+sBAXxOy5CyEaUFPllxOjrTua1NIAiAcvlx5lrREEpUOAG+j8fZA2hxxrHXe9mGxFgQQcYNiLiqkAggZEAEaDDQzMDYwMTE0Mzc1NiIMkOt9yqr4wKAGpv1cKoECOnrAh2P8Gm4HuWfnUH2sYoHDQjrDMhsdXbk6ID1b7kKBo2+Zj+bVk+oV4dFbvunUYfGdpx3ffTpFMT7VSucXe2y6yG86HayB30O/3uuN3phlQ44fqJfwzGaZMSosV4qVS9NsgGq7d1ojBDShxtqw1Z7eH1CW2AiIle8w93L0XtiuC34AKK5xeuHKGskFUPQVP3+QOLPxOtZCvNMQXXpKQ9UeculQ4fMjVZGhGPnO+dGYuU/uen4HT++Ja7ikLACZSwMJ/OfuzZGdZd7bO1ZfJZtG8sBBoyR1DvwC7oPReOBJENG05W9Lkm6VnqaYt9bWOsD0P9Px2a+EeUK6vv/vmQMw3ZbHugY6ngGa/z3zuqLtdCOv2IIfe9EQLX1cOdHZd71nlnWRf+cxchaPiak3mrvUySzxlTrfsZ1UB053yizpvdJm0I6HPOsFOVGtaGi6hWdPRF5cVUwzb+5DXGFfEKumD7GR5NLjnjrndIZZFp+ZTC4GWr/re/ywnphuPG6vrG50ezOjvNH13vvADcPAC6A3scYIwIQL3mXuxPMnFDsHHranFtioXA==
"""

# ======= END OF PASSWORD SECTION =======



# file paths
BASE_DATA_PATH = 'data'
WEATHER_PREFIX = 'weather_data'
VEHICLES_PREFIX = 'vehicles_data'
DEMOGRAPHICS_PREFIX = 'demographics_data'
DUMMY_PREFIX = 'dummy_data'

# RDS table names
TABLE_NAME_WEATHER_RAW = 'weather_raw'
TABLE_NAME_VEHICLES_RAW = 'vehicles_raw'
TABLE_DEMOGRAPHICS_RAW = 'demographics_raw'
TABLE_NAME_DUMMY = 'dummy_table'

# S3 bucket names
S3_BUCKET_NAME = 'my-dwl-bucket'



# Set AWS credentials from copied AWS CLI information for fewer copy pastes :
set_aws_data()
logger.info("CONSTANTS loaded.")
