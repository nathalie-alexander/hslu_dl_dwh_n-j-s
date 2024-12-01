#
# This code should be used to insert data from JSON files into a PostgreSQL table. 
# Make sure the table exists, i.e. the "RAW Tables Draft.sql" script has been executed.
#
# This code is still untested.
#
# Run this inside a Lambda function.

# TODO: 
# - Make loading from S3 work
# - Cache the files that have been loaded to avoid re-loading them. 
#   --> e.g. Make a txt-file with the names of the files that have been loaded and put txt-file into S3 as well
#   --> Maybe, Glue can do this for us?


import json
import psycopg2
import os
from datetime import datetime
import re
import pprint as pp
import boto3
import CONSTANTS

# for local debugging only
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'dwl'
os.environ['DB_USER'] = 'postgres'
os.environ['DB_PASSWORD'] = 'admin'

# Retrieve database credentials and configuration from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Initialize the database connection outside the handler for re-use in warm invocations
connection = None


def get_db_connection():
    global connection
    if connection is None or connection.closed != 0:
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    return connection


def lambda_handler(event, context):
    # Load JSON data from the event or directly within Lambda (if available as a file in the code package)
    data = json.loads(event['data'])
    
    # Insert data into PostgreSQL
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for entry in data:
        geometry = entry["geometry"]
        attributes = entry["attributes"]

        insert_query = """
        INSERT INTO shared_mobility_raw (
            timestamp, provider_id, provider_name, provider_timezone,
            provider_apps_ios_store_uri, provider_apps_android_store_uri,
            vehicle_id, available, pickup_type, vehicle_status_disabled,
            vehicle_status_reserved, geometry_x, geometry_y
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vehicle_id, timestamp) DO NOTHING;
        """

        values = (
            event["timestamp"],
            attributes.get("provider_id"),
            attributes.get("provider_name"),
            attributes.get("provider_timezone"),
            attributes.get("provider_apps_ios_store_uri"),
            attributes.get("provider_apps_android_store_uri"),
            attributes.get("id"),
            attributes.get("available"),
            attributes.get("pickup_type"),
            attributes.get("vehicle_status_disabled"),
            attributes.get("vehicle_status_reserved"),
            geometry.get("x"),
            geometry.get("y")
        )
        
        cursor.execute(insert_query, values)

    # Commit and close cursor
    conn.commit()
    cursor.close()

    # Return a success message
    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully.')
    }


def list_directory_files(directory, prefix=None):
    return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and (prefix is None or f.startswith(prefix))]


def tmp():
    # For local testing
    file = "data/dummy_data_2024-11-17_19-02-00.jsom"
    ts = utils.extract_timestamp_from_string(file)
    print("ts = ", ts)
    # with open(file) as f:
    #     event = {'data': f.read(),
    #              'timestamp': actual_date}
    #     lambda_handler(event, None)

    # dir = "../data"
    # prefix = "weather_data"
    # files = list_directory_files(dir, prefix)
    #
    # # check if file has already been loaded
    # try:
    #     with open("../data/loaded_files_weather.txt") as f:
    #         loaded_files = f.read().splitlines()
    #         print("loaded files: ")
    #         pp.pprint(loaded_files)
    #         files = [f for f in files if f not in loaded_files]
    # except FileNotFoundError:
    #     print("No cache file found. All files will be loaded.")
    #
    # # load files
    # for file in files:
    #     print("new file: ", file)
    #
    #     # write all file names into a txt-file as cache
    #     with open("../data/loaded_files_weather.txt", "a") as f:
    #         f.write(file + "\n")





import utils

if __name__ == '__main__':
    # s3_client = boto3.client('s3',
    #                          aws_access_key_id=CONSTANTS.AWS_ACCESS_KEY_ID,
    #                          aws_secret_access_key=CONSTANTS.AWS_SECRET_ACCESS_KEY,
    #                          aws_session_token=CONSTANTS.AWS_SESSION_TOKEN,
    #                          region_name=CONSTANTS.AWS_REGION)

    tmp()




