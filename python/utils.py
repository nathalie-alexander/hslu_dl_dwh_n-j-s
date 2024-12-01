import psycopg2
import CONSTANTS
import boto3
import os
import json
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                   format="%(asctime)s %(levelname)s - %(module)s.py %(funcName)s(): %(message)s",
                   datefmt='%H:%M:%S')


def extract_timestamp_from_string(file_name, format="\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"):
    """
    Extract the timestamp from a string that is formatted like "vehicles_data_2024-10-22_12-04-09.json"
    :param file_name: file name
    :param format: usually needs no change
    :return: date object
    """
    # Create regex pattern
    datepattern = re.compile(format)
    matcher = datepattern.search(file_name)

    if matcher is None:
        logger.warning("No matching date found.")
        return None

    # Extract date in the format "YYYY-MM-DD_HH-MM-SS"
    date_format = "%Y-%m-%d_%H-%M-%S"
    actual_date = datetime.strptime(matcher.group(0), date_format)
    return actual_date


def create_s3_client(aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None):
    """
    Create an S3 client and automatically determines if it is running in AWS Lambda or locally

    Provide the AWS credentials if running locally. If not provided, the credentials from the CONSTANTS file are used.

    :param aws_access_key_id: aws access key id from Cloud Access CLI information
    :param aws_secret_access_key: aws secret access key from Cloud Access CLI information
    :param aws_session_token: aws session token from Cloud Access CLI information
    :return:
    """


    # check if running in AWS Lambda or locally
    if 'LAMBDA_TASK_ROOT' in os.environ:
        logger.info("Create S3 client: We are in AWS Lambda.")
        return boto3.client('s3')
    else:
        logger.info("Create S3 client: We are running locally.")
        if aws_access_key_id is None and aws_secret_access_key is None and aws_session_token is None:
            logger.info("No credentials provided: Use AWS credentials from CONSTANTS file.")
            aws_access_key_id = CONSTANTS.AWS_ACCESS_KEY_ID
            aws_secret_access_key = CONSTANTS.AWS_SECRET_ACCESS_KEY
            aws_session_token = CONSTANTS.AWS_SESSION_TOKEN

        return boto3.client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            aws_session_token=aws_session_token)


def list_s3_directory_files(s3_client, bucket, directory, prefix=None):
    """
    List all files in a directory on S3

    :param s3_client: s3 client
    :param bucket: bucket name to list files from
    :param directory: directory to list files from
    :param prefix: only list files that start with this prefix
    :return:
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=directory)

    # Get all files in the directory (but no directories)
    all_files = [content['Key'] for content in response['Contents'] if not content['Key'].endswith('/')]

    # Filter files by prefix
    complete_prefix = directory + "/" + prefix if prefix is not None else None

    if complete_prefix is not None:
        # Filter files by prefix
        files = [f for f in all_files if f.startswith(complete_prefix)]
    else:
        files = []
        logger.info("No matching files found.")

    return files


def write_loaded_files_to_cache(files_to_append, s3_client, bucket, directory, data_prefix):
    """
    Write the names of the loaded files to a cache file on S3 (so we know which ones are in the database already)

    :param files_to_append: list of files to append to the cache file
    :param s3_client: s3 client
    :param bucket: bucket name
    :param directory: directory to store the cache file
    :param data_prefix: prefix for the cache file (either "weather" or "vehicles")
    :return:
    """
    try:
        # download existing cache file
        response = s3_client.get_object(Bucket=bucket, Key=directory + "/loaded_files_" + data_prefix + ".txt")
        loaded_files = response['Body'].read().decode('utf-8')
    except Exception as e:
        logger.warning("No cache file found. Create new one.")
        loaded_files = ""

    for file in files_to_append:
        if file in loaded_files:
            logger.warning("WARNING! File already loaded. Will not append it.")

    # append new files
    if files_to_append:
        loaded_files += "\n".join(files_to_append)
        loaded_files += "\n"

    # upload cache file
    s3_client.put_object(Bucket=bucket, Key=directory + "/loaded_files_" + data_prefix + ".txt", Body=loaded_files)


def read_loaded_files_from_cache(s3_client, bucket, directory, data_prefix):
    """
    Read the names of the loaded files from a cache file on S3 (so we know which ones are in the database already)

    :param s3_client: s3 client
    :param bucket: bucket name
    :param directory: directory to store the cache file
    :param data_prefix:  data prefix (either "weather" or "vehicles")
    :return:
    """
    try:
        # download existing cache file
        response = s3_client.get_object(Bucket=bucket, Key=directory + "/loaded_files_" + data_prefix + ".txt")
        loaded_files = response['Body'].read().decode('utf-8')
        return loaded_files.splitlines()
    except Exception as e:
        logger.warning("No cache file found. Return empty list.")
        return []


def clear_cache(s3_client, bucket, directory, data_prefix):
    """
    Clear the cache file on S3, i.e. delete the "loaded_files_xyz.txt" file

    :param s3_client: s3 client
    :param bucket: bucket name
    :param directory: directory to store the cache file
    :param data_prefix: data prefix (either "weather" or "vehicles")
    :return:
    """
    s3_client.delete_object(Bucket=bucket, Key=directory + "/loaded_files_" + data_prefix + ".txt")


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Create a connection to the database. Use the credentials from the CONSTANTS file.

    If the database does not exist, it will be created.

    :return: connection to the database
    """
    # Connect to the database
    try:
        connection = psycopg2.connect(
            host=CONSTANTS.DB_HOST_ENDPOINT,
            database=CONSTANTS.DB_NAME,
            user=CONSTANTS.DB_USER,
            password=CONSTANTS.DB_PASSWORD
        )
    except psycopg2.OperationalError as e:
        # check if "database" and "does not exists" is the error --> then create the database
        if "does not exist" in str(e) and "database" in str(e):
            logger.warning(f"Database {CONSTANTS.DB_NAME} does not exist. Create database now.")

            # Connect to the default database
            connection = psycopg2.connect(
                host=CONSTANTS.DB_HOST_ENDPOINT,
                user=CONSTANTS.DB_USER,
                password=CONSTANTS.DB_PASSWORD
            )

            # Set autocommit to True to enable database creation
            connection.autocommit = True

            # Create the database
            create_database(connection, CONSTANTS.DB_NAME)
            connection.close()

            # Connect to the database
            connection = psycopg2.connect(
                host=CONSTANTS.DB_HOST_ENDPOINT,
                database=CONSTANTS.DB_NAME,
                user=CONSTANTS.DB_USER,
                password=CONSTANTS.DB_PASSWORD
            )
        else:
            logger.warning("Database creation failed due to the following error:", e)
            return

    return connection


def create_table(connection, query):
    """
    Create a table in the database
    :param connection: connection to the database
    :param query: creation query
    :return:
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        logger.info(f"Table created successfully.")
    except Exception as e:
        logger.warning(f"Table could not be created due to the following error:", e)


def create_database(connection, db_name):
    """
    Create a database
    :param connection: connection to the database
    :param db_name: name of the database
    :return:
    """
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE {db_name}")
        connection.commit()
        cursor.close()
        logger.info(f"Database {db_name} created successfully.")
    except Exception as e:
        logger.warning(f"Database {db_name} could not be created due to the following error:", e)


def clear_table(connection, table_name):
    """
    Clear a table in the database, i.e. delete all rows

    :param connection: connection to the database
    :param table_name: name of the table
    :return:
    """
    try:
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        connection.commit()
        cursor.close()
        logger.info(f"Table {table_name} cleared successfully.")
    except Exception as e:
        logger.warning(f"Table {table_name} could not be cleared due to the following error:", e)


def load_file_from_s3(s3_client, s3_bucket_name, file):
    """
    Load a single file from S3

    :param s3_client: s3 client
    :param s3_bucket_name: bucket name
    :param file: file name to load (full path
    :return: file content
    """
    try:
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=file)
        format = "utf-8-sig" if file.endswith(".csv") else "utf-8"

        data = response['Body'].read().decode(format)
        return data
    except Exception as e:
        logger.error(f"Could not load data from S3 due to the following error:", e)
    return None


def dump_s3_bucket_to_local(s3_client, s3_bucket_name, directory, dry_run=False):
    """
    Dump the content of an S3 bucket to a local directory
    :param s3_client: Existing S3 client
    :param s3_bucket_name:
    :param directory:
    :param dry_run:
    :return:
    """

    # preprocessing
    if not directory.endswith("/"):
        directory += "/"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # actually dump the bucket
    response = s3_client.list_objects_v2(Bucket=s3_bucket_name)
    i = 1
    num_files = len(response['Contents'])
    for content in response['Contents']:
        key = content['Key']
        status = f"File {i}/{num_files}: {key}"
        if dry_run:
            status = "(DRY) " + status

        logger.info(status)

        if not dry_run:
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=key)
            data = response['Body'].read()
            with open(directory + "/" + key, "wb") as f:
                f.write(data)

        i += 1

    logger.info(f"Bucket {s3_bucket_name} dumped to {directory}.")
