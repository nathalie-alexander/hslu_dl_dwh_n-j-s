import json
import boto3
import psycopg2
import csv
from io import StringIO

# Database configuration
DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

# S3 configuration
S3_BUCKET = "hsludwlbucket.stella"
S3_FILE = "dim_time_daily.csv"

def create_insert_clean_time(event, context):
    # Connect to the database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Database connection failed: {str(e)}"
        }

    # Create table if it doesn't exist
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS time_clean (
            time_id BIGINT PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            date DATE NOT NULL,
            day INT NOT NULL,
            month INT NOT NULL,
            year INT NOT NULL,
            weekday VARCHAR(10) NOT NULL,
            week_of_year INT NOT NULL,
            is_weekend BOOLEAN NOT NULL
        );
        """
        cur.execute(create_table_query)
        conn.commit()
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Failed to create table: {str(e)}"
        }

    # Download the CSV file from S3
    try:
        s3 = boto3.client('s3')
        csv_file = s3.get_object(Bucket=S3_BUCKET, Key=S3_FILE)
        csv_content = csv_file['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Failed to fetch CSV file from S3: {str(e)}"
        }

    # Insert CSV data into PostgreSQL table
    try:
        for row in csv_reader:
            insert_query = """
                INSERT INTO time_clean (time_id, timestamp, date, day, month, year, weekday, week_of_year, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                row['time_id'],
                row['timestamp'],
                row['date'],
                row['day'],
                row['month'],
                row['year'],
                row['weekday'],
                row['week_of_year'],
                row['is_weekend']
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Failed to insert data into the database: {str(e)}"
        }
    finally:
        cur.close()
        conn.close()

    return {
        "statusCode": 200,
        "body": "Data successfully inserted into time_clean table."
    }