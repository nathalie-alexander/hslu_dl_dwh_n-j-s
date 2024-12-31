# Lambda function to create temp_vehicle_demographics_with_distances table and insert data
import boto3
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import io

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

S3_BUCKET = "hsludwlbucket.stella"
CSV_FILE_KEY = "temp_vehicle_demographics_with_distance.csv"

def lambda_handler(event, context):
    try:
        # Connect to S3 and fetch the CSV file
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=S3_BUCKET, Key=CSV_FILE_KEY)
        csv_data = response["Body"].read().decode("utf-8")

        # Load the CSV into a pandas DataFrame
        df = pd.read_csv(io.StringIO(csv_data))

        # Establish a connection to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS temp_distances (
            vehicle_id TEXT,
            latitude FLOAT,
            longitude FLOAT,
            rounded_latitude FLOAT,
            rounded_longitude FLOAT,
            rounded_timestamp TIMESTAMP,
            geo_nr TEXT,
            distance FLOAT,
            timestamp TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        conn.commit()

        # Prepare the data for bulk insertion
        data_to_insert = [
            (
                row["vehicle_id"], row["latitude"], row["longitude"], 
                row["rounded_latitude"], row["rounded_longitude"],
                row["rounded_timestamp"], row["geo_nr"], 
                row["distance"], row["timestamp"]
            )
            for _, row in df.iterrows()
        ]

        # Insert data into the table using bulk insert
        insert_query = """
        INSERT INTO temp_distances (
            vehicle_id, latitude, longitude, rounded_latitude, 
            rounded_longitude, rounded_timestamp, geo_nr, 
            distance, timestamp
        ) VALUES %s
        ON CONFLICT DO NOTHING;
        """
        execute_values(cur, insert_query, data_to_insert, page_size=1000)
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        return {"statusCode": 500, "body": f"Error inserting data into table: {str(e)}"}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return {"statusCode": 200, "body": "Data successfully inserted into PostgreSQL!"}
