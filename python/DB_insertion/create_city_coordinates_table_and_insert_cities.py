# lambda function to create city coordinates table and insert data
import json
import boto3
import pandas as pd
import psycopg2
import os
import io

def create_insert_city_coordinates(event, context):
    # PostgreSQL-connection details
    host = "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com"
    database = "rawdatadb"
    user = "postgres"
    password = "INSERT PW"
    port = 5432

    # S3-Bucket and file
    bucket_name = "hsludwlbucket.stella"
    file_name = "Cities_with_coordinates.csv"

    # create connection to S3-Bucket
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        csv_data = response["Body"].read().decode("utf-8")
    except Exception as e:
        return {"statusCode": 500, "body": f"Error reading CSV file from S3: {str(e)}"}

    # load CSV in DataFrame
    try:
        df = pd.read_csv(io.StringIO(csv_data), sep=";")
    except Exception as e:
        return {"statusCode": 500, "body": f"Error parsing CSV: {str(e)}"}

    # establish connection to the PostgreSQL-Database
    try:
        conn = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )
        cursor = conn.cursor()
    except Exception as e:
        return {"statusCode": 500, "body": f"Error connecting to PostgreSQL: {str(e)}"}

    # create table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS city_coordinates (
        city TEXT,
        coordinates TEXT,
        latitude FLOAT,
        longitude FLOAT
    );
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
    except Exception as e:
        return {"statusCode": 500, "body": f"Error creating table: {str(e)}"}

    # insert data into table
    insert_query = """
    INSERT INTO city_coordinates (city, coordinates, latitude, longitude)
    VALUES (%s, %s, %s, %s)
    """
    try:
        for _, row in df.iterrows():
            cursor.execute(
                insert_query, (row["city"], row["coordinates"], row["latitude"], row["longitude"])
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        return {"statusCode": 500, "body": f"Error inserting data: {str(e)}"}
    finally:
        cursor.close()
        conn.close()

    return {"statusCode": 200, "body": "Data successfully inserted into PostgreSQL!"}
