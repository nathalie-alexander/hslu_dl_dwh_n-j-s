#
# This code should be used to insert data from JSON files into a PostgreSQL table. 
# Make sure the table exists, i.e. the "RAW Tables Draft.sql" script has been executed.
#
# Run this from your local machine.

import csv
import utils
import CONSTANTS
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

def load_csv_to_db(file_path):
    """
    Load the data from the CSV file into the database
    :param file_path: file path INSIDE the S3 bucket
    :return:
    """

    # Load the data from the CSV file
    s3_client = utils.create_s3_client()
    data = utils.load_file_from_s3(s3_client, CONSTANTS.S3_BUCKET_NAME, file_path)
    reader = csv.DictReader(data.splitlines())

    # Establish connection to the database
    connection = utils.get_db_connection()
    cursor = connection.cursor()

    # Clear the table since we only have 1 csv file
    utils.clear_table(connection, CONSTANTS.TABLE_DEMOGRAPHICS_RAW)

    # Prepare the data for bulk insert for better performance
    data_to_insert = []
    for row in reader:

        # do some cleaning
        if row["VALUE_PERIOD"] == "2022-01-01" or row["VALUE_PERIOD"] == "2022-12-31":
            row["VALUE_PERIOD"] = "2022"
        elif row["VALUE_PERIOD"] == "2011/2022":
            # ignore these rows
            continue

        if row["VALUE"] == "":
            row["VALUE"] = "0"

        data_to_insert.append(tuple([
            row["GEO_NR"],
            row["GEO_NAME"],
            row["CLASS_HAB"],
            row["GEOM_PERIOD"],
            row["VARIABLE"],
            row["SOURCE"],
            int(row["VALUE_PERIOD"]),
            row["UNIT_VALUE"],
            float(row["VALUE"]),
            row["STATUS"]]
        ))

    # Bulk insert the data
    insert_query = f"""
        INSERT INTO {CONSTANTS.TABLE_DEMOGRAPHICS_RAW} (
            geo_nr, geo_name, class_hab, geom_period, variable, source,
            value_period, unit_value, value, status
        ) VALUES %s;
        """

    # bulk insert data
    execute_values(cursor, insert_query, data_to_insert, page_size=1000)

    # Commit and close the cursor
    connection.commit()
    cursor.close()

    # Close the connection
    connection.close()

    logger.info("Data loaded successfully.")

    # query to check the number of rows in the table
    query = f"SELECT COUNT(*) FROM {CONSTANTS.TABLE_DEMOGRAPHICS_RAW};"

    # query to check if table exists
    query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{CONSTANTS.TABLE_DEMOGRAPHICS_RAW}');"

    # query to check the first 5 rows in the table
    query = f"SELECT * FROM {CONSTANTS.TABLE_DEMOGRAPHICS_RAW} LIMIT 5;"

    # execute the query
    result = connection.execute(query)

import datetime
if __name__ == "__main__":
    # load_csv_to_db("data/demographics_data.csv")

    # Load the data from the CSV file
    # s3_client = utils.create_s3_client()

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dump_string = timestamp + ": DUMP STRING EMPTY"
    try:
        # Establish connection to the database
        connection = utils.get_db_connection()
        cursor = connection.cursor()

        # query to check the first 5 rows in the table
        query = f"""
SELECT EXISTS (
    SELECT FROM 
        pg_tables
    WHERE 
        schemaname = 'public' AND 
        tablename  = '{CONSTANTS.TABLE_DEMOGRAPHICS_RAW}'
    );"""

        dump_string = timestamp + " ========= NEW ENTRY ============="
        dump_string += "\n" + f"Checking if table '{CONSTANTS.TABLE_DEMOGRAPHICS_RAW}' exists..."
        dump_string += "\n" + (f"Query: {query}")

        # execute the query
        cursor.execute(query)
        exists = cursor.fetchone()[0]

        dump_string += "\n" + (f"Table '{CONSTANTS.TABLE_DEMOGRAPHICS_RAW}' exists: {exists}")

        if exists:

            # Count the number of rows in the table
            dump_string += "\n" + ("Counting the number of rows in the table...")
            query = f"SELECT COUNT(*) FROM {CONSTANTS.TABLE_DEMOGRAPHICS_RAW};"
            dump_string += "\n" + (f"Query: \n{query}")
            cursor.execute(query)
            row_count = cursor.fetchone()[0]
            dump_string += "\n" + (f"Number of rows in '{CONSTANTS.TABLE_DEMOGRAPHICS_RAW}': {row_count}")

            # Query the first 5 rows
            dump_string += "\n" + ("Querying the first 5 rows in the table...")
            query = f"SELECT * FROM {CONSTANTS.TABLE_DEMOGRAPHICS_RAW} LIMIT 5;"
            dump_string += "\n" + (f"Query: \n{query}")
            cursor.execute(query)
            rows = cursor.fetchall()

            dump_string += "\n" + (f"First 5 rows from '{CONSTANTS.TABLE_DEMOGRAPHICS_RAW}':")
            for row in rows:
                dump_string += "\n" + str(row)

        else:
            dump_string += "\n" + (f"WARNING: Table '{CONSTANTS.TABLE_DEMOGRAPHICS_RAW}' does not exist.")

    except Exception as e:
        dump_string += "\n" + (f"ERROR: {e}")
    finally:
        # Clean up and close the connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    print(dump_string)

    # print string to file on s3
    file_name = "WATCH_DOG_DEMOGRAPHICS_RAW.txt"
    s3_client = utils.create_s3_client()

    try:
        # download existing cache file
        response = s3_client.get_object(Bucket=CONSTANTS.S3_BUCKET_NAME, Key=file_name)
        loaded_files = response['Body'].read().decode('utf-8')
        dump_string = loaded_files + dump_string
    except Exception as e:
        dump_string += ("\nWARNING: No cache file found. Create new one.")

    print(dump_string)

    # upload cache file
    s3_client.put_object(Bucket=CONSTANTS.S3_BUCKET_NAME, Key=file_name, Body=dump_string)
    print("DONE DUMPING")
