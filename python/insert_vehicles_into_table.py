import json
import utils
import CONSTANTS
from psycopg2.extras import execute_values


def insert_vehicles_data():

    # Establish connection to the database
    connection = utils.get_db_connection()
    s3_client = utils.create_s3_client()

    # Read all json files from S3
    files = utils.list_s3_directory_files(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.VEHICLES_PREFIX)

    # only keep the files that have not been loaded yet, i.e. the ones not in the cache
    previously_loaded_files = utils.read_loaded_files_from_cache(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.VEHICLES_PREFIX)
    files_to_load = [file for file in files if file not in previously_loaded_files]

    nof_files = len(files_to_load)
    currentfile = 0
    for file in files_to_load:
        currentfile += 1
        print(f"Loading file {currentfile} of {nof_files}")

        # Load the data from the file
        data = utils.load_file_from_s3(s3_client, CONSTANTS.S3_BUCKET_NAME, file)
        data = json.loads(data)

        timestamp = utils.extract_timestamp_from_string(file)

        # Insert data into PostgreSQL
        insert_raw_data_vehicles_single_file(data, timestamp, connection)


def insert_raw_data_vehicles_single_file(data, timestamp, connection):
    # Insert data into PostgreSQL
    cursor = connection.cursor()

    data_to_insert = []
    for entry in data:
        geometry = entry["geometry"]
        attributes = entry["attributes"]

        data_to_insert.append(tuple([
            timestamp,
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
        ]))

    insert_query = f"""
        INSERT INTO {CONSTANTS.TABLE_NAME_VECHICLES_RAW} (
            timestamp, provider_id, provider_name, provider_timezone,
            provider_apps_ios_store_uri, provider_apps_android_store_uri,
            vehicle_id, available, pickup_type, vehicle_status_disabled,
            vehicle_status_reserved, geometry_x, geometry_y
        ) VALUES %s
        ON CONFLICT (vehicle_id, timestamp) DO NOTHING;
        """

    # bulk insert data
    execute_values(cursor, insert_query, data_to_insert, page_size=1000)

    # Commit and close cursor
    connection.commit()
    cursor.close()
