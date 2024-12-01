#
# This code should be used to insert data from JSON files into a PostgreSQL table. 
# Make sure the table exists, i.e. the "RAW Tables Draft.sql" script has been executed.
#
# This code is still untested.
#
# Run this inside a Lambda function.

# TODO: 
# - Make loading from S3 work
# - Cache the files that have been loaded to avoid re-loading them. --> e.g. Make a txt-file with the names of the files that have been loaded and put txt-file into S3 as well
#   --> Maybe, Glue can do this for us?


import json
from datetime import datetime, timezone
import utils
import CONSTANTS
from psycopg2.extras import execute_values


def lambda_handler(event, context):

    # Establish connection to the database
    connection = utils.get_db_connection()
    s3_client = utils.create_s3_client()

    # Read all json files from S3
    files = utils.list_s3_directory_files(CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.WEATHER_PREFIX)

    # only keep the files that have not been loaded yet, i.e. the ones not in the cache
    previously_loaded_files = utils.read_loaded_files_from_cache(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.WEATHER_PREFIX)
    files_to_load = [file for file in files if file not in previously_loaded_files]

    for file in files_to_load:
        # Load the data from the file
        data = utils.load_file_from_s3(s3_client, CONSTANTS.S3_BUCKET_NAME, file)
        data = json.loads(data)

        # Insert data into PostgreSQL
        insert_raw_data_weather_single_file(data, connection)

    # Return a success message
    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully.')
    }


def insert_raw_data_weather_single_file(data, connection):
    # Insert data into PostgreSQL
    cursor = connection.cursor()

    data_to_insert = []
    for entry in data:
        weather_info = entry["weather"]
        main_info = weather_info["main"]
        wind_info = weather_info["wind"]
        clouds_info = weather_info["clouds"]
        sys_info = weather_info["sys"]
        rain = weather_info.get("rain", {"1h": 0})

        # Convert UNIX timestamp to datetime
        timestamp = datetime.fromtimestamp(weather_info["dt"], tz=timezone.utc)
        sunrise = datetime.fromtimestamp(sys_info["sunrise"], tz=timezone.utc)
        sunset = datetime.fromtimestamp(sys_info["sunset"], tz=timezone.utc)

        data_to_insert.append(tuple([
            timestamp,
            entry["lat"],
            entry["lon"],
            weather_info["weather"][0]["main"],
            weather_info["weather"][0]["description"],
            weather_info["weather"][0]["icon"],
            main_info["temp"],
            main_info["feels_like"],
            main_info["temp_min"],
            main_info["temp_max"],
            main_info["pressure"],
            main_info["humidity"],
            main_info.get("sea_level"),
            main_info.get("grnd_level"),
            weather_info.get("visibility"),
            wind_info.get("speed"),
            wind_info.get("deg"),
            wind_info.get("gust"),
            rain.get("1h"),
            clouds_info.get("all"),
            sys_info.get("country"),
            sunrise,
            sunset
        ]))

    insert_query = f"""
        INSERT INTO {CONSTANTS.TABLE_NAME_WEATHER_RAW} (
            timestamp, latitude, longitude, main_weather, description, icon,
            temperature, feels_like, temp_min, temp_max, pressure, humidity,
            sea_level, grnd_level, visibility, wind_speed, wind_deg, wind_gust,
            rain, cloud_coverage, country, sunrise, sunset
        ) VALUES %s
        ON CONFLICT (latitude, longitude, timestamp) DO NOTHING;
        """

    # bulk insert data
    execute_values(cursor, insert_query, data_to_insert, page_size=1000)

    # Commit and close cursor
    connection.commit()
    cursor.close()


if __name__ == '__main__':
    # For local testing
    with open('../data/weather_data_2024-10-24_07-04-18.json') as f:
        event = {'data': f.read()}
        lambda_handler(event, None)
