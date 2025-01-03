import json
from datetime import datetime, timezone
from python.helpers import utils, CONSTANTS
from psycopg2.extras import execute_values

logger = utils.get_logger(__name__)


def insert_weather_data():

    logger.info("Inserting weather data into the database...")

    # Establish connection to the database
    connection = utils.get_db_connection()
    s3_client = utils.create_s3_client()

    # Read all json files from S3
    logger.info("Collecting all files in S3...")
    files = utils.list_s3_directory_files(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.WEATHER_PREFIX)
    logger.info(f"Number of files found on s3 bucket: {len(files)}")

    # only keep the files that have not been loaded yet, i.e. the ones not in the cache
    logger.info("Reading the cache...")
    previously_loaded_files = utils.read_loaded_files_from_cache(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.WEATHER_PREFIX)
    logger.info(f"Number of files in cache, i.e. already loaded: {len(previously_loaded_files)}")
    files_to_load = [file for file in files if file not in previously_loaded_files]

    nof_files = len(files_to_load)
    logger.info(f"Number of files to load: {nof_files}")

    if nof_files == 0:
        logger.info("No new files to load. Return.")
        return

    current_file = 0
    actually_loaded_files = []

    try:
        for file in files_to_load:
            current_file += 1
            logger.info(f"Loading file {file} ({current_file} of {nof_files})")

            # Load the data from the file
            data = utils.load_file_from_s3(s3_client, CONSTANTS.S3_BUCKET_NAME, file)
            data = json.loads(data)

            # Insert data into PostgreSQL
            insert_raw_data_weather_single_file(data, connection)

            actually_loaded_files.append(file)

    except Exception as e:
        logger.error(f"Error loading file {file}: {e}")
    finally:
        # write the loaded file to the cache
        utils.write_loaded_files_to_cache(actually_loaded_files,
                                          s3_client,
                                          CONSTANTS.S3_BUCKET_NAME,
                                          CONSTANTS.BASE_DATA_PATH,
                                          CONSTANTS.WEATHER_PREFIX)

    logger.info("Data inserted successfully.")


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


