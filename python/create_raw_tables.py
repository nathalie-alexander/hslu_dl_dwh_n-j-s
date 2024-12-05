"""
This script creates the raw tables in the AWS RDS PostgreSQL database.

Run this script from your local machine.
"""

import psycopg2
import CONSTANTS
import utils

creation_query_vehicle = f"""
CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_NAME_VEHICLES_RAW} (
    timestamp TIMESTAMP,
    provider_id VARCHAR(50),
    provider_name VARCHAR(100),
    provider_timezone VARCHAR(50),
    provider_apps_ios_store_uri TEXT,
    provider_apps_android_store_uri TEXT,
    vehicle_id VARCHAR(100),
    available BOOLEAN,
    pickup_type VARCHAR(50),
    vehicle_status_disabled BOOLEAN,
    vehicle_status_reserved BOOLEAN,
    geometry_x FLOAT,
    geometry_y FLOAT,
    PRIMARY KEY (vehicle_id, timestamp)
);
"""

creation_query_weather = f"""
CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_NAME_WEATHER_RAW} (
    timestamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    main_weather VARCHAR(50),
    description VARCHAR(100),
    icon VARCHAR(10),
    temperature FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    humidity INT,
    sea_level INT,
    grnd_level INT,
    visibility INT,
    wind_speed FLOAT,
    wind_deg INT,
    wind_gust FLOAT,
    rain FLOAT,
    cloud_coverage INT,
    country CHAR(2),
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    PRIMARY KEY (latitude, longitude, timestamp)
); 
"""

creation_query_demographics = f"""
CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_DEMOGRAPHICS_RAW} (
    id SERIAL PRIMARY KEY,
    geo_nr VARCHAR(10),
    geo_name VARCHAR(255),
    class_hab VARCHAR(50),
    geom_period DATE,
    variable VARCHAR(50),
    source VARCHAR(50),
    value_period INTEGER,
    unit_value VARCHAR(50),
    value DOUBLE PRECISION,
    status CHAR(1)
);
"""

creation_query_dummy = f"""
CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_NAME_DUMMY} (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    age INTEGER,
    profession VARCHAR(50),
    timestamp TIMESTAMP
);
"""


def create_tables():
    """
    Create the raw tables in the AWS RDS PostgreSQL database.
    :return:
    """
    # Connect to the database
    connection = utils.get_db_connection()

    # Create the tables
    print("Creating tables in the database...")
    utils.create_table(connection, creation_query_vehicle)
    utils.create_table(connection, creation_query_weather)
    utils.create_table(connection, creation_query_demographics)
    utils.create_table(connection, creation_query_dummy)

    # List the tables
    with connection.cursor() as cursor:
        SQL_query = """SELECT table_name 
                       FROM information_schema.tables 
                       WHERE table_schema='public'"""
        cursor.execute(SQL_query)
        tables = cursor.fetchall()
        print(f"No. of tables in the database: {len(tables)}")

        print("Tables in the database:")
        for table in tables:
            print(table[0])

    # Close the connection
    connection.commit()
    connection.close()


if __name__ == '__main__':
    create_tables()
