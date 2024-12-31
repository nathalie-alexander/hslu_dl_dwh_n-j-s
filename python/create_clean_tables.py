import psycopg2
import CONSTANTS
import utils

creation_query_weather = f"""
CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_NAME_WEATHER_CLEAN} (
    timestamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    wind_speed FLOAT,
    rain FLOAT,
    cloud_coverage INT,
    humidity INT,
    PRIMARY KEY (latitude, longitude, timestamp)
); 
"""

creation_query_vehicle = f"""
CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_NAME_VECHICLES_CLEAN} (
    timestamp TIMESTAMP,
    type VARCHAR(50),
    provider_id VARCHAR(50),
    vehicle_id VARCHAR(100),
    available BOOLEAN,
    pickup_type VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (vehicle_id, timestamp)
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