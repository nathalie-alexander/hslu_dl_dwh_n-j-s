import json
import psycopg2
import logging

# configurate logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    # PostgreSQL-connection details
    host = "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com"
    database = "rawdatadb"
    user = "postgres"
    password = "INSERT PW" 
    port = 5432

    try:
        logger.info("Connecting to the database...")

        # establish connection to database
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = connection.cursor()

        # delete existing rows in table
        delete_query = "DELETE FROM weather_clean;"
        cursor.execute(delete_query)
        connection.commit()
        logger.info("Existing entries in weather_clean table have been deleted.")

        # fetch data from `weather_raw`-table
        fetch_query = """
        SELECT 
            timestamp, 
            latitude, 
            longitude, 
            (temperature - 273.15) AS temperature,  -- Temperatur von Kelvin in Celsius umrechnen
            wind_speed, 
            rain, 
            cloud_coverage, 
            humidity
        FROM weather_raw;
        """
        cursor.execute(fetch_query)
        rows = cursor.fetchall()

        # input-query for the weather-clean table
        insert_query = """
        INSERT INTO weather_clean (
            timestamp, latitude, longitude, temperature, wind_speed, rain, cloud_coverage, humidity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, timestamp) DO NOTHING;
        """

        # insert data into weather_clean table
        entries = len(rows)
        for n, row in enumerate(rows, start=1):
            logger.info(f"Inserting row {n} of {entries}...")
            cursor.execute(insert_query, row)

        # close transaction
        connection.commit()
        logger.info("Data transfer completed successfully.")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # close connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
