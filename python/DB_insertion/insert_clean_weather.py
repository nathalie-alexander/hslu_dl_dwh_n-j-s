from python.helpers import utils

logger = utils.get_logger(__name__)

def insert_weather_data():
    try:
        logger.info("Inserting weather data into the database...")

        # Establish connection to the database
        connection = utils.get_db_connection()
        cursor = connection.cursor()
        
        # Query to fetch data from the raw weather table
        fetch_query = """
        SELECT 
            timestamp, 
            latitude, 
            longitude, 
            (temperature - 273.15) AS temperature,  -- Convert temperature from Kelvin to Celsius
            wind_speed, 
            rain, 
            cloud_coverage, 
            humidity
        FROM weather_raw;
        """
        
        cursor.execute(fetch_query)
        rows = cursor.fetchall()
        # rows = cursor.fetchone()

        
        # Prepare insert query for the clean table
        insert_query = """
        INSERT INTO weather_clean (
            timestamp, latitude, longitude, temperature, wind_speed, rain, cloud_coverage, humidity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (latitude, longitude, timestamp) DO NOTHING;
        """
        
        entries = len(rows)
        n = 1
        # Insert data into the clean table
        for row in rows:
            print(f"Inserting row {n} of {entries}...")
            cursor.execute(insert_query, row)
            n += 1
        
        # Commit transaction
        connection.commit()
        print("Data transfer completed successfully.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()