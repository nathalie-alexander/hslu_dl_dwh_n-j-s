import psycopg2
import utils
import CONSTANTS
import json
logger = utils.get_logger(__name__)

def insert_vehicles_data():
    try:
        logger.info("Inserting vehicles data into the database...")

        # Establish connection to the database
        connection = utils.get_db_connection()
        cursor = connection.cursor()
        
        # Query to fetch data from the raw vehicles table
        fetch_query = """
        SELECT 
            timestamp, 
            geometry_y AS latitude, 
            geometry_x AS longitude, 
            vehicle_id,
            provider_id,
            available, 
            pickup_type
        FROM vehicles_raw;
        """
        
        cursor.execute(fetch_query)
        rows = cursor.fetchall()

        # Fetch provider data from the providers table
        providers_query = """
        SELECT provider_id, vehicle_type FROM providers;
        """
        cursor.execute(providers_query)
        provider_rows = cursor.fetchall()

        # Create a dictionary for quick lookup
        providers_data = {row[0]: row[1] for row in provider_rows}

        # Prepare insert query for the clean table
        insert_query = """
        INSERT INTO vehicles_clean (
            timestamp, latitude, longitude, vehicle_id, type, provider_id, available, pickup_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vehicle_id, timestamp) DO NOTHING;
        """

        def get_vehicle_type(provider_id):
            """Find the vehicle type for the given provider_id in the providers table."""
            return providers_data.get(provider_id, [])

        entries = len(rows)
        n = 1
        
        # Insert data into the clean table
        for row in rows:
            timestamp, latitude, longitude, vehicle_id, provider_id, available, pickup_type = row
            
            # Find the vehicle_type for the provider_id in the providers table
            vehicle_types = get_vehicle_type(provider_id)

            # Determine the type: covered or uncovered
            if "Car" in vehicle_types and "Bike" in vehicle_types:
                type = "mixed"
            elif "Car" in vehicle_types:
                type = "covered"
            else:
                type = "uncovered"

            # Prepare data for insertion
            clean_row = (timestamp, latitude, longitude, vehicle_id, type, provider_id, available, pickup_type)

            print(f"Inserting row {n} of {entries}...")
            cursor.execute(insert_query, clean_row)
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
