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

        #TODO: read from table instead of json file

        # Read JSON file providers_data_Stella.json
        with open("C:/Users/natha/OneDrive - Hochschule Luzern/Project DWL/Code/providers_data_Stella.json", "r") as file:
            providers_data = json.load(file)

        # Prepare insert query for the clean table
        insert_query = """
        INSERT INTO vehicles_clean (
            timestamp, latitude, longitude, vehicle_id, type, provider_id, available, pickup_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vehicle_id, timestamp) DO NOTHING;
        """

        def get_vehicle_type(provider_id):
            """Find the vehicle type for the given provider_id in the JSON data."""
            for provider in providers_data:
                if provider["provider_id"] == provider_id:
                    return provider.get("vehicle_type", [])
            return []  # Return an empty list if provider_id is not found

        entries = len(rows)
        n = 1
        
        # Insert data into the clean table
        for row in rows:
            timestamp, latitude, longitude, vehicle_id, provider_id, available, pickup_type = row
            
            # Find the vehicle_type for the provider_id in the JSON data
            vehicle_types = get_vehicle_type(provider_id)

            #TODO: possibly adapt the covered/uncovered logic
            # Determine the type: covered or uncovered
            type = "covered" if "Car" in vehicle_types else "uncovered"

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
