from python.helpers import utils
import json

logger = utils.get_logger(__name__)

def insert_providers_data():
    try:
        logger.info("Inserting providers data into the database...")

        # Establish connection to the database
        connection = utils.get_db_connection()
        cursor = connection.cursor()
        
        # Load JSON file
        file_path = 'C:/Users/natha/OneDrive - Hochschule Luzern/Project DWL/Static Data/providers_data_2024-10-20_13-09-16.json'
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Insert query
        insert_query = """
        INSERT INTO providers (
            provider_id, name, ttl, language, vehicle_type, timezone, rental_apps, url, email, phone_number, last_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (provider_id) DO NOTHING;
        """
        
        # Insert data into the table
        for item in data:
            cursor.execute(insert_query, (
                item.get("provider_id"),
                item.get("name"),
                item.get("ttl"),
                item.get("language"),
                item.get("vehicle_type"),
                item.get("timezone"),
                json.dumps(item.get("rental_apps", {})),  # Convert JSON to string
                item.get("url"),
                item.get("email"),
                item.get("phone_number"),
                item.get("last_updated")
            ))
        
        # Commit the transaction
        connection.commit()
        print("Data inserted successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()