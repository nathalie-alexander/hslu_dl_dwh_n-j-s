from python.helpers import utils

logger = utils.get_logger(__name__)

def insert_vehicles_data():
    try:
        logger.info("Merging and inserting data into vehicles_clean table...")
        
        # Establish connection to the database
        connection = utils.get_db_connection()
        cursor = connection.cursor()
        
        # SQL query to merge and insert data
        merge_query = """
        INSERT INTO vehicles_clean (
            timestamp, latitude, longitude, vehicle_id, type, provider_id, available, pickup_type
        )
        SELECT 
            v.timestamp,
            v.geometry_y AS latitude,
            v.geometry_x AS longitude,
            v.vehicle_id,
            CASE
                WHEN 'Car' = ANY(p.vehicle_type) AND 'Bike' = ANY(p.vehicle_type) THEN 'mixed'
                WHEN 'Car' = ANY(p.vehicle_type) THEN 'covered'
                ELSE 'uncovered'
            END AS type,
            v.provider_id,
            v.available,
            v.pickup_type
        FROM 
            vehicles_raw v
        LEFT JOIN 
            providers p
        ON 
            v.provider_id = p.provider_id
        ON CONFLICT (vehicle_id, timestamp) DO NOTHING;
        """
        
        # Execute the merge query
        cursor.execute(merge_query)
        
        # Commit the transaction
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
