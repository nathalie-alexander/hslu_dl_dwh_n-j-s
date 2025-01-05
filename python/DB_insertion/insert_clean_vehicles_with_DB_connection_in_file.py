import json
import psycopg2


def insert_clean_vehicles(event, context):
    try:
        print("Merging and inserting data into vehicles_clean table...")

        # establish connection to DB
        connection = psycopg2.connect(
            host="rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com", 
            database="rawdatadb", 
            user="postgres",
            password="INSERT PW",
            port=5432 
        )
        cursor = connection.cursor()

        # SQL-query to join and insert the data
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

        # execute SQL-query
        cursor.execute(merge_query)

        # confirm transaction
        connection.commit()
        print("Data transfer completed successfully.")
    
    except Exception as e:
        print(f"Error: {e}")
        
    finally:
        # close DB-connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
