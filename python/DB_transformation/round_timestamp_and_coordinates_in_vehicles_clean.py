import psycopg2

# Database configuration
DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

# Batch size (number of rows to process per update)
BATCH_SIZE = 5000

def transform_vehicles_timestamp_coordinates(event, context):
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Add new columns for rounded timestamp and rounded coordinates if not exist
        alter_query = """
        ALTER TABLE vehicles_clean 
        ADD COLUMN IF NOT EXISTS rounded_timestamp TIMESTAMP,
        ADD COLUMN IF NOT EXISTS rounded_latitude FLOAT,
        ADD COLUMN IF NOT EXISTS rounded_longitude FLOAT;
        """
        cur.execute(alter_query)
        
        # Process updates in batches
        while True:
            update_query = f"""
            WITH cte AS (
                SELECT vehicle_id, timestamp
                FROM vehicles_clean
                WHERE rounded_timestamp IS NULL  -- Only process rows not yet updated
                LIMIT {BATCH_SIZE}
            )
            UPDATE vehicles_clean
            SET
                rounded_timestamp = DATE_TRUNC('hour', vehicles_clean.timestamp),
                rounded_latitude = ROUND(vehicles_clean.latitude::NUMERIC, 1),
                rounded_longitude = ROUND(vehicles_clean.longitude::NUMERIC, 1)
            FROM cte
            WHERE vehicles_clean.vehicle_id = cte.vehicle_id
              AND vehicles_clean.timestamp = cte.timestamp;
            """
            cur.execute(update_query)
            
            # Commit the current batch
            conn.commit()
            
            # Check if there are more rows to process
            cur.execute("SELECT COUNT(*) FROM vehicles_clean WHERE rounded_timestamp IS NULL;")
            remaining_rows = cur.fetchone()[0]
            if remaining_rows == 0:
                break
        
    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Error updating vehicles_clean: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
    return {
        "statusCode": 200,
        "body": "vehicles_clean successfully updated with rounded timestamp and coordinates."
    }
