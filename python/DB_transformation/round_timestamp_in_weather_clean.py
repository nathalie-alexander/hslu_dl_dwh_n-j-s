import psycopg2

# Database configuration
DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

def transform_weather_timestamp(event, context):
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Add new column for rounded timestamp
        alter_query = """
        ALTER TABLE weather_clean 
        ADD COLUMN rounded_timestamp TIMESTAMP;
        """
        cur.execute(alter_query)
        
        # Update rounded timestamp using DATE_TRUNC
        update_query = """
        UPDATE weather_clean
        SET
            rounded_timestamp = DATE_TRUNC('hour', timestamp);
        """
        cur.execute(update_query)
        conn.commit()
        
    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Error updating weather_clean: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
    return {
        "statusCode": 200,
        "body": "weather_clean successfully updated with rounded timestamps."
    }
