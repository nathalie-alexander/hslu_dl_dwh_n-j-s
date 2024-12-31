# lambda function to update weather_clean with new primary key (weather_id)

import psycopg2

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

def lambda_handler(event, context):
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Drop the existing primary key constraint
        drop_pkey_query = """
        ALTER TABLE weather_clean DROP CONSTRAINT IF EXISTS weather_clean_pkey;
        """
        cur.execute(drop_pkey_query)

        # Add a new column 'weather_id' and make it the new primary key
        add_column_query = """
        ALTER TABLE weather_clean 
        ADD COLUMN IF NOT EXISTS weather_id SERIAL PRIMARY KEY;
        """
        cur.execute(add_column_query)
        
        # Commit changes
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Error modifying weather_clean: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return {
        "statusCode": 200,
        "body": "weather_clean updated with new primary key 'weather_id'."
    }
