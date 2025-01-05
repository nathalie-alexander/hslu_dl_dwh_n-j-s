# lambda function to create fact_sheet
import psycopg2

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

def create_fact_distances(event, context):
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # create fact_distances query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS fact_distances (
            time_id BIGINT NOT NULL,
            vehicle_id VARCHAR(100) NOT NULL,
            geo_nr VARCHAR(10) NOT NULL,
            weather_id BIGINT NOT NULL,
            distance FLOAT,
            PRIMARY KEY (time_id, vehicle_id, geo_nr, weather_id),
            FOREIGN KEY (time_id) REFERENCES time_clean (time_id),
            FOREIGN KEY (geo_nr) REFERENCES demographics_clean (geo_nr),
            FOREIGN KEY (weather_id) REFERENCES weather_clean (weather_id)
        );
        """

        cur.execute(create_table_query)
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Error creating fact_sheet: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
    return {
        "statusCode": 200,
        "body": "fact_sheet table created successfully."
    }
