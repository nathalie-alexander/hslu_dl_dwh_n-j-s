# lambda function to create helper-table for joining vehicles and demographics:

import psycopg2

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

def lambda_handler(event, context):
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Drop the table if it already exists (optional)
        cur.execute("DROP TABLE IF EXISTS temp_vehicle_demographics;")

        # Create the regular table
        create_table_query = """
        CREATE TABLE temp_vehicle_demographics AS
        SELECT 
            v.vehicle_id,
            v.rounded_latitude,
            v.rounded_longitude,
            v.rounded_timestamp,
            d.geo_nr
        FROM vehicles_clean v
        JOIN city_coordinates c
          ON v.rounded_latitude = c.latitude
         AND v.rounded_longitude = c.longitude
        JOIN demographics_clean d
          ON c.city = d.geo_name;
        """
        cur.execute(create_table_query)
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Error creating temp_vehicle_demographics table: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
    return {
        "statusCode": 200,
        "body": "Regular table temp_vehicle_demographics created successfully."
    }
#####################################################################################################
#####################################################################################################
# create helper table to left join weather and vehicles_demographics

import psycopg2

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

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

        # Perform the join with weather_clean
        create_weather_table_query = """
        CREATE TABLE temp_vehicle_weather AS
        SELECT 
            tvd.vehicle_id,
            tvd.geo_nr,
            tvd.rounded_timestamp,
            w.weather_id
        FROM temp_vehicle_demographics tvd
        LEFT JOIN weather_clean w
        ON tvd.rounded_latitude = w.latitude
        AND tvd.rounded_longitude = w.longitude
        AND tvd.rounded_timestamp = w.rounded_timestamp;
        """
        cur.execute(create_weather_table_query)
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Error joining with weather_clean: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return {
        "statusCode": 200,
        "body": "temp_vehicle_weather table created successfully."
    }

##################################################################################################
##################################################################################################
# create fact_sheet table and left joint time_id with vehicle_demographics_weather

import psycopg2

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

def lambda_handler(event, context):
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Insert data into fact_sheet while ignoring duplicates
        insert_query = """
        INSERT INTO fact_sheet (time_id, vehicle_id, geo_nr, weather_id, distance)
        SELECT 
            t.time_id,
            tvw.vehicle_id,
            tvw.geo_nr,
            tvw.weather_id,
            NULL AS distance
        FROM temp_vehicle_weather tvw
        LEFT JOIN time_clean t
          ON tvw.rounded_timestamp = t.timestamp
        WHERE tvw.weather_id IS NOT NULL
        ON CONFLICT (time_id, vehicle_id, geo_nr, weather_id) DO NOTHING;
        """
        
        # Execute the query
        cur.execute(insert_query)
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": f"Error inserting data into fact_sheet: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return {
        "statusCode": 200,
        "body": "Data successfully inserted into fact_sheet, duplicates ignored."
    }
