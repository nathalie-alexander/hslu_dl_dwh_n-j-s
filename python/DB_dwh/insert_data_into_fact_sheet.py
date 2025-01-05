# lambda function to create helper-table for joining vehicles and demographics:

import psycopg2

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

def merge_vehicles_demographics(event, context):
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
'''
Between the temp_vehicle_demographics table and the temp_vehicle_weather table we had to download the
temp_vehicle_demographics data to externally calculate the distances. We had to do this because the
data load was to large for processing it in a lambda function. The data including the distances was
reuploaded into temp_distances via the code in file "insert_distnaces_into_temp_distances.py".
'''

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

def merge_vehicles_demographics_weather(event, context):
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
        FROM temp_distances tvd
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
# left joint time_id with vehicle_demographics_weather into fact_distances

import psycopg2

DB_CONFIG = {
    "host": "rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com",
    "database": "rawdatadb",
    "user": "postgres",
    "password": "INSERT PW",
    "port": 5432
}

def merge_vehicles_demo_weather_time(event, context):
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Create the fact_distances table if it does not exist
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

        # Insert data into fact_distances while ignoring duplicates
        insert_query = """
        INSERT INTO fact_distances (time_id, vehicle_id, geo_nr, weather_id, distance)
        SELECT 
            t.time_id,
            tvw.vehicle_id,
            tvw.geo_nr,
            tvw.weather_id,
            tvw.distance
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
            "body": f"Error creating or inserting data into fact_distances: {str(e)}"
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return {
        "statusCode": 200,
        "body": "Table fact_distances created and data inserted successfully, duplicates ignored."
    }
