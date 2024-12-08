import json
import create_clean_tables
import CONSTANTS
import insert_clean_weather
import insert_clean_vehicle_sql
import insert_clean_vehicles


def lambda_handler(event, context):

    # print("LAMBDA_HANDLER: Creating clean tables in the database...")
    # create_clean_tables.create_tables()

    # print("LAMBDA_HANDLER: Inserting clean weather data into the database...")
    # insert_clean_weather.insert_weather_data()

    print("LAMBDA_HANDLER: Inserting clean vehicles data into the database...")
    # insert_clean_vehicle_sql.insert_vehicles_data()
    insert_clean_vehicles.insert_vehicles_data()

    print("LAMBDA_HANDLER: Data inserted successfully.")

    return {
        'statusCode': 200,
        'body': json.dumps('Loading done!')
    }


if __name__ == "__main__":
    lambda_handler(None, None)