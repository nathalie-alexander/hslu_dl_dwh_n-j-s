import json
import create_raw_tables
import CONSTANTS
import insert_demo_data_into_table


def lambda_handler(event, context):

    # print("LAMBDA_HANDLER: Creating tables in the database...")
    # create_raw_tables.create_tables()

    print("LAMBDA_HANDLER: Inserting data into the database...")
    path = CONSTANTS.BASE_DATA_PATH + "/" + CONSTANTS.DEMOGRAPHICS_PREFIX + ".csv"
    insert_demo_data_into_table.load_demographics_data_to_db(path)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


if __name__ == "__main__":
    lambda_handler(None, None)