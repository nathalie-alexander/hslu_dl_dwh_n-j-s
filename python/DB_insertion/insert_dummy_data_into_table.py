import json
from python.helpers import utils, CONSTANTS
import pprint as pp


def lambda_handler(event, context):

    # Establish connection to the database and S3DUMMY_PREFIX
    connection = utils.get_db_connection()
    s3_client = utils.create_s3_client()

    utils.clear_cache(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.DUMMY_PREFIX)

    # Read all json files from S3
    files = utils.list_s3_directory_files(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.DUMMY_PREFIX)
    print("Files found on s3 bucket:")
    pp.pprint(files)

    # only keep the files that have not been loaded yet, i.e. the ones not in the cache
    loaded_files = utils.read_loaded_files_from_cache(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.DUMMY_PREFIX)
    print("Files already loaded:")
    pp.pprint(loaded_files)

    files_to_load = [file for file in files if file not in loaded_files]
    print("Files to actually load:")
    pp.pprint(files_to_load)

    for file in files_to_load:
        # Insert data into PostgreSQLDUMMY_PREFIXDUMMY_PREFIX
        insert_raw_data_dummy_single_file(file, connection, s3_client)

    # Write the loaded files to the cache
    utils.write_loaded_files_to_cache(files_to_load, s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.DUMMY_PREFIX)

    # Return a success message
    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully.')
    }


def insert_raw_data_dummy_single_file(file_path, connection, s3_client):

    data = utils.load_file_from_s3(s3_client, CONSTANTS.S3_BUCKET_NAME, file_path)
    data = json.loads(data)
    timestamp = utils.extract_timestamp_from_string(file_path)

    # Insert data into PostgreSQL
    cursor = connection.cursor()

    print("Data:")
    pp.pprint(data)

    for entry in data:

        print("Entry:")
        pp.pprint(entry)

        insert_query = f"""
        INSERT INTO {CONSTANTS.TABLE_NAME_DUMMY} (
            timestamp, name, age, profession
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

        values = (
            timestamp,
            entry["name"],
            entry["age"],
            entry["profession"]
        )

        cursor.execute(insert_query, values)

    # Commit and close cursor
    connection.commit()
    cursor.close()


if __name__ == "__main__":
    lambda_handler(None, None)
