import boto3
import utils
import CONSTANTS


def lambda_handler(event, context):

    # get connection
    connection = utils.get_db_connection()

    # clear all tables
    # utils.clear_table(connection, CONSTANTS.TABLE_NAME_DUMMY)
    # utils.clear_table(connection, CONSTANTS.TABLE_NAME_WEATHER_RAW)
    utils.clear_table(connection, CONSTANTS.TABLE_NAME_VEHICLES_RAW)
    # utils.clear_table(connection, CONSTANTS.TABLE_DEMOGRAPHICS_RAW)
    s3_client = utils.create_s3_client()
    utils.clear_cache(s3_client, CONSTANTS.S3_BUCKET_NAME, CONSTANTS.BASE_DATA_PATH, CONSTANTS.VEHICLES_PREFIX)


if __name__ == '__main__':
    lambda_handler(None, None)