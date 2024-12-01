import csv
import utils
import CONSTANTS
from psycopg2.extras import execute_values
import logging
import datetime

logger = logging.getLogger(__name__)


def load_demographics_data_to_db(file_path):
    """
    Load the data from the CSV file into the database
    :param file_path: file path INSIDE the S3 bucket
    :return:
    """

    # Load the data from the CSV file
    s3_client = utils.create_s3_client()
    data = utils.load_file_from_s3(s3_client, CONSTANTS.S3_BUCKET_NAME, file_path)
    reader = csv.DictReader(data.splitlines())

    # Establish connection to the database
    connection = utils.get_db_connection()
    cursor = connection.cursor()

    # Clear the table since we only have 1 csv file
    utils.clear_table(connection, CONSTANTS.TABLE_DEMOGRAPHICS_RAW)

    # Prepare the data for bulk insert for better performance
    data_to_insert = []
    for row in reader:

        # do some cleaning
        if row["VALUE_PERIOD"] == "2022-01-01" or row["VALUE_PERIOD"] == "2022-12-31":
            row["VALUE_PERIOD"] = "2022"
        elif row["VALUE_PERIOD"] == "2011/2022":
            # ignore these rows
            continue

        if row["VALUE"] == "":
            row["VALUE"] = "0"

        data_to_insert.append(tuple([
            row["GEO_NR"],
            row["GEO_NAME"],
            row["CLASS_HAB"],
            row["GEOM_PERIOD"],
            row["VARIABLE"],
            row["SOURCE"],
            int(row["VALUE_PERIOD"]),
            row["UNIT_VALUE"],
            float(row["VALUE"]),
            row["STATUS"]]
        ))

    # Bulk insert the data
    insert_query = f"""
        INSERT INTO {CONSTANTS.TABLE_DEMOGRAPHICS_RAW} (
            geo_nr, geo_name, class_hab, geom_period, variable, source,
            value_period, unit_value, value, status
        ) VALUES %s;
        """

    # bulk insert data
    execute_values(cursor, insert_query, data_to_insert, page_size=1000)

    # Commit and close the cursor
    connection.commit()
    cursor.close()

    # Close the connection
    connection.close()

    logger.info("Data loaded successfully.")


if __name__ == "__main__":
    path = CONSTANTS.BASE_DATA_PATH + "/" + CONSTANTS.DEMOGRAPHICS_PREFIX + ".csv"
    load_demographics_data_to_db(path)
