# Lambda Function to tranform the demographics data and insert into new table

import json
import psycopg2


def lambda_handler(event, context):
    try:
        # create connection to the PostgreSQL-Database
        conn = psycopg2.connect(
            host="rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com", 
            database="rawdatadb", 
            user="postgres",
            password="INSERT PW",
            port=5432 
        )
        cursor = conn.cursor()

        # SQL-command to create the cleaned table
        sql = """
        CREATE TABLE IF NOT EXISTS demographics_clean AS
        SELECT 
            geo_nr,
            geo_name,
            class_hab,
            geom_period,
            variable,
            value_period,
            value
        FROM 
            demographics_raw
        WHERE 
            geo_name IN ('Zürich', 'Bern', 'Genève', 'Lugano', 'Basel')
            AND 
            variable IN (
                'pop2022', 'pop2011', 'dens_pop_22', 'pop2000', 'pop1990', 
                'pop1980', 'pop1970', 'pop1930', 'pop_sexF', 'pop_sexM', 
                'pop_civ_sin_t', 'pop_civ_mar_t', 'pop_civ_par_t', 'pop_civ_div_t', 
                'cage_t', 'cage_0_4', 'cage_5_9', 'cage_10_14', 'cage_15_19', 
                'cage_20_24', 'cage_25_29', 'cage_30_34', 'cage_35_39', 
                'cage_40_44', 'cage_45_49', 'cage_50_54', 'cage_55_59', 
                'cage_60_64', 'cage_65_69', 'cage_70_74', 'cage_75_79', 
                'cage_80_84', 'cage_85_89', 'cage_90_94', 'cage_95_99', 
                'cage_100p', 'nat_ch', 'nat_etr', 'HH_t', 'HH_1p', 'HH_2p', 
                'HH_3p', 'HH_4p', 'HH_5pp', 'HH_moy'
            );
        """

        # execute SQL-command
        cursor.execute(sql)

        # save changes
        conn.commit()

        # give back success message
        return {
            "statusCode": 200,
            "body": "Tabelle demographics_filtered wurde erfolgreich erstellt (ohne unit_value)."
        }

    except Exception as e:
        # error handling
        return {
            "statusCode": 500,
            "body": f"Ein Fehler ist aufgetreten: {str(e)}"
        }

    finally:
        # close connection and cursor
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
