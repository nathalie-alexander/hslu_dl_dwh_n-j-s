# Lambda Function to transform the demographics data and insert into new table

import json
import psycopg2


def lambda_handler(event, context):
    import json
import os
import psycopg2


def lambda_handler(event, context):
    # establish connection with DB
    try:
        conn = psycopg2.connect(
            host="rawdatadb.cl0q24wcqwfj.us-east-1.rds.amazonaws.com", 
            database="rawdatadb", 
            user="postgres",
            password="INSERT PW",
            port=5432 
        )
        cursor = conn.cursor()
        
        # SQL-query to delete the existing table and create a new one
        sql = """
        DROP TABLE IF EXISTS demographics_clean;

        CREATE TABLE demographics_clean AS
        WITH pivot_data AS (
            SELECT 
                geo_nr,
                geo_name,
                class_hab,
                geom_period,
                variable,
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
                )
        )
        SELECT 
            geo_nr,
            geo_name,
            class_hab,
            geom_period,
            MAX(CASE WHEN variable = 'pop2022' THEN value END) AS pop2022,
            MAX(CASE WHEN variable = 'pop2011' THEN value END) AS pop2011,
            MAX(CASE WHEN variable = 'dens_pop_22' THEN value END) AS dens_pop_22,
            MAX(CASE WHEN variable = 'pop2000' THEN value END) AS pop2000,
            MAX(CASE WHEN variable = 'pop1990' THEN value END) AS pop1990,
            MAX(CASE WHEN variable = 'pop1980' THEN value END) AS pop1980,
            MAX(CASE WHEN variable = 'pop1970' THEN value END) AS pop1970,
            MAX(CASE WHEN variable = 'pop1930' THEN value END) AS pop1930,
            MAX(CASE WHEN variable = 'pop_sexF' THEN value END) AS pop_sexF,
            MAX(CASE WHEN variable = 'pop_sexM' THEN value END) AS pop_sexM,
            MAX(CASE WHEN variable = 'pop_civ_sin_t' THEN value END) AS pop_civ_sin_t,
            MAX(CASE WHEN variable = 'pop_civ_mar_t' THEN value END) AS pop_civ_mar_t,
            MAX(CASE WHEN variable = 'pop_civ_par_t' THEN value END) AS pop_civ_par_t,
            MAX(CASE WHEN variable = 'pop_civ_div_t' THEN value END) AS pop_civ_div_t,
            MAX(CASE WHEN variable = 'cage_t' THEN value END) AS cage_t,
            MAX(CASE WHEN variable = 'cage_0_4' THEN value END) AS cage_0_4,
            MAX(CASE WHEN variable = 'cage_5_9' THEN value END) AS cage_5_9,
            MAX(CASE WHEN variable = 'cage_10_14' THEN value END) AS cage_10_14,
            MAX(CASE WHEN variable = 'cage_15_19' THEN value END) AS cage_15_19,
            MAX(CASE WHEN variable = 'cage_20_24' THEN value END) AS cage_20_24,
            MAX(CASE WHEN variable = 'cage_25_29' THEN value END) AS cage_25_29,
            MAX(CASE WHEN variable = 'cage_30_34' THEN value END) AS cage_30_34,
            MAX(CASE WHEN variable = 'cage_35_39' THEN value END) AS cage_35_39,
            MAX(CASE WHEN variable = 'cage_40_44' THEN value END) AS cage_40_44,
            MAX(CASE WHEN variable = 'cage_45_49' THEN value END) AS cage_45_49,
            MAX(CASE WHEN variable = 'cage_50_54' THEN value END) AS cage_50_54,
            MAX(CASE WHEN variable = 'cage_55_59' THEN value END) AS cage_55_59,
            MAX(CASE WHEN variable = 'cage_60_64' THEN value END) AS cage_60_64,
            MAX(CASE WHEN variable = 'cage_65_69' THEN value END) AS cage_65_69,
            MAX(CASE WHEN variable = 'cage_70_74' THEN value END) AS cage_70_74,
            MAX(CASE WHEN variable = 'cage_75_79' THEN value END) AS cage_75_79,
            MAX(CASE WHEN variable = 'cage_80_84' THEN value END) AS cage_80_84,
            MAX(CASE WHEN variable = 'cage_85_89' THEN value END) AS cage_85_89,
            MAX(CASE WHEN variable = 'cage_90_94' THEN value END) AS cage_90_94,
            MAX(CASE WHEN variable = 'cage_95_99' THEN value END) AS cage_95_99,
            MAX(CASE WHEN variable = 'cage_100p' THEN value END) AS cage_100p,
            MAX(CASE WHEN variable = 'nat_ch' THEN value END) AS nat_ch,
            MAX(CASE WHEN variable = 'nat_etr' THEN value END) AS nat_etr,
            MAX(CASE WHEN variable = 'HH_t' THEN value END) AS HH_t,
            MAX(CASE WHEN variable = 'HH_1p' THEN value END) AS HH_1p,
            MAX(CASE WHEN variable = 'HH_2p' THEN value END) AS HH_2p,
            MAX(CASE WHEN variable = 'HH_3p' THEN value END) AS HH_3p,
            MAX(CASE WHEN variable = 'HH_4p' THEN value END) AS HH_4p,
            MAX(CASE WHEN variable = 'HH_5pp' THEN value END) AS HH_5pp,
            MAX(CASE WHEN variable = 'HH_moy' THEN value END) AS HH_moy
        FROM pivot_data
        GROUP BY 
            geo_nr, geo_name, class_hab, geom_period;
        """

        # SQL execution
        cursor.execute(sql)
        conn.commit()
        
        return {
            "statusCode": 200,
            "body": "Tabelle demographics_clean wurde erfolgreich neu erstellt."
        }
    
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Fehler: {str(e)}"
        }
    
    finally:
        if conn:
            cursor.close()
            conn.close()
