# Impact of Weather and Population Characteristics on Shared Mobility Usage

Hochschule Luzern - Data Lake and Data Warehouse HS2024

Group name: N-J-S

Students:
- Nathalie Alexander
- Joel Erni
- Stella Fischer

## Technical aspects
- data ingestion via one lambda function: the coordinates of the vehicles are rounded and the directly used to get the weather data
- data was collected in 2 different buckets in 2 different accounts. This was chosen as I kind of backup scenario. In order to not have redundant data, in account 1 the lambda function was run at 7:00, 13:00, 19:00 while in account 2 the lambda function was scheduled at 10:00, 16:00, 22:00

## Instructions to run the code

1. Create a lambda in AWS with following settings:
   1. lambda layer: own created lambda layer using python 3.11 with installed packages pandas, requests, boto3, datetime 
   2. Timeout: 15 min (about 4-6 min are needed, but we wanted to be on the safe side)
   3. Memory: 512 MB 
   4. Ephemeral storage: 512 MB
2. Copy code from different files into the lambda function
3. For the data ingestion, the lambda function needs to be scheduled. This can be done by creating a CloudWatch event rule with following settings:
   1. Event Source: Schedule
   2. Cron expression: `0 7,13,19 * * ? *` (for the first account) and `0 10,16,22 * * ? *` (for the second account)
   3. Target: Lambda function


## Code Structure
- `data/`: contains the data for local tests
- `python/DB_creation/`: contains the code to create the tables in the database
- `python/DB_dwh/`: contains the code to create the tables in the data warehouse
- `python/DB_insertion/`: contains the code to insert the data into the tables in the database
- `python/DB_transformation/`: contains the code to transform the data in the database
- `python/helpers/`: contains helper functions to connect to database and s3 buckets
- `python/ingestion/`: contains the code to ingest the data
- `SQL/`: contains the SQL queries to create the tables in the database and data warehouse
