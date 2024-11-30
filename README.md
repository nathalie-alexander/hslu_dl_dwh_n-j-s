# Impact of Weather and Population Characteristics on Shared Mobility Usage

Hochschule Luzern - Data Lake and Data Warehouse HS2024

Group name: N-J-S

Students:
- Nathalie Alexander
- Joel Erni
- Stella Fischer

## Technical aspects
- data ingestion via one lambda function: the coordinates of the vehicles are rounded and the directly used to get the weather data

- data was collected in 2 different buckets in 2 different accounts. This was chosen as I kind of backup scenario. In order to not have redundant data, in account 1 the lambda function was run at 7:00, 13:00, 19:00 which in account to the lambda function was scheduled at 10:00, 16:00, 22:00

## Instructions to run the code

File: **dl_dwh_01_data_ingestion.py**
Lambda function in AWS
Lambda settings: 
- lambda layer: own created lambda layer using python 3.11 with installed packages pandas, requests, boto3, datetime
- Timeout: 15 min (about 4-6 min are needed, but we wanted to be on the safe side)
- Memory: 512 MB
- Ephemeral storage: 512 MB


File: **dl_dwh_02_copy2bucket.py**
Lambda function in AWS
Lambda settings: 
- lambda layer: preinstalled python 3.11 layer
- Timeout: 15 min 
- Memory: 2000 MB
- Ephemeral storage: 512 MB
