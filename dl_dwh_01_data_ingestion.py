import os
import json
from datetime import datetime, timedelta, timezone

import requests
import boto3
import pandas as pd

API_KEY = os.environ['API_KEY']
s3 = boto3.client('s3')

def fetch_vehicle_data(base_url, offset):
    """Fetch vehicle data from the API"""
    response = requests.get(
        f'{base_url}/find?offset={str(offset)}&geometryFormat=esrijson',
        headers={'accept': 'application/json'}
        )
    vehicels = response.json() if response.status_code == 200 else []
    return vehicels
    
def round_coordinates(latlng, precision):
    """Round the coordinates to a specified precision"""
    lat, lon = latlng
    return round(lat, precision), round(lon, precision)

def fetch_weather_data(lat, lon):
    """Fetch weather data from the OpenWeatherMap API"""
    response = requests.get(
        f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}'
        )
    return response.json()
    
def get_switzerland_time():
    """Get the current time in Switzerland"""
    # Get the current UTC time
    current_time_utc = datetime.now(timezone.utc)

    # Switzerland is normally UTC+1, but UTC+2 during DST
    cet_offset = timedelta(hours=1)
    cest_offset = timedelta(hours=2)

    # Approximate check for Daylight Saving Time (last Sunday of March to last Sunday of October)
    start_dst = datetime(current_time_utc.year, 3, 31)  # Last day of March
    start_dst -= timedelta(days=start_dst.weekday() + 1)  # Last Sunday of March
    end_dst = datetime(current_time_utc.year, 10, 31)    # Last day of October
    end_dst -= timedelta(days=end_dst.weekday() + 1)     # Last Sunday of October

    # Determine if it's currently DST in Switzerland
    if start_dst <= current_time_utc.replace(tzinfo=None) < end_dst:
        current_time_switzerland = current_time_utc + cest_offset  # CEST (UTC+2)
    else:
        current_time_switzerland = current_time_utc + cet_offset  # CET (UTC+1)

    return current_time_switzerland.strftime("%Y-%m-%d_%H-%M-%S")


def lambda_handler(event, context):
    base_url = 'https://api.sharedmobility.ch/v1/sharedmobility'
    i = 0
    all_vehicels = []

    while True:
        data = fetch_vehicle_data(base_url, i)
        if not data:
            break
        else:
            all_vehicels.extend(data)
            i += 50 # 50 entries per page

    # Generate timestamp and format it for the file name
    timestamp = get_switzerland_time()

    # Create the file name with the timestamp
    bucket_name = "hsludwlbucket"
    file_name = f"vehicles_data_{timestamp}.json"
    
    # Save the vehicle data to S3 with the timestamped file name
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(all_vehicels)
    )

    # Create a DataFrame and extract coordinates
    df = pd.DataFrame(all_vehicels)
    df['latlng'] = df['geometry'].apply(lambda geo: (geo['y'], geo['x']))
    df['rounded_latlng'] = df['latlng'].apply(lambda x: round_coordinates(x, 1))

    # Extract unique rounded coordinates
    unique_latlng = df['rounded_latlng'].unique()
    unique_latlng_list = list(unique_latlng)

    # Fetch weather data for each unique lat/lon
    weather_data = []
    for lat, lon in unique_latlng_list:
        weather = fetch_weather_data(lat, lon)
        weather_data.append({
            'lat': lat,
            'lon': lon,
            'weather': weather
        })

    # Save the weather data to S3
    weather_file_name = f"weather_data_{timestamp}.json"
    
    s3.put_object(
        Bucket=bucket_name,
        Key=weather_file_name,
        Body=json.dumps(weather_data)
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": "Vehicle and weather data saved to S3",
            "vehicle_file": file_name,
            "weather_file": weather_file_name
        })
    }
