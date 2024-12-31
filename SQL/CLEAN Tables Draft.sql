CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_NAME_WEATHER_CLEAN} (
    timestamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    wind_speed FLOAT,
    rain FLOAT,
    cloud_coverage INT,
    humidity INT,
    PRIMARY KEY (latitude, longitude, timestamp)
);

CREATE TABLE IF NOT EXISTS {CONSTANTS.TABLE_NAME_VECHICLES_CLEAN} (
    timestamp TIMESTAMP,
    type VARCHAR(50),
    provider_id VARCHAR(50),
    vehicle_id VARCHAR(100),
    available BOOLEAN,
    pickup_type VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (vehicle_id, timestamp)
);
