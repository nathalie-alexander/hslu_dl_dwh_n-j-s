CREATE TABLE IF NOT EXISTS shared_mobility_raw (
    timestamp TIMESTAMP,
    provider_id VARCHAR(50),
    provider_name VARCHAR(100),
    provider_timezone VARCHAR(50),
    provider_apps_ios_store_uri TEXT,
    provider_apps_android_store_uri TEXT,
    vehicle_id VARCHAR(100),
    available BOOLEAN,
    pickup_type VARCHAR(50),
    vehicle_status_disabled BOOLEAN,
    vehicle_status_reserved BOOLEAN,
    geometry_x FLOAT,
    geometry_y FLOAT,
    PRIMARY KEY (vehicle_id, timestamp)
);

CREATE TABLE IF NOT EXISTS weather_data_raw (
    timestamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    main_weather VARCHAR(50),
    description VARCHAR(100),
    icon VARCHAR(10),
    temperature FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    humidity INT,
    sea_level INT,
    grnd_level INT,
    visibility INT,
    wind_speed FLOAT,
    wind_deg INT,
    wind_gust FLOAT,
    rain FLOAT,
    cloud_coverage INT,
    country CHAR(2),
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    PRIMARY KEY (latitude, longitude, timestamp)
);

CREATE TABLE IF NOT EXISTS demographic_data_raw (
    id SERIAL PRIMARY KEY,
    geo_nr VARCHAR(10),
    geo_name VARCHAR(255),
    class_hab VARCHAR(50),
    geom_period DATE,
    variable VARCHAR(50),
    source VARCHAR(50),
    value_period INTEGER,
    unit_value VARCHAR(50),
    value DOUBLE PRECISION,
    status CHAR(1)
);
