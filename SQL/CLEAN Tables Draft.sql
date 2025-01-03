CREATE TABLE IF NOT EXISTS weather_clean (
    weather_id int PRIMARY KEY,
    timestamp TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    wind_speed FLOAT,
    rain FLOAT,
    cloud_coverage INT,
    humidity INT,
    rounded_timestamp TIMESTAMP,
);

CREATE TABLE IF NOT EXISTS vehicles_clean (
    timestamp TIMESTAMP,
    vehicle_id VARCHAR(100),
    type VARCHAR(50),
    provider_id VARCHAR(50),
    available BOOLEAN,
    pickup_type VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    rounded_timestamp TIMESTAMP,
    rounded_latitude FLOAT,
    rounded_longitude FLOAT,
    PRIMARY KEY (vehicle_id, timestamp)
);

CREATE TABLE IF NOT EXISTS time_clean (
    time_id int PRIMARY KEY,
    timestamp TIMESTAMP,
    date DATE,
    day int,
    month int,
    year int,
    weekday VARCHAR(50),
    week_of_year int,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS demographics_clean (
    geo_nr VARCHAR(10),
    geo_name VARCHAR(255),
    class_hab VARCHAR(50),
    geom_period DATE,
    pop2022 INTEGER,
    pop2021 INTEGER,
    dens_pop_22 FLOAT,
    pop2000 INTEGER,
    pop1990 INTEGER,
    pop1980 INTEGER,
    pop1970 INTEGER,
    pop1930 INTEGER,
    pop_sexf INTEGER,
    pop_sexm INTEGER,
    pop_civ_sin_t INTEGER,
    pop_civ_mar_t INTEGER
);

CREATE TABLE IF NOT EXISTS fact_distances (
    time_id int,
    vehicle_id VARCHAR(100),
    weather_id int,
    geo_nr VARCHAR(10),
    distance FLOAT,
    primary key (time_id, vehicle_id, weather_id, geo_nr)
);