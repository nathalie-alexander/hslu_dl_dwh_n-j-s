## Raw Tables

Here are the raw tables that store the data collected from the various sources. The data is stored in its original form and is not transformed in any way. The raw tables are used to store the data as it is collected from the sources. The data is then transformed and loaded into the processed tables for further analysis.

They are all stored in the `rawdatadb` database.

### **Table: vehicles_raw**
| Column                       | Data Type      | Description                                  |
|------------------------------|----------------|----------------------------------------------|
| `timestamp`                  | TIMESTAMP      | Time of the record.                         |
| `provider_id`                | VARCHAR(50)    | Unique identifier for the provider.         |
| `provider_name`              | VARCHAR(100)   | Name of the provider.                       |
| `provider_timezone`          | VARCHAR(50)    | Timezone of the provider.                   |
| `provider_apps_ios_store_uri`| TEXT           | iOS app store URI of the provider.          |
| `provider_apps_android_store_uri` | TEXT     | Android app store URI of the provider.      |
| `vehicle_id`                 | VARCHAR(100)   | Unique identifier for the vehicle.          |
| `available`                  | BOOLEAN        | Availability status of the vehicle.         |
| `pickup_type`                | VARCHAR(50)    | Type of pickup.                             |
| `vehicle_status_disabled`    | BOOLEAN        | Indicates if the vehicle is disabled.       |
| `vehicle_status_reserved`    | BOOLEAN        | Indicates if the vehicle is reserved.       |
| `geometry_x`                 | FLOAT          | X-coordinate of the vehicle's location.     |
| `geometry_y`                 | FLOAT          | Y-coordinate of the vehicle's location.     |
| **Primary Key**              | `(vehicle_id, timestamp)` | Composite key of vehicle ID and timestamp. |

### **Table: weather_raw**
| Column           | Data Type      | Description                                 |
|------------------|----------------|---------------------------------------------|
| `timestamp`      | TIMESTAMP      | Time of the weather record.                 |
| `latitude`       | FLOAT          | Latitude of the location.                   |
| `longitude`      | FLOAT          | Longitude of the location.                  |
| `main_weather`   | VARCHAR(50)    | General weather category (e.g., Clear).     |
| `description`    | VARCHAR(100)   | Detailed weather description.               |
| `icon`           | VARCHAR(10)    | Icon code for weather representation.       |
| `temperature`    | FLOAT          | Current temperature in Celsius.             |
| `feels_like`     | FLOAT          | Feels-like temperature in Celsius.          |
| `temp_min`       | FLOAT          | Minimum temperature in Celsius.             |
| `temp_max`       | FLOAT          | Maximum temperature in Celsius.             |
| `pressure`       | INT            | Atmospheric pressure in hPa.                |
| `humidity`       | INT            | Humidity percentage.                        |
| `sea_level`      | INT            | Atmospheric pressure at sea level (hPa).    |
| `grnd_level`     | INT            | Atmospheric pressure at ground level (hPa). |
| `visibility`     | INT            | Visibility in meters.                       |
| `wind_speed`     | FLOAT          | Wind speed in meters per second.            |
| `wind_deg`       | INT            | Wind direction in degrees.                  |
| `wind_gust`      | FLOAT          | Wind gust speed in meters per second.       |
| `rain`           | FLOAT          | Rain in the last hour.                      |
| `cloud_coverage` | INT            | Cloud coverage percentage.                  |
| `country`        | CHAR(2)        | Country code (ISO 3166-1 alpha-2).          |
| `sunrise`        | TIMESTAMP      | Sunrise time.                               |
| `sunset`         | TIMESTAMP      | Sunset time.                                |
| **Primary Key**  | `(latitude, longitude, timestamp)` | Composite key of location and time.         |

### **Table: demographics_raw**
| Column         | Data Type        | Description                                |
|----------------|------------------|--------------------------------------------|
| `id`           | SERIAL           | Unique identifier for the record.         |
| `geo_nr`       | VARCHAR(10)      | Geographical number.                      |
| `geo_name`     | VARCHAR(255)     | Name of the geographical area.            |
| `class_hab`    | VARCHAR(50)      | Classification of habitation.             |
| `geom_period`  | DATE             | Period of the data collection.            |
| `variable`     | VARCHAR(50)      | Variable name for the data.               |
| `source`       | VARCHAR(50)      | Source of the demographic data.           |
| `value_period` | INTEGER          | Period value associated with the data.    |
| `unit_value`   | VARCHAR(50)      | Unit of the recorded value.               |
| `value`        | DOUBLE PRECISION | Actual value recorded.                    |
| `status`       | CHAR(1)          | Status indicator for the data.            |
| **Primary Key**| `id`             | Unique ID for each record.                |

# Clean Tables

### **Table 1: `weather_clean`**

| Column           | Data Type  | Description                                      |
|-------------------|------------|--------------------------------------------------|
| `weather_id`      | INT        | Unique identifier for weather data (Primary Key).|
| `timestamp`       | TIMESTAMP  | Exact time of the weather observation.           |
| `latitude`        | FLOAT      | Latitude of the location.                        |
| `longitude`       | FLOAT      | Longitude of the location.                       |
| `temperature`     | FLOAT      | Temperature recorded at the location.            |
| `wind_speed`      | FLOAT      | Speed of the wind at the location.               |
| `rain`            | FLOAT      | Rainfall measured at the location.               |
| `cloud_coverage`  | INT        | Percentage of cloud coverage.                    |
| `humidity`        | INT        | Humidity level as a percentage.                  |
| `rounded_timestamp` | TIMESTAMP | Timestamp rounded to the nearest interval.       |

---

### **Table 2: `vehicles_clean`**

| Column             | Data Type   | Description                                          |
|---------------------|-------------|------------------------------------------------------|
| `timestamp`         | TIMESTAMP   | Exact time of the vehicle's status record.           |
| `vehicle_id`        | VARCHAR(100)| Unique identifier for the vehicle (Primary Key part).|
| `type`              | VARCHAR(50) | Type of the vehicle (e.g., scooter, bike).           |
| `provider_id`       | VARCHAR(50) | Identifier of the provider offering the vehicle.     |
| `available`         | BOOLEAN     | Indicates if the vehicle is available.               |
| `pickup_type`       | VARCHAR(50) | Type of pickup (e.g., free-floating).                |
| `latitude`          | FLOAT       | Latitude of the vehicle's location.                  |
| `longitude`         | FLOAT       | Longitude of the vehicle's location.                 |
| `rounded_timestamp` | TIMESTAMP   | Timestamp rounded to the nearest interval.           |
| `rounded_latitude`  | FLOAT       | Rounded latitude of the location.                    |
| `rounded_longitude` | FLOAT       | Rounded longitude of the location.                   |

---

### **Table 3: `time_clean`**

| Column         | Data Type   | Description                                           |
|-----------------|-------------|-------------------------------------------------------|
| `time_id`       | INT         | Unique identifier for the time record (Primary Key).  |
| `timestamp`     | TIMESTAMP   | Exact timestamp associated with the record.           |
| `date`          | DATE        | Date associated with the timestamp.                  |
| `day`           | INT         | Day of the month.                                    |
| `month`         | INT         | Month of the year.                                   |
| `year`          | INT         | Year of the timestamp.                               |
| `weekday`       | VARCHAR(50) | Name of the day of the week (e.g., Monday).          |
| `week_of_year`  | INT         | Week number of the year.                             |
| `is_weekend`    | BOOLEAN     | Indicates if the date falls on a weekend.            |

---

### **Table 4: `demographics_clean`**

| Column            | Data Type   | Description                                             |
|--------------------|-------------|---------------------------------------------------------|
| `geo_nr`           | VARCHAR(10) | Unique geographical number.                             |
| `geo_name`         | VARCHAR(255)| Name of the geographical area.                          |
| `class_hab`        | VARCHAR(50) | Classification of the habitation type.                  |
| `geom_period`      | DATE        | Period associated with the geographical measurement.     |
| `pop2022`          | INTEGER     | Population data for 2022.                               |
| `pop2021`          | INTEGER     | Population data for 2021.                               |
| `dens_pop_22`      | FLOAT       | Population density for 2022.                            |
| `pop2000`          | INTEGER     | Population data for 2000.                               |
| `pop1990`          | INTEGER     | Population data for 1990.                               |
| `pop1980`          | INTEGER     | Population data for 1980.                               |
| `pop1970`          | INTEGER     | Population data for 1970.                               |
| `pop1930`          | INTEGER     | Population data for 1930.                               |
| `pop_sexf`         | INTEGER     | Female population count.                                |
| `pop_sexm`         | INTEGER     | Male population count.                                  |
| `pop_civ_sin_t`    | INTEGER     | Population with single civil status.                    |
| `pop_civ_mar_t`    | INTEGER     | Population with married civil status.                   |

### **Table: `fact_distances`**

| Column        | Data Type     | Description                                          |
|---------------|---------------|------------------------------------------------------|
| `time_id`     | INT           | Foreign key referencing the `time_clean` table.      |
| `vehicle_id`  | VARCHAR(100)  | Foreign key referencing the `vehicles_clean` table.  |
| `weather_id`  | INT           | Foreign key referencing the `weather_clean` table.   |
| `geo_nr`      | VARCHAR(10)   | Foreign key referencing the `demographics_clean` table. |
| `distance`    | FLOAT         | Distance traveled or covered.                        | 

**Primary Key:** Combination of `time_id`, `vehicle_id`, `weather_id`, and `geo_nr`.