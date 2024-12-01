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
