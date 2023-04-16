# ETL Pipeline for Device Data Aggregation
### PAIR Finance Assignment
This project provides an ETL (Extract, Transform, Load) pipeline that pulls device data from a PostgreSQL table, performs data aggregation, and stores the aggregated data in a MySQL database. 

The pipeline is designed to run inside a Docker container and be executed by the `docker-compose up` command.

The ETL pipeline is composed of three main functions:

### `extract_data()`
- Retrieves the data from the PostgreSQL table for the last hour.
- Returns a pandas DataFrame containing the device data.
### `transform_data(df)`
- Takes the DataFrame from `extract_data()` as input.
- Converts the Unix timestamp to a datetime object and sets it as the DataFrame index.
- Splits the location JSON into latitude and longitude columns.
- Calculates the distance between consecutive data points for each device using the *Haversine distance* formula.
- Aggregates the data by calculating the maximum temperature, number of data points, and total distance for each device on an hourly basis.
- Returns the aggregated DataFrame.
### `load_data(df_agg)`
- Takes the aggregated DataFrame from `transform_data()` as input.
- Appends the aggregated data to the MySQL table.

### The ETL pipeline runs in a loop:

Every hour, the script extracts data for the last hour, transforms it, and loads it into the MySQL table.
If there's an error during the ETL process, the script will wait for 1 minute before retrying.





