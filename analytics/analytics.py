import os
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from time import sleep
from geopy.distance import great_circle

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

# Connect to PostgreSQL
while True:
    try:
        psql_engine = create_engine(os.environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')
# Create destination table if not exists
metadata = MetaData()
aggregated_data = Table(
    'aggregated_data', metadata,
    Column('time', String),
    Column('device_id', String),
    Column('max_temperature', Integer),
    Column('data_points', Integer),
    Column('total_distance', Float)
)
metadata.create_all(mysql_engine)

def extract_data():
    query = "SELECT * FROM devices;"
    df = pd.read_sql_query(query, psql_engine)
    return df

def haversine_distance(lat1, lon1, lat2, lon2):
    """
        Calculate the great-circle distance (Haversine distance) between two points on the Earth's surface,
        given their latitude and longitude coordinates.
        param lat1: latitude of the first point
        param lon1: longitude of the first point
        param lat2: latitude of the second point
        param lon2: longitude of the second point
        return: great-circle distance between the two points in kilometers
    """
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371 * c


def transform_data(df):
    # Convert time to datetime and set as index
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)

    # Split location JSON into latitude and longitude
    df[['latitude', 'longitude']] = pd.json_normalize(df['location'].apply(eval)).astype(float)

    # Calculate distance
    df['distance'] = 0.0
    for device_id in df['device_id'].unique():
        device_data = df[df['device_id'] == device_id].copy()
        device_data['distance'] = haversine_distance(device_data['latitude'], device_data['longitude'],
                                                     device_data['latitude'].shift(), device_data['longitude'].shift()).fillna(0)
        df.loc[df['device_id'] == device_id, 'distance'] = device_data['distance']

    # Aggregate data
    agg_data = {
        'temperature': ['max'],
        'device_id': ['count'],
        'distance': ['sum']
    }
    df_agg = df.groupby([pd.Grouper(freq='H'), 'device_id']).agg(agg_data)
    df_agg.columns = ['max_temperature', 'data_points', 'total_distance']
    df_agg.reset_index(inplace=True)

    return df_agg

def load_data(df_agg):
    df_agg.to_sql('aggregated_data', mysql_engine, if_exists='append', index=False)

while True:
    try:
        # Extract data
        df = extract_data()

        # Transform data
        df_agg = transform_data(df)

        # Load data
        load_data(df_agg)

        print("Data aggregation successful.")

        sleep(3600)  # Run the ETL process every hour
    except Exception as e:
        print(f"Error during ETL process: {e}")
        sleep(60)  # Retry after 1 minute if there's an error
