from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

import json

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    @task()
    def extract_weather_data_from_api(lat, long):
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        api_endpoint = f'/v1/forecast?latitude={lat}&longitude={long}&current_weather=true'
        response = http_hook.run(api_endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data. Status: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        return {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'humidity': current_weather.get('humidity', 0),
            'wind_speed': current_weather['windspeed'],
            'wind_direction': current_weather['winddirection'],
            'weather_code': current_weather['weathercode']
        }

    @task()
    def load_weather_data_to_postgres(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                humidity INTEGER,
                wind_speed FLOAT,
                wind_direction FLOAT,
                weather_code INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cursor.execute(
            """
            INSERT INTO weather_data (latitude, longitude, temperature, humidity, wind_speed, wind_direction, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (transformed_data['latitude'], transformed_data['longitude'],
             transformed_data['temperature'], transformed_data['humidity'],
             transformed_data['wind_speed'], transformed_data['wind_direction'],
             transformed_data['weather_code'])
        )

        conn.commit()
        cursor.close()
        conn.close()

    # DAG Workflow
    weather_data = extract_weather_data_from_api(LATITUDE, LONGITUDE)
    transformed_data = transform_weather_data(weather_data)
    load_weather_data_to_postgres(transformed_data)
