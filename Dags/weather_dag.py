from airflow import DAG 
from datetime import datetime, timedelta 
from airflow.providers.http.sensors.http import HttpSensor
import json 
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os
import requests

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_farenheit,
        "Feels Like (F)": feels_like_farenheit,
        "Minimun Temp (F)": min_temp_farenheit,
        "Maximum Temp (F)": max_temp_farenheit,
        "Pressure": pressure,
        "Humidty": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time                        
    }
    
    df_data = pd.DataFrame([transformed_data])
    output_dir = "/usr/local/airflow/output"
    os.makedirs(output_dir, exist_ok=True)
    dt_string = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(output_dir, f"current_weather_data_portland_{dt_string}.csv")
    print(f"[DEBUG] Writing file to: {file_path}")
    df_data.to_csv(file_path, index=False)

    # Push file path to XCom for downstream task
    task_instance.xcom_push(key='file_path', value=file_path)

def send_csv_to_discord(task_instance):
    file_path = task_instance.xcom_pull(task_ids='transform_load_weather_data', key='file_path')

    DISCORD_BOT_TOKEN = "********************************************************"  # <-- Replace with your Discord bot token
    DISCORD_CHANNEL_ID = "************************"        # <-- Replace with your Discord channel ID
    
    url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}"
    }
    
    with open(file_path, 'rb') as f:
        files = {
            "file": (os.path.basename(file_path), f)
        }
        data = {
            "content": "Here is the latest Portland weather data CSV."
        }
        response = requests.post(url, headers=headers, data=data, files=files)
    
    if response.status_code in [200, 201]:
        print("CSV file sent successfully to Discord!")
    else:
        print(f"Failed to send CSV to Discord. Status: {response.status_code}")
        print(response.text)
        raise Exception("Discord file upload failed.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 21),
    'email': ['mostamohamed22@gmail.com'], 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('weather_dag', default_args=default_args, schedule='@daily', catchup=False) as dag:
    
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&appid=**********************************'  # <-- Replace with your open weather api
    )
    
    extract_weather_data = HttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&APPID=**********************************',  # <-- Replace with your open weather api
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )
    
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )
    
    send_csv_to_discord_op = PythonOperator(
        task_id='send_csv_to_discord',
        python_callable=send_csv_to_discord
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> send_csv_to_discord_op
