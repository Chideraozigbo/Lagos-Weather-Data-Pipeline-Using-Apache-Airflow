from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
import pandas as pd






def kelvin_to_fahrenheit(temp_in_kelvin):
	temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
	return temp_in_fahrenheit


def transform_and_load_data(task_instance):
	data = task_instance.xcom_pull(task_ids='extract_data')
	city = data['name']
	weather_description = data['weather'][0]['description']
	temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp'])
	feels_like_fahrenheit = kelvin_to_fahrenheit(data['main']['feels_like'])
	min_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_min'])
	max_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_max'])
	pressure = data['main']['pressure']
	humidity = data['main']['humidity']
	wind_of_speed = data['wind']['speed']
	time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
	sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
	sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

	transformed_data = { 'City': city,
		'Description': weather_description,
		'Temperature (F)': temp_fahrenheit,
		'Feels like (F)': feels_like_fahrenheit,
		'Minimum Temp (F)': min_temp_fahrenheit,
		'Maximum Temp (F)': max_temp_fahrenheit,
		'Pressure': pressure,
		'Humidity': humidity,
		'Wind_of_speed': wind_of_speed,
		'Time_of_record': time_of_record,
		'Sunrise Time': sunrise_time,
		'Sunset Time': sunset_time
	}



	transformed_data_list = [transformed_data]
	new_df = pd.DataFrame(transformed_data_list)

	now = datetime.now()
	dt_string = now.strftime('%d%m%Y%H%M%S')
	dt_string = 'current_weather_data_for_lagos_' + dt_string
	new_df.to_csv(f's3://weatherdataapiproject/{dt_string}.csv', index=False)




default_args = {
	'owner': 'Chidera',
	'start_date': datetime(2024, 4, 23),
	'email': ['chideraozigbo@gmail.com'],
	'email_on_failure': True,
	'email_on_retry': True,
	'retries': 2,
	'retry_delay': timedelta(minutes=2)
}



etl_dag = DAG(
	'etl_pipeline',
	default_args=default_args,
	schedule_interval = '@daily',
	catchup=False
)



sensor = HttpSensor(
	task_id = 'is_weather_api_available',
	http_conn_id = 'weathermap_api',
	endpoint = '/data/2.5/weather?q=lagos&appid=c8a16cd20f242ea8bfe560e57e1d48',
	timeout = 20,
	dag = etl_dag
)

extract_weather_data = SimpleHttpOperator(
	task_id="extract_data", 
	http_conn_id = "weathermap_api",
    	method="GET",
    	endpoint="/data/2.5/weather?q=lagos&appid=c8a16cd20f242ea8bfe560e57e1d48",
    	response_filter=lambda response: json.loads(response.text),
    	log_response = True,
    	dag= etl_dag
)

transform_and_load =  PythonOperator(
	task_id = 'transform_and_load_data',
	python_callable = transform_and_load_data,
	dag = etl_dag
)

sensor >> extract_weather_data >> transform_and_load
