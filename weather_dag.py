import json

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago


CITIES = ["Kyiv", "Kharkiv", "Odesa", "Zhmerynka"]

DAG_ID = "uku-weather-dag-v1"
HISTORIC_API_ENDPOINT = "data/3.0/onecall/timemachine"
GEO_API_ENDPOINT = "geo/1.0/direct"


def _process_weather(ti, city):
    info = ti.xcom_pull(f"{city}_group.extract_data_{city}")

    timestamp = info['data'][0]["dt"]
    temp = info["data"][0]["temp"]
    humidity = info["data"][0]["humidity"]
    cloudiness = info["data"][0]["clouds"]
    wind_speed = info["data"][0]["wind_speed"]

    return timestamp, city, temp, humidity, cloudiness, wind_speed


with DAG(dag_id=DAG_ID, schedule_interval="@daily", start_date=days_ago(5), catchup=True) as dag:
    db_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="sqlite_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures
        (
            timestamp TIMESTAMP,
            city VARCHAR(32),
            temp FLOAT,
            humidity FLOAT,
            cloudiness FLOAT,
            wind_speed FLOAT
        );"""
    )

    check_api = HttpSensor(
        task_id=f"check_api",
        http_conn_id="weather_3_api_conn",
        endpoint=HISTORIC_API_ENDPOINT,
        request_params={
            "lat": "44.34", # random data from docs
            "lon": "10.99",
            "dt": "1681074000",
            "appid": Variable.get("WEATHER_API_KEY")
        }
    )

    for city in CITIES:
        
        with TaskGroup(group_id=f'{city}_group') as task_group:

            get_city_coordinates = SimpleHttpOperator(
                task_id=f"get_{city}_coordinates",
                http_conn_id="weather_3_api_conn",
                endpoint=GEO_API_ENDPOINT,
                data={"appid": Variable.get("WEATHER_API_KEY"), "q": city, "limit": 5},
                method="GET",
                response_filter=lambda x: (json.loads(x.text)[0].get("lat"), json.loads(x.text)[0].get("lon")),
                log_response=True
            )

            extract_data = SimpleHttpOperator(
                task_id=f"extract_data_{city}",
                http_conn_id="weather_3_api_conn",
                endpoint=HISTORIC_API_ENDPOINT,
                data={
                    "appid": Variable.get("WEATHER_API_KEY"),
                    "lat": f"{{{{ti.xcom_pull(task_ids='{city}_group.get_{city}_coordinates')[0]}}}}",
                    "lon": f"{{{{ti.xcom_pull(task_ids='{city}_group.get_{city}_coordinates')[0]}}}}",
                    "dt": "{{ execution_date.int_timestamp }}"
                },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True
            )

            process_data = PythonOperator(
                task_id=f"process_data_{city}",
                python_callable=_process_weather,
                op_kwargs={
                    "city": city
                }
            )

            inject_data = SqliteOperator(
                task_id=f"inject_data_{city}",
                sqlite_conn_id="sqlite_conn",
                sql=f"""
                INSERT INTO measures (timestamp, city, temp, humidity, cloudiness, wind_speed) VALUES
                ({{{{ti.xcom_pull(task_ids='{city}_group.process_data_{city}')[0]}}}},
                '{{{{ti.xcom_pull(task_ids='{city}_group.process_data_{city}')[1]}}}}',
                {{{{ti.xcom_pull(task_ids='{city}_group.process_data_{city}')[2]}}}},
                {{{{ti.xcom_pull(task_ids='{city}_group.process_data_{city}')[3]}}}},
                {{{{ti.xcom_pull(task_ids='{city}_group.process_data_{city}')[4]}}}},
                {{{{ti.xcom_pull(task_ids='{city}_group.process_data_{city}')[5]}}}});
                """,
            )

        get_city_coordinates >> extract_data >> process_data >> inject_data

        db_create >> check_api >> task_group
