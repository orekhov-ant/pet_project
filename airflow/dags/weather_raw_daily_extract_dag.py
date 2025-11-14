import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator, get_current_context
from weather_raw_daily.extract import extract_weather_raw_daily

default_args = {
    "owner": "Orekhov_Anton",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

api_key = os.getenv("METEOSTAT_API_KEY")

def get_target_date() -> str:
    """
    Возвращает logical_date в формате "YYYY-MM-DD"
    :return: "2025-11-01"
    """
    context = get_current_context()
    logical_date = context["logical_date"]  # pendulum DateTime
    target_date = logical_date.date().isoformat()  # IDE не видит метода .date() к сожалению
    print(f"target gate: {target_date}, type: {type(target_date)}")
    return target_date

def run_extract_weather_raw_daily():
    """
    Обертка для PythonOperator.
     - берем target_date Airflow и передаем строку 'YYYY-MM-DD' в extract_weather_raw_daily
    """
    target_date = get_target_date()
    extract_weather_raw_daily(target_date)

with DAG(
    dag_id="weather_raw_daily_extract",
    start_date=datetime(2025, 11, 10),
    schedule_interval="30 10 * * *",
    catchup=True,
    default_args=default_args,
    max_active_runs=1,
    tags=["weather", "raw", "meteostat"],
) as dag:

    # Этот сенсор проверяет только код 200.
    check_api = HttpSensor(
        task_id="check_meteostat_api_alive",
        http_conn_id="meteostat_api",
        endpoint="/point/daily",
        request_params={
            "lat": 55.7558,
            "lon": 37.6173,
            "start": "{{ ds }}",
            "end": "{{ ds }}",
        },
        headers={
            "x-rapidapi-key": api_key,
            "x-rapidapi-host": "meteostat.p.rapidapi.com",
        },
        mode="reschedule",
        poke_interval=60,
        timeout=60*10,
    )

    check_db = SqlSensor(
        task_id="check_postgres_alive",
        conn_id="raw_postgres",
        sql="SELECT 1",
        mode="reschedule",
        poke_interval=60,
        timeout=60*10,
    )

    extract_task = PythonOperator(
        task_id="extract_weather_raw_daily",
        python_callable=run_extract_weather_raw_daily,
    )

    [check_api, check_db] >> extract_task

