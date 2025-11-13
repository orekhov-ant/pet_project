from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, get_current_context
from weather_raw_daily.extract import extract_weather_raw_daily


default_args = {
    "owner": "Orekhov_Anton",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="weather_raw_daily_extract",
    start_date=datetime(2025, 11, 1),
    schedule_interval="30 10 * * *",
    catchup=True,
    default_args=default_args,
    tags=["weather", "raw", "meteostat"],
) as dag:
    # Заглушка дага
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    start >> finish