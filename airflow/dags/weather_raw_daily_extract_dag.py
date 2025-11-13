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

def run_extract_weather_raw_daily():
    """
    Обертка для PythonOperator.
     - берем logical_date Airflow
     - вычитаем 1 день (данные за предыдущий день)
     - передаем строку 'YYYY-MM-DD' в extract_weather_raw_daily
    """
    context = get_current_context()
    logical_date = context["logical_date"]  # pendulum DateTime
    target_date = logical_date.date().isoformat()     # IDE не видит метода .date() к сожалению
    print(f"target gate: {target_date}, type: {type(target_date)}")
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

    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract_weather_raw_daily",
        python_callable=run_extract_weather_raw_daily,
    )

    finish = EmptyOperator(task_id="finish")

    start >> extract_task >> finish

if __name__ == "__main__":
    pass