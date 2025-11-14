from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "Orekhov_Anton",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}
# Одинаковый с extract start_date и interval, но transform ждет extract.
with DAG(
    dag_id="weather_daily_transform",
    start_date=datetime(2025, 11, 10),
    schedule_interval="30 10 * * *",
    catchup=True,
    default_args=default_args,
    max_active_runs=1,
    tags=["weather", "staged", "meteostat"]
) as dag:

    # Airflow сопоставит logical_date.
    # Если extract за 2025-11-10, то transform за 2025-11-10 будет ждать success.
    wait_for_extract = ExternalTaskSensor(
        task_id="wait_for_extract_weather_daily",
        external_dag_id="weather_daily_extract",
        external_task_id="extract_weather_daily",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60*10,
        timeout=60*60*6,
    )

    task2 = EmptyOperator(
        task_id="empty2"
    )

    wait_for_extract >> task2