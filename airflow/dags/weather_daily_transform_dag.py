from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from weather_daily.utils import get_target_date
from weather_daily.transform import transform_weather_daily

default_args = {
    "owner": "Orekhov_Anton",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def run_transform_weather_daily():
    """
    Обертка для PythonOperator.
     - берем target_date Airflow и передаем строку 'YYYY-MM-DD' в transform_weather_daily
    """
    target_date = get_target_date()
    transform_weather_daily(target_date)

# У extract_dag и transform_dag одинаковый с start_date и interval, но transform ждет extract.
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

    check_db = SqlSensor(
        task_id="check_postgres_alive",
        conn_id="raw_postgres",
        sql="SELECT 1",
        mode="reschedule",
        poke_interval=60,
        timeout=60*10,
    )

    transform_task = PythonOperator(
        task_id="transform_weather_daily",
        python_callable=run_transform_weather_daily,
    )

    wait_for_extract >> check_db >> transform_task