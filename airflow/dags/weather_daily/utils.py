import os
import time
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extensions import connection as pg_connection
from airflow.operators.python import get_current_context

# === connections ===
def get_pg_connection(retries: int = 3, delay: int = 2, max_delay: int = 30) -> pg_connection:
    """
    Универсальное соединение с Postgres. Работает локально (.env.local) и внутри docker (.env из .yml).
    :param retries: Максимальное количество попыток подключения.
    :param delay: Начальная задержка между попытками (секунды).
    :param max_delay: Максимальная задержка между попытками (секунды).
    :return: Открытое соединение psycopg2.
    :raises RuntimeError:
        - если не найдены обязательные переменные окружения для открытия соединения.
        - если не удалось подключиться к postgres за указанное число попыток.
    """

    required_env_vars = [
        "POSTGRES_DATA_HOST",
        "POSTGRES_DATA_PORT",
        "POSTGRES_DATA_DB",
        "POSTGRES_DATA_USER",
        "POSTGRES_DATA_PASSWORD",
    ]

    missing = [var for var in required_env_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(f"Не найдены переменные окружения: {missing}")

    host = os.getenv("POSTGRES_DATA_HOST")
    port = os.getenv("POSTGRES_DATA_PORT")
    dbname = os.getenv("POSTGRES_DATA_DB")
    user = os.getenv("POSTGRES_DATA_USER")
    password = os.getenv("POSTGRES_DATA_PASSWORD")

    conn = None
    current_delay = delay

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=host,
                port=int(port),
                dbname=dbname,
                user=user,
                password=password,
            )
        except OperationalError as e:
            if attempt == retries:
                raise RuntimeError(
                    f"Не удалось подключиться к postgres за {retries} попыток."
                    f"{e}"
                )
            time.sleep(current_delay)
            current_delay = min(current_delay * 2, max_delay)

    return conn

# === helpers ===
def get_target_date() -> str:
    """
    Возвращает logical_date из контекста airflow в формате "YYYY-MM-DD"
    :return: "2025-11-01"
    """
    context = get_current_context()
    logical_date = context["logical_date"]  # pendulum DateTime
    target_date = logical_date.date().isoformat()  # IDE не видит метода .date() к сожалению
    print(f"target gate: {target_date}, type: {type(target_date)}")
    return target_date

def is_running_locally() -> bool:
    """
    Определяет, запущен ли скрипт локально.
    """
    if Path('/.dockerenv').exists():
        print("Запущено в docker.")
        return False
    print("Запущено в локально.")
    return True

def find_env_path(file_name: str) -> str:
    """
    Находит путь до .env файла поднимаясь в верх по каталогам.
    :param file_name: Имя .env или .env.local файла, который ищем
    :return: путь до переданного env файла в виде строки.
    """
    current_dir = os.path.dirname(__file__)
    while True:
        possible_path = os.path.join(current_dir, file_name)
        if os.path.exists(possible_path):
            return possible_path
        new_dir = os.path.dirname(current_dir)  # метод возвращает путь до родительской папки для файла или другой папки
        if new_dir == current_dir:
            raise FileNotFoundError(f'{file_name} не найден')   # защита от бесконечного цикла, когда путь не обрезается
        current_dir = new_dir

def load_project_env_if_locally():
    """
    Нужно для запуска локально. Загружает нужный .env.local
    """
    if is_running_locally():
        load_dotenv(dotenv_path=find_env_path(".env.local"))
        print("Загружен .env.local")
