import os
import requests
import psycopg2
from psycopg2.extras import Json
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone



def is_running_locally() -> bool:
    """
    Определяет, запущен ли скрипт локально.
    """
    if Path('/.dockerenv').exists():
        return False
    return True

def find_env_path(file_name: str):
    """
    Находит путь до .env файла поднимаясь в верх по каталогам.
    :param file_name: Имя .env или .env.local файла, который ищем
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

def load_project_env():
    """
    Загружает нужный env
    """
    if is_running_locally():
        load_dotenv(dotenv_path=find_env_path(".env.local"))
        print("Загружен .env.local")
    else:
        load_dotenv(dotenv_path=find_env_path(".env"))
        print("Загружен .env")

def upsert_by_delete(connection, latitude: float, longitude: float, record: dict):
    """
    Сначала удаляем запись за день+координаты, потом вставляем свежие данные.
    :param connection: объект-соединение с postgres.
    :param latitude: широта.
    :param longitude: долгота.
    :param record: объект из Meteostat Point Daily API: {"date": "YYYY-MM-DD", "tavg": 123, ...}
    """
    if not record.get("date"):
        raise ValueError("В ответе нет поля date.")
    # приводим формат времени из "2025-11-01 00:00:00" в формат объекта datetime (2025, 11, 1, 0, 0) c меткой utc
    api_dt = datetime.fromisoformat(record.get("date")).replace(tzinfo=timezone.utc)
    with connection.cursor() as curr:
        # delete целевого среза
        curr.execute("""
            DELETE FROM raw.weather_per_day
            WHERE api_dt = %s AND lat = %s AND lon = %s
            """,
            (api_dt, float(latitude), float(longitude))
        )

        # insert свежих данных
        curr.execute("""
            INSERT INTO raw.weather_per_day (api_dt, lat, lon, raw_payload)
            VALUES (%s, %s, %s, %s)
            """,
            (api_dt, float(latitude), float(longitude), Json(record))
        )

# 1. Приоритет: если есть .env.local, то подгружаем его, иначе обычный .env.
load_project_env()
# 2. Переменные конекшена для записи в базу.
API_KEY = os.getenv("METEOSTAT_API_KEY")
PG_HOST = os.getenv("POSTGRES_DATA_HOST")        # имя хоста по месту запуска скрипта
PG_PORT = int(os.getenv("POSTGRES_DATA_PORT"))
PG_DB = os.getenv("POSTGRES_DATA_DB")           # название БД
PG_SCHEMA = os.getenv("POSTGRES_DATA_SCHEMA")
PG_USER = os.getenv("POSTGRES_DATA_USER")
PG_PWD = os.getenv("POSTGRES_DATA_PASSWORD")

# словарь с координатами городов, потом где-то размещу
cities_geo = {
    "moscow": (55.7558,37.6173),
    "spb": (59.9375, 30.3086),
    "kazan":   (55.7963, 49.1088),
}

# 3. Формируем запрос api.
target_date = "2025-11-02"
base_url = "https://meteostat.p.rapidapi.com/point/daily"
params = {
    "lat": cities_geo["moscow"][0],
    "lon": cities_geo["moscow"][1],
    "start": target_date,    # YYYY-MM-DD
    "end": target_date,
    "units": "metric"
}
headers = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "meteostat.p.rapidapi.com"
}

# 4. Получаем ответ.
response = requests.get(base_url, params=params, headers=headers, timeout=10)

if response.status_code == 200:
    print("API доступен")
else:
    print(f"Ошибка API,  {response.status_code}: {response.text}")

data = response.json()
rows = data.get("data", [])
if not isinstance(rows, list):
    print("Структура ответа неожиданная. Поле data не является списком.")
elif len(rows) == 0:
    print("На выбранную дату данных нет. Возможен временной лаг публикации или неверная дата.")
elif len(rows) == 1:
    row = rows[0]
    date = row.get("date")
    tavg = row.get("tavg")
    print(f"Средняя температура на {date} составляет {tavg} C.")
    print(rows)
else:
    print(f"Структура ответа неожиданная. Получено {len(rows)} записей за период (ожидаемое: 1 запись)")

# 5. Загружаем ответ в базу.
# В контекстном менеджере открытие сетевого соединения с сервером postgres. Этот объект - одно активное соединение
# при завершении без ошибок - вызывает conn.commit(), при ошибках conn.rollback(). Но не вызывает conn.close()
# with conn управляет транзакцией, а не жизненным циклом соединения.
with psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PWD
) as conn:
    # объект, через который выполняются sql запросы в контекстном менеджере
    with conn.cursor() as cur_check:      # объект буферизует результаты и управляет состоянием запроса.
        cur_check.execute("SELECT version();")
        print("PostgreSQL версия: ", cur_check.fetchone()[0])    # при выходе with conn.cursor() всегда вызывается cur.close()
    if not isinstance(rows, list):
        raise ValueError("Структура ответа неожиданная: 'rows' не список")
    if len(rows) == 1:
        upsert_by_delete(conn, cities_geo["moscow"][0], cities_geo["moscow"][1], rows[0])
        print("Перезаписали срез за дату+координаты.")
    else:
        raise ValueError(f"Структура ответа неожиданная. Получено {len(rows)} записей. Ожидание: 1 запись.")
conn.close()