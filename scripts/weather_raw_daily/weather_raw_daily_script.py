# === imports ===
import os
from pathlib import Path
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv


# === constants ===
# словарь с координатами городов, потом где-то размещу
cities_geo = {
    "moscow": (55.7558,37.6173),
    "spb": (59.9375, 30.3086),
    "kazan":   (55.7963, 49.1088),
}

# === helpers ===
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

# === atomic EXTRACT steps ===
def upsert_by_delete(connection, city: str, latitude: float, longitude: float, record: dict):
    """
    Сначала удаляем запись за день+координаты, потом вставляем свежие данные.
    :param connection: объект-соединение с postgres.
    :param city: название города.
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
            WHERE api_dt = %s AND city_name = %s
            """,
            (api_dt, city)
        )

        # insert свежих данных
        curr.execute("""
            INSERT INTO raw.weather_per_day (api_dt, city_name, lat, lon, raw_payload)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (api_dt, city, float(latitude), float(longitude), Json(record))
        )

# === Core Logic ===
def extract_weather_raw_daily(target_date: str):
    """
    Из API Meteostat забирает json с погодой по городам из cities_geo. Затем записывает в базу.
    :param target_date:
    :return:
    """
    # 1. Приоритет: если есть .env.local, то подгружаем его, иначе обычный .env.
    load_project_env()
    # 2. Переменные соединения для записи в базу.
    API_KEY = os.getenv("METEOSTAT_API_KEY")
    PG_HOST = os.getenv("POSTGRES_DATA_HOST")        # имя хоста по месту запуска скрипта
    PG_PORT = int(os.getenv("POSTGRES_DATA_PORT"))
    PG_DB = os.getenv("POSTGRES_DATA_DB")           # название БД
    PG_USER = os.getenv("POSTGRES_DATA_USER")
    PG_PWD = os.getenv("POSTGRES_DATA_PASSWORD")

    # 3. Формируем запрос api.
    base_url = "https://meteostat.p.rapidapi.com/point/daily"
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "meteostat.p.rapidapi.com"
    }

    # 4. Открываем одно соединение для записи всех городов.
    with psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PWD
    ) as conn:
        # Цикл по имеющимся городам.
        for city_name, (lat, lon) in cities_geo.items():
            params = {
                "lat": lat,
                "lon": lon,
                "start": target_date,
                "end": target_date,
                "units": "metric"
            }
            # Сохраняем ответ в память.
            response = requests.get(base_url, params=params, headers=headers, timeout=10)

            if response.status_code != 200:
                print(f"{city_name} Ошибка API, {response.status_code}: {response.text}.")
                continue

            data = response.json()
            rows = data.get("data", [])

            if not isinstance(rows, list):
                print(f"{city_name} Структура ответа неожиданная, объект не является списком.")
                continue

            if len(rows) == 0:
                print(f"{city_name} за {target_date} нет данных, возможен временной лаг публикации или неверная дата.")
                continue

            if len(rows) > 1:
                print(f"{city_name} Структура ответа неожиданная. Получено {len(rows)} записей (ожидание: 1 запись).")
                continue

            # Одна запись - нормальный случай.
            row = rows[0]

            # Проверяем доступность базы.
            with conn.cursor() as cur_check:
                cur_check.execute("SELECT 1;")
                if cur_check.fetchone() is None:
                    raise RuntimeError("База недоступна. SELECT 1 is None.")

            # Записываем в базу город отдельно.
            upsert_by_delete(conn, city_name, lat, lon, row)
    # Закрываем соединение после записи всех городов.
    conn.close()
    print("Extract завершен.")

# --- manual run ---
if __name__ == "__main__":
    extract_weather_raw_daily("2025-11-01")