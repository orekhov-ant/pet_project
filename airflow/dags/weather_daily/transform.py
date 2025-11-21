from typing import Any, Dict
# свои пакеты.
from psycopg2.extensions import connection as pg_connection
from weather_daily.utils import load_project_env_if_locally, get_pg_connection


def transform_weather_daily(target_date: str):
    # Подгружаем .env.local при локальном запуске.
    load_project_env_if_locally()

    conn: pg_connection = get_pg_connection()

    try:
        with conn:
            with conn.cursor() as cur_del:
                cur_del.execute(
                    """
                    DELETE FROM staged.weather_per_day
                    WHERE weather_date = %s
                    """,
                    (target_date,),
                )
                print(f"[transform] Удалили staged.weather_per_day за {target_date}.")

            with conn.cursor(name="raw_weather_cursor") as cur_src, conn.cursor() as cur_ins:
                cur_src.execute(
                    """
                    SELECT
                        api_dt::date AS weather_date,
                        city_name,
                        ingestion_time,
                        raw_payload
                    FROM raw.weather_per_day
                    WHERE api_dt::date = %s
                    """,
                    (target_date,),
                )

                batch_size = 100
                total_inserted = 0

                while True:
                    rows = cur_src.fetchmany(batch_size)
                    if not rows:
                        break

                    for weather_date, city, ingestion_time, raw_payload in rows:
                        record: Dict[str, Any] = raw_payload

                        tavg = record.get("tavg")
                        tmin = record.get("tmin")
                        tmax = record.get("tmax")
                        prcp = record.get("prcp")
                        snow = record.get("snow")
                        wdir = record.get("wdir")
                        wspd = record.get("wspd")
                        wpgt = record.get("wpgt")
                        pres = record.get("pres")
                        tsun = record.get("tsun")

                        try:
                            cur_ins.execute(
                                """
                                INSERT INTO staged.weather_per_day (
                                    weather_date,
                                    city,
                                    ingestion_time,
                                    -- transform_time проставится в DEFAULT now()
                                    tavg,
                                    tmin,
                                    tmax,
                                    prcp,
                                    snow,
                                    wdir,
                                    wspd,
                                    wpgt,
                                    pres,
                                    tsun
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    weather_date,
                                    city,
                                    ingestion_time,
                                    tavg,
                                    tmin,
                                    tmax,
                                    prcp,
                                    snow,
                                    wdir,
                                    wspd,
                                    wpgt,
                                    pres,
                                    tsun,
                                ),
                            )
                            total_inserted += 1

                        except Exception as e:
                            raise RuntimeError(
                                f"[transform] Ошибка вставки за дату: {target_date}, город: {city}, исключение: {e}"
                                f"raw_payload={record}"
                            ) from e

                print(f"[transform] За {target_date} в staged.weather_per_day вставлено строк: {total_inserted}")

    finally:
        conn.close()
        print("[transform] Соединение с postgres закрыто.")

# === local run ===
if __name__ == "__main__":
    transform_weather_daily("2025-11-01")




