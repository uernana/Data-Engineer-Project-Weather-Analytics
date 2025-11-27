# dags/weather_current_dag.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import time
import logging
from datetime import datetime, timedelta
import requests
import psycopg2

# ---------------------------------------------
# CONFIG
# ---------------------------------------------
API_KEY = "57c2842bfac979c9a3e8b4297d85185d"  

CITIES = [
    "Hanoi", "Sapa", "Ha Long", "Hai Duong",
    "Hue", "Vinh", "Da Nang", "Quy Nhon", "Nha Trang", "Da Lat",
    "Ho Chi Minh", "Binh Duong", "Can Tho", "Vinh Long"
]

def get_db_conn():
    """Connect to Neon PostgreSQL"""
    return psycopg2.connect(
        host="ep-odd-frog-a1x7walb.ap-southeast-1.aws.neon.tech",
        dbname="neondb",
        user="neondb_owner",
        password="npg_uPxqG8dr5OeH",
        sslmode="require",
        options="-c search_path=public"
    )

# ---------------------------------------------
# FETCH WEATHER FROM API
# ---------------------------------------------
def fetch_weather_dict(city: str):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": API_KEY, "units": "metric", "lang": "vi"}

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()

        if int(data.get("cod", 0)) != 200:
            logging.warning("OpenWeatherMap error for %s: %s", city, data.get("message"))
            return None

        return {
            "coord_lon": data["coord"]["lon"],
            "coord_lat": data["coord"]["lat"],
            "weather_id": data["weather"][0]["id"],
            "weather_main": data["weather"][0]["main"],
            "description": data["weather"][0]["description"],
            "weather_icon": data["weather"][0]["icon"],
            "base": data.get("base"),
            "temp": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "temp_min": data["main"]["temp_min"],
            "temp_max": data["main"]["temp_max"],
            "pressure": data["main"]["pressure"],
            "humidity": data["main"]["humidity"],
            "visibility": data.get("visibility"),
            "wind_speed": data["wind"].get("speed"),
            "wind_deg": data["wind"].get("deg"),
            "wind_gust": data["wind"].get("gust"),
            "clouds_all": data["clouds"].get("all"),
            "dt": data["dt"],
            "country": data["sys"]["country"],
            "sunrise": data["sys"]["sunrise"],
            "sunset": data["sys"]["sunset"],
            "timezone": data["timezone"],
            "city_id": data["id"],
            "city_name": data["name"],
        }

    except Exception as e:
        logging.exception("Failed fetch %s: %s", city, e)
        return None

# ---------------------------------------------
# UPSERT CITY
# ---------------------------------------------
def upsert_city(cur, ev):
    sql = """
    INSERT INTO public.cities (
        city_id, city_name, country, coord_lat, coord_lon, timezone
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (city_id) DO UPDATE SET
        city_name = EXCLUDED.city_name,
        country = EXCLUDED.country,
        coord_lat = EXCLUDED.coord_lat,
        coord_lon = EXCLUDED.coord_lon,
        timezone = EXCLUDED.timezone;
    """

    cur.execute(sql, (
        ev["city_id"], ev["city_name"], ev["country"],
        ev["coord_lat"], ev["coord_lon"], ev["timezone"]
    ))

# ---------------------------------------------
# UPSERT CURRENT WEATHER
# ---------------------------------------------
def upsert_current_weather(cur, ev):
    sql = """
    INSERT INTO public.current_weather (
        city_id, dt, weather_id, weather_main, description, base,
        temp, feels_like, temp_min, temp_max, pressure, humidity,
        visibility, wind_speed, wind_deg, wind_gust, clouds_all,
        sunrise, sunset
    )
    VALUES (
        %s, to_timestamp(%s), %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        to_timestamp(%s), to_timestamp(%s)
    )
    ON CONFLICT (city_id,dt) DO UPDATE SET
        weather_id   = EXCLUDED.weather_id,
        weather_main = EXCLUDED.weather_main,
        description  = EXCLUDED.description,
        base         = EXCLUDED.base,
        temp         = EXCLUDED.temp,
        feels_like   = EXCLUDED.feels_like,
        temp_min     = EXCLUDED.temp_min,
        temp_max     = EXCLUDED.temp_max,
        pressure     = EXCLUDED.pressure,
        humidity     = EXCLUDED.humidity,
        visibility   = EXCLUDED.visibility,
        wind_speed   = EXCLUDED.wind_speed,
        wind_deg     = EXCLUDED.wind_deg,
        wind_gust    = EXCLUDED.wind_gust,
        clouds_all   = EXCLUDED.clouds_all,
        sunrise      = EXCLUDED.sunrise,
        sunset       = EXCLUDED.sunset;
    """

    cur.execute(sql, (
        ev["city_id"], ev["dt"], ev["weather_id"], ev["weather_main"], ev["description"],
        ev["base"], ev["temp"], ev["feels_like"], ev["temp_min"], ev["temp_max"],
        ev["pressure"], ev["humidity"], ev["visibility"], ev["wind_speed"],
        ev["wind_deg"], ev["wind_gust"], ev["clouds_all"], ev["sunrise"], ev["sunset"]
    ))

# ---------------------------------------------
# MAIN TASK
# ---------------------------------------------
def fetch_and_load_current(**ctx):
    conn = get_db_conn()
    cur = conn.cursor()

    inserted, errors = 0, 0

    try:
        for city in CITIES:
            ev = fetch_weather_dict(city)
            if ev is None:
                errors += 1
                continue

            try:
                upsert_city(cur, ev)
                upsert_current_weather(cur, ev)
                inserted += 1
            except Exception:
                logging.exception("DB failed for city: %s", city)
                errors += 1

            time.sleep(0.4)  # avoid API rate limit

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    logging.info("FINISHED inserted=%d errors=%d", inserted, errors)
    return {"inserted": inserted, "errors": errors}

# ---------------------------------------------
# DAG DEFINITION
# ---------------------------------------------
default_args = {
    "owner": "weather_etl",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_current_dag_new",
    default_args=default_args,
    schedule="0 */2 * * *",
    start_date=datetime(2025,11,27),
    catchup=False,
    max_active_runs=1,
    tags=["weather"],
) as dag:

    task_fetch_and_load = PythonOperator(
        task_id="fetch_and_load_current",
        python_callable=fetch_and_load_current,
    )
