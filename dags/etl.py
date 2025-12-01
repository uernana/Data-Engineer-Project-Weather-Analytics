import os
import time
import logging
import requests
import psycopg2
from datetime import datetime

API_KEY = os.getenv("API_KEY")

CITIES = [
    "Hanoi", "Sapa", "Ha Long", "Hai Duong",
    "Hue", "Vinh", "Da Nang", "Quy Nhon", "Nha Trang", "Da Lat",
    "Ho Chi Minh", "Binh Duong", "Can Tho", "Vinh Long"
]

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("ep-lucky-hill-a1fqzkvf.ap-southeast-1.aws.neon.tech"),
        dbname=os.getenv("neon_db"),
        user=os.getenv("neondb_owner"),
        password=os.getenv("npg_uPxqG8dr5OeH"),
        sslmode="require",
        options="-c search_path=public"
    )

def fetch_weather_dict(city: str):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": API_KEY, "units": "metric", "lang": "vi"}

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if int(data.get("cod", 0)) != 200:
            return None

        return {
            "coord_lon": data["coord"]["lon"],
            "coord_lat": data["coord"]["lat"],
            "weather_id": data["weather"][0]["id"],
            "weather_main": data["weather"][0]["main"],
            "description": data["weather"][0]["description"],
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
    except:
        return None

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

def main():
    conn = get_db_conn()
    cur = conn.cursor()

    for city in CITIES:
        ev = fetch_weather_dict(city)
        if ev:
            upsert_city(cur, ev)
            upsert_current_weather(cur, ev)
            print(f"Inserted: {city}")
        else:
            print(f"Failed: {city}")

        time.sleep(0.4)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
