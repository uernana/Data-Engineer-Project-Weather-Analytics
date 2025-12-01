# **Weather ETL Project Summary**

## Data source
Link data: https://openweathermap.org/api

Data modeling: Normalized schema; optimized for ingestion and fast dashboard queries.

```
CREATE TABLE IF NOT EXISTS cities (
  city_id        INT PRIMARY KEY,  -- OpenWeatherMap city id
  city_name      TEXT NOT NULL,
  country        TEXT,
  coord_lat      FLOAT,
  coord_lon      FLOAT,
  timezone       TEXT
);

```

```
CREATE TABLE IF NOT EXISTS current_weather (
  city_id        INT REFERENCES cities(city_id),
  dt             TIMESTAMPTZ NOT NULL,  -- API response time (dt)
  weather_id     INT,
  weather_main   TEXT,
  description    TEXT,
  temp           FLOAT,
  feels_like     FLOAT,
  temp_min       FLOAT,
  temp_max       FLOAT,
  pressure       INT,
  humidity       INT,
  visibility     INT,
  wind_speed     FLOAT,
  wind_deg       INT,
  clouds_all     INT,
  sunrise        TIMESTAMPTZ,
  sunset         TIMESTAMPTZ
);

```

## Ingestion design & scheduling

+ Language / tools: Python

+ Libraries: requests for API calls, psycopg2 for PostgreSQL connection

+ ETL workflow:

1. Fetch data from OpenWeatherMap API for a list of cities

2. Upsert city information into cities table

3. Upsert current weather into current_weather table

4. Log failures and inserted records

## Storage Layer

+ Database: PostgreSQL hosted on Neon

+ Tables:

1. cities – stores city metadata

2. current_weather – stores latest weather readings per city

+ Schema considerations:

  + cities.city_id primary key

  + current_weather primary key on (city_id, dt) for uniqueness

  + Upsert strategy ensures no duplicates

## Orchestration / Scheduling

+ Scheduler: GitHub Actions

+ Frequency: Hourly (cron: "0 * * * *")

+ Workflow:

1. Checkout repository

2. Setup Python environment

3. Install dependencies (requests, psycopg2-binary if needed)

4. Run ETL script

+ Cost: Free tier (limited minutes, shared runner)

+ Limitations:

  + Not built for heavy or complex data pipelines

  + No advanced monitoring, alerting, or orchestration features

  + Potential API/network issues from cloud runners

## BI / Visualization Layer

+ Tool: Looker Studio (Google Data Studio)

+ Connection: PostgreSQL in Neon → Looker Studio via connector

+ Capabilities:

  + Display current weather per city

  + Filter by date/time and city name

## Scaling & cost considerations

+ Entire pipeline uses free services (Neon free tier + GitHub Actions free minutes + Looker Studio)

+ Constraints:

  + Neon free tier limits connections and storage

  + GitHub Actions free minutes limit frequency and duration of ETL

  + Not suitable for high-frequency, long-term, or multi-source data pipelines

## Summary Insight:

This setup is excellent for learning, prototyping, and small-scale weather data collection and visualization, but for production-level workloads, you would need a more robust orchestration platform (like Airflow Cloud, Prefect, or Dagster) and a cloud database with higher throughput and IP access control.
