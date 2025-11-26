## Architecture

1. Data source: OpenWeatherMap (Current Weather + 5 day / 3 hour forecast). 

2. Ingestion layer (Python): small Python microservice (requests) called by Airflow tasks.

3. Storage: PostgreSQL (single database). Tables: cities, current_weather, forecast_weather, plus audit / metadata / dwh_views.

4. Orchestration: Airflow DAGs schedule pulls (e.g., current weather every 5–15 mins; forecast every 1–3 hours).

5. Transformations: SQL transforms in Postgres or Airflow-PostgresOperator: clean, enrich, aggregate, materialized views.

6. Monitoring & QA: Airflow task-level alerts, DQ checks (nulls, ranges, schema drift), Prometheus/Statsd optional.

7. Visualization: Looker Studio (connect to PostgreSQL via its PostgreSQL connector). Whitelist Looker Studio IPs or use secure tunnel.


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

```
CREATE TABLE IF NOT EXISTS forecast_weather (
  city_id        INT REFERENCES cities(city_id),
  dt_txt         TIMESTAMPTZ NOT NULL,  -- forecast timestamp
  dt             TIMESTAMPTZ NOT NULL,
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
  wind_speed     DOUBLE PRECISION,
  wind_deg       INT,
  clouds_all     INT,
  pop            DOUBLE PRECISION,  -- probability of precipitation
);

```

## Ingestion design & scheduling

+ Current weather: schedule every 5 minutes to 1 day (depends on API limits and business needs).

+ Forecast: schedule every 1 hour to 1 day (forecast doesn't change as frequently).

+ Backfills: one-off DAG to backfill historical data if needed.

+ Rate-limit planning: OpenWeatherMap free/paid plans have call limits and One Call API options — design schedule to stay under your quota; batch multiple cities per minute to avoid 429s.

## Airflow DAG design (sketch)

Two DAGs or one DAG with branching:

+ weather_current_dag: runs every 5–15m or 1 day; tasks:

start --> fetch_current_task (PythonOperator calling load_current for a batch of cities) --> quality_checks_task (PythonOperator/SQLSensor) --> audit_task (log counts, store in etl_audit) --> notify_on_failure (email/Slack alert)

+ weather_forecast_dag: runs hourly or daily; tasks:

fetch_forecast_task (fetch & upsert forecasts) --> aggregate_materialized_views (optional) --> audit and notify

## Looker Studio connection & best practices

+ Connector: use Looker Studio's built-in PostgreSQL connector (JDBC). In Looker Studio: Create → Data source → PostgreSQL. 
Google Cloud Documentation

+ Network: Looker Studio needs to reach your DB; if DB is in a VPC/Cloud SQL, whitelist Looker Studio IP ranges or use Cloud SQL integration. Looker Studio may require allowing inbound traffic from Google IPs — see your DB provider docs. 
Google Developer forums

+ Security: prefer a read-only DB user and restrict to required tables/views; use SSL.

+ Performance:

    + Expose materialized views or aggregated views (e.g., mv_latest_current, mv_forecast_24h_avg) rather than huge raw tables.

    + Limit the number of rows Looker Studio queries (set query limits or use pre-aggregated tables).

+ Connection via Neon (if using Neon): Neon docs show steps to connect to Looker Studio (get connection string, create data source)

## Scaling & cost considerations

1. API cost: monitor usage; OpenWeather pricing varies by plan and endpoint (One Call vs single endpoints). Batch requests and consider One Call to reduce total calls. 
OpenWeatherMap
+1

2. DB growth: forecast rows per city: ~40 forecast rows per city per 5 days (3-hour steps). Partition forecast_weather by month or year for large fleets.

3. Airflow workers: scale worker pool for parallel city ingestion; use rate limiting to avoid API 429s.
