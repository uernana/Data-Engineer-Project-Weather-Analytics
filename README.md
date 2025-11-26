## Architecture

1. Data source: OpenWeatherMap (Current Weather + 5 day / 3 hour forecast). 

2. Ingestion layer (Python): small Python microservice (requests) called by Airflow tasks.

3. Storage: PostgreSQL (single database). Tables: cities, current_weather, forecast_weather, plus audit / metadata / dwh_views.

4. Orchestration: Airflow DAGs schedule pulls (e.g., current weather every 5–15 mins; forecast every 1–3 hours).

5. Transformations: SQL transforms in Postgres or Airflow-PostgresOperator: clean, enrich, aggregate, materialized views.

6. Monitoring & QA: Airflow task-level alerts, DQ checks (nulls, ranges, schema drift), Prometheus/Statsd optional.

7. Visualization: Looker Studio (connect to PostgreSQL via its PostgreSQL connector). Whitelist Looker Studio IPs or use secure tunnel.
