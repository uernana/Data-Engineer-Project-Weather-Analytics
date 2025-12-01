-- Cities table
CREATE TABLE IF NOT EXISTS public.cities (
    city_id BIGINT PRIMARY KEY,
    city_name TEXT NOT NULL,
    country TEXT,
    coord_lat DOUBLE PRECISION,
    coord_lon DOUBLE PRECISION,
    timezone INTEGER
);

-- Current weather table
CREATE TABLE IF NOT EXISTS public.current_weather (
    city_id BIGINT REFERENCES public.cities(city_id) ON DELETE CASCADE,
    dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    weather_id INTEGER,
    weather_main TEXT,
    description TEXT,
    base TEXT,
    temp DOUBLE PRECISION,
    feels_like DOUBLE PRECISION,
    temp_min DOUBLE PRECISION,
    temp_max DOUBLE PRECISION,
    pressure INTEGER,
    humidity INTEGER,
    visibility INTEGER,
    wind_speed DOUBLE PRECISION,
    wind_deg INTEGER,
    wind_gust DOUBLE PRECISION,
    clouds_all INTEGER,
    sunrise TIMESTAMP WITHOUT TIME ZONE,
    sunset TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (city_id, dt)
);
