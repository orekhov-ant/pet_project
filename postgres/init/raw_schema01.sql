CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staged;

CREATE TABLE raw.weather_per_day (
    id BIGSERIAL PRIMARY KEY,
    api_dt TIMESTAMPTZ NOT NULL,
    city_name TEXT NOT NULL DEFAULT 'unknown',
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload JSONB NOT NULL,
    UNIQUE (api_dt, city_name)
);

CREATE TABLE IF NOT EXISTS staged.weather_per_day (
    id BIGSERIAL PRIMARY KEY,
    weather_date DATE NOT NULL,
    city TEXT NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL,
    transform_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    tavg DOUBLE PRECISION,
    tmin DOUBLE PRECISION,
    tmax DOUBLE PRECISION,
    prcp DOUBLE PRECISION,
    snow SMALLINT,
    wdir SMALLINT,
    wspd DOUBLE PRECISION,
    wpgt DOUBLE PRECISION,
    pres DOUBLE PRECISION,
    tsun SMALLINT,
    UNIQUE (weather_date, city)
);

