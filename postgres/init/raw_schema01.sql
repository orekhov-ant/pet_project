CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.weather_per_day (
    id BIGSERIAL PRIMARY KEY,
    api_dt TIMESTAMPTZ NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload JSONB NOT NULL,
    UNIQUE (api_dt, lat, lon)
);

