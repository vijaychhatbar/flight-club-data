SELECT DISTINCT
    icao24,
    TRIM(callsign)      AS callsign,
    origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,
    baro_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    geo_altitude,
    squawk,
    spi,
    position_source,
    fetch_timestamp,
    ingestion_time,
    CASE
        WHEN on_ground             THEN 'Ground'
        WHEN baro_altitude < 10000 THEN 'Low'
        WHEN baro_altitude < 30000 THEN 'Medium'
        ELSE 'High'
    END AS altitude_category,
    CASE
        WHEN velocity IS NULL THEN NULL
        WHEN velocity < 100   THEN 'Slow'
        WHEN velocity < 400   THEN 'Medium'
        ELSE 'Fast'
    END AS speed_category,
    to_timestamp(fetch_timestamp)                     AS fetch_datetime,
    DATE_TRUNC('hour', to_timestamp(fetch_timestamp)) AS fetch_hour,
    DATE_TRUNC('day',  to_timestamp(fetch_timestamp)) AS fetch_date
FROM {{ source('aviation', 'raw_flight_data') }}
WHERE
    latitude  IS NOT NULL
    AND longitude IS NOT NULL
    AND latitude  BETWEEN -90  AND 90
    AND longitude BETWEEN -180 AND 180
