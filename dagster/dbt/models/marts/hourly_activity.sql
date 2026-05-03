SELECT
    fetch_hour,
    COUNT(*)                                        AS total_flights,
    COUNT(DISTINCT icao24)                         AS unique_aircraft,
    AVG(baro_altitude)                             AS avg_altitude,
    AVG(velocity)                                  AS avg_velocity,
    SUM(CASE WHEN on_ground     THEN 1 ELSE 0 END) AS ground_count,
    SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) AS airborne_count
FROM {{ ref('stg_flights') }}
GROUP BY fetch_hour
ORDER BY fetch_hour
