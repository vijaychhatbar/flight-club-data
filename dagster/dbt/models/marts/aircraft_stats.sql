SELECT
    icao24,
    callsign,
    origin_country,
    COUNT(*)                                        AS total_observations,
    AVG(baro_altitude)                             AS avg_altitude,
    MAX(baro_altitude)                             AS max_altitude,
    AVG(velocity)                                  AS avg_velocity,
    MAX(velocity)                                  AS max_velocity,
    MIN(fetch_datetime)                            AS first_seen,
    MAX(fetch_datetime)                            AS last_seen,
    SUM(CASE WHEN on_ground     THEN 1 ELSE 0 END) AS ground_observations,
    SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) AS airborne_observations
FROM {{ ref('stg_flights') }}
GROUP BY icao24, callsign, origin_country
ORDER BY total_observations DESC
