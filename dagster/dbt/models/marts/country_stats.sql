SELECT
    origin_country,
    COUNT(*)                                        AS total_observations,
    COUNT(DISTINCT icao24)                         AS unique_aircraft,
    AVG(baro_altitude)                             AS avg_altitude,
    AVG(velocity)                                  AS avg_velocity,
    MIN(fetch_datetime)                            AS first_seen,
    MAX(fetch_datetime)                            AS last_seen,
    SUM(CASE WHEN on_ground     THEN 1 ELSE 0 END) AS ground_count,
    SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) AS airborne_count
FROM {{ ref('stg_flights') }}
WHERE origin_country IS NOT NULL
GROUP BY origin_country
ORDER BY total_observations DESC
