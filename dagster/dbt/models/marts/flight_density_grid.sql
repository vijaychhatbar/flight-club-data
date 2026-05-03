SELECT
    FLOOR(latitude)  AS lat_grid,
    FLOOR(longitude) AS lon_grid,
    COUNT(*)                AS flight_count,
    COUNT(DISTINCT icao24)  AS unique_aircraft,
    AVG(baro_altitude)      AS avg_altitude
FROM {{ ref('stg_flights') }}
WHERE NOT on_ground
GROUP BY lat_grid, lon_grid
HAVING flight_count > 5
ORDER BY flight_count DESC
