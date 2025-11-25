#!/usr/bin/env python3
"""
Aviation Data Pipeline with Dagster
Transforms Iceberg raw data into analytics-ready DuckDB tables.

Ideally tasks should not be running directly on dagster workers, but rather in dedicated compute environments. Example: Kubernetes pods, Docker containers, or cloud functions.
But for my homelab setup this is acceptable.

For simplicity I am using local filesystem paths for Iceberg and DuckDB storage.
"""

import os
from typing import Dict, Any
import duckdb
from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)
from pyiceberg.catalog import load_catalog
import logging

logger = logging.getLogger(__name__)

# Configuration
ICEBERG_CATALOG_PATH = os.getenv("ICEBERG_CATALOG_PATH", "/data/iceberg-warehouse")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "aviation")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/analytics/aviation.duckdb")


def get_iceberg_table():
    """Get Iceberg table connection"""
    catalog_config = {
        "type": "sql",
        "uri": f"sqlite:///{ICEBERG_CATALOG_PATH}/catalog.db",
        "warehouse": ICEBERG_CATALOG_PATH,
    }
    catalog = load_catalog("local", **catalog_config)
    return catalog.load_table(f"{ICEBERG_NAMESPACE}.flight_data")


def get_duckdb_conn():
    """Get DuckDB connection"""
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    # Ensure the database file and directory have proper permissions for Metabase (user 2000)
    # Metabase needs write access to the directory to create WAL files when connecting
    # I had lot of issues with permission denied errors otherwise. This could be better handled with proper user/group ownership
    try:
        analytics_dir = os.path.dirname(DUCKDB_PATH)
        os.chmod(analytics_dir, 0o777)  # rwxrwxrwx - allow WAL file creation
        os.chmod(DUCKDB_PATH, 0o666)  # rw-rw-rw-
        logger.info(f"Set permissions on {DUCKDB_PATH} and {analytics_dir}")
    except Exception as e:
        logger.warning(f"Could not set permissions: {e}")
    return conn


# Assets for dagster
@asset(description="Raw flight data from Iceberg", compute_kind="iceberg", group_name="raw_data")
def raw_flight_data(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Read raw flight data from Iceberg table
    """
    context.log.info("Reading raw flight data from Iceberg")

    table = get_iceberg_table()
    df = table.scan().to_pandas()

    context.log.info(f"Loaded {len(df):,} records from Iceberg")
    context.log.info(f"Date range: {df['fetch_timestamp'].min()} to {df['fetch_timestamp'].max()}")

    # Store summary stats
    stats = {
        "total_records": len(df),
        "unique_aircraft": df["icao24"].nunique(),
        "countries": df["origin_country"].nunique(),
        "date_range_start": int(df["fetch_timestamp"].min()) if len(df) > 0 else None,
        "date_range_end": int(df["fetch_timestamp"].max()) if len(df) > 0 else None,
    }

    context.log.info(f"Stats: {stats}")

    # Write to DuckDB staging table
    conn = get_duckdb_conn()
    conn.execute("DROP TABLE IF EXISTS raw_flight_data")
    conn.execute("CREATE TABLE raw_flight_data AS SELECT * FROM df")
    conn.close()

    return stats


@asset(
    description="Cleaned and enriched flight data",
    compute_kind="duckdb",
    group_name="transformed",
    deps=[raw_flight_data],
)
def cleaned_flights(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Clean and enrich flight data:
    - Remove duplicates
    - Filter invalid positions
    - Add derived fields
    - Handle nulls
    """
    context.log.info("Cleaning flight data")

    conn = get_duckdb_conn()

    # Create cleaned table with enrichments
    query = """
    CREATE OR REPLACE TABLE cleaned_flights AS
    SELECT DISTINCT
        icao24,
        TRIM(callsign) as callsign,
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
        -- Derived fields
        CASE 
            WHEN on_ground THEN 'Ground'
            WHEN baro_altitude < 10000 THEN 'Low'
            WHEN baro_altitude < 30000 THEN 'Medium'
            ELSE 'High'
        END as altitude_category,
        CASE
            WHEN velocity IS NULL THEN NULL
            WHEN velocity < 100 THEN 'Slow'
            WHEN velocity < 400 THEN 'Medium'
            ELSE 'Fast'
        END as speed_category,
        -- Convert unix timestamp to datetime
        to_timestamp(fetch_timestamp) as fetch_datetime,
        DATE_TRUNC('hour', to_timestamp(fetch_timestamp)) as fetch_hour,
        DATE_TRUNC('day', to_timestamp(fetch_timestamp)) as fetch_date
    FROM raw_flight_data
    WHERE 
        latitude IS NOT NULL 
        AND longitude IS NOT NULL
        AND latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
    """

    conn.execute(query)

    # Get stats
    stats = conn.execute(
        """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT icao24) as unique_aircraft,
            COUNT(DISTINCT origin_country) as countries,
            MIN(fetch_datetime) as earliest,
            MAX(fetch_datetime) as latest
        FROM cleaned_flights
    """
    ).fetchone()

    result = {
        "total_records": stats[0],
        "unique_aircraft": stats[1],
        "countries": stats[2],
        "earliest": str(stats[3]),
        "latest": str(stats[4]),
    }

    context.log.info(f"Cleaned data stats: {result}")

    conn.close()
    return result


@asset(
    description="Aggregated flight statistics by country",
    compute_kind="duckdb",
    group_name="analytics",
    deps=[cleaned_flights],
)
def country_stats(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Aggregate flight statistics by country
    """
    context.log.info("Computing country statistics")

    conn = get_duckdb_conn()

    query = """
    CREATE OR REPLACE TABLE country_stats AS
    SELECT 
        origin_country,
        COUNT(*) as total_observations,
        COUNT(DISTINCT icao24) as unique_aircraft,
        AVG(baro_altitude) as avg_altitude,
        AVG(velocity) as avg_velocity,
        MIN(fetch_datetime) as first_seen,
        MAX(fetch_datetime) as last_seen,
        SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) as ground_count,
        SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) as airborne_count
    FROM cleaned_flights
    WHERE origin_country IS NOT NULL
    GROUP BY origin_country
    ORDER BY total_observations DESC
    """

    conn.execute(query)

    # Get top countries
    top_countries = conn.execute(
        """
        SELECT origin_country, total_observations, unique_aircraft
        FROM country_stats
        ORDER BY total_observations DESC
        LIMIT 10
    """
    ).fetchall()

    result = {
        "top_countries": [
            {"country": row[0], "observations": row[1], "aircraft": row[2]} for row in top_countries
        ]
    }

    context.log.info(f"Top 10 countries by observations: {result['top_countries']}")

    conn.close()
    return result


@asset(
    description="Time-series flight activity data",
    compute_kind="duckdb",
    group_name="analytics",
    deps=[cleaned_flights],
)
def hourly_activity(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Aggregate flight activity by hour
    """
    context.log.info("Computing hourly activity")

    conn = get_duckdb_conn()

    query = """
    CREATE OR REPLACE TABLE hourly_activity AS
    SELECT 
        fetch_hour,
        COUNT(*) as total_flights,
        COUNT(DISTINCT icao24) as unique_aircraft,
        AVG(baro_altitude) as avg_altitude,
        AVG(velocity) as avg_velocity,
        SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) as ground_count,
        SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) as airborne_count
    FROM cleaned_flights
    GROUP BY fetch_hour
    ORDER BY fetch_hour
    """

    conn.execute(query)

    # Get summary
    summary = conn.execute(
        """
        SELECT 
            COUNT(*) as total_hours,
            AVG(total_flights) as avg_flights_per_hour,
            MAX(total_flights) as max_flights_hour
        FROM hourly_activity
    """
    ).fetchone()

    result = {
        "total_hours": summary[0],
        "avg_flights_per_hour": round(summary[1], 2),
        "max_flights_hour": summary[2],
    }

    context.log.info(f"Hourly activity stats: {result}")

    conn.close()
    return result


@asset(
    description="Aircraft-specific statistics",
    compute_kind="duckdb",
    group_name="analytics",
    deps=[cleaned_flights],
)
def aircraft_stats(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Aggregate statistics per aircraft
    """
    context.log.info("Computing aircraft statistics")

    conn = get_duckdb_conn()

    query = """
    CREATE OR REPLACE TABLE aircraft_stats AS
    SELECT 
        icao24,
        callsign,
        origin_country,
        COUNT(*) as total_observations,
        AVG(baro_altitude) as avg_altitude,
        MAX(baro_altitude) as max_altitude,
        AVG(velocity) as avg_velocity,
        MAX(velocity) as max_velocity,
        MIN(fetch_datetime) as first_seen,
        MAX(fetch_datetime) as last_seen,
        SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) as ground_observations,
        SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) as airborne_observations
    FROM cleaned_flights
    GROUP BY icao24, callsign, origin_country
    ORDER BY total_observations DESC
    """

    conn.execute(query)

    # Get most active aircraft
    top_aircraft = conn.execute(
        """
        SELECT icao24, callsign, total_observations
        FROM aircraft_stats
        ORDER BY total_observations DESC
        LIMIT 10
    """
    ).fetchall()

    result = {
        "top_aircraft": [
            {"icao24": row[0], "callsign": row[1], "observations": row[2]} for row in top_aircraft
        ]
    }

    context.log.info(f"Top 10 most tracked aircraft: {result['top_aircraft']}")

    conn.close()
    return result


@asset(
    description="Geospatial flight density heatmap data",
    compute_kind="duckdb",
    group_name="analytics",
    deps=[cleaned_flights],
)
def flight_density_grid(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Create a grid-based flight density map (1-degree resolution)
    """
    context.log.info("Computing flight density grid")

    conn = get_duckdb_conn()

    query = """
    CREATE OR REPLACE TABLE flight_density_grid AS
    SELECT 
        FLOOR(latitude) as lat_grid,
        FLOOR(longitude) as lon_grid,
        COUNT(*) as flight_count,
        COUNT(DISTINCT icao24) as unique_aircraft,
        AVG(baro_altitude) as avg_altitude
    FROM cleaned_flights
    WHERE NOT on_ground
    GROUP BY lat_grid, lon_grid
    HAVING flight_count > 5  -- Filter out sparse areas
    ORDER BY flight_count DESC
    """

    conn.execute(query)

    # Get hotspot summary
    hotspots = conn.execute(
        """
        SELECT lat_grid, lon_grid, flight_count
        FROM flight_density_grid
        ORDER BY flight_count DESC
        LIMIT 10
    """
    ).fetchall()

    result = {
        "top_hotspots": [{"lat": row[0], "lon": row[1], "flights": row[2]} for row in hotspots]
    }

    context.log.info(f"Top 10 flight density hotspots: {result['top_hotspots']}")

    conn.close()
    return result


# Define jobs
daily_analytics_job = define_asset_job(
    name="daily_analytics",
    selection=AssetSelection.all(),
    description="Daily analytics pipeline - process all flight data",
)

# Define schedules
daily_schedule = ScheduleDefinition(
    job=daily_analytics_job,
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
    description="Run daily analytics at 2 AM",
)

# Definitions
defs = Definitions(
    assets=[
        raw_flight_data,
        cleaned_flights,
        country_stats,
        hourly_activity,
        aircraft_stats,
        flight_density_grid,
    ],
    jobs=[daily_analytics_job],
    schedules=[daily_schedule],
)
