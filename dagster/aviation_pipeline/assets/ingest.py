from typing import Any, Dict

from dagster import AssetExecutionContext, asset

from ..resources.duckdb import DuckDBResource
from ..resources.iceberg import IcebergResource
from pydantic import BaseModel


class RawFlightDataStats(BaseModel):
    total_records: int
    unique_aircraft: int
    countries: int
    date_range_start: int | None
    date_range_end: int | None


@asset(
    description="Raw flight data read from Iceberg and staged into DuckDB",
    compute_kind="iceberg",
    group_name="raw_data",
)
def raw_flight_data(
    context: AssetExecutionContext,
    iceberg: IcebergResource,
    duckdb: DuckDBResource,
) -> RawFlightDataStats:
    context.log.info("Reading raw flight data from Iceberg")

    table = iceberg.get_table("flight_data")
    df = table.scan().to_pandas()

    context.log.info(f"Loaded {len(df):,} records from Iceberg")

    stats = RawFlightDataStats(
        total_records=len(df),
        unique_aircraft=int(df["icao24"].nunique()),
        countries=int(df["origin_country"].nunique()),
        date_range_start=int(df["fetch_timestamp"].min()) if len(df) > 0 else None,
        date_range_end=int(df["fetch_timestamp"].max()) if len(df) > 0 else None,
    )

    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS raw_flight_data")
        conn.execute("CREATE TABLE raw_flight_data AS SELECT * FROM df")

    context.log.info(f"Staged {stats.total_records:,} records to DuckDB. Stats: {stats}")
    return stats
