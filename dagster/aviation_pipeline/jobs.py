from dagster import AssetSelection, define_asset_job

daily_analytics_job = define_asset_job(
    name="daily_analytics",
    selection=AssetSelection.all(),
    description="Process all flight data from Iceberg through to DuckDB analytics tables",
)
