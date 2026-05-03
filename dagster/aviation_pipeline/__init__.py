from pathlib import Path

from dagster import Definitions, EnvVar
from dagster_dbt import DbtCliResource

from .assets import aviation_dbt_assets, raw_flight_data
from .jobs import daily_analytics_job
from .resources import DuckDBResource, IcebergResource
from .schedules import daily_schedule

_DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

defs = Definitions(
    assets=[raw_flight_data, aviation_dbt_assets],
    resources={
        "iceberg": IcebergResource(
            catalog_path=EnvVar("ICEBERG_CATALOG_PATH"),
            namespace=EnvVar("ICEBERG_NAMESPACE"),
        ),
        "duckdb": DuckDBResource(
            database_path=EnvVar("DUCKDB_PATH"),
        ),
        "dbt": DbtCliResource(project_dir=str(_DBT_PROJECT_DIR)),
    },
    jobs=[daily_analytics_job],
    schedules=[daily_schedule],
)
