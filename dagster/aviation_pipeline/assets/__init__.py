from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from .ingest import raw_flight_data

_DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"


class _SourceTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if (
            dbt_resource_props["resource_type"] == "source"
            and dbt_resource_props["name"] == "raw_flight_data"
        ):
            return AssetKey("raw_flight_data")
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=_DBT_PROJECT_DIR / "target" / "manifest.json",
    dagster_dbt_translator=_SourceTranslator(),
)
def aviation_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()


__all__ = ["raw_flight_data", "aviation_dbt_assets"]
