from dagster import ConfigurableResource
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table


class IcebergResource(ConfigurableResource):
    catalog_path: str
    namespace: str

    def get_table(self, table_name: str = "flight_data") -> Table:
        catalog = load_catalog(
            "local",
            type="sql",
            uri=f"sqlite:///{self.catalog_path}/catalog.db",
            warehouse=self.catalog_path,
        )
        return catalog.load_table(f"{self.namespace}.{table_name}")
