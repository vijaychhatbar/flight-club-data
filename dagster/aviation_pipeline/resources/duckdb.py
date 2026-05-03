import os
from contextlib import contextmanager
from typing import Iterator

import duckdb
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    database_path: str

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
        conn = duckdb.connect(self.database_path)
        try:
            yield conn
        finally:
            conn.close()
            # Metabase (uid 2000) needs write access to the directory to create WAL files
            try:
                analytics_dir = os.path.dirname(self.database_path)
                os.chmod(analytics_dir, 0o777)
                if os.path.exists(self.database_path):
                    os.chmod(self.database_path, 0o666)
            except Exception:
                pass
