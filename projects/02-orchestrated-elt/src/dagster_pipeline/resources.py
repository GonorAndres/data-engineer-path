"""Dagster resources for the insurance claims ELT pipeline.

Provides a ``DuckDBResource`` that gives assets a managed DuckDB connection.
The resource is configurable: pass a ``db_path`` for a persistent database
or omit it (default ``":memory:"``) for ephemeral runs and testing.
"""

from contextlib import contextmanager
from typing import Generator

import duckdb
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    """Dagster-managed DuckDB connection.

    Attributes:
        db_path: Path to a DuckDB database file, or ``":memory:"`` for
            an in-memory database (the default).
    """

    db_path: str = ":memory:"

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Yield a DuckDB connection, closing it when the context exits.

        Usage inside an asset::

            @asset
            def my_asset(duckdb_resource: DuckDBResource):
                with duckdb_resource.get_connection() as con:
                    con.execute("SELECT 1")
        """
        con = duckdb.connect(self.db_path)
        try:
            yield con
        finally:
            con.close()

    def get_runner(self) -> duckdb.DuckDBPyConnection:
        """Return a raw connection (caller is responsible for closing).

        Useful when the connection must outlive a single context block,
        for example when the ``PipelineRunner`` manages its own lifecycle.
        """
        return duckdb.connect(self.db_path)
