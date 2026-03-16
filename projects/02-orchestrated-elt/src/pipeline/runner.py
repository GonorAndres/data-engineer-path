"""Core pipeline runner for the insurance claims ELT pipeline.

This module is deliberately orchestrator-agnostic.  It provides a
``PipelineRunner`` class that Dagster assets, Airflow operators, and a
plain Cloud Run container can all call.  Every method returns structured
metadata (row counts, timing, errors) so the calling orchestrator can
surface it in its UI.

Typical usage::

    runner = PipelineRunner(db_path=None)  # in-memory DuckDB
    runner.load_raw_tables()
    meta = runner.execute_sql_layer("staging")
    runner.close()
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from pipeline.config import DATA_DIR, RAW_TABLES, SQL_DIR, SQL_LAYERS

# ---------------------------------------------------------------------------
# Result data classes
# ---------------------------------------------------------------------------


@dataclass
class LayerResult:
    """Outcome of executing one SQL layer."""

    layer_name: str
    tables: dict[str, int] = field(default_factory=dict)  # table -> row count
    elapsed_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Outcome of a full pipeline run."""

    layers: list[LayerResult] = field(default_factory=list)
    total_elapsed_seconds: float = 0.0
    success: bool = True
    error_message: str | None = None


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


class PipelineRunner:
    """Runs the claims warehouse ELT pipeline against DuckDB.

    Args:
        db_path: Path to a DuckDB file.  ``None`` for in-memory.
        sql_dir: Directory containing SQL layer subdirectories.
        data_dir: Directory containing the raw CSV files.
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        sql_dir: Path | None = None,
        data_dir: Path | None = None,
    ) -> None:
        self._sql_dir = sql_dir or SQL_DIR
        self._data_dir = data_dir or DATA_DIR

        if db_path is not None:
            db_path = Path(db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._con = duckdb.connect(str(db_path))
        else:
            self._con = duckdb.connect(":memory:")

    # -- public API ---------------------------------------------------------

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return the underlying DuckDB connection."""
        return self._con

    def load_raw_tables(
        self,
        data_dir: Path | None = None,
    ) -> dict[str, int]:
        """Load CSV files into ``raw_*`` DuckDB tables.

        Args:
            data_dir: Override for the default data directory.

        Returns:
            Dict mapping table name to row count.
        """
        data_dir = data_dir or self._data_dir
        row_counts: dict[str, int] = {}

        for csv_file, table_name in RAW_TABLES.items():
            filepath = data_dir / csv_file
            if not filepath.exists():
                raise FileNotFoundError(
                    f"Missing data file: {filepath}"
                )
            self._con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS "
                f"SELECT * FROM read_csv_auto('{filepath}')"
            )
            count = self._con.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            row_counts[table_name] = count

        return row_counts

    def execute_sql_layer(
        self,
        layer_name: str,
        sql_files: list[str] | None = None,
    ) -> LayerResult:
        """Execute all SQL files for a single pipeline layer.

        Args:
            layer_name: One of ``staging``, ``intermediate``, ``marts``,
                ``reports``.
            sql_files: Override list of filenames.  If ``None``, uses the
                canonical list from :mod:`pipeline.config`.

        Returns:
            A :class:`LayerResult` with per-table row counts and timing.
        """
        if sql_files is None:
            for name, files in SQL_LAYERS:
                if name == layer_name:
                    sql_files = files
                    break
            else:
                return LayerResult(
                    layer_name=layer_name,
                    errors=[f"Unknown layer: {layer_name}"],
                )

        result = LayerResult(layer_name=layer_name)
        start = time.monotonic()

        for sql_file in sql_files:
            filepath = self._sql_dir / layer_name / sql_file
            if not filepath.exists():
                result.errors.append(f"SQL file not found: {filepath}")
                continue
            try:
                sql_text = filepath.read_text(encoding="utf-8")
                self._con.execute(sql_text)
                table_name = sql_file.replace(".sql", "")
                count = self._con.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                result.tables[table_name] = count
            except Exception as exc:
                result.errors.append(f"{sql_file}: {exc}")

        result.elapsed_seconds = round(time.monotonic() - start, 3)
        return result

    def run_full_pipeline(
        self,
        data_dir: Path | None = None,
    ) -> PipelineResult:
        """Execute the entire ELT pipeline end-to-end.

        1. Load raw CSVs.
        2. Run staging, intermediate, marts, and reports layers.

        Args:
            data_dir: Override the default data directory.

        Returns:
            A :class:`PipelineResult` summarizing every layer.
        """
        pipeline_result = PipelineResult()
        overall_start = time.monotonic()

        try:
            self.load_raw_tables(data_dir=data_dir)

            for layer_name, sql_files in SQL_LAYERS:
                layer_result = self.execute_sql_layer(
                    layer_name, sql_files
                )
                pipeline_result.layers.append(layer_result)
                if layer_result.errors:
                    pipeline_result.success = False
                    pipeline_result.error_message = (
                        f"Errors in layer '{layer_name}': "
                        + "; ".join(layer_result.errors)
                    )
                    break

        except Exception as exc:
            pipeline_result.success = False
            pipeline_result.error_message = str(exc)

        pipeline_result.total_elapsed_seconds = round(
            time.monotonic() - overall_start, 3
        )
        return pipeline_result

    def get_table_row_count(self, table_name: str) -> int:
        """Return the row count for a single table.

        Args:
            table_name: DuckDB table name.

        Returns:
            Number of rows, or -1 if the table does not exist.
        """
        try:
            result = self._con.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()
            return result[0] if result else -1
        except duckdb.CatalogException:
            return -1

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._con.close()


# ---------------------------------------------------------------------------
# Convenience CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the full pipeline from the command line."""
    runner = PipelineRunner()
    result = runner.run_full_pipeline()

    if result.success:
        print("Pipeline completed successfully.")
        for layer in result.layers:
            print(f"\n  Layer: {layer.layer_name}  "
                  f"({layer.elapsed_seconds:.2f}s)")
            for table, count in layer.tables.items():
                print(f"    {table:<40s} {count:>6,d} rows")
    else:
        print(f"Pipeline FAILED: {result.error_message}", file=sys.stderr)
        sys.exit(1)

    print(f"\nTotal elapsed: {result.total_elapsed_seconds:.2f}s")
    runner.close()


if __name__ == "__main__":
    main()
