"""Tests for the core pipeline runner.

These tests exercise ``PipelineRunner`` end-to-end using an in-memory
DuckDB database and the SQL/CSV files from Project 1.  No GCP credentials
or external services are required.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure src/ is on the import path so ``pipeline.*`` resolves.
_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# Also add Project 1's src/ so the data generator can be imported if needed.
_PROJECT_01_SRC = (
    Path(__file__).resolve().parent.parent.parent
    / "01-claims-warehouse"
    / "src"
)
if str(_PROJECT_01_SRC) not in sys.path:
    sys.path.insert(0, str(_PROJECT_01_SRC))

from pipeline.config import DATA_DIR, SQL_DIR
from pipeline.runner import PipelineRunner


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner() -> PipelineRunner:
    """Create an in-memory PipelineRunner."""
    return PipelineRunner(db_path=None, sql_dir=SQL_DIR, data_dir=DATA_DIR)


@pytest.fixture(autouse=True)
def _ensure_sample_data() -> None:
    """Generate sample data if the CSVs do not exist yet."""
    expected = DATA_DIR / "claims.csv"
    if not expected.exists():
        from data_generator import ClaimsDataGenerator

        generator = ClaimsDataGenerator(seed=42)
        generator.generate_all(str(DATA_DIR))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLoadRawTables:
    """Tests for PipelineRunner.load_raw_tables."""

    def test_loads_all_raw_tables(self, runner: PipelineRunner) -> None:
        """All five raw tables should be created with positive row counts."""
        counts = runner.load_raw_tables()
        assert len(counts) == 5
        for table_name, count in counts.items():
            assert table_name.startswith("raw_")
            assert count > 0
        runner.close()

    def test_missing_data_dir_raises(self) -> None:
        """A missing data directory should raise FileNotFoundError."""
        bad_runner = PipelineRunner(
            db_path=None,
            data_dir=Path("/nonexistent/path"),
        )
        with pytest.raises(FileNotFoundError):
            bad_runner.load_raw_tables()
        bad_runner.close()


class TestExecuteSqlLayer:
    """Tests for individual layer execution."""

    def test_staging_layer(self, runner: PipelineRunner) -> None:
        """Staging layer should produce 5 tables with positive row counts."""
        runner.load_raw_tables()
        result = runner.execute_sql_layer("staging")
        assert result.layer_name == "staging"
        assert len(result.tables) == 5
        assert not result.errors
        for table_name, count in result.tables.items():
            assert table_name.startswith("stg_")
            assert count > 0
        runner.close()

    def test_intermediate_layer(self, runner: PipelineRunner) -> None:
        """Intermediate layer should produce 3 tables."""
        runner.load_raw_tables()
        runner.execute_sql_layer("staging")
        result = runner.execute_sql_layer("intermediate")
        assert result.layer_name == "intermediate"
        assert len(result.tables) == 3
        assert not result.errors
        runner.close()

    def test_marts_layer(self, runner: PipelineRunner) -> None:
        """Marts layer should produce 6 tables."""
        runner.load_raw_tables()
        runner.execute_sql_layer("staging")
        runner.execute_sql_layer("intermediate")
        result = runner.execute_sql_layer("marts")
        assert result.layer_name == "marts"
        assert len(result.tables) == 6
        assert not result.errors
        runner.close()

    def test_reports_layer(self, runner: PipelineRunner) -> None:
        """Reports layer should produce 2 tables."""
        runner.load_raw_tables()
        runner.execute_sql_layer("staging")
        runner.execute_sql_layer("intermediate")
        runner.execute_sql_layer("marts")
        result = runner.execute_sql_layer("reports")
        assert result.layer_name == "reports"
        assert len(result.tables) == 2
        assert not result.errors
        runner.close()

    def test_unknown_layer_returns_error(self, runner: PipelineRunner) -> None:
        """An unknown layer name should produce an error, not an exception."""
        result = runner.execute_sql_layer("nonexistent_layer")
        assert result.errors
        assert "Unknown layer" in result.errors[0]
        runner.close()


class TestFullPipeline:
    """Tests for end-to-end pipeline execution."""

    def test_full_pipeline_succeeds(self, runner: PipelineRunner) -> None:
        """The full pipeline should complete without errors."""
        result = runner.run_full_pipeline()
        assert result.success
        assert result.error_message is None
        assert len(result.layers) == 4
        assert result.total_elapsed_seconds > 0

        # Verify each layer produced the expected number of tables.
        expected_table_counts = {
            "staging": 5,
            "intermediate": 3,
            "marts": 6,
            "reports": 2,
        }
        for layer in result.layers:
            assert len(layer.tables) == expected_table_counts[layer.layer_name]
        runner.close()

    def test_pipeline_timing_is_recorded(
        self, runner: PipelineRunner
    ) -> None:
        """Each layer and the overall pipeline should have timing data."""
        result = runner.run_full_pipeline()
        assert result.total_elapsed_seconds >= 0
        for layer in result.layers:
            assert layer.elapsed_seconds >= 0
        runner.close()


class TestGetTableRowCount:
    """Tests for the get_table_row_count helper."""

    def test_existing_table(self, runner: PipelineRunner) -> None:
        """Should return the actual row count for a loaded table."""
        runner.load_raw_tables()
        count = runner.get_table_row_count("raw_claims")
        assert count > 0
        runner.close()

    def test_nonexistent_table(self, runner: PipelineRunner) -> None:
        """Should return -1 for a table that does not exist."""
        count = runner.get_table_row_count("this_table_does_not_exist")
        assert count == -1
        runner.close()
