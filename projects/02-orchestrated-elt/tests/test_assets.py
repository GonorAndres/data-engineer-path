"""Tests for Dagster Software-Defined Assets.

Uses ``materialize_to_memory`` to test asset execution without a
persistent database or running Dagster instance.  No GCP credentials
are required.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure src/ is on the import path.
_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

_PROJECT_01_SRC = (
    Path(__file__).resolve().parent.parent.parent
    / "01-claims-warehouse"
    / "src"
)
if str(_PROJECT_01_SRC) not in sys.path:
    sys.path.insert(0, str(_PROJECT_01_SRC))

from dagster import materialize
from pipeline.config import DATA_DIR

from dagster_pipeline.assets import (
    intermediate_layer,
    marts_layer,
    raw_data,
    reports_layer,
    staging_layer,
)
from dagster_pipeline.resources import DuckDBResource


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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


class TestRawDataAsset:
    """Tests for the raw_data asset."""

    def test_raw_data_materializes(self) -> None:
        """raw_data should materialize successfully."""
        result = materialize(
            assets=[raw_data],
            resources={"duckdb_resource": DuckDBResource(db_path=":memory:")},
        )
        assert result.success

    def test_raw_data_metadata_includes_generated_flag(self) -> None:
        """raw_data metadata should indicate whether data was generated."""
        result = materialize(
            assets=[raw_data],
            resources={"duckdb_resource": DuckDBResource(db_path=":memory:")},
        )
        assert result.success
        events = result.get_asset_materialization_events()
        assert len(events) == 1
        metadata = events[0].step_materialization_data.materialization.metadata
        assert "generated" in metadata


class TestStagingLayerAsset:
    """Tests for the staging_layer asset."""

    def test_staging_materializes(self) -> None:
        """staging_layer should materialize successfully."""
        result = materialize(
            assets=[raw_data, staging_layer],
            resources={"duckdb_resource": DuckDBResource(db_path=":memory:")},
        )
        assert result.success

    def test_staging_metadata_has_row_counts(self) -> None:
        """staging_layer metadata should include per-table row counts."""
        result = materialize(
            assets=[raw_data, staging_layer],
            resources={"duckdb_resource": DuckDBResource(db_path=":memory:")},
        )
        events = [
            e
            for e in result.get_asset_materialization_events()
            if e.step_materialization_data.materialization.asset_key.path
            == ["staging_layer"]
        ]
        assert len(events) == 1
        metadata = events[0].step_materialization_data.materialization.metadata
        assert "elapsed_seconds" in metadata

        # At least one rows/* key should be present.
        row_keys = [k for k in metadata if k.startswith("rows/")]
        assert len(row_keys) >= 1


class TestFullAssetGraph:
    """Test materializing the complete asset graph."""

    def test_all_layers_materialize(self) -> None:
        """All five assets should materialize end-to-end."""
        result = materialize(
            assets=[
                raw_data,
                staging_layer,
                intermediate_layer,
                marts_layer,
                reports_layer,
            ],
            resources={"duckdb_resource": DuckDBResource(db_path=":memory:")},
        )
        assert result.success
        events = result.get_asset_materialization_events()
        # Expect one materialization event per asset.
        assert len(events) == 5
