"""Tests for BigQuery schemas and table spec helpers.

Validates schema structure, field types, and table spec formatting.
"""

from __future__ import annotations

import pytest

from schemas import (
    STREAMING_DEAD_LETTERS_SCHEMA,
    STREAMING_HOURLY_SUMMARY_SCHEMA,
    STREAMING_LATE_ARRIVALS_SCHEMA,
    get_dlq_table_spec,
    get_late_table_spec,
    get_summary_table_spec,
)

# Valid BigQuery field types
_VALID_BQ_TYPES = {"STRING", "INTEGER", "FLOAT", "TIMESTAMP", "BOOLEAN", "BYTES", "RECORD"}


class TestStreamingHourlySummarySchema:
    """Tests for the streaming hourly summary schema."""

    def test_summary_schema_has_required_fields(self):
        """Summary schema must have all streaming-specific fields."""
        field_names = {f["name"] for f in STREAMING_HOURLY_SUMMARY_SCHEMA["fields"]}
        expected = {
            "window_start",
            "window_end",
            "coverage_type",
            "claim_count",
            "total_amount_mxn",
            "avg_amount_mxn",
            "min_amount_mxn",
            "max_amount_mxn",
            "pane_timing",
            "firing_id",
        }
        assert expected.issubset(field_names), f"Missing fields: {expected - field_names}"


class TestLateArrivalsSchema:
    """Tests for the late arrivals schema."""

    def test_late_arrivals_schema_has_required_fields(self):
        """Late arrivals schema must track timing and lateness info."""
        field_names = {f["name"] for f in STREAMING_LATE_ARRIVALS_SCHEMA["fields"]}
        expected = {
            "claim_id",
            "original_timestamp",
            "arrival_timestamp",
            "lateness_seconds",
            "raw_data",
            "processing_timestamp",
        }
        assert expected.issubset(field_names), f"Missing fields: {expected - field_names}"


class TestDeadLettersSchema:
    """Tests for the dead letters schema."""

    def test_dlq_schema_has_required_fields(self):
        """DLQ schema must include error_type for categorization."""
        field_names = {f["name"] for f in STREAMING_DEAD_LETTERS_SCHEMA["fields"]}
        expected = {"raw_data", "error_reason", "error_type", "processing_timestamp"}
        assert expected.issubset(field_names), f"Missing fields: {expected - field_names}"


class TestSchemaFieldTypes:
    """Cross-schema validation tests."""

    @pytest.mark.parametrize(
        "schema,label",
        [
            (STREAMING_HOURLY_SUMMARY_SCHEMA, "summary"),
            (STREAMING_LATE_ARRIVALS_SCHEMA, "late_arrivals"),
            (STREAMING_DEAD_LETTERS_SCHEMA, "dead_letters"),
        ],
    )
    def test_all_schemas_have_valid_types(self, schema, label):
        """Every field type must be a valid BigQuery type."""
        for field in schema["fields"]:
            assert field["type"] in _VALID_BQ_TYPES, (
                f"Schema '{label}', field '{field['name']}' has invalid type: {field['type']}"
            )


class TestTableSpecFormatting:
    """Tests for table spec helper functions."""

    def test_table_spec_formatting(self):
        """Table specs must follow project:dataset.table format."""
        specs = [
            get_summary_table_spec("my-project", "my_dataset"),
            get_late_table_spec("my-project", "my_dataset"),
            get_dlq_table_spec("my-project", "my_dataset"),
        ]
        for spec in specs:
            assert ":" in spec, f"Missing colon in table spec: {spec}"
            assert "." in spec, f"Missing dot in table spec: {spec}"
            project_part, table_part = spec.split(":", 1)
            dataset_part, table_name = table_part.split(".", 1)
            assert project_part == "my-project"
            assert dataset_part == "my_dataset"
            assert len(table_name) > 0
