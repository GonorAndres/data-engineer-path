"""Tests for DoFn transforms.

Uses Apache Beam's TestPipeline (DirectRunner) so no GCP credentials required.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

import pytest

from transforms import (
    ComputeStreamingSummary,
    EnrichClaim,
    ExtractCoverageKey,
    ParseAndValidateClaim,
)


def _make_claim(
    claim_id: str = "c-001",
    coverage: str = "auto_colision",
    amount: float = 50000.0,
    ts: str = "2026-01-15T10:30:00+00:00",
) -> dict:
    """Factory for test claim dicts."""
    return {
        "claim_id": claim_id,
        "policy_id": "POL-100001",
        "accident_date": "2026-01-15",
        "cause_of_loss": "colision_vehicular",
        "estimated_amount": amount,
        "coverage_type": coverage,
        "claimant_state": "Ciudad de Mexico",
        "currency": "MXN",
        "timestamp": ts,
    }


class TestParseAndValidateClaim:
    """Tests for ParseAndValidateClaim DoFn."""

    def test_valid_json_bytes_parsed(self):
        """Valid JSON bytes should parse into a claim dict."""
        claim = _make_claim()
        raw_bytes = json.dumps(claim).encode("utf-8")

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([raw_bytes])
                | beam.ParDo(ParseAndValidateClaim()).with_outputs(
                    ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid"
                )
            )
            assert_that(result.valid, is_not_empty(), label="ValidNotEmpty")

    def test_invalid_json_to_dead_letter(self):
        """Non-JSON bytes should go to dead letter."""
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([b"this is not json{{{"])
                | beam.ParDo(ParseAndValidateClaim()).with_outputs(
                    ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid"
                )
            )
            assert_that(
                result[ParseAndValidateClaim.DEAD_LETTER_TAG],
                is_not_empty(),
                label="DLQNotEmpty",
            )

    def test_missing_required_field_to_dead_letter(self):
        """Claim missing a required field should go to dead letter."""
        claim = _make_claim()
        del claim["claim_id"]
        raw_bytes = json.dumps(claim).encode("utf-8")

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([raw_bytes])
                | beam.ParDo(ParseAndValidateClaim()).with_outputs(
                    ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid"
                )
            )
            assert_that(
                result[ParseAndValidateClaim.DEAD_LETTER_TAG],
                is_not_empty(),
                label="MissingFieldDLQ",
            )

    def test_negative_amount_to_dead_letter(self):
        """Negative estimated_amount should go to dead letter."""
        claim = _make_claim(amount=-1000.0)
        raw_bytes = json.dumps(claim).encode("utf-8")

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([raw_bytes])
                | beam.ParDo(ParseAndValidateClaim()).with_outputs(
                    ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid"
                )
            )
            assert_that(
                result[ParseAndValidateClaim.DEAD_LETTER_TAG],
                is_not_empty(),
                label="NegativeAmountDLQ",
            )

    def test_timestamp_assigned_from_event(self):
        """Parsed claim should carry the event timestamp for windowing."""
        claim = _make_claim(ts="2026-01-15T10:30:00+00:00")
        raw_bytes = json.dumps(claim).encode("utf-8")

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([raw_bytes])
                | beam.ParDo(ParseAndValidateClaim()).with_outputs(
                    ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid"
                )
            )

            def check_has_timestamp(elements):
                assert len(elements) == 1
                # The element should be the parsed claim dict
                assert elements[0]["timestamp"] == "2026-01-15T10:30:00+00:00"

            assert_that(result.valid, check_has_timestamp, label="TimestampCheck")


class TestEnrichClaim:
    """Tests for EnrichClaim DoFn."""

    def test_adds_processing_timestamp(self):
        """Enriched claim should have a processing_timestamp."""
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([_make_claim()])
                | beam.ParDo(EnrichClaim())
            )

            def check_processing_ts(elements):
                assert len(elements) == 1
                assert "processing_timestamp" in elements[0]
                # Should be a valid ISO timestamp
                datetime.fromisoformat(elements[0]["processing_timestamp"])

            assert_that(result, check_processing_ts, label="HasProcessingTS")

    def test_adds_validation_status(self):
        """Enriched claim should have validation_status='valid'."""
        with TestPipeline() as p:
            result = (
                p
                | beam.Create([_make_claim()])
                | beam.ParDo(EnrichClaim())
            )

            def check_validation(elements):
                assert len(elements) == 1
                assert elements[0]["validation_status"] == "valid"

            assert_that(result, check_validation, label="HasValidationStatus")


class TestExtractCoverageKey:
    """Tests for ExtractCoverageKey DoFn."""

    def test_extracts_coverage_and_full_claim(self):
        """Should yield (coverage_type, full_claim_dict)."""
        claim = _make_claim(coverage="vida_individual", amount=100000.0)

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([claim])
                | beam.ParDo(ExtractCoverageKey())
            )

            def check_keyed(elements):
                assert len(elements) == 1
                key, value = elements[0]
                assert key == "vida_individual"
                # Value should be the full claim dict, not just amount
                assert isinstance(value, dict)
                assert value["estimated_amount"] == 100000.0
                assert value["claim_id"] == "c-001"

            assert_that(result, check_keyed, label="KeyedCorrectly")

    def test_missing_coverage_defaults_to_unknown(self):
        """Missing coverage_type should default to 'unknown'."""
        claim = _make_claim()
        del claim["coverage_type"]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create([claim])
                | beam.ParDo(ExtractCoverageKey())
            )

            def check_unknown(elements):
                assert len(elements) == 1
                key, _ = elements[0]
                assert key == "unknown"

            assert_that(result, check_unknown, label="UnknownCoverage")


class TestComputeStreamingSummary:
    """Tests for windowed aggregation with pane tracking."""

    def test_aggregation_produces_correct_counts(self):
        """Three claims should produce a summary with claim_count=3."""
        from apache_beam import window
        from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode

        claims = [
            _make_claim(claim_id="c1", amount=10000.0, ts="2026-01-15T10:05:00+00:00"),
            _make_claim(claim_id="c2", amount=20000.0, ts="2026-01-15T10:30:00+00:00"),
            _make_claim(claim_id="c3", amount=30000.0, ts="2026-01-15T10:55:00+00:00"),
        ]

        with TestPipeline() as p:
            result = (
                p
                | beam.Create(claims)
                | "Parse" >> beam.ParDo(ParseAndValidateClaim()).with_outputs(
                    ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid"
                )
            )

            summaries = (
                result.valid
                | "Window" >> beam.WindowInto(
                    window.FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_summary(elements):
                assert len(elements) == 1
                s = elements[0]
                assert s["coverage_type"] == "auto_colision"
                assert s["claim_count"] == 3
                assert s["total_amount_mxn"] == 60000.0
                assert s["avg_amount_mxn"] == 20000.0
                assert s["min_amount_mxn"] == 10000.0
                assert s["max_amount_mxn"] == 30000.0
                # Streaming-specific fields
                assert "pane_timing" in s
                assert "firing_id" in s

            assert_that(summaries, check_summary, label="AggregationCheck")
