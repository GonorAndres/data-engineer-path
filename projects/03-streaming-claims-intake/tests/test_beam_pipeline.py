"""Tests for the Beam batch pipeline.

Uses Apache Beam's TestPipeline (DirectRunner) so no GCP credentials
are required.
"""

from __future__ import annotations

from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.transforms.trigger import AfterWatermark

import pytest

from beam_pipeline import ComputeHourlySummary, ExtractCoverageKey, ParseClaim


def _make_claim(
    claim_id: str = "c-001",
    coverage: str = "auto_colision",
    amount: float = 50000.0,
    ts: str = "2026-01-15T10:30:00+00:00",
) -> dict:
    return {
        "claim_id": claim_id,
        "policy_id": "POL-100001",
        "accident_date": "2026-01-15",
        "cause_of_loss": "colision_vehicular",
        "estimated_amount": amount,
        "coverage_type": coverage,
        "currency": "MXN",
        "timestamp": ts,
        "processing_timestamp": ts,
        "validation_status": "valid",
    }


class TestParseClaim:
    """Tests for the ParseClaim DoFn."""

    def test_valid_dict_passes_through(self):
        with TestPipeline() as p:
            claims = [_make_claim()]
            result = (
                p
                | beam.Create(claims)
                | beam.ParDo(ParseClaim()).with_outputs(
                    ParseClaim.DEAD_LETTER_TAG, main="valid"
                )
            )
            assert_that(result.valid, is_not_empty(), label="ValidNotEmpty")

    def test_invalid_json_goes_to_dead_letter(self):
        with TestPipeline() as p:
            bad_data = [b"this is not valid json{{{"]
            result = (
                p
                | beam.Create(bad_data)
                | beam.ParDo(ParseClaim()).with_outputs(
                    ParseClaim.DEAD_LETTER_TAG, main="valid"
                )
            )
            assert_that(
                result[ParseClaim.DEAD_LETTER_TAG],
                is_not_empty(),
                label="DLQNotEmpty",
            )

    def test_string_json_parses_correctly(self):
        with TestPipeline() as p:
            import json

            claim = _make_claim(claim_id="str-test")
            json_strings = [json.dumps(claim)]
            result = (
                p
                | beam.Create(json_strings)
                | beam.ParDo(ParseClaim()).with_outputs(
                    ParseClaim.DEAD_LETTER_TAG, main="valid"
                )
            )
            assert_that(result.valid, is_not_empty(), label="StringParseValid")


class TestExtractCoverageKey:
    """Tests for the ExtractCoverageKey DoFn."""

    def test_extracts_coverage_and_amount(self):
        with TestPipeline() as p:
            claims = [_make_claim(coverage="vida_individual", amount=100000.0)]
            result = (
                p
                | beam.Create(claims)
                | beam.ParDo(ExtractCoverageKey())
            )
            assert_that(result, equal_to([("vida_individual", 100000.0)]))

    def test_missing_coverage_defaults_to_unknown(self):
        with TestPipeline() as p:
            claim = _make_claim()
            del claim["coverage_type"]
            result = (
                p
                | beam.Create([claim])
                | beam.ParDo(ExtractCoverageKey())
            )
            assert_that(result, equal_to([("unknown", 50000.0)]))


class TestComputeHourlySummary:
    """Tests for windowed aggregation."""

    def test_single_window_single_coverage(self):
        """Three claims in the same hour, same coverage -> one summary row."""
        with TestPipeline() as p:
            # All timestamps within the same hour (10:00-11:00 UTC)
            claims = [
                _make_claim(claim_id="c1", amount=10000.0, ts="2026-01-15T10:05:00+00:00"),
                _make_claim(claim_id="c2", amount=20000.0, ts="2026-01-15T10:30:00+00:00"),
                _make_claim(claim_id="c3", amount=30000.0, ts="2026-01-15T10:55:00+00:00"),
            ]

            result = (
                p
                | beam.Create(claims)
                | "Parse" >> beam.ParDo(ParseClaim()).with_outputs(
                    ParseClaim.DEAD_LETTER_TAG, main="valid"
                )
            )

            summaries = (
                result.valid
                | "Window" >> beam.WindowInto(
                    window.FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeHourlySummary())
            )

            def check_summary(elements):
                assert len(elements) == 1, f"Expected 1 summary, got {len(elements)}"
                s = elements[0]
                assert s["coverage_type"] == "auto_colision"
                assert s["claim_count"] == 3
                assert s["total_amount_mxn"] == 60000.0
                assert s["avg_amount_mxn"] == 20000.0
                assert s["min_amount_mxn"] == 10000.0
                assert s["max_amount_mxn"] == 30000.0

            assert_that(summaries, check_summary)

    def test_two_windows_separate_hours(self):
        """Claims in different hours should produce separate summaries."""
        with TestPipeline() as p:
            claims = [
                _make_claim(claim_id="c1", amount=10000.0, ts="2026-01-15T10:15:00+00:00"),
                _make_claim(claim_id="c2", amount=20000.0, ts="2026-01-15T11:15:00+00:00"),
            ]

            result = (
                p
                | beam.Create(claims)
                | "Parse" >> beam.ParDo(ParseClaim()).with_outputs(
                    ParseClaim.DEAD_LETTER_TAG, main="valid"
                )
            )

            summaries = (
                result.valid
                | "Window" >> beam.WindowInto(
                    window.FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeHourlySummary())
            )

            def check_two_windows(elements):
                assert len(elements) == 2, f"Expected 2 summaries, got {len(elements)}"
                counts = sorted([e["claim_count"] for e in elements])
                assert counts == [1, 1]

            assert_that(summaries, check_two_windows)

    def test_multiple_coverage_types(self):
        """Different coverage types in same window -> separate summaries."""
        with TestPipeline() as p:
            claims = [
                _make_claim(
                    claim_id="c1", coverage="auto_colision",
                    amount=10000.0, ts="2026-01-15T10:10:00+00:00",
                ),
                _make_claim(
                    claim_id="c2", coverage="vida_individual",
                    amount=80000.0, ts="2026-01-15T10:20:00+00:00",
                ),
            ]

            result = (
                p
                | beam.Create(claims)
                | "Parse" >> beam.ParDo(ParseClaim()).with_outputs(
                    ParseClaim.DEAD_LETTER_TAG, main="valid"
                )
            )

            summaries = (
                result.valid
                | "Window" >> beam.WindowInto(
                    window.FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeHourlySummary())
            )

            def check_two_coverages(elements):
                assert len(elements) == 2, f"Expected 2 summaries, got {len(elements)}"
                coverages = sorted([e["coverage_type"] for e in elements])
                assert coverages == ["auto_colision", "vida_individual"]

            assert_that(summaries, check_two_coverages)
