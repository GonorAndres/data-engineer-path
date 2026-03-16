"""Integration tests for the streaming pipeline.

Tests the full pipeline graph using TestPipeline (DirectRunner).
No GCP credentials required.
"""

from __future__ import annotations

import json

import apache_beam as beam
from apache_beam import window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterProcessingTime,
    AfterWatermark,
)
from apache_beam.transforms.window import Duration, FixedWindows

import pytest

from transforms import (
    ComputeStreamingSummary,
    EnrichClaim,
    ExtractCoverageKey,
    ParseAndValidateClaim,
)
from pipeline_options import StreamingClaimsPipelineOptions


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
        "claimant_state": "Ciudad de Mexico",
        "currency": "MXN",
        "timestamp": ts,
    }


def _build_test_pipeline_graph(p, raw_bytes, window_size=3600):
    """Helper to build the core pipeline graph for testing.

    Returns (summaries, dead_letters) PCollections.
    """
    raw = p | beam.Create(raw_bytes)

    parsed = raw | "Parse" >> beam.ParDo(
        ParseAndValidateClaim()
    ).with_outputs(ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid")

    valid_claims = parsed.valid
    dead_letters = parsed[ParseAndValidateClaim.DEAD_LETTER_TAG]

    enriched = valid_claims | "Enrich" >> beam.ParDo(EnrichClaim())

    windowed = enriched | "Window" >> beam.WindowInto(
        FixedWindows(window_size),
        trigger=AfterWatermark(),
        accumulation_mode=AccumulationMode.DISCARDING,
    )

    keyed = windowed | "Key" >> beam.ParDo(ExtractCoverageKey())
    grouped = keyed | "Group" >> beam.GroupByKey()
    summaries = grouped | "Summarize" >> beam.ParDo(ComputeStreamingSummary())

    return summaries, dead_letters


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_end_to_end_valid_claims(self):
        """Valid claims flow through the entire pipeline to summaries."""
        claims = [
            _make_claim(claim_id="c1", amount=10000.0, ts="2026-01-15T10:05:00+00:00"),
            _make_claim(claim_id="c2", amount=20000.0, ts="2026-01-15T10:30:00+00:00"),
            _make_claim(claim_id="c3", amount=30000.0, ts="2026-01-15T10:55:00+00:00"),
        ]
        raw_bytes = [json.dumps(c).encode("utf-8") for c in claims]

        with TestPipeline() as p:
            summaries, dead_letters = _build_test_pipeline_graph(p, raw_bytes)

            def check_summaries(elements):
                assert len(elements) == 1
                s = elements[0]
                assert s["claim_count"] == 3
                assert s["total_amount_mxn"] == 60000.0
                assert s["coverage_type"] == "auto_colision"

            assert_that(summaries, check_summaries, label="Summaries")

    def test_end_to_end_mixed_valid_and_invalid(self):
        """Mix of valid and invalid claims routes correctly."""
        valid_claim = _make_claim(claim_id="valid-1", amount=50000.0)
        invalid_claim = _make_claim(claim_id="invalid-1", amount=-1000.0)
        raw_bytes = [
            json.dumps(valid_claim).encode("utf-8"),
            json.dumps(invalid_claim).encode("utf-8"),
        ]

        with TestPipeline() as p:
            summaries, dead_letters = _build_test_pipeline_graph(p, raw_bytes)

            def check_one_valid(elements):
                assert len(elements) == 1
                assert elements[0]["claim_count"] == 1

            assert_that(summaries, check_one_valid, label="OneValidSummary")
            assert_that(dead_letters, is_not_empty(), label="HasDeadLetters")


class TestDeduplication:
    """Tests for duplicate claim handling."""

    def test_deduplication_removes_duplicate_claim_ids(self):
        """Duplicate claim_ids within a window should still be grouped correctly.

        Note: deduplication via BagState requires keyed input. This test
        verifies that GroupByKey with claim_id-keyed data merges duplicates.
        """
        # Same claim_id, different amounts -- represents a duplicate publish
        claims = [
            _make_claim(claim_id="dup-1", amount=10000.0, ts="2026-01-15T10:05:00+00:00"),
            _make_claim(claim_id="dup-1", amount=10000.0, ts="2026-01-15T10:06:00+00:00"),
            _make_claim(claim_id="unique-1", amount=20000.0, ts="2026-01-15T10:07:00+00:00"),
        ]
        raw_bytes = [json.dumps(c).encode("utf-8") for c in claims]

        with TestPipeline() as p:
            raw = p | beam.Create(raw_bytes)

            parsed = raw | "Parse" >> beam.ParDo(
                ParseAndValidateClaim()
            ).with_outputs(ParseAndValidateClaim.DEAD_LETTER_TAG, main="valid")

            enriched = parsed.valid | "Enrich" >> beam.ParDo(EnrichClaim())

            windowed = enriched | "Window" >> beam.WindowInto(
                FixedWindows(3600),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.DISCARDING,
            )

            # Deduplicate by claim_id: GroupByKey merges same keys,
            # then take the first element from each group
            deduped = (
                windowed
                | "KeyByClaimId" >> beam.Map(lambda c: (c["claim_id"], c))
                | "GroupByClaimId" >> beam.GroupByKey()
                | "TakeFirst" >> beam.Map(lambda kv: list(kv[1])[0])
            )

            keyed = deduped | "Key" >> beam.ParDo(ExtractCoverageKey())
            grouped = keyed | "Group" >> beam.GroupByKey()
            summaries = grouped | "Summarize" >> beam.ParDo(ComputeStreamingSummary())

            def check_deduped(elements):
                assert len(elements) == 1
                # After dedup: dup-1 (once) + unique-1 = 2 claims
                assert elements[0]["claim_count"] == 2

            assert_that(summaries, check_deduped, label="DedupCheck")


class TestDeadLetterRouting:
    """Tests for dead letter capture."""

    def test_dead_letter_captures_parse_errors(self):
        """Non-JSON input should be captured in dead letters."""
        raw_bytes = [b"not json at all", b"also {{{ not json"]

        with TestPipeline() as p:
            summaries, dead_letters = _build_test_pipeline_graph(p, raw_bytes)

            def check_parse_errors(elements):
                assert len(elements) == 2
                for dl in elements:
                    assert dl["error_type"] == "parse"
                    assert "JSON parse error" in dl["error_reason"]

            assert_that(dead_letters, check_parse_errors, label="ParseErrors")

    def test_dead_letter_captures_validation_errors(self):
        """Validation failures should be captured with error_type='validate'."""
        bad_claims = [
            # Missing claim_id
            {"policy_id": "POL-1", "estimated_amount": 1000, "coverage_type": "auto_colision",
             "accident_date": "2026-01-15", "timestamp": "2026-01-15T10:00:00+00:00"},
            # Negative amount
            {"claim_id": "c1", "policy_id": "POL-1", "estimated_amount": -500,
             "coverage_type": "auto_colision", "accident_date": "2026-01-15",
             "timestamp": "2026-01-15T10:00:00+00:00"},
        ]
        raw_bytes = [json.dumps(c).encode("utf-8") for c in bad_claims]

        with TestPipeline() as p:
            summaries, dead_letters = _build_test_pipeline_graph(p, raw_bytes)

            def check_validation_errors(elements):
                assert len(elements) == 2
                for dl in elements:
                    assert dl["error_type"] == "validate"

            assert_that(dead_letters, check_validation_errors, label="ValidationErrors")


class TestPipelineOptions:
    """Tests for custom pipeline options."""

    def test_pipeline_options_parsed_correctly(self):
        """Custom options should parse from argv."""
        from apache_beam.options.pipeline_options import PipelineOptions

        argv = [
            "--input_subscription=projects/test/subscriptions/test-sub",
            "--output_project=test-project",
            "--output_dataset=test_dataset",
            "--window_size_seconds=1800",
            "--allowed_lateness_seconds=7200",
            "--early_firing_interval_seconds=60",
        ]

        options = PipelineOptions(argv)
        streaming_opts = options.view_as(StreamingClaimsPipelineOptions)

        assert streaming_opts.input_subscription == "projects/test/subscriptions/test-sub"
        assert streaming_opts.output_project == "test-project"
        assert streaming_opts.output_dataset == "test_dataset"
        assert streaming_opts.window_size_seconds == 1800
        assert streaming_opts.allowed_lateness_seconds == 7200
        assert streaming_opts.early_firing_interval_seconds == 60

    def test_pipeline_with_custom_window_size(self):
        """Pipeline should work with a custom (non-default) window size."""
        claims = [
            _make_claim(claim_id="c1", amount=10000.0, ts="2026-01-15T10:00:00+00:00"),
            _make_claim(claim_id="c2", amount=20000.0, ts="2026-01-15T10:20:00+00:00"),
            _make_claim(claim_id="c3", amount=30000.0, ts="2026-01-15T10:40:00+00:00"),
        ]
        raw_bytes = [json.dumps(c).encode("utf-8") for c in claims]

        with TestPipeline() as p:
            # 30-minute windows: c1+c2 in first window, c3 in second
            summaries, dead_letters = _build_test_pipeline_graph(
                p, raw_bytes, window_size=1800
            )

            def check_two_windows(elements):
                assert len(elements) == 2
                counts = sorted([e["claim_count"] for e in elements])
                assert counts == [1, 2]

            assert_that(summaries, check_two_windows, label="CustomWindowSize")
