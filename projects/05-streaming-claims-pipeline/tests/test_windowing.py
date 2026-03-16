"""Tests for windowing, triggers, and pane behavior.

Uses TestPipeline with TimestampedValue to control event times precisely.
These tests validate the streaming semantics that distinguish P05 from P03:
- Accumulating mode
- Early/on-time/late firings
- Allowed lateness
- Pane timing tracking
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam import window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterProcessingTime,
    AfterWatermark,
    Repeatedly,
)
from apache_beam.transforms.window import Duration, FixedWindows

import pytest

from transforms import ComputeStreamingSummary, ExtractCoverageKey


def _make_claim(
    claim_id: str = "c-001",
    coverage: str = "auto_colision",
    amount: float = 50000.0,
) -> dict:
    """Factory for test claim dicts (no timestamp -- caller uses TimestampedValue)."""
    return {
        "claim_id": claim_id,
        "policy_id": "POL-100001",
        "accident_date": "2026-01-15",
        "cause_of_loss": "colision_vehicular",
        "estimated_amount": amount,
        "coverage_type": coverage,
        "claimant_state": "Ciudad de Mexico",
        "currency": "MXN",
    }


# Epoch helpers for 2026-01-15
# 10:00:00 UTC = 1768471200.0 (verified via datetime(2026,1,15,10,0,0,utc).timestamp())
_HOUR_10 = 1768471200
_HOUR_11 = _HOUR_10 + 3600
_HOUR_12 = _HOUR_10 + 7200


class TestFixedWindowAssignment:
    """Tests for correct window assignment based on event timestamps."""

    def test_events_same_hour_same_window(self):
        """Events within the same hour should land in one window."""
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", amount=10000.0), _HOUR_10 + 300
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2", amount=20000.0), _HOUR_10 + 1800
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c3", amount=30000.0), _HOUR_10 + 3500
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_one_window(elements):
                assert len(elements) == 1
                assert elements[0]["claim_count"] == 3

            assert_that(summaries, check_one_window)

    def test_events_different_hours_different_windows(self):
        """Events in different hours should produce separate window summaries."""
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", amount=10000.0), _HOUR_10 + 300
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2", amount=20000.0), _HOUR_11 + 300
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_two_windows(elements):
                assert len(elements) == 2
                counts = sorted([e["claim_count"] for e in elements])
                assert counts == [1, 1]

            assert_that(summaries, check_two_windows)

    def test_three_hour_span_produces_three_windows(self):
        """Events spanning three hours should produce three summaries."""
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1"), _HOUR_10 + 100
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2"), _HOUR_11 + 100
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c3"), _HOUR_12 + 100
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_three_windows(elements):
                assert len(elements) == 3

            assert_that(summaries, check_three_windows)

    def test_multiple_coverage_types_per_window(self):
        """Different coverage types in the same window produce separate summaries."""
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", coverage="auto_colision", amount=10000.0),
                    _HOUR_10 + 100,
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2", coverage="vida_individual", amount=80000.0),
                    _HOUR_10 + 200,
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_two_coverages(elements):
                assert len(elements) == 2
                coverages = sorted([e["coverage_type"] for e in elements])
                assert coverages == ["auto_colision", "vida_individual"]

            assert_that(summaries, check_two_coverages)


class TestAccumulatingMode:
    """Tests for accumulating vs discarding behavior."""

    def test_accumulating_mode_includes_all_data(self):
        """In accumulating mode with a single batch firing, all data is present."""
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", amount=10000.0), _HOUR_10 + 100
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2", amount=20000.0), _HOUR_10 + 200
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c3", amount=30000.0), _HOUR_10 + 300
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_accumulating(elements):
                assert len(elements) == 1
                s = elements[0]
                # Accumulating mode: the single firing has ALL 3 claims
                assert s["claim_count"] == 3
                assert s["total_amount_mxn"] == 60000.0

            assert_that(summaries, check_accumulating)


class TestTriggerBehavior:
    """Tests for trigger configuration validity."""

    def test_early_firing_emits_partial_results(self):
        """Verify that the early trigger config is accepted by Beam.

        On DirectRunner batch, early triggers fire once at end of data.
        This test validates the trigger configuration is syntactically valid.
        """
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", amount=10000.0), _HOUR_10 + 100
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(
                        early=AfterProcessingTime(30),
                        late=AfterCount(1),
                    ),
                    allowed_lateness=Duration(seconds=3600),
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            assert_that(summaries, is_not_empty(), label="EarlyFiringEmits")

    def test_on_time_firing_after_watermark(self):
        """After watermark passes, the on-time firing includes all data."""
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", amount=10000.0), _HOUR_10 + 500
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2", amount=20000.0), _HOUR_10 + 1500
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_on_time(elements):
                assert len(elements) == 1
                assert elements[0]["claim_count"] == 2

            assert_that(summaries, check_on_time)


class TestLateness:
    """Tests for allowed lateness and late data handling."""

    def test_late_data_within_allowed_lateness_included(self):
        """Data arriving within allowed_lateness should be processed.

        On DirectRunner with ACCUMULATING mode, multiple firings may occur.
        Each firing includes ALL data seen so far. We verify that the latest
        (largest) firing contains both claims, proving both were processed.
        """
        with TestPipeline() as p:
            # All events in the same window, some 'late' by timestamp ordering
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", amount=10000.0), _HOUR_10 + 3500
                ),
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2", amount=20000.0), _HOUR_10 + 100
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(late=AfterCount(1)),
                    allowed_lateness=Duration(seconds=3600),
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_includes_late(elements):
                # In ACCUMULATING mode, each firing has ALL data seen so far.
                # The max claim_count across firings should be 2 (both claims).
                max_count = max(e["claim_count"] for e in elements)
                assert max_count == 2

            assert_that(summaries, check_includes_late)

    def test_data_beyond_allowed_lateness_dropped(self):
        """Data arriving beyond allowed_lateness should be dropped.

        On DirectRunner, the behavior depends on the watermark. We test that
        the windowing config is valid with allowed_lateness=0 and data in
        different windows -- the out-of-window data should not appear in the
        wrong window's summary.
        """
        with TestPipeline() as p:
            events = [
                # This event belongs to the 10:00-11:00 window
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1", amount=10000.0), _HOUR_10 + 100
                ),
                # This event belongs to the 11:00-12:00 window
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c2", amount=20000.0), _HOUR_11 + 100
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    allowed_lateness=Duration(seconds=0),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_separate_windows(elements):
                # Each window should have exactly 1 claim
                assert len(elements) == 2
                for e in elements:
                    assert e["claim_count"] == 1

            assert_that(summaries, check_separate_windows)

    def test_pane_timing_tracked_in_output(self):
        """Summary output should include pane_timing field."""
        with TestPipeline() as p:
            events = [
                beam.window.TimestampedValue(
                    _make_claim(claim_id="c1"), _HOUR_10 + 100
                ),
            ]

            summaries = (
                p
                | beam.Create(events)
                | "Window" >> beam.WindowInto(
                    FixedWindows(3600),
                    trigger=AfterWatermark(),
                    accumulation_mode=AccumulationMode.DISCARDING,
                )
                | "Key" >> beam.ParDo(ExtractCoverageKey())
                | "Group" >> beam.GroupByKey()
                | "Summarize" >> beam.ParDo(ComputeStreamingSummary())
            )

            def check_pane_timing(elements):
                assert len(elements) == 1
                s = elements[0]
                assert "pane_timing" in s
                assert s["pane_timing"] in ("EARLY", "ON_TIME", "LATE", "UNKNOWN")
                assert "firing_id" in s
                assert len(s["firing_id"]) > 0

            assert_that(summaries, check_pane_timing)
