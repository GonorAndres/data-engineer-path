"""Tests for the streaming claims simulator.

Validates event generation logic including late and out-of-order events --
the streaming-specific additions beyond P03's simulator.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from streaming_simulator import (
    generate_event,
    generate_late_event,
    generate_malformed_event,
    generate_out_of_order_event,
    generate_valid_event,
)

_REQUIRED_FIELDS = {
    "claim_id",
    "policy_id",
    "accident_date",
    "cause_of_loss",
    "estimated_amount",
    "coverage_type",
    "claimant_state",
    "currency",
    "timestamp",
}


class TestValidEvent:
    """Tests for generate_valid_event."""

    def test_valid_event_has_all_fields(self):
        """A valid event must include every required field."""
        event = generate_valid_event()
        assert _REQUIRED_FIELDS.issubset(event.keys()), (
            f"Missing fields: {_REQUIRED_FIELDS - event.keys()}"
        )

    def test_valid_event_amount_positive(self):
        """Claim amount must always be positive."""
        for _ in range(50):
            event = generate_valid_event()
            assert event["estimated_amount"] > 0

    def test_valid_event_amount_capped(self):
        """Claim amount must not exceed 5M MXN."""
        for _ in range(100):
            event = generate_valid_event()
            assert event["estimated_amount"] <= 5_000_000.0

    def test_valid_event_timestamp_is_recent(self):
        """Valid event timestamp should be within the last 60 seconds."""
        event = generate_valid_event()
        ts = datetime.fromisoformat(event["timestamp"])
        now = datetime.now(timezone.utc)
        delta = abs((now - ts).total_seconds())
        assert delta < 60, f"Timestamp {ts} is {delta}s from now -- too old for a 'current' event"


class TestLateEvent:
    """Tests for generate_late_event."""

    def test_late_event_timestamp_is_old(self):
        """Late event timestamp should be 5-60 minutes in the past."""
        for _ in range(20):
            event = generate_late_event()
            ts = datetime.fromisoformat(event["timestamp"])
            now = datetime.now(timezone.utc)
            age_minutes = (now - ts).total_seconds() / 60
            assert 4.9 <= age_minutes <= 61, (
                f"Late event age {age_minutes:.1f}min outside expected range [5, 60]"
            )

    def test_late_event_has_all_fields(self):
        """A late event is still a valid event with all required fields."""
        event = generate_late_event()
        assert _REQUIRED_FIELDS.issubset(event.keys()), (
            f"Missing fields: {_REQUIRED_FIELDS - event.keys()}"
        )


class TestOutOfOrderEvent:
    """Tests for generate_out_of_order_event."""

    def test_out_of_order_timestamp_slightly_old(self):
        """Out-of-order event timestamp should be 1-5 minutes in the past."""
        for _ in range(20):
            event = generate_out_of_order_event()
            ts = datetime.fromisoformat(event["timestamp"])
            now = datetime.now(timezone.utc)
            age_minutes = (now - ts).total_seconds() / 60
            assert 0.9 <= age_minutes <= 5.1, (
                f"OOO event age {age_minutes:.1f}min outside expected range [1, 5]"
            )


class TestEventDispatcher:
    """Tests for generate_event dispatcher function."""

    def test_event_type_distribution(self):
        """Over 1000 events, verify approximate rate distribution."""
        malformed_count = 0
        late_count = 0
        out_of_order_count = 0
        valid_count = 0
        n = 2000

        now = datetime.now(timezone.utc)

        for _ in range(n):
            event = generate_event(
                malformed_rate=0.10,
                late_rate=0.15,
                out_of_order_rate=0.20,
            )
            # Classify by inspection
            if "_injected_defect" in event:
                malformed_count += 1
            else:
                ts = datetime.fromisoformat(event["timestamp"])
                age_minutes = (now - ts).total_seconds() / 60
                if age_minutes > 4.5:
                    late_count += 1
                elif age_minutes > 0.8:
                    out_of_order_count += 1
                else:
                    valid_count += 1

        # Allow wide tolerance (rates are stochastic)
        malformed_rate = malformed_count / n
        late_rate = late_count / n
        ooo_rate = out_of_order_count / n
        valid_rate = valid_count / n

        assert 0.05 <= malformed_rate <= 0.18, f"Malformed rate {malformed_rate:.2f} out of range"
        assert 0.08 <= late_rate <= 0.25, f"Late rate {late_rate:.2f} out of range"
        assert 0.10 <= ooo_rate <= 0.30, f"OOO rate {ooo_rate:.2f} out of range"
        assert 0.35 <= valid_rate <= 0.70, f"Valid rate {valid_rate:.2f} out of range"
