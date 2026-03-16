"""Tests for the claims event simulator.

All tests run without GCP credentials -- no Pub/Sub connection needed.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from claims_simulator import (
    CAUSES_OF_LOSS,
    COVERAGE_TYPES,
    generate_event,
    generate_malformed_event,
    generate_valid_event,
    publish_events,
)


class TestGenerateValidEvent:
    """Tests for valid claim event generation."""

    def test_returns_dict(self):
        event = generate_valid_event()
        assert isinstance(event, dict)

    def test_has_all_required_fields(self):
        event = generate_valid_event()
        required = {
            "claim_id",
            "policy_id",
            "accident_date",
            "cause_of_loss",
            "estimated_amount",
            "coverage_type",
            "timestamp",
        }
        assert required.issubset(event.keys())

    def test_amount_is_positive(self):
        for _ in range(50):
            event = generate_valid_event()
            assert event["estimated_amount"] > 0

    def test_amount_capped_at_5m_mxn(self):
        for _ in range(100):
            event = generate_valid_event()
            assert event["estimated_amount"] <= 5_000_000.0

    def test_coverage_type_is_valid(self):
        for _ in range(20):
            event = generate_valid_event()
            assert event["coverage_type"] in COVERAGE_TYPES

    def test_cause_of_loss_is_valid(self):
        for _ in range(20):
            event = generate_valid_event()
            assert event["cause_of_loss"] in CAUSES_OF_LOSS

    def test_accident_date_is_valid_iso_date(self):
        event = generate_valid_event()
        parsed = date.fromisoformat(event["accident_date"])
        assert parsed <= date.today()

    def test_policy_id_format(self):
        event = generate_valid_event()
        assert event["policy_id"].startswith("POL-")

    def test_currency_is_mxn(self):
        event = generate_valid_event()
        assert event["currency"] == "MXN"

    def test_serializes_to_json(self):
        event = generate_valid_event()
        data = json.dumps(event)
        roundtrip = json.loads(data)
        assert roundtrip["claim_id"] == event["claim_id"]


class TestGenerateMalformedEvent:
    """Tests for intentionally malformed events."""

    def test_has_injected_defect_marker(self):
        event = generate_malformed_event()
        assert "_injected_defect" in event

    def test_defect_is_known_type(self):
        known_defects = {"missing_field", "negative_amount", "bad_date", "bad_coverage"}
        for _ in range(50):
            event = generate_malformed_event()
            assert event["_injected_defect"] in known_defects

    def test_missing_field_defect_actually_removes_field(self):
        """At least some missing_field defects should actually lack a required field."""
        required = {"claim_id", "policy_id", "estimated_amount", "coverage_type"}
        found_missing = False
        for _ in range(200):
            event = generate_malformed_event()
            if event["_injected_defect"] == "missing_field":
                if not required.issubset(event.keys()):
                    found_missing = True
                    break
        assert found_missing, "No missing_field defect actually removed a required field"

    def test_negative_amount_defect(self):
        found = False
        for _ in range(200):
            event = generate_malformed_event()
            if event["_injected_defect"] == "negative_amount":
                assert event["estimated_amount"] < 0
                found = True
                break
        assert found, "Never generated a negative_amount defect"


class TestGenerateEvent:
    """Tests for the combined event generator with malformed rate."""

    def test_all_valid_when_rate_zero(self):
        for _ in range(50):
            event = generate_event(malformed_rate=0.0)
            assert "_injected_defect" not in event

    def test_all_malformed_when_rate_one(self):
        for _ in range(50):
            event = generate_event(malformed_rate=1.0)
            assert "_injected_defect" in event

    def test_default_rate_produces_mix(self):
        """With default ~5% malformed rate, 500 events should have some of each."""
        events = [generate_event() for _ in range(500)]
        valid_count = sum(1 for e in events if "_injected_defect" not in e)
        malformed_count = sum(1 for e in events if "_injected_defect" in e)
        assert valid_count > 0
        assert malformed_count > 0


class TestPublishEvents:
    """Tests for the publish_events function with mocked Pub/Sub."""

    @patch("claims_simulator.pubsub_v1.PublisherClient")
    def test_publishes_correct_number(self, mock_publisher_class):
        """Check that publish_events publishes the right number of messages."""
        mock_publisher = MagicMock()
        mock_publisher_class.return_value = mock_publisher
        mock_publisher.topic_path.return_value = "projects/test/topics/claims-events"

        mock_future = MagicMock()
        mock_future.result.return_value = "msg-id-123"
        mock_publisher.publish.return_value = mock_future

        counts = publish_events(
            project_id="test-project",
            topic_id="claims-events",
            rate=100.0,  # Fast rate for test speed
            duration=1,  # 1 second
        )

        assert isinstance(counts, dict)
        assert counts["total"] > 0
        assert mock_publisher.publish.call_count == counts["total"]

    @patch("claims_simulator.pubsub_v1.PublisherClient")
    def test_published_data_is_valid_json(self, mock_publisher_class):
        """Verify each published message is valid JSON."""
        mock_publisher = MagicMock()
        mock_publisher_class.return_value = mock_publisher
        mock_publisher.topic_path.return_value = "projects/test/topics/claims-events"

        mock_future = MagicMock()
        mock_future.result.return_value = "msg-id-123"
        mock_publisher.publish.return_value = mock_future

        publish_events(
            project_id="test-project",
            topic_id="claims-events",
            rate=50.0,
            duration=1,
            malformed_rate=0.0,  # All valid for this test
        )

        for call in mock_publisher.publish.call_args_list:
            # data is passed as a keyword argument to publisher.publish()
            data_bytes = call.kwargs.get("data") or call[0][1]
            parsed = json.loads(data_bytes.decode("utf-8"))
            assert "claim_id" in parsed
