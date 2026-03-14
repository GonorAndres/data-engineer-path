"""Tests for the Cloud Run push subscriber.

All tests use the Flask test client and mock GCP dependencies, so they
run without GCP credentials or emulator.
"""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

import pytest

from subscriber import app, validate_claim


@pytest.fixture()
def client():
    """Flask test client."""
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def _make_push_envelope(payload: dict | bytes) -> dict:
    """Build a Pub/Sub push envelope from a payload dict or raw bytes."""
    if isinstance(payload, dict):
        data = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    else:
        data = base64.b64encode(payload).decode("utf-8")
    return {
        "message": {
            "data": data,
            "messageId": "test-msg-001",
            "attributes": {"event_type": "claim_submission"},
        },
        "subscription": "projects/test/subscriptions/claims-events-push",
    }


VALID_CLAIM = {
    "claim_id": "c-001",
    "policy_id": "POL-123456",
    "accident_date": "2026-01-15",
    "cause_of_loss": "colision_vehicular",
    "estimated_amount": 50000.00,
    "coverage_type": "auto_colision",
    "currency": "MXN",
    "timestamp": "2026-01-15T12:00:00+00:00",
}


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_returns_healthy_status(self, client):
        resp = client.get("/health")
        data = resp.get_json()
        assert data["status"] == "healthy"
        assert data["service"] == "claims-subscriber"


class TestValidateClaim:
    """Unit tests for the validate_claim function."""

    def test_valid_claim_returns_no_errors(self):
        errors = validate_claim(VALID_CLAIM)
        assert errors == []

    def test_missing_claim_id(self):
        claim = {k: v for k, v in VALID_CLAIM.items() if k != "claim_id"}
        errors = validate_claim(claim)
        assert any("claim_id" in e for e in errors)

    def test_missing_policy_id(self):
        claim = {k: v for k, v in VALID_CLAIM.items() if k != "policy_id"}
        errors = validate_claim(claim)
        assert any("policy_id" in e for e in errors)

    def test_missing_estimated_amount(self):
        claim = {k: v for k, v in VALID_CLAIM.items() if k != "estimated_amount"}
        errors = validate_claim(claim)
        assert any("estimated_amount" in e for e in errors)

    def test_negative_amount(self):
        claim = {**VALID_CLAIM, "estimated_amount": -1000}
        errors = validate_claim(claim)
        assert any("positive" in e for e in errors)

    def test_zero_amount(self):
        claim = {**VALID_CLAIM, "estimated_amount": 0}
        errors = validate_claim(claim)
        assert any("positive" in e for e in errors)

    def test_invalid_date(self):
        claim = {**VALID_CLAIM, "accident_date": "not-a-date"}
        errors = validate_claim(claim)
        assert any("date" in e.lower() for e in errors)

    def test_empty_coverage_type(self):
        claim = {**VALID_CLAIM, "coverage_type": ""}
        errors = validate_claim(claim)
        assert any("coverage_type" in e for e in errors)

    def test_non_numeric_amount(self):
        claim = {**VALID_CLAIM, "estimated_amount": "abc"}
        errors = validate_claim(claim)
        assert any("not a number" in e for e in errors)


class TestPushHandler:
    """Integration tests for POST /push."""

    def test_no_message_returns_400(self, client):
        resp = client.post("/push", json={})
        assert resp.status_code == 400

    @patch("subscriber.write_to_bigquery")
    def test_valid_message_returns_200(self, mock_bq, client):
        mock_bq.return_value = None
        envelope = _make_push_envelope(VALID_CLAIM)
        resp = client.post("/push", json=envelope)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["claim_id"] == "c-001"

    @patch("subscriber.write_to_bigquery")
    def test_valid_message_calls_bigquery(self, mock_bq, client):
        mock_bq.return_value = None
        envelope = _make_push_envelope(VALID_CLAIM)
        client.post("/push", json=envelope)
        mock_bq.assert_called_once()
        written_claim = mock_bq.call_args[0][0]
        assert written_claim["claim_id"] == "c-001"
        assert written_claim["validation_status"] == "valid"
        assert "processing_timestamp" in written_claim

    @patch("subscriber.publish_to_dlq")
    def test_invalid_message_routes_to_dlq(self, mock_dlq, client):
        bad_claim = {**VALID_CLAIM, "estimated_amount": -500}
        envelope = _make_push_envelope(bad_claim)
        resp = client.post("/push", json=envelope)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "routed_to_dlq"
        mock_dlq.assert_called_once()

    @patch("subscriber.publish_to_dlq")
    def test_unparseable_message_routes_to_dlq(self, mock_dlq, client):
        envelope = _make_push_envelope(b"this is not json{{{")
        resp = client.post("/push", json=envelope)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "routed_to_dlq"
        assert data["reason"] == "unparseable"
        mock_dlq.assert_called_once()

    @patch("subscriber.publish_to_dlq")
    def test_missing_required_field_routes_to_dlq(self, mock_dlq, client):
        claim_no_id = {k: v for k, v in VALID_CLAIM.items() if k != "claim_id"}
        envelope = _make_push_envelope(claim_no_id)
        resp = client.post("/push", json=envelope)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "routed_to_dlq"

    @patch("subscriber.write_to_bigquery")
    def test_bigquery_failure_returns_500(self, mock_bq, client):
        mock_bq.side_effect = RuntimeError("BQ connection failed")
        envelope = _make_push_envelope(VALID_CLAIM)
        resp = client.post("/push", json=envelope)
        assert resp.status_code == 500
