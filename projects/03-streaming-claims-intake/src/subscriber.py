"""Cloud Run push subscriber for Pub/Sub claim events.

Receives push-delivered messages, validates them, enriches with processing
metadata, writes valid claims to BigQuery, and routes invalid messages to a
dead-letter topic.

Run locally:
    export FLASK_APP=src/subscriber.py
    flask run --port 8080

Deploy to Cloud Run:
    gcloud run deploy claims-subscriber \
        --source . --region us-central1 \
        --set-env-vars PROJECT_ID=my-project,BQ_DATASET=claims_raw,DLQ_TOPIC=claims-events-dlq
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from typing import Any

from flask import Flask, Request, jsonify, request

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Structured JSON logging (Cloud Logging compatible)
# ---------------------------------------------------------------------------

_log_handler = logging.StreamHandler(sys.stdout)
_log_handler.setFormatter(
    logging.Formatter(
        json.dumps(
            {
                "severity": "%(levelname)s",
                "message": "%(message)s",
                "module": "%(module)s",
                "timestamp": "%(asctime)s",
            }
        )
    )
)
logger = logging.getLogger("claims_subscriber")
logger.setLevel(logging.INFO)
logger.addHandler(_log_handler)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

PROJECT_ID = os.environ.get("PROJECT_ID", "local-project")
BQ_DATASET = os.environ.get("BQ_DATASET", "claims_raw")
BQ_TABLE = os.environ.get("BQ_TABLE", "streaming_claims")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "claims-events-dlq")

# ---------------------------------------------------------------------------
# Lazy-loaded clients (avoid import errors when running tests without GCP)
# ---------------------------------------------------------------------------

_bq_client = None
_publisher = None


def get_bq_client():
    """Return a cached BigQuery client instance."""
    global _bq_client  # noqa: PLW0603
    if _bq_client is None:
        from google.cloud import bigquery

        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def get_publisher():
    """Return a cached Pub/Sub publisher instance."""
    global _publisher  # noqa: PLW0603
    if _publisher is None:
        from google.cloud import pubsub_v1

        _publisher = pubsub_v1.PublisherClient()
    return _publisher


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

REQUIRED_FIELDS: list[str] = [
    "claim_id",
    "policy_id",
    "accident_date",
    "estimated_amount",
    "coverage_type",
]


def validate_claim(claim: dict[str, Any]) -> list[str]:
    """Validate a claim event and return a list of error descriptions.

    Returns an empty list if the claim is valid.
    """
    errors: list[str] = []

    # Required fields
    for field in REQUIRED_FIELDS:
        if field not in claim:
            errors.append(f"missing required field: {field}")

    if errors:
        # If required fields are missing, skip further checks
        return errors

    # Amount must be positive
    try:
        amount = float(claim["estimated_amount"])
        if amount <= 0:
            errors.append(f"estimated_amount must be positive, got {amount}")
    except (TypeError, ValueError):
        errors.append(f"estimated_amount is not a number: {claim['estimated_amount']}")

    # Date must be parseable
    try:
        parsed = date.fromisoformat(str(claim["accident_date"]))
        if parsed > date.today():
            errors.append(f"accident_date is in the future: {claim['accident_date']}")
    except ValueError:
        errors.append(f"accident_date is not a valid ISO date: {claim['accident_date']}")

    # coverage_type must not be empty
    if not claim.get("coverage_type"):
        errors.append("coverage_type is empty")

    return errors


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


def enrich_claim(claim: dict[str, Any]) -> dict[str, Any]:
    """Add processing metadata to a validated claim."""
    claim["processing_timestamp"] = datetime.now(timezone.utc).isoformat()
    claim["validation_status"] = "valid"
    return claim


def write_to_bigquery(claim: dict[str, Any]) -> None:
    """Insert a single validated claim row into BigQuery."""
    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_ref, [claim])
    if errors:
        logger.error("BigQuery insert errors: %s", errors)
        raise RuntimeError(f"BigQuery insert failed: {errors}")


def publish_to_dlq(raw_message: bytes, reason: str) -> None:
    """Publish an invalid message to the dead-letter topic."""
    publisher = get_publisher()
    topic_path = publisher.topic_path(PROJECT_ID, DLQ_TOPIC)
    publisher.publish(
        topic_path,
        data=raw_message,
        error_reason=reason,
        original_timestamp=datetime.now(timezone.utc).isoformat(),
    )
    logger.info("Routed invalid message to DLQ: %s", reason)


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------


@app.route("/health", methods=["GET"])
def health():
    """Health-check endpoint for Cloud Run readiness probes."""
    return jsonify({"status": "healthy", "service": "claims-subscriber"}), 200


@app.route("/push", methods=["POST"])
def push_handler():
    """Handle Pub/Sub push-delivered messages.

    Pub/Sub sends a JSON envelope:
    {
        "message": {
            "data": "<base64-encoded payload>",
            "messageId": "...",
            "attributes": {...}
        },
        "subscription": "projects/.../subscriptions/..."
    }

    Returns 200 to ACK, 400/500 to NACK.
    """
    envelope: dict[str, Any] = request.get_json(silent=True) or {}
    message = envelope.get("message")

    if not message:
        logger.warning("Received request with no Pub/Sub message envelope")
        return jsonify({"error": "no Pub/Sub message"}), 400

    # Decode the base64 payload
    raw_data = base64.b64decode(message.get("data", ""))

    try:
        claim = json.loads(raw_data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        publish_to_dlq(raw_data, f"JSON parse error: {exc}")
        # Return 200 so Pub/Sub does not redeliver
        return jsonify({"status": "routed_to_dlq", "reason": "unparseable"}), 200

    # Validate
    validation_errors = validate_claim(claim)

    if validation_errors:
        reason = "; ".join(validation_errors)
        claim["validation_status"] = "invalid"
        claim["validation_errors"] = validation_errors
        publish_to_dlq(json.dumps(claim).encode("utf-8"), reason)
        logger.info("Invalid claim %s: %s", claim.get("claim_id", "unknown"), reason)
        return jsonify({"status": "routed_to_dlq", "reason": reason}), 200

    # Enrich and write
    enriched = enrich_claim(claim)
    try:
        write_to_bigquery(enriched)
    except Exception:
        logger.exception("Failed to write claim %s to BigQuery", claim.get("claim_id"))
        # Return 500 so Pub/Sub retries
        return jsonify({"error": "BigQuery write failed"}), 500

    logger.info("Processed claim %s successfully", claim["claim_id"])
    return jsonify({"status": "ok", "claim_id": claim["claim_id"]}), 200


# ---------------------------------------------------------------------------
# Local dev entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
