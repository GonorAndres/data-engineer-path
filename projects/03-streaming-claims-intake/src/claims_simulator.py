"""Claims event simulator that publishes to Pub/Sub.

Generates realistic Mexican insurance claim events and publishes them to a
Pub/Sub topic. Approximately 5% of events are intentionally malformed to
exercise dead-letter routing downstream.

Works with both real Pub/Sub and the local emulator
(set PUBSUB_EMULATOR_HOST=localhost:8085).

Usage:
    python src/claims_simulator.py --project my-project --topic claims-events --rate 5 --duration 60
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
from faker import Faker
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

fake = Faker("es_MX")

# Mexican insurance domain constants
COVERAGE_TYPES: list[str] = [
    "auto_colision",
    "auto_robo_total",
    "auto_responsabilidad_civil",
    "gastos_medicos_mayores",
    "vida_individual",
    "hogar_incendio",
    "hogar_robo",
    "responsabilidad_civil_general",
]

CAUSES_OF_LOSS: list[str] = [
    "colision_vehicular",
    "robo_con_violencia",
    "robo_sin_violencia",
    "incendio",
    "inundacion",
    "terremoto",
    "enfermedad",
    "accidente_personal",
    "fallecimiento",
    "dano_a_terceros",
    "vandalismo",
    "fenomeno_natural",
]


def generate_valid_event() -> dict[str, Any]:
    """Generate a realistic, well-formed insurance claim event.

    All monetary amounts are in MXN. Amounts follow a log-normal distribution
    to mimic real claim severity patterns.
    """
    accident_date = fake.date_between(start_date="-90d", end_date="today")
    coverage = random.choice(COVERAGE_TYPES)

    # Log-normal distribution for claim amounts (realistic severity curve).
    # Median ~25k MXN, occasional large claims up to ~500k+.
    raw_amount = float(np.random.lognormal(mean=10.1, sigma=1.2))
    estimated_amount = round(min(raw_amount, 5_000_000.0), 2)  # Cap at 5M MXN

    return {
        "claim_id": str(uuid.uuid4()),
        "policy_id": f"POL-{fake.unique.random_int(min=100000, max=999999)}",
        "accident_date": accident_date.isoformat(),
        "cause_of_loss": random.choice(CAUSES_OF_LOSS),
        "estimated_amount": estimated_amount,
        "coverage_type": coverage,
        "claimant_state": fake.state(),
        "currency": "MXN",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def generate_malformed_event() -> dict[str, Any]:
    """Generate an intentionally malformed event for dead-letter testing.

    Picks a random defect: missing required field, negative amount,
    invalid date, or garbage coverage type.
    """
    event = generate_valid_event()
    defect = random.choice(["missing_field", "negative_amount", "bad_date", "bad_coverage"])

    if defect == "missing_field":
        # Remove a required field at random
        field = random.choice(["claim_id", "policy_id", "estimated_amount", "coverage_type"])
        del event[field]
    elif defect == "negative_amount":
        event["estimated_amount"] = round(-abs(event["estimated_amount"]), 2)
    elif defect == "bad_date":
        event["accident_date"] = "not-a-date"
    elif defect == "bad_coverage":
        event["coverage_type"] = ""

    event["_injected_defect"] = defect  # marker for debugging
    return event


def generate_event(malformed_rate: float = 0.05) -> dict[str, Any]:
    """Generate a single claim event; ~malformed_rate fraction are bad."""
    if random.random() < malformed_rate:
        return generate_malformed_event()
    return generate_valid_event()


def publish_events(
    project_id: str,
    topic_id: str,
    rate: float,
    duration: int,
    malformed_rate: float = 0.05,
) -> int:
    """Publish claim events to Pub/Sub at the specified rate.

    Args:
        project_id: GCP project ID.
        topic_id: Pub/Sub topic name.
        rate: Target events per second.
        duration: Total seconds to run.
        malformed_rate: Fraction of events that should be malformed.

    Returns:
        Total number of events published.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    interval = 1.0 / rate if rate > 0 else 1.0
    total_published = 0
    end_time = time.monotonic() + duration

    logger.info(
        "Starting simulator: project=%s topic=%s rate=%.1f/s duration=%ds",
        project_id,
        topic_id,
        rate,
        duration,
    )

    while time.monotonic() < end_time:
        event = generate_event(malformed_rate=malformed_rate)
        data = json.dumps(event).encode("utf-8")

        future = publisher.publish(
            topic_path,
            data=data,
            event_type="claim_submission",
            source="claims_simulator",
        )
        message_id = future.result(timeout=30)
        total_published += 1

        if total_published % 10 == 0:
            logger.info(
                "Published %d events (last message_id=%s)", total_published, message_id
            )

        time.sleep(interval)

    logger.info("Simulator finished. Total events published: %d", total_published)
    return total_published


def main() -> None:
    """CLI entry point for the claims simulator."""
    parser = argparse.ArgumentParser(
        description="Publish synthetic insurance claim events to Pub/Sub"
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--topic", default="claims-events", help="Pub/Sub topic name")
    parser.add_argument("--rate", type=float, default=5.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Seconds to run")
    parser.add_argument(
        "--malformed-rate",
        type=float,
        default=0.05,
        help="Fraction of events that are intentionally malformed (default 0.05)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    publish_events(
        project_id=args.project,
        topic_id=args.topic,
        rate=args.rate,
        duration=args.duration,
        malformed_rate=args.malformed_rate,
    )


if __name__ == "__main__":
    main()
