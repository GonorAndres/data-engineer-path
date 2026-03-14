"""Set up Pub/Sub topics and subscriptions for the claims intake pipeline.

Creates the necessary Pub/Sub infrastructure:
- claims-events topic (main ingest)
- claims-events-push subscription (for Cloud Run subscriber)
- claims-events-pull subscription (for Dataflow batch reads)
- claims-events-dlq topic (dead-letter)
- claims-events-dlq-pull subscription (for monitoring/replay)

Works with the Pub/Sub emulator:
    export PUBSUB_EMULATOR_HOST=localhost:8085
    python src/pubsub_setup.py --project my-project

Or against real GCP:
    python src/pubsub_setup.py --project my-gcp-project --push-endpoint https://my-service-xyz.run.app/push
"""

from __future__ import annotations

import argparse
import logging
import os

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


def create_topic(publisher: pubsub_v1.PublisherClient, project: str, topic_id: str) -> str:
    """Create a Pub/Sub topic if it does not already exist.

    Returns the fully qualified topic path.
    """
    topic_path = publisher.topic_path(project, topic_id)
    try:
        publisher.create_topic(request={"name": topic_path})
        logger.info("Created topic: %s", topic_path)
    except AlreadyExists:
        logger.info("Topic already exists: %s", topic_path)
    return topic_path


def create_subscription(
    subscriber: pubsub_v1.SubscriberClient,
    project: str,
    topic_path: str,
    subscription_id: str,
    push_endpoint: str | None = None,
    ack_deadline_seconds: int = 60,
) -> str:
    """Create a Pub/Sub subscription (push or pull).

    Args:
        subscriber: SubscriberClient instance.
        project: GCP project ID.
        topic_path: Fully qualified topic path.
        subscription_id: Subscription name.
        push_endpoint: If set, creates a push subscription.
        ack_deadline_seconds: ACK deadline (default 60s for batch processing).

    Returns:
        Fully qualified subscription path.
    """
    sub_path = subscriber.subscription_path(project, subscription_id)

    request = {
        "name": sub_path,
        "topic": topic_path,
        "ack_deadline_seconds": ack_deadline_seconds,
    }

    if push_endpoint:
        request["push_config"] = {"push_endpoint": push_endpoint}

    try:
        subscriber.create_subscription(request=request)
        sub_type = "push" if push_endpoint else "pull"
        logger.info("Created %s subscription: %s", sub_type, sub_path)
    except AlreadyExists:
        logger.info("Subscription already exists: %s", sub_path)

    return sub_path


def setup_all(project: str, push_endpoint: str | None = None) -> None:
    """Create all topics and subscriptions for the claims pipeline.

    Args:
        project: GCP project ID.
        push_endpoint: Cloud Run URL for push subscription (e.g. https://...run.app/push).
            If None, creates pull-only subscriptions (suitable for emulator/local dev).
    """
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    emulator = os.environ.get("PUBSUB_EMULATOR_HOST")
    if emulator:
        logger.info("Using Pub/Sub emulator at %s", emulator)

    # --- Main claims topic ---
    claims_topic = create_topic(publisher, project, "claims-events")

    # Push subscription (for Cloud Run subscriber)
    if push_endpoint:
        create_subscription(
            subscriber,
            project,
            claims_topic,
            "claims-events-push",
            push_endpoint=push_endpoint,
        )
    else:
        # In emulator mode, create as pull (no Cloud Run endpoint)
        create_subscription(
            subscriber,
            project,
            claims_topic,
            "claims-events-push",
        )
        logger.info(
            "No --push-endpoint provided; created claims-events-push as pull subscription "
            "(use pull for local dev, push for Cloud Run deployment)"
        )

    # Pull subscription (for Dataflow batch reads)
    create_subscription(
        subscriber,
        project,
        claims_topic,
        "claims-events-pull",
        ack_deadline_seconds=120,  # Longer deadline for batch processing
    )

    # --- Dead-letter topic ---
    dlq_topic = create_topic(publisher, project, "claims-events-dlq")

    # DLQ pull subscription (for monitoring and replay)
    create_subscription(
        subscriber,
        project,
        dlq_topic,
        "claims-events-dlq-pull",
    )

    logger.info("Pub/Sub setup complete for project: %s", project)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Set up Pub/Sub topics and subscriptions")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--push-endpoint",
        default=None,
        help="Cloud Run push endpoint URL (omit for emulator/local dev)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    setup_all(project=args.project, push_endpoint=args.push_endpoint)


if __name__ == "__main__":
    main()
