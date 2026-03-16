#!/usr/bin/env bash
# run_local.sh -- Run the streaming claims pipeline locally with Pub/Sub emulator.
#
# Prerequisites:
#   - gcloud CLI with Pub/Sub emulator component installed
#   - Python venv with project dependencies installed
#
# What this script does:
#   1. Starts the Pub/Sub emulator on localhost:8085
#   2. Creates topic and subscription
#   3. Starts the streaming pipeline on DirectRunner (background)
#   4. Runs the simulator to publish events
#   5. Cleans up on exit
#
# Usage:
#   cd projects/05-streaming-claims-pipeline
#   chmod +x scripts/run_local.sh
#   ./scripts/run_local.sh

set -euo pipefail

PROJECT_ID="local-streaming-test"
TOPIC="claims-events"
SUBSCRIPTION="claims-events-sub"
EMULATOR_HOST="localhost:8085"

# Trap to clean up background processes on exit
cleanup() {
    echo "[run_local] Cleaning up..."
    kill "$EMULATOR_PID" 2>/dev/null || true
    kill "$PIPELINE_PID" 2>/dev/null || true
    echo "[run_local] Done."
}
trap cleanup EXIT

echo "========================================"
echo " P05: Streaming Claims Pipeline (Local)"
echo "========================================"

# 1. Start Pub/Sub emulator
echo "[run_local] Starting Pub/Sub emulator on ${EMULATOR_HOST}..."
gcloud beta emulators pubsub start --host-port="${EMULATOR_HOST}" &
EMULATOR_PID=$!
sleep 3

export PUBSUB_EMULATOR_HOST="${EMULATOR_HOST}"

# 2. Create topic and subscription
echo "[run_local] Creating topic '${TOPIC}' and subscription '${SUBSCRIPTION}'..."
python -c "
from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('${PROJECT_ID}', '${TOPIC}')
try:
    publisher.create_topic(request={'name': topic_path})
    print(f'  Created topic: {topic_path}')
except Exception as e:
    print(f'  Topic exists or error: {e}')

subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path('${PROJECT_ID}', '${SUBSCRIPTION}')
try:
    subscriber.create_subscription(request={'name': sub_path, 'topic': topic_path})
    print(f'  Created subscription: {sub_path}')
except Exception as e:
    print(f'  Subscription exists or error: {e}')
"

# 3. Start the streaming pipeline (DirectRunner, background)
echo "[run_local] Starting streaming pipeline on DirectRunner..."
python src/streaming_pipeline.py \
    --runner DirectRunner \
    --input_subscription "projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION}" \
    --output_project "${PROJECT_ID}" \
    --output_dataset "claims_analytics" \
    --window_size_seconds 60 \
    --early_firing_interval_seconds 10 \
    --allowed_lateness_seconds 120 &
PIPELINE_PID=$!
sleep 2

# 4. Run the simulator
echo "[run_local] Starting simulator (60s, 5 events/sec, 10% late, 15% OOO)..."
python src/streaming_simulator.py \
    --project "${PROJECT_ID}" \
    --topic "${TOPIC}" \
    --rate 5 \
    --duration 60 \
    --late-rate 0.10 \
    --out-of-order-rate 0.15

echo "[run_local] Simulator finished. Waiting for pipeline to process remaining events..."
sleep 5

echo "[run_local] Local run complete."
