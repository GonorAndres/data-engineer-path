#!/bin/bash
# ---------------------------------------------------------------------------
# Local development script for the Streaming Claims Intake pipeline.
#
# Starts the Pub/Sub emulator, creates topics/subscriptions, launches the
# Flask subscriber, and runs the claims simulator.
#
# Prerequisites:
#   - gcloud CLI with Pub/Sub emulator component:
#       gcloud components install pubsub-emulator
#   - Python venv activated with project dependencies installed:
#       python -m venv .venv && source .venv/bin/activate
#       pip install -e ".[dev]"
#
# Usage:
#   chmod +x scripts/run_local.sh
#   ./scripts/run_local.sh
# ---------------------------------------------------------------------------

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-local-project}"
EMULATOR_PORT="${EMULATOR_PORT:-8085}"
SUBSCRIBER_PORT="${SUBSCRIBER_PORT:-8080}"
SIMULATOR_RATE="${SIMULATOR_RATE:-3}"
SIMULATOR_DURATION="${SIMULATOR_DURATION:-30}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    echo -e "\n${YELLOW}Shutting down...${NC}"
    # Kill background processes
    if [[ -n "${EMULATOR_PID:-}" ]]; then
        kill "$EMULATOR_PID" 2>/dev/null || true
        echo "  Stopped Pub/Sub emulator (PID $EMULATOR_PID)"
    fi
    if [[ -n "${SUBSCRIBER_PID:-}" ]]; then
        kill "$SUBSCRIBER_PID" 2>/dev/null || true
        echo "  Stopped subscriber (PID $SUBSCRIBER_PID)"
    fi
    echo -e "${GREEN}Cleanup complete.${NC}"
}

trap cleanup EXIT

echo -e "${GREEN}=== Streaming Claims Intake - Local Dev ===${NC}"
echo "Project ID: $PROJECT_ID"
echo ""

# ---------------------------------------------------------------------------
# 1. Start Pub/Sub emulator
# ---------------------------------------------------------------------------
echo -e "${YELLOW}[1/4] Starting Pub/Sub emulator on port $EMULATOR_PORT...${NC}"
gcloud beta emulators pubsub start --project="$PROJECT_ID" --host-port="localhost:$EMULATOR_PORT" &
EMULATOR_PID=$!
sleep 3

export PUBSUB_EMULATOR_HOST="localhost:$EMULATOR_PORT"
echo "  PUBSUB_EMULATOR_HOST=$PUBSUB_EMULATOR_HOST"

# ---------------------------------------------------------------------------
# 2. Create topics and subscriptions
# ---------------------------------------------------------------------------
echo -e "${YELLOW}[2/4] Creating Pub/Sub topics and subscriptions...${NC}"
python src/pubsub_setup.py --project "$PROJECT_ID"

# ---------------------------------------------------------------------------
# 3. Start the Flask subscriber
# ---------------------------------------------------------------------------
echo -e "${YELLOW}[3/4] Starting Flask subscriber on port $SUBSCRIBER_PORT...${NC}"
export PROJECT_ID
export FLASK_APP=src/subscriber.py

# In local mode without BigQuery, the subscriber will fail on BQ writes.
# Set BQ_DATASET to a dummy value; for full local testing, use BigQuery
# emulator or mock the BQ client.
export BQ_DATASET="claims_raw"
export DLQ_TOPIC="claims-events-dlq"

flask run --port "$SUBSCRIBER_PORT" &
SUBSCRIBER_PID=$!
sleep 2

echo "  Subscriber running at http://localhost:$SUBSCRIBER_PORT"
echo "  Health check: http://localhost:$SUBSCRIBER_PORT/health"

# ---------------------------------------------------------------------------
# 4. Run the simulator
# ---------------------------------------------------------------------------
echo -e "${YELLOW}[4/4] Running claims simulator (rate=${SIMULATOR_RATE}/s, duration=${SIMULATOR_DURATION}s)...${NC}"
python src/claims_simulator.py \
    --project "$PROJECT_ID" \
    --topic claims-events \
    --rate "$SIMULATOR_RATE" \
    --duration "$SIMULATOR_DURATION"

echo ""
echo -e "${GREEN}=== Simulation complete ===${NC}"
echo "Events were published to the emulator. In a full setup, the push"
echo "subscription would forward them to the subscriber at /push."
echo ""
echo "To manually test the subscriber:"
echo "  curl -X POST http://localhost:$SUBSCRIBER_PORT/push \\"
echo "    -H 'Content-Type: application/json' \\"
echo '    -d '"'"'{"message":{"data":"'$(echo '{"claim_id":"test","policy_id":"POL-123","accident_date":"2026-01-15","cause_of_loss":"colision","estimated_amount":50000,"coverage_type":"auto_colision","timestamp":"2026-01-15T12:00:00Z"}' | base64)'"}}'\'
echo ""
echo "Press Ctrl+C to stop all services."
wait
