#!/usr/bin/env bash
# Start the Yak follower broker on port 8002.
set -euo pipefail

export BROKER_ID="broker-2"
export ROLE="follower"
export HOST="127.0.0.1"
export PORT="8002"
export PEER_URL="http://127.0.0.1:8001"

echo "Starting Yak follower  (broker_id=$BROKER_ID, port=$PORT)"
python3 -m uvicorn broker.app:app --host "$HOST" --port "$PORT" --reload
