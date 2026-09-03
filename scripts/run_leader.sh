#!/usr/bin/env bash
# Start the Yak leader broker on port 8001.
set -euo pipefail

export BROKER_ID="broker-1"
export ROLE="leader"
export HOST="127.0.0.1"
export PORT="8001"
export PEER_URL="http://127.0.0.1:8002"

echo "Starting Yak leader  (broker_id=$BROKER_ID, port=$PORT)"
python3 -m uvicorn broker.app:app --host "$HOST" --port "$PORT" --reload
