#!/bin/bash

echo "Resetting Demo State..."

echo "1. Clearing local data directories (./data/*)..."
rm -rf ./data/*

echo "2. Flushing Redis lease and HWM keys..."
# Assumes Redis is running on localhost:6379 without auth
docker exec yak-redis redis-cli DEL "yak:leader:lease" "yak:hwm" > /dev/null 2>&1 || redis-cli DEL "yak:leader:lease" "yak:hwm" > /dev/null 2>&1 || true

echo "3. Removing consumer offset file (if outside data directory)..."
rm -f ./consumer_offset.txt

echo "Reset complete! You can now start the brokers fresh."
