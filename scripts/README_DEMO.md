# Live Failover Demo Sequence

This document describes the exact sequence for performing a live demonstration of Yak's distributed failover and data durability capabilities.

## Setup
Ensure you have Redis running in your environment (e.g., WSL) on `localhost:6379`. Open 4 separate terminal windows. In each terminal, navigate to the `yak` project root and activate the virtual environment (`source .venv/bin/activate`).

## Step 1: Start the Nodes and Clients

**Terminal A (System 1 - Leader broker):**
```bash
REDIS_HOST=localhost BROKER_ID=broker-1 ROLE=leader HOST=127.0.0.1 PORT=8001 PEER_URL=http://127.0.0.1:8002 uvicorn broker.app:app --port 8001
```

**Terminal B (System 2 - Follower broker):**
```bash
REDIS_HOST=localhost BROKER_ID=broker-2 ROLE=follower HOST=127.0.0.1 PORT=8002 PEER_URL=http://127.0.0.1:8001 uvicorn broker.app:app --port 8002
```

**Terminal D (System 4 - Consumer):** (Start this before the producer so we can watch it read)
```bash
python -m consumer.consumer_cli follow --interval 1.0
```

**Terminal C (System 3 - Producer):**
```bash
python -m producer.producer_cli batch --count 40 --delay 0.5
```

## Step 2: The Manual Crash (Failover Trigger)
While the producer is sending messages (around msg-15 or so), quickly switch to **Terminal A (Leader)** and crash the process by pressing `Ctrl+C` or using `kill -9 <pid>`.

*(Note: If you are re-running the demo without restarting Redis, `broker-2` might actually hold the lease. You can confirm the active leader beforehand via `curl localhost:8001/metadata/leader`)*

## Step 3: Expected Observable Sequence

You should observe the following sequence of events live in your terminals:

1. **Terminal B (Follower):** You will see logs indicating that the `yak:leader:lease` expired, followed by `broker-2` successfully acquiring the lease and self-promoting to the `leader` role.
2. **Terminal C (Producer):** The producer will log a connection failure (or a `409 NOT_LEADER` / `503 REPLICATION_FAILED` if the crash happened midway). It will then log that it is "Rediscovering leader...", find `broker-2`, and seamlessly resume sending the remaining messages exactly where it left off, with **no gaps** in the numbered messages.
3. **Terminal D (Consumer):** The consumer will similarly experience a brief failure polling the dead node, log the failover, re-discover the new leader (`broker-2`), and resume consuming until it catches up to `msg-39`. 

## Step 4: Verification

Once the producer finishes sending all 40 messages (msg-0 through msg-39), run the verification script to prove zero data loss:

```bash
python scripts/verify_no_loss.py
```

This will assert that every single message exists exactly once and in the correct order in the final leader's local on-disk log (`./data/broker-2/log.jsonl`).

## Re-running the Demo

To cleanly reset the state of the cluster (clearing on-disk logs, the consumer offset, and the Redis keys), run:
```bash
bash scripts/reset_demo.sh
```
