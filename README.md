# Yak — Distributed Message Broker Demo

Yak is a lightweight distributed message broker built with **FastAPI**, **Redis**, and plain Python. It demonstrates leader–follower replication, lease-based leader election, and simple produce/consume workflows.

## Project Layout

```
yak/
├── requirements.txt          # Python dependencies
├── common/
│   ├── models.py             # Shared Pydantic models
│   └── redis_client.py       # Singleton Redis client factory
├── broker/
│   ├── app.py                # FastAPI application (leader or follower)
│   ├── log_store.py          # Append-only JSON-lines log
│   └── config.py             # Environment-driven configuration
├── producer/
│   └── producer_cli.py       # Intelligent producer with leader discovery & retry
├── consumer/
│   └── consumer_cli.py       # Consumer with offset tracking & follow mode
├── scripts/
│   ├── run_leader.sh         # Start broker-1 as leader on :8001
│   ├── run_follower.sh       # Start broker-2 as follower on :8002
│   ├── demo_failover.sh      # Live failover demo reference commands
│   ├── reset_demo.sh         # Clean reset for re-runs
│   ├── verify_no_loss.py     # Post-demo data-loss assertion
│   └── README_DEMO.md        # Step-by-step demo narration guide
├── docs/
│   └── architecture.md       # Design decisions, trade-offs, API reference
└── tests/
    ├── test_election.py       # Leader election / lease unit tests
    ├── test_write_path.py     # Write / replicate / consume integration tests
    └── test_rubric_checks.py  # Strict rubric: replication 503, HWM guard, role race
```

## Prerequisites

| Dependency | Version | Notes |
|------------|---------|-------|
| Python     | 3.11+   |       |
| Redis      | 7+      | Run inside WSL via Docker |

### Starting Redis (WSL + Docker)

```bash
wsl docker run -d --name yak-redis -p 6379:6379 redis:7-alpine
```

## Quick Start

```bash
# 1. Create a virtualenv and install dependencies
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Start the leader broker (terminal 1)
bash scripts/run_leader.sh       # http://127.0.0.1:8001

# 3. Start the follower broker (terminal 2)
bash scripts/run_follower.sh     # http://127.0.0.1:8002

# 4. Verify
curl http://127.0.0.1:8001/health
# => {"broker_id":"broker-1","role":"leader"}

curl http://127.0.0.1:8002/health
# => {"broker_id":"broker-2","role":"follower"}
```

### Running on Windows (PowerShell)

If you don't have bash available, you can start the brokers directly:

```powershell
# Leader
$env:BROKER_ID="broker-1"; $env:ROLE="leader"; $env:PORT="8001"; $env:PEER_URL="http://127.0.0.1:8002"
uvicorn broker.app:app --host 127.0.0.1 --port 8001 --reload

# Follower (separate terminal)
$env:BROKER_ID="broker-2"; $env:ROLE="follower"; $env:PORT="8002"; $env:PEER_URL="http://127.0.0.1:8001"
uvicorn broker.app:app --host 127.0.0.1 --port 8002 --reload
```

## Configuration

All configuration is read from environment variables with local-dev defaults:

| Variable            | Default                    | Description                         |
|---------------------|----------------------------|-------------------------------------|
| `BROKER_ID`         | `broker-1`                 | Unique node identifier              |
| `ROLE`              | `leader`                   | Initial role (`leader` / `follower`)|
| `HOST`              | `127.0.0.1`                | Bind address                        |
| `PORT`              | `8001`                     | Bind port                           |
| `PEER_URL`          | `http://127.0.0.1:8002`    | Base URL of the peer broker         |
| `REDIS_HOST`        | `localhost`                | Redis server host                   |
| `REDIS_PORT`        | `6379`                     | Redis server port                   |
| `LEASE_KEY`         | `yak:leader:lease`         | Redis key for leader lease          |
| `LEASE_TTL_SECONDS` | `5`                        | Leader lease TTL in seconds         |
| `HWM_KEY`           | `yak:hwm`                  | Redis key for the high-water mark   |

## Running Tests

```bash
.venv\Scripts\python.exe -m pytest tests/ -v   # Windows
# or
python -m pytest tests/ -v                      # Linux/macOS
```

