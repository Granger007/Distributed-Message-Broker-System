"""Yak broker — FastAPI application.

Provides leader election, message production with leader->follower replication,
internal replication endpoints, and safe consumption up to high-water mark (HWM).
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime

import requests
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from broker.config import cfg
from broker.election import (
    broker_state,
    parse_lease_value,
    start_election,
    stop_election,
)
from broker.log_store import AppendLog
from common.models import (
    ConsumeResponse,
    LeaderInfo,
    Message,
    ProduceRequest,
    ProduceResponse,
    ReplicateRequest,
)
from common.redis_client import get_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("yak.app")

# Shared log store instance for this broker node
log_store = AppendLog()

# Lua script to move HWM forward only
_UPDATE_HWM_LUA = """
local current = redis.call("GET", KEYS[1])
local new_hwm = tonumber(ARGV[1])
if not current or new_hwm > tonumber(current) then
    redis.call("SET", KEYS[1], tostring(new_hwm))
    return new_hwm
else
    return tonumber(current)
end
"""


# ---------------------------------------------------------------------------
# Lifespan: start / stop the election background thread
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(application: FastAPI):  # noqa: ARG001
    r = get_redis()
    logger.info(
        "[%s] starting election loop (lease_key=%s, ttl=%ds)",
        cfg.broker_id,
        cfg.lease_key,
        cfg.lease_ttl_seconds,
    )
    start_election(r)
    yield
    logger.info("[%s] shutting down election loop", cfg.broker_id)
    stop_election()


app = FastAPI(
    title="Yak Broker",
    description="Distributed message broker node",
    version="0.3.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_leader_url_best_effort() -> str | None:
    """Determine best-effort leader URL from local state or Redis."""
    snap = broker_state.snapshot()
    if snap["role"] == "leader" and snap["lease_value"]:
        return cfg.self_url

    try:
        r = get_redis()
        raw = r.get(cfg.lease_key)
        if raw:
            owner = parse_lease_value(raw)
            return cfg.broker_urls.get(owner["broker_id"])
    except Exception:
        pass

    return cfg.peer_url


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> dict:
    """Lightweight liveness / readiness probe.

    Returns the live role from the election loop.
    """
    snap = broker_state.snapshot()
    return {
        "broker_id": cfg.broker_id,
        "role": snap["role"],
        "term": snap["term"],
    }


@app.get("/metadata/leader", response_model=LeaderInfo)
def metadata_leader():
    """Return the current leader's URL and term.

    Works on both brokers regardless of who is the current leader.
    """
    snap = broker_state.snapshot()

    if snap["role"] == "leader" and snap["lease_value"]:
        owner = parse_lease_value(snap["lease_value"])
        return LeaderInfo(
            leader_url=cfg.self_url,
            term=owner["term"],
        )

    try:
        r = get_redis()
        raw = r.get(cfg.lease_key)
    except Exception:
        return JSONResponse(
            status_code=503,
            content={"detail": "Redis unavailable, cannot determine leader"},
        )

    if raw is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "no leader elected yet"},
        )

    try:
        owner = parse_lease_value(raw)
    except Exception:
        return JSONResponse(
            status_code=503,
            content={"detail": "corrupt lease value"},
        )

    leader_id = owner["broker_id"]
    leader_url = cfg.broker_urls.get(leader_id, "unknown")
    return LeaderInfo(leader_url=leader_url, term=owner["term"])


@app.post("/produce", response_model=ProduceResponse)
def produce(req: ProduceRequest):
    """Publish a message.

    Must be handled by the leader node. If called on a follower, returns
    HTTP 409 NOT_LEADER with the leader's URL.
    """
    snap = broker_state.snapshot()
    if snap["role"] != "leader":
        leader_url = get_leader_url_best_effort()
        return JSONResponse(
            status_code=409,
            content={"error": "NOT_LEADER", "leader_url": leader_url},
        )

    # Leader path: append to local log.
    # Double-check role under the state lock to close the race window between
    # the snapshot() call above and the actual log append below.  A follower
    # that won the lease between those two lines would otherwise slip through.
    with broker_state._lock:
        if broker_state.role != "leader":
            leader_url = get_leader_url_best_effort()
            return JSONResponse(
                status_code=409,
                content={"error": "NOT_LEADER", "leader_url": leader_url},
            )
        offset = log_store.append(req.key, req.value)
    logger.info(
        "[%s] [PRODUCE] Assigned offset=%d (key=%s, value=%s)",
        cfg.broker_id,
        offset,
        req.key,
        req.value,
    )

    # Synchronously replicate to follower
    replicate_url = f"{cfg.peer_url}/internal/replicate"
    payload = {"offset": offset, "value": req.value, "key": req.key}
    replicated = False

    for attempt in range(1, 4):
        try:
            logger.info(
                "[%s] [REPLICATE] Attempting replication to %s for offset=%d (attempt %d/3)",
                cfg.broker_id,
                replicate_url,
                offset,
                attempt,
            )
            resp = requests.post(replicate_url, json=payload, timeout=2.0)
            if resp.status_code == 200 and resp.json().get("acked_offset") == offset:
                replicated = True
                logger.info(
                    "[%s] [REPLICATE] ACK received from follower for offset=%d",
                    cfg.broker_id,
                    offset,
                )
                break
        except Exception as e:
            logger.warning(
                "[%s] [REPLICATE] Replication attempt %d failed: %s",
                cfg.broker_id,
                attempt,
                e,
            )
            time.sleep(0.1 * (2 ** (attempt - 1)))

    if not replicated:
        logger.error(
            "[%s] [REPLICATE] Failed to replicate offset=%d after 3 attempts",
            cfg.broker_id,
            offset,
        )
        return JSONResponse(
            status_code=503,
            content={"error": "REPLICATION_FAILED"},
        )

    # Update Redis HWM key (only moves forward)
    try:
        r = get_redis()
        r.eval(_UPDATE_HWM_LUA, 1, cfg.hwm_key, str(offset))
        logger.info(
            "[%s] [HWM] Advanced Redis HWM to offset=%d",
            cfg.broker_id,
            offset,
        )
    except Exception as e:
        logger.error("[%s] [HWM] Failed to update Redis HWM: %s", cfg.broker_id, e)

    return ProduceResponse(offset=offset, committed=True)


@app.post("/internal/replicate")
def internal_replicate(req: ReplicateRequest) -> dict:
    """Internal endpoint called by the leader to replicate log entries to followers."""
    log_store.append_with_offset(req.offset, req.key, req.value)
    logger.info(
        "[%s] [REPLICATE-ACK] Replicated and ACKed offset=%d",
        cfg.broker_id,
        req.offset,
    )
    return {"acked_offset": req.offset}


@app.get("/consume", response_model=ConsumeResponse)
def consume(from_offset: int = 0, max_messages: int = Query(default=50, alias="max")):
    """Consume committed messages starting at `from_offset`."""
    try:
        r = get_redis()
        raw_hwm = r.get(cfg.hwm_key)
        hwm = int(raw_hwm) if raw_hwm is not None else -1
    except Exception:
        hwm = -1

    entries = log_store.read_from(from_offset)
    committed_entries = [e for e in entries if e.offset <= hwm]
    batch = committed_entries[:max_messages]

    messages = [
        Message(
            offset=e.offset,
            key=e.key,
            value=e.value,
            timestamp=datetime.fromisoformat(e.timestamp),
        )
        for e in batch
    ]
    next_offset = batch[-1].offset + 1 if batch else from_offset
    return ConsumeResponse(messages=messages, next_offset=next_offset, hwm=hwm)
