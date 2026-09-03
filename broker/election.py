"""Redis-lease-based leader election for Yak brokers.

Uses the atomic ``SET key value NX EX ttl`` pattern to claim the lease, and
a Lua script for safe renewal (only extends TTL if the value still belongs to
this broker).  A background daemon thread calls ``acquire_or_renew_lease``
every ``LEASE_TTL_SECONDS / 2`` and updates the shared ``broker_state``.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import redis

from broker.config import cfg

logger = logging.getLogger("yak.election")

# ---------------------------------------------------------------------------
# Lua script: renew TTL only if the current value matches.
# KEYS[1] = lease key
# ARGV[1] = expected value (must match what's stored)
# ARGV[2] = new TTL in seconds
# Returns 1 on success, 0 if the value didn't match (someone else owns it).
# ---------------------------------------------------------------------------
_RENEW_LUA = """
local current = redis.call("GET", KEYS[1])
if current == ARGV[1] then
    redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
    return 1
else
    return 0
end
"""


# ---------------------------------------------------------------------------
# Shared mutable state visible to FastAPI request handlers
# ---------------------------------------------------------------------------
@dataclass
class BrokerState:
    """In-memory snapshot of this broker's current election state."""

    role: str = "follower"          # "leader" | "follower"
    term: int = 0                   # monotonically increasing per successful acquisition
    lease_value: str = ""           # the value we wrote into the lease key
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def update(self, *, role: str, term: int, lease_value: str) -> None:
        with self._lock:
            self.role = role
            self.term = term
            self.lease_value = lease_value

    def snapshot(self) -> dict:
        with self._lock:
            return {"role": self.role, "term": self.term, "lease_value": self.lease_value}


# Module-level singleton — imported by app.py and anywhere else that needs it.
broker_state = BrokerState()


# ---------------------------------------------------------------------------
# Core election primitive
# ---------------------------------------------------------------------------
def _make_lease_value(broker_id: str, term: int) -> str:
    """Encode broker identity + fencing token into the lease value."""
    return json.dumps({"broker_id": broker_id, "term": term, "nonce": uuid.uuid4().hex[:8]})


def parse_lease_value(raw: str) -> dict:
    """Decode a lease value written by ``_make_lease_value``."""
    return json.loads(raw)


def acquire_or_renew_lease(r: redis.Redis, broker_id: str) -> bool:
    """Try to become (or stay) leader via the Redis lease.

    Returns ``True`` if this broker is the leader after the call, ``False``
    otherwise.

    The three branches:
    1. Key absent / expired → ``SET key value NX EX ttl`` (atomic claim).
    2. Key present, owned by *us* → Lua ``EXPIRE`` only if value matches.
    3. Key present, owned by someone else → return ``False``.
    """
    lease_key = cfg.lease_key
    ttl = cfg.lease_ttl_seconds

    current_value: Optional[str] = r.get(lease_key)

    if current_value is None:
        # ---- Branch 1: lease is free, try to claim it --------------------
        new_term = broker_state.term + 1
        new_value = _make_lease_value(broker_id, new_term)
        ok = r.set(lease_key, new_value, nx=True, ex=ttl)
        if ok:
            broker_state.update(role="leader", term=new_term, lease_value=new_value)
            return True
        # Another broker beat us to it between GET and SET NX — fall through.
        return False

    # Key exists — parse it to see who owns it.
    try:
        owner = parse_lease_value(current_value)
    except (json.JSONDecodeError, KeyError):
        # Corrupted value; don't touch it, treat as "owned by someone else".
        return False

    if owner["broker_id"] == broker_id:
        # ---- Branch 2: we own it, renew TTL atomically -------------------
        renewed = r.eval(_RENEW_LUA, 1, lease_key, current_value, str(ttl))
        if renewed == 1:
            # Keep the same term; we're just extending our existing lease.
            broker_state.update(role="leader", term=owner["term"], lease_value=current_value)
            return True
        # Value changed between our GET and the Lua EVAL — lost the lease.
        return False

    # ---- Branch 3: someone else holds the lease --------------------------
    return False


# ---------------------------------------------------------------------------
# Background election loop
#
# SYSTEM INVARIANT:
# The ElectionLoop background thread is the SOLE path responsible for updating
# broker_state.role. No API endpoint, HTTP handler, or external trigger may
# directly flip self_role. All role transitions MUST originate from the lease
# state observed during this periodic loop.
# ---------------------------------------------------------------------------
class ElectionLoop:
    """Daemon thread that periodically attempts to acquire / renew the lease."""

    def __init__(self, r: redis.Redis, broker_id: str, interval: float) -> None:
        self._redis = r
        self._broker_id = broker_id
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="yak-election")
        self._thread.start()
        logger.info("[%s] election loop started (interval=%.1fs)", self._broker_id, self._interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval * 2)

    def _run(self) -> None:
        previous_role = broker_state.role
        while not self._stop_event.is_set():
            try:
                is_leader = acquire_or_renew_lease(self._redis, self._broker_id)
                new_role = "leader" if is_leader else "follower"

                if new_role != previous_role:
                    snap = broker_state.snapshot()
                    if new_role == "leader":
                        from broker.app import log_store
                        resuming_offset = log_store.get_last_offset() + 1
                        logger.info(
                            "[%s] promoted to LEADER (term=%d), resuming at offset %d",
                            self._broker_id, snap["term"], resuming_offset,
                        )
                    else:
                        # We lost leadership — update state explicitly.
                        broker_state.update(role="follower", term=snap["term"], lease_value="")
                        logger.info(
                            "[%s] lost leadership, becoming FOLLOWER",
                            self._broker_id,
                        )
                    previous_role = new_role

            except redis.RedisError:
                logger.exception("[%s] Redis error during election tick", self._broker_id)

            self._stop_event.wait(self._interval)


# Convenience: module-level instance (created lazily by app.py on startup).
_election_loop: Optional[ElectionLoop] = None


def start_election(r: redis.Redis) -> ElectionLoop:
    """Create and start the election loop.  Called once from the FastAPI lifespan.

    Renewal interval is set to TTL / 3 (≈1.67 s for the default 5 s TTL).
    This keeps the interval well under TTL, leaving a comfortable ≥2× margin
    before lease expiry even if one renewal tick is delayed by GC or I/O.
    Compare: TTL/2 = 2.5 s only gives a 2.5 s safety window, which is too
    tight when network latency and GC pauses are considered.
    """
    global _election_loop
    interval = max(1.0, cfg.lease_ttl_seconds / 3.0)
    _election_loop = ElectionLoop(r, cfg.broker_id, interval)
    _election_loop.start()
    return _election_loop


def stop_election() -> None:
    """Gracefully stop the election loop."""
    if _election_loop is not None:
        _election_loop.stop()
