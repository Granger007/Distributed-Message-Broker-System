# Yak Architecture Reference

> Quick-reference for the pen-and-paper viva. Covers every design decision, trade-off, and API shape.

---

## 1. System Overview

Yak runs as **two identical broker processes** (`broker/app.py`) whose *role* — leader or follower — is determined at runtime by a Redis lease. A third party (Redis) acts as the arbiter of who holds the lease. Clients (producer, consumer) discover the leader via `/metadata/leader` and can redirect themselves on failure.

```
┌───────────────┐          ┌──────────────────┐
│  Producer CLI │──POST──► │  Leader Broker   │──┐ /internal/replicate
└───────────────┘          │  (e.g. :8001)    │  ▼
                           └──────────────────┘ ┌────────────────────┐
┌───────────────┐          ┌──────────────────┐ │  Follower Broker   │
│  Consumer CLI │──GET───► │  Any Broker      │ │  (e.g. :8002)      │
└───────────────┘          │  /consume        │ └────────────────────┘
                           └──────────────────┘
                                    │ lease key + HWM key
                                    ▼
                           ┌──────────────────┐
                           │      Redis       │
                           └──────────────────┘
```

---

## 2. Leader Election — Redis Lease Mechanism

### How It Works

Every broker runs a **background election thread** (`ElectionLoop`) that ticks every `TTL / 3` seconds (≈1.67 s for the default 5 s TTL). Each tick calls `acquire_or_renew_lease()`:

| Condition | Action | Outcome |
|-----------|--------|---------|
| Lease key absent / expired | `SET yak:leader:lease <value> NX EX <ttl>` | Atomic claim. Winner becomes leader. |
| Key present, owned by **this** broker | Lua script: `if GET == expected then EXPIRE` | Safe renewal — TTL extended atomically |
| Key present, owned by **another** broker | No-op | Broker stays / becomes follower |

The lease value is a JSON blob: `{"broker_id": "broker-1", "term": 3, "nonce": "a1b2c3d4"}`. The `term` is a monotonically increasing fencing token.

### Why `SET key value NX EX <ttl>` and NOT `SETNX` + `EXPIRE`

`SETNX` followed by `EXPIRE` is **two separate commands**. If the process crashes (or is killed by the OS) between the two commands, the key exists with **no TTL** — it becomes an immortal lock that blocks all future leaders indefinitely, requiring manual intervention to delete it.

`SET key value NX EX ttl` is a single atomic command. The key either gets created with its TTL in one round-trip, or it doesn't exist. There is no window where the key can be left without a TTL.

### Renewal Safety

Renewal uses a **Lua script** (executed atomically on the Redis server):

```lua
local current = redis.call("GET", KEYS[1])
if current == ARGV[1] then
    redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
    return 1
else
    return 0
end
```

This prevents a race where broker A reads the key, broker B steals it, and broker A then blindly extends B's TTL thinking it still owns the lease. The Lua script's atomicity guarantees the check-and-extend happens in a single server-side step.

### Renewal Interval vs TTL

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| `LEASE_TTL_SECONDS` | 5 s | Failover detection window (follower waits at most this long to discover leader is gone) |
| Renewal interval | `TTL / 3` ≈ 1.67 s | Leaves a **≥2× safety margin** before expiry. Even if two consecutive renewals are delayed by GC pauses or I/O, the lease still survives. Using `TTL / 2` only gives a 1× margin — too tight. |

### System Invariant

> **The `ElectionLoop` background thread is the SOLE path that changes `broker_state.role`.** No API handler, HTTP request, or external signal may directly flip the role. All role transitions are derived from the lease state observed during the periodic tick.

---

## 3. Replication — Synchronous Follower ACK

### Write Path (Leader Only)

```
POST /produce
    │
    ├─ 1. Double-check role under BrokerState lock  ──► 409 if not leader
    │
    ├─ 2. Append to local log (disk write, gets offset N)
    │
    ├─ 3. POST /internal/replicate {offset, key, value} to peer
    │      ├─ Success (200 + acked_offset == N)
    │      │       └─► 4. Update Redis HWM to N via Lua (only moves forward)
    │      │                └─► 5. Return 200 ProduceResponse {offset, committed: true}
    │      └─ Failure after 3 retries
    │               └─► Return 503 REPLICATION_FAILED
    │                   (local log has entry, HWM is NOT advanced)
```

### Why Synchronous Replication?

**Durability guarantee**: The leader only advances the High-Water Mark (HWM) *after* the follower has confirmed it persisted the entry. This means:
- Consumers only ever read **fully-replicated** data.
- If the leader crashes immediately after returning `200`, the follower already has the entry — no data loss.

**The cost**: Every produce call adds a follower round-trip to its latency (≈1–5 ms on localhost, higher on a WAN). This is the classic **latency vs. durability trade-off**:
- Async replication → lower latency, risk of data loss on leader crash.
- Sync replication → higher latency, zero data loss on single-node failure.

### Role Race Guard

The `/produce` handler performs a **double-check**:
1. **Outer check**: `broker_state.snapshot()` — fast, unlocked read.
2. **Inner check**: Inside `broker_state._lock` — prevents a role flip that occurs between the outer snapshot and the actual `log_store.append()` call from silently succeeding.

---

## 4. High-Water Mark (HWM) — Read-Your-Writes Safety

The HWM is stored as a single Redis key (`yak:hwm`) containing the offset of the **last fully-replicated message**.

### Update Rule

HWM is advanced using a Lua script that only moves it **forward** (never backwards):

```lua
local current = redis.call("GET", KEYS[1])
local new_hwm = tonumber(ARGV[1])
if not current or new_hwm > tonumber(current) then
    redis.call("SET", KEYS[1], tostring(new_hwm))
    return new_hwm
else
    return tonumber(current)
end
```

This means even if a stale leader attempts to update HWM, it cannot reduce it and confuse consumers.

### Consume Filtering

`GET /consume` reads the HWM from Redis on every request and **filters the local log**:

```python
committed_entries = [e for e in entries if e.offset <= hwm]
```

Even if the local JSONL log has extra entries (e.g., a leader appended before replication failed), consumers **never see** un-replicated data. This enforces **read-your-writes consistency** — a consumer is guaranteed that any message it reads has been durably replicated.

---

## 5. API Reference

### `POST /produce`

Publish a message. Must be called on the leader.

**Request body:**
```json
{ "value": "hello", "key": "optional-key" }
```

**Responses:**

| Status | Body | Meaning |
|--------|------|---------|
| 200 | `{"offset": 5, "committed": true}` | Written and replicated |
| 409 | `{"error": "NOT_LEADER", "leader_url": "http://..."}` | Wrong node; redirect to `leader_url` |
| 503 | `{"error": "REPLICATION_FAILED"}` | Leader wrote locally but follower unreachable; safe to retry with idempotency key |

---

### `POST /internal/replicate`

Internal endpoint called by the leader to push log entries to the follower. Not for external use.

**Request body:**
```json
{ "offset": 5, "key": "optional-key", "value": "hello" }
```

**Response:**
```json
{ "acked_offset": 5 }
```

The follower appends idempotently — if `offset` already exists in its log, it returns ACK without re-writing.

---

### `GET /consume`

Read committed messages from the log.

**Query parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `from_offset` | int | 0 | First offset to read (inclusive) |
| `max` | int | 50 | Maximum messages to return |

**Response:**
```json
{
  "messages": [
    {"offset": 3, "key": "k1", "value": "hello", "timestamp": "2024-..."},
    {"offset": 4, "key": null, "value": "world", "timestamp": "2024-..."}
  ],
  "next_offset": 5,
  "hwm": 7
}
```

- `messages` contains only entries with `offset <= hwm`.
- `next_offset` is the offset to pass in the next poll call.
- `hwm` is the current high-water mark (latest committed offset cluster-wide).

---

### `GET /metadata/leader`

Discover who the current leader is. Works on any broker.

**Response (200):**
```json
{ "leader_url": "http://127.0.0.1:8001", "term": 3 }
```

**Response (503):** If Redis is unreachable or no leader has been elected yet.

---

### `GET /health`

Liveness probe. Always returns 200.

**Response:**
```json
{ "broker_id": "broker-1", "role": "leader", "term": 3 }
```

---

## 6. Failover Sequence (Timeline)

```
t=0   Leader (broker-1) crashes / is killed
t=0   Redis lease key still exists with TTL=5s remaining
t≤5s  Follower (broker-2) election tick fires:
        → GET yak:leader:lease → key present, owned by broker-1
        → return False (stay follower)
t=5s  Redis TTL expires, lease key deleted
t≤5+1.67s  Follower election tick fires:
        → GET yak:leader:lease → key absent
        → SET yak:leader:lease {broker-2, term=N+1} NX EX 5 → OK
        → broker_state.role = "leader"
        → log: "[broker-2] promoted to LEADER (term=N+1)"
t≈7s  Producer gets ConnectionError or 503 on next produce attempt
        → calls discover_leader() → /metadata/leader on broker-2 → "http://...8002"
        → retries produce against broker-2 → success
t≈7s  Consumer similarly fails then rediscovers broker-2 and resumes polling
```

Maximum failover time = lease TTL + one election interval = 5 + 1.67 ≈ **6.67 seconds**.
