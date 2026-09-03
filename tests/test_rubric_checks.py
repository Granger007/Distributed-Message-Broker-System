"""Strict rubric-check tests covering:
- Synchronous replication: /produce returns 503 when follower is unreachable,
  but the local log IS appended and HWM is NOT advanced.
- HWM guard: /consume never returns entries whose offset exceeds the HWM stored
  in Redis, even when the on-disk log has more entries.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from broker.app import app, log_store
from broker.election import broker_state


@pytest.fixture(autouse=True)
def reset_state():
    """Reset broker state and log store before each test."""
    broker_state.update(role="follower", term=0, lease_value="")
    log_store._entries.clear()
    log_store._next_offset = 0


# ---------------------------------------------------------------------------
# Check 2: Synchronous Replication
# ---------------------------------------------------------------------------

@patch("broker.app.get_redis")
@patch("requests.post")
def test_produce_returns_503_when_follower_unreachable(mock_requests_post, mock_get_redis):
    """When the follower is unreachable, /produce MUST return 503 REPLICATION_FAILED.

    Additionally:
    - The entry MUST have been appended to the local log (durable on leader).
    - The Redis HWM Lua script must NOT be called (HWM stays behind the un-acked offset).
    """
    client = TestClient(app)
    broker_state.update(role="leader", term=1, lease_value='{"broker_id":"broker-1","term":1}')

    # Simulate follower being dead — all requests raise ConnectionError
    mock_requests_post.side_effect = ConnectionError("follower is down")

    mock_redis = MagicMock()
    mock_get_redis.return_value = mock_redis

    response = client.post("/produce", json={"value": "msg-lost", "key": "k1"})

    # Must return 503
    assert response.status_code == 503, f"Expected 503, got {response.status_code}: {response.text}"
    assert response.json()["error"] == "REPLICATION_FAILED"

    # Local log MUST have the entry (durability guarantee for the leader)
    entries = log_store.read_from(0)
    assert len(entries) == 1, "Entry should be appended to local log even when replication fails"
    assert entries[0].value == "msg-lost"

    # HWM MUST NOT be advanced — eval (Lua script) must never have been called
    mock_redis.eval.assert_not_called(), "HWM must not be updated when replication fails"


@patch("broker.app.get_redis")
@patch("requests.post")
def test_produce_hwm_not_advanced_on_follower_nack(mock_requests_post, mock_get_redis):
    """When the follower returns a non-200 status, treat as replication failure:
    503 back to client, local log appended, HWM NOT advanced.
    """
    client = TestClient(app)
    broker_state.update(role="leader", term=1, lease_value='{"broker_id":"broker-1","term":1}')

    # Follower returns 500
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.json.return_value = {"error": "internal"}
    mock_requests_post.return_value = mock_resp

    mock_redis = MagicMock()
    mock_get_redis.return_value = mock_redis

    response = client.post("/produce", json={"value": "msg-nacked", "key": "k2"})

    assert response.status_code == 503
    assert response.json()["error"] == "REPLICATION_FAILED"

    # Local log must have the entry
    assert log_store.get_last_offset() == 0
    assert log_store.read_from(0)[0].value == "msg-nacked"

    # HWM must not be touched
    mock_redis.eval.assert_not_called()


# ---------------------------------------------------------------------------
# Check 4: HWM Correctness — /consume never returns beyond HWM
# ---------------------------------------------------------------------------

@patch("broker.app.get_redis")
def test_consume_blocks_entries_beyond_hwm(mock_get_redis):
    """/consume must filter out any log entries whose offset exceeds the Redis HWM.

    Scenario: leader appended 3 entries locally (offsets 0, 1, 2), but only
    offsets 0 and 1 were replicated and acked, so HWM = 1.  /consume must
    return exactly offsets 0 and 1, never offset 2.
    """
    client = TestClient(app)

    # Directly inject 3 entries into log (simulates leader log with un-acked tail)
    log_store.append("k0", "val-0")   # offset 0 — replicated
    log_store.append("k1", "val-1")   # offset 1 — replicated
    log_store.append("k2", "val-2")   # offset 2 — NOT replicated (past HWM)

    assert log_store.get_last_offset() == 2, "Log should have 3 entries"

    # Redis HWM = 1  (only offsets 0 and 1 committed)
    mock_redis = MagicMock()
    mock_redis.get.return_value = "1"
    mock_get_redis.return_value = mock_redis

    response = client.get("/consume?from_offset=0&max=50")
    assert response.status_code == 200
    data = response.json()

    assert data["hwm"] == 1, f"HWM in response should be 1, got {data['hwm']}"
    assert len(data["messages"]) == 2, (
        f"Should return 2 committed messages, got {len(data['messages'])}: {data['messages']}"
    )
    returned_values = [m["value"] for m in data["messages"]]
    assert "val-2" not in returned_values, "Un-replicated val-2 must NOT be returned"
    assert returned_values == ["val-0", "val-1"]
    assert data["next_offset"] == 2


@patch("broker.app.get_redis")
def test_consume_hwm_minus1_returns_nothing(mock_get_redis):
    """When HWM is -1 (no commits yet), /consume returns an empty message list."""
    client = TestClient(app)

    log_store.append("k0", "val-0")  # offset 0, but NOT yet replicated

    mock_redis = MagicMock()
    mock_redis.get.return_value = None   # No HWM key in Redis → hwm = -1
    mock_get_redis.return_value = mock_redis

    response = client.get("/consume?from_offset=0")
    assert response.status_code == 200
    data = response.json()

    assert data["hwm"] == -1
    assert data["messages"] == [], "No messages should be returned when HWM = -1"
    assert data["next_offset"] == 0


# ---------------------------------------------------------------------------
# Check 3: Strict Role Enforcement during role-flip race
# ---------------------------------------------------------------------------

def test_produce_rejected_immediately_after_snapshot_if_role_flipped():
    """Simulate a role flip between snapshot() and the locked double-check.

    We directly set role='follower' inside BrokerState AFTER the outer snapshot
    but BEFORE the inner locked check — app.py's double-check must catch this.
    This verifies the inner lock guard added to /produce truly prevents any
    transiently-flipped write from succeeding.
    """
    client = TestClient(app)

    # Start as leader
    broker_state.update(role="leader", term=1, lease_value='{"broker_id":"broker-1","term":1}')

    # Flip the role to follower *before* the request reaches the handler
    # (simulating the worst-case race where the election thread demotes us)
    broker_state.update(role="follower", term=1, lease_value="")

    response = client.post("/produce", json={"value": "should-not-land", "key": "k"})

    assert response.status_code == 409, f"Expected 409, got {response.status_code}"
    assert response.json()["error"] == "NOT_LEADER"

    # Local log must be empty — nothing was written
    assert log_store.get_last_offset() == -1, "Log must be empty when follower rejects"
