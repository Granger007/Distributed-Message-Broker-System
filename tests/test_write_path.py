"""Unit and integration tests for Phase 2 — Write Path (produce, replicate, consume)."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from broker.app import app, log_store
from broker.config import cfg
from broker.election import broker_state
from common.models import ProduceRequest, ReplicateRequest


@pytest.fixture(autouse=True)
def reset_state():
    """Reset broker state and log store before each test."""
    broker_state.update(role="follower", term=0, lease_value="")
    log_store._entries.clear()
    log_store._next_offset = 0


@patch("broker.app.get_redis")
def test_produce_on_follower_returns_409(mock_get_redis):
    """Follower must reject /produce with 409 NOT_LEADER and best-known leader_url."""
    client = TestClient(app)
    broker_state.update(role="follower", term=1, lease_value="")

    # Mock Redis returning lease owned by broker-2
    mock_redis = MagicMock()
    mock_redis.get.return_value = '{"broker_id": "broker-2", "term": 1}'
    mock_get_redis.return_value = mock_redis

    response = client.post("/produce", json={"value": "hello", "key": "k1"})
    assert response.status_code == 409
    data = response.json()
    assert data["error"] == "NOT_LEADER"
    assert data["leader_url"] == cfg.broker_urls.get("broker-2")


def test_internal_replicate_appends_log():
    """Follower endpoint /internal/replicate appends log idempotently."""
    client = TestClient(app)

    response = client.post("/internal/replicate", json={"offset": 0, "value": "val1", "key": "k1"})
    assert response.status_code == 200
    assert response.json() == {"acked_offset": 0}

    assert log_store.get_last_offset() == 0
    entries = log_store.read_from(0)
    assert len(entries) == 1
    assert entries[0].value == "val1"

    # Idempotent re-replicate
    response2 = client.post("/internal/replicate", json={"offset": 0, "value": "val1", "key": "k1"})
    assert response2.status_code == 200
    assert response2.json() == {"acked_offset": 0}
    assert len(log_store.read_from(0)) == 1


@patch("broker.app.get_redis")
@patch("requests.post")
def test_produce_on_leader_success(mock_requests_post, mock_get_redis):
    """Leader /produce appends log, replicates to peer, updates HWM, and returns 200."""
    client = TestClient(app)
    broker_state.update(role="leader", term=1, lease_value='{"broker_id": "broker-1", "term": 1}')

    # Mock peer replicate response
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"acked_offset": 0}
    mock_requests_post.return_value = mock_resp

    # Mock redis eval for HWM
    mock_redis = MagicMock()
    mock_get_redis.return_value = mock_redis

    response = client.post("/produce", json={"value": "msg1", "key": "k1"})
    assert response.status_code == 200
    data = response.json()
    assert data == {"offset": 0, "committed": True}

    # Verify replication request was made
    mock_requests_post.assert_called_once()
    _, kwargs = mock_requests_post.call_args
    assert kwargs["json"] == {"offset": 0, "value": "msg1", "key": "k1"}

    # Verify Redis HWM update
    mock_redis.eval.assert_called_once()


@patch("broker.app.get_redis")
def test_consume_filters_by_hwm(mock_get_redis):
    """GET /consume returns only messages with offset <= HWM."""
    client = TestClient(app)

    # Directly add entries to log
    log_store.append("k1", "committed_val")
    log_store.append("k2", "uncommitted_val")

    # Mock Redis HWM to 0 (only first message committed)
    mock_redis = MagicMock()
    mock_redis.get.return_value = "0"
    mock_get_redis.return_value = mock_redis

    response = client.get("/consume?from_offset=0")
    assert response.status_code == 200
    data = response.json()
    assert data["hwm"] == 0
    assert len(data["messages"]) == 1
    assert data["messages"][0]["value"] == "committed_val"
    assert data["messages"][0]["offset"] == 0
    assert data["next_offset"] == 1
