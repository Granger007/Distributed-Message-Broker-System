"""Unit tests for Phase 1 & 3 — Leader Election & Lease Dynamics using FakeRedis."""

import time
import pytest
import fakeredis

from broker.config import cfg
from broker.election import acquire_or_renew_lease, broker_state


@pytest.fixture(autouse=True)
def reset_broker_state():
    """Reset the global broker_state before each test."""
    broker_state.update(role="follower", term=0, lease_value="")


def test_election_first_broker_wins_loser_fails():
    """Requirement 4 (a & b):
    - First broker to call acquire_or_renew_lease wins (returns True).
    - Second broker calling acquire_or_renew_lease loses (returns False).
    """
    r = fakeredis.FakeRedis(decode_responses=True)

    # Broker 1 attempts to claim lease
    won_b1 = acquire_or_renew_lease(r, "broker-1")
    assert won_b1 is True
    snap_b1 = broker_state.snapshot()
    assert snap_b1["role"] == "leader"
    assert snap_b1["term"] == 1

    # Broker 2 attempts to claim lease while broker 1 holds it
    won_b2 = acquire_or_renew_lease(r, "broker-2")
    assert won_b2 is False


def test_election_renewal_by_owner_succeeds():
    """Requirement 4 (d):
    A broker renewing its own lease within TTL never gets rejected.
    """
    r = fakeredis.FakeRedis(decode_responses=True)

    # Broker 1 claims lease initially
    assert acquire_or_renew_lease(r, "broker-1") is True
    term1 = broker_state.snapshot()["term"]

    # Broker 1 renews its lease
    assert acquire_or_renew_lease(r, "broker-1") is True
    snap_renew = broker_state.snapshot()
    assert snap_renew["role"] == "leader"
    assert snap_renew["term"] == term1  # Term remains same on renewal


def test_election_lease_expiry_allows_reacquisition():
    """Requirement 4 (c):
    After TTL expiry with no renewal, the lease key expires and can be claimed by another broker ID.
    """
    r = fakeredis.FakeRedis(decode_responses=True)

    # Broker 1 claims lease
    assert acquire_or_renew_lease(r, "broker-1") is True

    # Fast forward / expire key in Redis
    r.delete(cfg.lease_key)

    # Broker 2 attempts to claim expired lease
    won_b2 = acquire_or_renew_lease(r, "broker-2")
    assert won_b2 is True
    snap_b2 = broker_state.snapshot()
    assert snap_b2["role"] == "leader"
    assert snap_b2["term"] == 2
