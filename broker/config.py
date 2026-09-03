"""Environment-driven configuration for a Yak broker node."""

from __future__ import annotations

import os


class BrokerConfig:
    """Reads broker configuration from environment variables with sensible
    local-development defaults.

    Environment variables
    ---------------------
    BROKER_ID          Unique identifier for this broker (default: "broker-1")
    ROLE               Initial role: "leader" or "follower" (default: "leader")
                       Note: the role can change at runtime during leader election.
    HOST               Bind address (default: "127.0.0.1")
    PORT               Bind port   (default: 8001)
    PEER_URL           Base URL of the other broker (default: "http://127.0.0.1:8002")
    LEASE_KEY          Redis key used for the leader lease (default: "yak:leader:lease")
    LEASE_TTL_SECONDS  TTL in seconds for the leader lease (default: 5)
    HWM_KEY            Redis key for the high-water mark (default: "yak:hwm")
    """

    def __init__(self) -> None:
        self.broker_id: str = os.environ.get("BROKER_ID", "broker-1")
        self.role: str = os.environ.get("ROLE", "leader")
        self.host: str = os.environ.get("HOST", "127.0.0.1")
        self.port: int = int(os.environ.get("PORT", "8001"))
        self.peer_url: str = os.environ.get("PEER_URL", "http://127.0.0.1:8002")
        self.lease_key: str = os.environ.get("LEASE_KEY", "yak:leader:lease")
        self.lease_ttl_seconds: int = int(os.environ.get("LEASE_TTL_SECONDS", "5"))
        self.hwm_key: str = os.environ.get("HWM_KEY", "yak:hwm")

        # Derived: this broker's own base URL and a broker_id → URL lookup
        # used by /metadata/leader to resolve the lease owner to a URL.
        self.self_url: str = f"http://{self.host}:{self.port}"
        self.broker_urls: dict[str, str] = self._build_broker_url_map()

    def _build_broker_url_map(self) -> dict[str, str]:
        """Build a broker_id → base_url map from env hints.

        We always know our own id→url.  For the peer we extract its id from
        ``PEER_ID`` (with a sensible default derived from ``BROKER_ID``).
        """
        peer_id = os.environ.get("PEER_ID", "broker-2" if self.broker_id == "broker-1" else "broker-1")
        return {
            self.broker_id: self.self_url,
            peer_id: self.peer_url,
        }

    def __repr__(self) -> str:
        return (
            f"BrokerConfig(broker_id={self.broker_id!r}, role={self.role!r}, "
            f"host={self.host!r}, port={self.port}, peer_url={self.peer_url!r})"
        )


# Module-level singleton so other modules can simply ``from broker.config import cfg``.
cfg = BrokerConfig()
