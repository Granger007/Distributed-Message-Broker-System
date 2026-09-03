"""Singleton Redis client constructed from environment variables."""

from __future__ import annotations

import os

import redis

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    """Return a shared redis.Redis instance (created once, reused thereafter).

    Reads connection parameters from the environment:
      - REDIS_HOST  (default: "localhost")
      - REDIS_PORT  (default: 6379)
    """
    global _client
    if _client is None:
        host = os.environ.get("REDIS_HOST", "localhost")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        _client = redis.Redis(host=host, port=port, decode_responses=True)
    return _client
