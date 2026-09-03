"""Pydantic models shared by all nodes in the Yak distributed message broker."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    """A single record stored in the append-only log."""

    offset: int
    key: Optional[str] = None
    value: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProduceRequest(BaseModel):
    """Payload sent by a producer to publish a message."""

    value: str
    key: Optional[str] = None


class ProduceResponse(BaseModel):
    """Acknowledgement returned after a successful produce."""

    offset: int
    committed: bool


class ReplicateRequest(BaseModel):
    """Payload the leader sends to a follower for log replication."""

    offset: int
    value: str
    key: Optional[str] = None


class LeaderInfo(BaseModel):
    """Metadata about the current leader."""

    leader_url: str
    term: int


class ConsumeResponse(BaseModel):
    """Batch of messages returned to a consumer."""

    messages: list[Message] = Field(default_factory=list)
    next_offset: int
    hwm: int  # high-water mark – latest committed offset
