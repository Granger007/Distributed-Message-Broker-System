"""Append-only log backed by a JSON-lines file on disk.

Each broker stores its log under ``./data/<BROKER_ID>/log.jsonl``.
On startup the log replays the file (if it exists) to recover ``last_offset``.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

from broker.config import cfg


class LogEntry(NamedTuple):
    """In-memory representation of a single log record."""

    offset: int
    key: str | None
    value: str
    timestamp: str  # ISO-8601


class AppendLog:
    """Simple offset-indexed log persisted as one JSON object per line.

    File layout (``./data/<broker_id>/log.jsonl``)::

        {"offset": 0, "key": null, "value": "hello", "timestamp": "..."}
        {"offset": 1, "key": "k1", "value": "world", "timestamp": "..."}
    """

    def __init__(self, broker_id: str | None = None) -> None:
        bid = broker_id or cfg.broker_id
        self._dir = Path("data") / bid
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / "log.jsonl"

        # In-memory mirror of the on-disk log.
        self._entries: list[LogEntry] = []
        self._next_offset: int = 0

        self._recover()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, key: str | None, value: str) -> int:
        """Append a record and return the assigned offset."""
        offset = self._next_offset
        ts = datetime.now(timezone.utc).isoformat()
        entry = LogEntry(offset=offset, key=key, value=value, timestamp=ts)
        self._entries.append(entry)
        self._next_offset = offset + 1

        # Persist to disk immediately (crash-safe enough for a demo).
        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry._asdict()) + "\n")

        return offset

    def append_with_offset(self, offset: int, key: str | None, value: str) -> bool:
        """Idempotently append an entry with a specific offset (used during replication).

        If offset already exists in the log (offset <= get_last_offset()), returns True.
        Otherwise appends the entry, updates next_offset, and persists to disk.
        """
        if offset <= self.get_last_offset():
            return True

        ts = datetime.now(timezone.utc).isoformat()
        entry = LogEntry(offset=offset, key=key, value=value, timestamp=ts)
        self._entries.append(entry)
        self._next_offset = max(self._next_offset, offset + 1)

        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry._asdict()) + "\n")

        return True

    def read_from(self, offset: int) -> list[LogEntry]:
        """Return all entries starting at *offset* (inclusive)."""
        if offset < 0 or offset >= self._next_offset:
            return []
        return list(self._entries[offset:])

    def get_last_offset(self) -> int:
        """Return the offset of the most recently appended entry, or -1 if the
        log is empty."""
        return self._next_offset - 1

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _recover(self) -> None:
        """Replay the JSONL file to rebuild in-memory state."""
        if not self._path.exists():
            return

        with open(self._path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                entry = LogEntry(
                    offset=obj["offset"],
                    key=obj.get("key"),
                    value=obj["value"],
                    timestamp=obj["timestamp"],
                )
                self._entries.append(entry)
                self._next_offset = entry.offset + 1
