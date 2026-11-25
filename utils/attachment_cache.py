"""A byte-budgeted, TTL-bounded LRU for attachments of recently sent messages.

The message log can re-upload images and videos of deleted messages, which needs the
bytes before Discord's CDN link dies. Those bytes used to live in an unbounded dict
that was only cleared after 500 messages, which could reach gigabytes on a 512 MB
instance. This cache keeps a hard byte budget instead.
"""

from __future__ import annotations

import os
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional

DEFAULT_MAX_MB = 24.0
DEFAULT_TTL_S = 21600.0
MAX_FILE_BYTES = 7 * 1024 * 1024


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


class AttachmentCache:
    """message_id -> list of {filename, content_type, data}, oldest evicted first."""

    def __init__(
        self,
        max_bytes: Optional[int] = None,
        ttl_s: Optional[float] = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_bytes is None:
            max_bytes = int(_env_float("ATTACHMENT_CACHE_MB", DEFAULT_MAX_MB) * 1024 * 1024)
        if ttl_s is None:
            ttl_s = _env_float("ATTACHMENT_CACHE_TTL_S", DEFAULT_TTL_S)
        self.max_bytes: int = max(0, int(max_bytes))
        self.ttl_s: float = float(ttl_s)
        self.per_file_limit: int = min(MAX_FILE_BYTES, self.max_bytes // 4)
        self._clock = clock
        # message_id -> {"items": [...], "bytes": int, "at": float}
        self._entries: "OrderedDict[int, Dict[str, Any]]" = OrderedDict()
        self._bytes: int = 0
        self.hits: int = 0
        self.misses: int = 0
        self.evictions: int = 0
        self.expired: int = 0
        self.skipped: int = 0

    # ------------------------------------------------------------------ helpers
    def accepts(self, size: Optional[int]) -> bool:
        """Whether a file of ``size`` bytes may be cached at all."""
        return size is not None and 0 < size <= self.per_file_limit

    def _drop(self, message_id: int) -> Optional[Dict[str, Any]]:
        entry = self._entries.pop(message_id, None)
        if entry is not None:
            self._bytes -= entry["bytes"]
        return entry

    def _expire(self) -> None:
        if self.ttl_s <= 0:
            return
        cutoff = self._clock() - self.ttl_s
        # Entries are kept in insertion/touch order, but TTL counts from the first
        # put, so scan everything (the cache only ever holds a few dozen messages).
        for message_id in [m for m, e in self._entries.items() if e["at"] < cutoff]:
            self._drop(message_id)
            self.expired += 1

    def _evict(self) -> None:
        while self._bytes > self.max_bytes and self._entries:
            message_id = next(iter(self._entries))
            self._drop(message_id)
            self.evictions += 1

    # ---------------------------------------------------------------------- API
    def put(
        self,
        message_id: int,
        filename: str,
        content_type: Optional[str],
        data: bytes,
    ) -> bool:
        """Add one attachment for ``message_id``. Returns False if it was refused."""
        self._expire()
        size = len(data)
        if not self.accepts(size):
            self.skipped += 1
            return False
        entry = self._entries.get(message_id)
        if entry is None:
            entry = {"items": [], "bytes": 0, "at": self._clock()}
            self._entries[message_id] = entry
        entry["items"].append(
            {"filename": filename, "content_type": content_type, "data": data}
        )
        entry["bytes"] += size
        self._bytes += size
        self._entries.move_to_end(message_id)
        self._evict()
        return message_id in self._entries

    def get(self, message_id: int) -> List[Dict[str, Any]]:
        self._expire()
        entry = self._entries.get(message_id)
        if entry is None:
            self.misses += 1
            return []
        self.hits += 1
        self._entries.move_to_end(message_id)
        return list(entry["items"])

    def pop(self, message_id: int) -> List[Dict[str, Any]]:
        """Remove and return the attachments for ``message_id`` (empty if none)."""
        self._expire()
        entry = self._drop(message_id)
        if entry is None:
            self.misses += 1
            return []
        self.hits += 1
        return entry["items"]

    def clear(self) -> None:
        self._entries.clear()
        self._bytes = 0

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, message_id: object) -> bool:
        return message_id in self._entries

    @property
    def bytes(self) -> int:
        return self._bytes

    def stats(self) -> Dict[str, int]:
        return {
            "entries": len(self._entries),
            "bytes": self._bytes,
            "max_bytes": self.max_bytes,
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "expired": self.expired,
            "skipped": self.skipped,
        }
