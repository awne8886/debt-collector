"""Structured error recorder: fingerprinted, deduplicated, optionally persisted."""

import asyncio
import hashlib
import logging
import re
import time
import traceback as _traceback
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import discord
from pymongo.errors import PyMongoError

from core.config import ERROR_BUFFER_SIZE, ERROR_PERSIST

log: logging.Logger = logging.getLogger("debt-collector")


_ERROR_DIGIT_RE = re.compile(r"\d{2,}")


@dataclass
class ErrorRecord:
    """One deduplicated failure signature with its full context."""

    fingerprint: str
    where: str
    exc_type: str
    message: str
    stack: str
    first_seen: float
    last_seen: float
    count: int = 1
    guild_id: Optional[int] = None
    guild_name: Optional[str] = None
    channel_id: Optional[int] = None
    user_id: Optional[int] = None
    user_name: Optional[str] = None
    command: Optional[str] = None

    def short_id(self) -> str:
        return self.fingerprint[:8]

    def summary(self) -> str:
        scope: str = self.guild_name or (
            "global" if self.guild_id is None else str(self.guild_id)
        )
        badge: str = f" x{self.count}" if self.count > 1 else ""
        tail: str = f" - `{self.command}`" if self.command else ""
        return (
            f"`{self.short_id()}` <t:{int(self.last_seen)}:R> - **{self.where}**{badge}\n"
            f"\u2514 `{self.exc_type}: {self.message[:140]}`\n"
            f"\u2514 {scope}{tail}"
        )


class ErrorRecorder:
    """Fingerprinted, deduplicated, optionally persisted error buffer."""

    def __init__(self, capacity: int = ERROR_BUFFER_SIZE) -> None:
        self._records: "OrderedDict[str, ErrorRecord]" = OrderedDict()
        self._capacity: int = max(10, capacity)
        self._total: int = 0
        self._started: float = time.time()
        self._bot: Any = None  # set by bind(); nothing is persisted before that

    def bind(self, bot: Any) -> None:
        """Attach the bot whose database the records are persisted to."""
        self._bot = bot

    @staticmethod
    def _fingerprint(where: str, exc_type: str, message: str) -> str:
        normalized: str = _ERROR_DIGIT_RE.sub("#", message)[:200]
        return hashlib.sha256(
            f"{where}|{exc_type}|{normalized}".encode("utf-8")
        ).hexdigest()

    def record(
        self,
        where: str,
        err: Any,
        *,
        guild: Optional[discord.Guild] = None,
        channel: Optional[Any] = None,
        user: Optional[discord.abc.User] = None,
        command: Optional[str] = None,
    ) -> ErrorRecord:
        now: float = time.time()
        self._total += 1

        if isinstance(err, BaseException):
            exc_type: str = type(err).__name__
            message: str = str(err) or exc_type
            stack: str = "".join(
                _traceback.format_exception(type(err), err, err.__traceback__)
            )[-3500:]
        else:
            exc_type = "Message"
            message = str(err)
            stack = ""

        fingerprint: str = self._fingerprint(where, exc_type, message)
        existing: Optional[ErrorRecord] = self._records.get(fingerprint)
        if existing is not None:
            existing.count += 1
            existing.last_seen = now
            if stack:
                existing.stack = stack
            self._records.move_to_end(fingerprint)
            record: ErrorRecord = existing
        else:
            record = ErrorRecord(
                fingerprint=fingerprint,
                where=where,
                exc_type=exc_type,
                message=message[:600],
                stack=stack,
                first_seen=now,
                last_seen=now,
                guild_id=guild.id if guild is not None else None,
                guild_name=guild.name if guild is not None else None,
                channel_id=getattr(channel, "id", None),
                user_id=getattr(user, "id", None),
                user_name=str(user) if user is not None else None,
                command=command,
            )
            self._records[fingerprint] = record
            while len(self._records) > self._capacity:
                self._records.popitem(last=False)

        if ERROR_PERSIST:
            self._schedule_persist(record)
        return record

    def _schedule_persist(self, record: ErrorRecord) -> None:
        runner: Any = self._bot
        if runner is None or not hasattr(runner, "spawn"):
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        runner.spawn(self._persist(record), name=f"errpersist:{record.short_id()}")

    async def _persist(self, record: ErrorRecord) -> None:
        try:
            await asyncio.to_thread(
                self._bot.settings.meta.update_one,
                {"key": f"error:{record.fingerprint}"},
                {
                    "$set": {
                        "value": {
                            "where": record.where,
                            "exc_type": record.exc_type,
                            "message": record.message,
                            "guild_id": record.guild_id,
                            "command": record.command,
                            "last_seen": record.last_seen,
                        }
                    },
                    "$inc": {"occurrences": 1},
                },
                True,
            )
        except PyMongoError as exc:
            log.debug("Error persistence failed: %s", exc)

    def recent(
        self,
        *,
        where: Optional[str] = None,
        guild_id: Optional[int] = None,
    ) -> List[ErrorRecord]:
        items: List[ErrorRecord] = list(reversed(list(self._records.values())))
        if where:
            needle: str = where.casefold()
            items = [r for r in items if needle in r.where.casefold()]
        if guild_id is not None:
            items = [r for r in items if r.guild_id in (None, guild_id)]
        return items

    def get(self, short_id: str) -> Optional[ErrorRecord]:
        needle: str = short_id.strip().casefold()
        if not needle:
            return None
        for record in self._records.values():
            if record.fingerprint.startswith(needle):
                return record
        return None

    def clear(self) -> int:
        removed: int = len(self._records)
        self._records.clear()
        return removed

    def stats(self) -> Dict[str, Any]:
        by_type: Dict[str, int] = {}
        for record in self._records.values():
            by_type[record.exc_type] = by_type.get(record.exc_type, 0) + record.count
        return {
            "unique": len(self._records),
            "total": self._total,
            "uptime": time.time() - self._started,
            "by_type": sorted(by_type.items(), key=lambda kv: kv[1], reverse=True),
        }


class ErrorRecorderHandler(logging.Handler):
    """Bridges the logging tree into the recorder so nothing lives only in stderr."""

    def __init__(self, recorder: ErrorRecorder) -> None:
        super().__init__(level=logging.ERROR)
        self._recorder: ErrorRecorder = recorder

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if record.exc_info and record.exc_info[1] is not None:
                self._recorder.record(f"log:{record.name}", record.exc_info[1])
            else:
                self._recorder.record(f"log:{record.name}", record.getMessage())
        except Exception:  # a logging handler must never raise
            pass
