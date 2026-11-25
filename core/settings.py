"""MongoDB persistence: the multi-tenant settings manager and its client options."""

import asyncio
import copy
import logging
import os
import re
from typing import Any, Dict, List, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from core.config import (
    AI_HISTORY_COLLECTION_NAME,
    DEFAULT_SETTINGS,
    MONGO_COLLECTION_NAME,
    MONGO_DB_NAME,
)

log: logging.Logger = logging.getLogger("debt-collector")


# --------------------------------------------------------------------------- #
# TASK 1 — Persistence layer
# --------------------------------------------------------------------------- #


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge stored values over defaults so new default keys survive old documents."""
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


COUNTING_SAVE_TTL_S: int = 2 * 86400  # save counters are per UTC day


# Fail fast instead of hanging worker threads: a slow Atlas used to hold a call for
# up to 10 s. Options written in MONGO_URI itself take precedence over these.
MONGO_CLIENT_DEFAULTS: Dict[str, Any] = {
    "maxPoolSize": 50,
    "retryWrites": True,
    "serverSelectionTimeoutMS": 5_000,
    "connectTimeoutMS": 5_000,
    "timeoutMS": 8_000,  # client-side limit for a whole operation, retries included
}


def mongo_client_options(uri: str) -> Dict[str, Any]:
    """MONGO_CLIENT_DEFAULTS minus any option the URI already sets (case-insensitive)."""
    query: str = uri.split("?", 1)[1] if "?" in uri else ""
    in_uri: set = {
        part.split("=", 1)[0].strip().lower()
        for part in re.split(r"[&;]", query)
        if part
    }
    return {
        key: value
        for key, value in MONGO_CLIENT_DEFAULTS.items()
        if key.lower() not in in_uri
    }


class MultiTenantSettingsManager:
    def __init__(self) -> None:
        mongo_uri: Optional[str] = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise RuntimeError("MONGO_URI environment variable is not set.")
        self._client: MongoClient = MongoClient(
            mongo_uri, **mongo_client_options(mongo_uri)
        )
        self._collection: Collection = self._client[MONGO_DB_NAME][
            MONGO_COLLECTION_NAME
        ]
        self.ai_history: Collection = self._client[MONGO_DB_NAME][
            AI_HISTORY_COLLECTION_NAME
        ]
        self.cases: Collection = self._client[MONGO_DB_NAME]["mod_cases"]
        self.meta: Collection = self._client[MONGO_DB_NAME]["bot_meta"]
        self.reminders: Collection = self._client[MONGO_DB_NAME]["reminders"]
        self.backups: Collection = self._client[MONGO_DB_NAME]["server_backups"]
        self.counting: Collection = self._client[MONGO_DB_NAME]["counting"]
        self.counting_saves: Collection = self._client[MONGO_DB_NAME]["counting_saves"]
        self._cache: Dict[int, Dict[str, Any]] = {}
        self._warming: set = set()
        self._cache_hits: int = 0
        self._cache_misses: int = 0

    def get_settings(self, guild_id: int) -> Dict[str, Any]:
        if guild_id in self._cache:
            return self._cache[guild_id]

        settings: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS)
        settings["guildid"] = str(guild_id)
        try:
            doc: Optional[Dict[str, Any]] = self._collection.find_one(
                {"guildid": str(guild_id)}
            )
        except PyMongoError as exc:
            log.error("Mongo read failed for guild %s: %s", guild_id, exc)
            return settings

        if doc is not None:
            doc.pop("_id", None)
            _deep_merge(settings, doc)

        self._cache[guild_id] = settings
        return settings

    def update_settings(self, guild_id: int, payload: Dict[str, Any]) -> bool:
        current: Dict[str, Any] = self.get_settings(guild_id)
        current.update(copy.deepcopy(payload))
        current["guildid"] = str(guild_id)
        self._cache[guild_id] = current
        try:
            self._collection.update_one(
                {"guildid": str(guild_id)},
                {"$set": {**payload, "guildid": str(guild_id)}},
                upsert=True,
            )
            return True
        except PyMongoError as exc:
            log.error("Mongo upsert failed for guild %s: %s", guild_id, exc)
            self.evict_cache(guild_id)
            return False

    def update_fields(self, guild_id: int, fields: Dict[str, Any]) -> bool:
        """Write individual (optionally dotted) fields without replacing whole subdocuments."""
        current: Dict[str, Any] = self.get_settings(guild_id)
        for path, value in fields.items():
            node: Dict[str, Any] = current
            parts: List[str] = path.split(".")
            for part in parts[:-1]:
                nxt = node.get(part)
                if not isinstance(nxt, dict):
                    nxt = {}
                    node[part] = nxt
                node = nxt
            node[parts[-1]] = copy.deepcopy(value)
        self._cache[guild_id] = current
        try:
            self._collection.update_one(
                {"guildid": str(guild_id)},
                {"$set": {**fields, "guildid": str(guild_id)}},
                upsert=True,
            )
            return True
        except PyMongoError as exc:
            log.error("Mongo field update failed for guild %s: %s", guild_id, exc)
            self.evict_cache(guild_id)
            return False

    async def push_fields(self, guild_id: int, fields: Dict[str, Any]) -> bool:
        return await asyncio.to_thread(self.update_fields, guild_id, fields)

    def get_meta(self, key: str) -> Optional[Any]:
        try:
            doc = self.meta.find_one({"key": key})
        except PyMongoError as exc:
            log.error("Mongo meta read failed for %s: %s", key, exc)
            return None
        return (doc or {}).get("value")

    def set_meta(self, key: str, value: Any) -> None:
        try:
            self.meta.update_one({"key": key}, {"$set": {"value": value}}, upsert=True)
        except PyMongoError as exc:
            log.error("Mongo meta write failed for %s: %s", key, exc)

    def ensure_indexes(self) -> None:
        for collection, spec, options in (
            (self._collection, "guildid", {}),
            (self.ai_history, "channel_id", {"unique": True}),
            (self.ai_history, "updated_at", {"expireAfterSeconds": 30 * 86400}),
            (self.meta, "key", {"unique": True}),
            (self.cases, "guildid", {}),
            (self.cases, "target_id", {}),
            (self.reminders, "due_at", {}),
            (self.reminders, "user_id", {}),
            (self.backups, "createdAt", {"expireAfterSeconds": 259200}),
            (self.counting, "guild_id", {}),
            (
                self.counting_saves,
                [("guild_id", 1), ("user_id", 1), ("date", 1)],
                {},
            ),
            (
                self.counting_saves,
                "created_at",
                {"expireAfterSeconds": COUNTING_SAVE_TTL_S},
            ),
        ):
            try:
                collection.create_index(spec, **options)
            except PyMongoError as exc:
                log.warning(
                    "Index on %s.%s not created: %s", collection.name, spec, exc
                )

    def evict_cache(self, guild_id: int) -> None:
        self._cache.pop(guild_id, None)

    async def fetch_settings(self, guild_id: int) -> Dict[str, Any]:
        if guild_id in self._cache:
            return self._cache[guild_id]
        return await asyncio.to_thread(self.get_settings, guild_id)

    async def push_settings(self, guild_id: int, payload: Dict[str, Any]) -> bool:
        return await asyncio.to_thread(self.update_settings, guild_id, payload)

    def peek_settings(self, guild_id: int) -> Dict[str, Any]:
        """Cache-only read. Never touches the network, so it is safe on the event loop.

        On a miss it returns a defaults copy and schedules an off-loop warm-up.
        """
        cached: Optional[Dict[str, Any]] = self._cache.get(guild_id)
        if cached is not None:
            self._cache_hits += 1
            return cached

        self._cache_misses += 1
        if guild_id not in self._warming:
            self._warming.add(guild_id)
            try:
                asyncio.get_running_loop().create_task(self._warm(guild_id))
            except RuntimeError:
                self._warming.discard(guild_id)

        fallback: Dict[str, Any] = copy.deepcopy(DEFAULT_SETTINGS)
        fallback["guildid"] = str(guild_id)
        return fallback

    async def _warm(self, guild_id: int) -> None:
        try:
            await asyncio.to_thread(self.get_settings, guild_id)
        except PyMongoError as exc:
            log.error("Settings warm-up failed for guild %s: %s", guild_id, exc)
        finally:
            self._warming.discard(guild_id)

    async def ping(self) -> bool:
        """Off-loop database liveness probe."""
        try:
            await asyncio.to_thread(self._client.admin.command, "ping")
            return True
        except PyMongoError as exc:
            log.warning("Database ping failed: %s", exc)
            return False

    def close(self) -> None:
        """Close the Mongo client (blocking; call from a thread)."""
        self._client.close()

    def cache_stats(self) -> Dict[str, int]:
        return {
            "entries": len(self._cache),
            "hits": self._cache_hits,
            "misses": self._cache_misses,
        }
