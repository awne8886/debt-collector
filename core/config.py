"""Logging setup, environment-driven constants, superusers and the settings defaults."""

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log: logging.Logger = logging.getLogger("debt-collector")

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

ERROR_BUFFER_SIZE: int = int(os.getenv("ERROR_BUFFER_SIZE", "120"))
ERROR_PERSIST: bool = os.getenv("ERROR_PERSIST", "1") == "1"


def _parse_id_set(raw: Optional[str]) -> frozenset:
    """Parse a comma/space separated snowflake list; ignores malformed entries."""
    if not raw:
        return frozenset()
    ids: set = set()
    for chunk in re.split(r"[,\s]+", raw.strip()):
        if chunk.isdigit():
            ids.add(int(chunk))
        elif chunk:
            log.warning("Ignoring non-numeric SUPERUSER_IDS entry: %r", chunk)
    return frozenset(ids)


# --------------------------------------------------------------------------- #
# Global constants
# --------------------------------------------------------------------------- #


@dataclass
class SnipedMessage:
    author: str
    author_avatar: Optional[str]
    content: str
    created_at: datetime
    deleted_at: datetime


@dataclass
class AfkPing:
    author: str
    content: str
    timestamp: float
    jump_url: str


@dataclass
class AfkRecord:
    reason: str
    since: float
    pings: List[AfkPing] = field(default_factory=list)


AFK_GRACE_SECONDS: float = float(os.getenv("AFK_GRACE_SECONDS", "15"))
COMMAND_PREFIX: str = os.getenv("DEFAULT_PREFIX", "!")
MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "debt_collector")
MONGO_COLLECTION_NAME: str = os.getenv("MONGO_SETTINGS_COLLECTION", "guild_settings")
AI_HISTORY_COLLECTION_NAME: str = os.getenv("MONGO_AI_COLLECTION", "ai_history")

# Snowflake-only. Username matching was removed: Discord usernames are user-mutable
# and re-claimable, so name-based authorization is a privilege-escalation vector.
# Superusers skip every permission check, rate limit and owner check in every
# server, so the list is exactly what SUPERUSER_IDS says. The built-in fallback is
# only used (with a warning) when the variable is unset or holds no valid IDs.
DEFAULT_SUPERUSER_IDS: frozenset = frozenset(
    {1120393965485703219, 600689350686146562, 760531428881465366}
)


def resolve_superuser_ids(raw: Optional[str]) -> frozenset:
    """Return the superuser set for a SUPERUSER_IDS value (pure apart from logging)."""
    if raw is None or not raw.strip():
        log.warning(
            "SUPERUSER_IDS is not set; falling back to the built-in superuser list. "
            "Set SUPERUSER_IDS to control who bypasses every permission check."
        )
        return DEFAULT_SUPERUSER_IDS
    parsed = _parse_id_set(raw)
    if not parsed:
        log.warning(
            "SUPERUSER_IDS contains no valid user IDs; falling back to the built-in "
            "superuser list."
        )
        return DEFAULT_SUPERUSER_IDS
    return parsed


SUPERUSER_IDS: frozenset = resolve_superuser_ids(os.getenv("SUPERUSER_IDS"))

DEFAULT_SETTINGS: Dict[str, Any] = {
    "echoset": False,
    "autoreact": {"triggers": {}},
    "autorespond": {"enabled": False, "triggers": {}},
    "prefix": "!",
    "joinroles": [],
    "reactionroles": {},
    "remind": {
        "enabled": False,
        "interval": 181,
        "role_id": None,
        "channel_id": None,
        "message": "Reminder!",
    },
    "autopurge": {"channels": {}, "exempt_roles": []},
    "counting": {"channel_id": None, "default_saves": 3, "ruiner_role_id": None},
    "ai": {
        "enabled": False,
        "channels": [],
        "probability": 10.0,
        "cooldown": 60.0,
        "persona": "You are a helpful Discord bot.",
        "personas": {},
        "channel_personas": {},
        "optout": [],
        "ignored_users": [],
        "ignored_roles": [],
        "max_tokens": 300,
        "temperature": 0.9,
        "word_limit": 120,
        "daily_limit": 0,
        "models": {},
        "provider_order": ["openrouter", "gemini", "groq"],
    },
    "automod": {
        "invites": False,
        "links": False,
        "spam": False,
        "caps": False,
        "mentions": False,
        "spam_limit": 6,
        "mention_limit": 5,
        "exempt_roles": [],
    },
    "welcome": {
        "enabled": False,
        "channel_id": None,
        "message": "",
        "embed": False,
        "title": "",
        "color": "5865F2",
        "image": "",
        "thumbnail": True,
        "ping": True,
        "dm_message": "",
    },
    "goodbye": {
        "enabled": False,
        "channel_id": None,
        "message": "",
        "embed": False,
        "title": "",
        "color": "ED4245",
        "image": "",
        "thumbnail": True,
        "ping": False,
        "dm_message": "",
    },
    "starboard": {
        "enabled": False,
        "channel_id": None,
        "threshold": 3,
        "self_star": False,
        "allow_bots": False,
        "show_count": False,
        "blocked_emojis": [],
        "ignored_channels": [],
        "webhook_id": None,
        "webhook_token": None,
        "posted": {},
    },
    "tags": {},
    "tempbans": {},
    "sticky": {},
    "warns": {},
    "raid": {
        "enabled": False,
        "auto": True,
        "until": 0,
        "join_threshold": 8,
        "window": 20,
        "min_account_age_hours": 24,
        "action": "quarantine",
        "quarantine_role_id": None,
    },
    "quarantined": {},
    "screening": {
        "enabled": False,
        "action": "log",
        "threshold": 4,
        "min_account_age_hours": 24,
        "flag_default_avatar": True,
        "flag_no_public_flags": False,
        "created_near_ban_minutes": 60,
        "evasion_days": 30,
        "name_patterns": [],
    },
    "messagelog": {
        "enabled": False,
        "channel_id": None,
        "ignored_channels": [],
        "cache_attachments": False,
    },
    "modlog": {"enabled": False, "channel_id": None},
    "modmail": {
        "enabled": False,
        "staff_roles": [],
        "category_id": None,
        "log_channel_id": None,
        "ping_staff": True,
        "transcripts": True,
        "delete_after": 10,
        "dm_transcript": True,
        "panel_title": "Contact staff",
        "panel_message": "",
        "welcome_title": "",
        "welcome_message": "",
        "blocked": [],
        "tickets": {},
    },
}
