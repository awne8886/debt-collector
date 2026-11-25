"""Startup registrations that feature modules make when they are imported.

DebtCollectorBot.setup_hook (core/bot.py) reads these lists and never imports feature
modules itself. Each list fills in import order, which main.py fixes; append to them,
never rebind them.
"""

import logging
from typing import Any, List

log: logging.Logger = logging.getLogger("debt-collector")

# tasks.Loop objects. setup_hook starts each one that is not running, in this order.
STARTUP_LOOPS: List[Any] = []
# Zero-argument view factories (the View classes). setup_hook calls add_view(factory()).
PERSISTENT_VIEWS: List[Any] = []
# discord.ui.DynamicItem classes. setup_hook calls add_dynamic_items(*DYNAMIC_ITEMS).
DYNAMIC_ITEMS: List[Any] = []
