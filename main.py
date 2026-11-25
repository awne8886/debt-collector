"""Entrypoint. Render runs `python main.py`.

Importing core.bot creates the bot. Importing each feature module registers that
feature's commands, listeners, loops and views on it, so the import order below is
fixed: setup_hook starts the loops in registration order (core/registry.py), and
features.pipeline, which overrides on_message, comes last. No module imports main.
"""

import core.bot  # noqa: F401  # creates the bot singleton
import core.checks  # noqa: F401  # global checks: rate limit, staff gate
import core.modlog  # noqa: F401  # after-invoke mod-log hook
import features.reactions  # noqa: F401
import features.help  # noqa: F401
import features.errors  # noqa: F401
import features.counting  # noqa: F401
import features.roles  # noqa: F401
import features.utility  # noqa: F401
import features.purge  # noqa: F401
import features.autopurge  # noqa: F401
import features.server_settings  # noqa: F401
import features.reminders  # noqa: F401
import features.automations  # noqa: F401
import features.ai  # noqa: F401
import features.ai_commands  # noqa: F401
import features.sticky  # noqa: F401
import features.moderation  # noqa: F401
import features.raid  # noqa: F401
import features.quarantine  # noqa: F401
import features.automod  # noqa: F401
import features.screening  # noqa: F401
import features.starboard  # noqa: F401
import features.remindme  # noqa: F401
import features.messagelog  # noqa: F401
import features.modmail  # noqa: F401
import features.greeter  # noqa: F401
import features.server_clone  # noqa: F401
import features.maintenance  # noqa: F401
import features.info  # noqa: F401
import features.pipeline  # noqa: F401
from core.runner import run

if __name__ == "__main__":
    run()
