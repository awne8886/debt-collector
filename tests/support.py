"""Helpers shared by the tests (not a test module)."""

import contextlib
import sys
from typing import Any, Iterator
from unittest.mock import patch


@contextlib.contextmanager
def patch_bot(fake: Any) -> Iterator[Any]:
    """Replace the `bot` singleton in every module that imported it.

    Code that uses the bot is split across main, core.* and features.* modules, and
    each one looks `bot` up in its own globals, so a path that crosses modules needs
    all of them patched.
    """
    with contextlib.ExitStack() as stack:
        for name, module in list(sys.modules.items()):
            if module is None:
                continue
            if name == "main" or name.startswith(("core.", "features.")):
                if "bot" in vars(module):
                    stack.enter_context(patch.object(module, "bot", fake))
        yield fake
