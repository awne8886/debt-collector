"""Download the pinned wireproxy release into ./bin/ at build time.

Usage (from the repository root):

    python scripts/fetch_wireproxy.py [destination-directory]

The version and SHA-256 come from utils/warp.py, so this can't drift from what the
bot would download at runtime. Point WIREPROXY_PATH at the printed path so the bot
doesn't download wireproxy (10 MB) on every boot.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from utils import warp  # noqa: E402


def main(argv: list) -> int:
    destination = os.path.abspath(argv[1] if len(argv) > 1 else os.path.join(ROOT, "bin"))
    os.makedirs(destination, exist_ok=True)
    machine = warp._machine()
    if machine not in warp._WIREPROXY_ASSETS:
        print(f"No pinned wireproxy build for {machine}; skipping.", file=sys.stderr)
        return 0
    target = os.path.join(destination, "wireproxy")
    try:
        warp.download_wireproxy(target, machine)
    except Exception as exc:  # a build should not fail because of an optional binary
        print(f"wireproxy download failed: {exc}", file=sys.stderr)
        return 1
    print(target)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
