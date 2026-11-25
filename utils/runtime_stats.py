"""Process-level numbers for logs, /diagnose and /health: memory and event-loop lag."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from collections import deque
from typing import Deque, Dict, Optional

_MB = 1024 * 1024


def rss_bytes() -> Optional[int]:
    """Current resident set size, from /proc/self/statm; getrusage's peak as a fallback."""
    try:
        with open("/proc/self/statm", "rb") as handle:
            resident_pages = int(handle.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError, AttributeError):
        pass
    return peak_rss_bytes()


def peak_rss_bytes() -> Optional[int]:
    """Peak resident set size of this process (ru_maxrss), or None if unavailable."""
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ImportError, OSError, ValueError):
        return None
    # Linux reports KiB, macOS reports bytes.
    return int(peak) if sys.platform == "darwin" else int(peak) * 1024


def to_mb(value: Optional[int]) -> Optional[float]:
    return None if value is None else round(value / _MB, 1)


def thread_count() -> Optional[int]:
    try:
        with open("/proc/self/status", "rb") as handle:
            for line in handle:
                if line.startswith(b"Threads:"):
                    return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    try:
        import threading

        return threading.active_count()
    except Exception:
        return None


class LoopLagMonitor:
    """Samples how late the event loop wakes up, to spot blocking calls.

    Every ``interval`` seconds it sleeps and records how much longer than asked the
    sleep took. A healthy loop shows a few milliseconds; a synchronous database call
    or CPU-heavy work on the loop shows up as hundreds or thousands.
    """

    def __init__(self, interval: float = 2.0, window: int = 150) -> None:
        self.interval: float = interval
        self.samples: Deque[float] = deque(maxlen=window)
        self.task: Optional["asyncio.Task[None]"] = None

    def record(self, lag_seconds: float) -> None:
        self.samples.append(max(0.0, lag_seconds))

    async def run(self) -> None:
        while True:
            started = time.perf_counter()
            await asyncio.sleep(self.interval)
            self.record(time.perf_counter() - started - self.interval)

    def start(self) -> None:
        if self.task is None or self.task.done():
            self.task = asyncio.get_running_loop().create_task(
                self.run(), name="loop-lag-monitor"
            )

    def stop(self) -> None:
        if self.task is not None:
            self.task.cancel()
            self.task = None

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def stats(self) -> Dict[str, Optional[float]]:
        if not self.samples:
            return {"last_ms": None, "p50_ms": None, "max_ms": None, "window_s": 0.0}
        ordered = sorted(self.samples)
        return {
            "last_ms": round(self.samples[-1] * 1000, 1),
            "p50_ms": round(ordered[len(ordered) // 2] * 1000, 1),
            "max_ms": round(ordered[-1] * 1000, 1),
            "window_s": round(len(self.samples) * self.interval, 0),
        }
