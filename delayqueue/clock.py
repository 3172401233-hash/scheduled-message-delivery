"""Clock abstraction. The queue never reads wall time directly; callers
inject a clock so scheduling is deterministic and testable."""

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Current time in seconds (any monotonic-enough epoch)."""
        ...


class SystemClock:
    """Wall-clock implementation for production use."""

    def now(self) -> float:
        return time.time()


class ManualClock:
    """Deterministic clock for tests and simulations.

    Supports arbitrary jumps, including backwards jumps, to exercise
    clock-rollback behaviour without sleeping.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, delta: float) -> float:
        self._now += delta
        return self._now

    def set(self, value: float) -> None:
        self._now = float(value)
