"""Deterministic fake clock. No real sleeps anywhere in tests."""

from __future__ import annotations


class FakeClock:
    def __init__(self, start_mono: float = 1000.0, start_utc: float = 1_700_000_000.0) -> None:
        self.mono = start_mono
        self.utc = start_utc

    @property
    def now_mono(self) -> float:
        return self.mono

    @property
    def now_utc(self) -> float:
        return self.utc

    def advance(self, seconds: float, utc_seconds: float | None = None) -> None:
        self.mono += seconds
        self.utc += seconds if utc_seconds is None else utc_seconds
