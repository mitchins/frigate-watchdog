"""In-memory store double implementing the engine's StoreView protocol.

The real store lives in frigate_watchdog.store; this fake mirrors its
latch/accounting semantics for deterministic engine tests. It also records
every mutation so tests can assert on persisted accounting, not private
engine attributes.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass

from frigate_watchdog.constants import (
    BUDGET_WINDOW_S,
    MIN_ATTEMPT_INTERVAL_S,
)


@dataclass(frozen=True)
class FakeIncident:
    incident_id: str
    camera: str
    latched: bool = False
    resolved: bool = False


@dataclass(frozen=True)
class FakeAccounting:
    attempts_in_window: int
    last_attempt_at: float | None
    cooldown_until: float


class FakeStore:
    def __init__(self) -> None:
        self.acc = 0.0  # accumulated-runtime clock, advanced by the harness
        self._armed: set[str] = set()
        self.incidents: dict[str, FakeIncident] = {}  # camera -> latest
        self.all_incidents: list[FakeIncident] = []
        self.attempts: list[tuple[str, float, str | None]] = []  # (camera, at_acc, outcome)
        self.auth_latches: set[str] = set()
        self.ack_log: list[str] = []
        self._ids = itertools.count(1)

    # --- StoreView protocol ---

    def armed(self, camera: str) -> bool:
        return camera in self._armed

    def incident(self, camera: str) -> FakeIncident | None:
        return self.incidents.get(camera)

    def accounting(self, camera: str) -> FakeAccounting:
        times = [at for cam, at, _ in self.attempts if cam == camera]
        recent = [at for at in times if self.acc - at <= BUDGET_WINDOW_S]
        last = max(times) if times else None
        cooldown = (last + MIN_ATTEMPT_INTERVAL_S) if last is not None else 0.0
        return FakeAccounting(len(recent), last, cooldown)

    def last_attempt_any(self) -> float | None:
        return max((at for _, at, _ in self.attempts), default=None)

    def auth_latched(self, camera: str) -> bool:
        return camera in self.auth_latches

    # --- mutations (mirrors of the real store's write API) ---

    def mark_armed(self, camera: str) -> None:
        self._armed.add(camera)

    def open_incident(self, camera: str) -> FakeIncident:
        inc = FakeIncident(incident_id=f"inc-{next(self._ids)}", camera=camera)
        self.incidents[camera] = inc
        self.all_incidents.append(inc)
        return inc

    def resolve_incident(self, camera: str) -> None:
        inc = self.incidents.get(camera)
        if inc is not None:
            self.incidents[camera] = FakeIncident(
                inc.incident_id, camera, latched=False, resolved=True
            )

    def reserve_attempt(self, camera: str) -> str:
        from frigate_watchdog.store import StoreError

        inc = self.incidents.get(camera)
        if inc is None or inc.resolved:
            inc = self.open_incident(camera)
        elif inc.latched:
            raise StoreError("refusing to reserve: this outage already consumed its attempt")
        self.incidents[camera] = FakeIncident(inc.incident_id, camera, latched=True)
        attempt_id = f"att-{next(self._ids)}"
        self.attempts.append((camera, self.acc, None))
        return attempt_id

    def record_outcome(self, camera: str, outcome: str) -> None:
        cam, at, _ = self.attempts[-1]
        assert cam == camera
        self.attempts[-1] = (cam, at, outcome)

    def latch_incident(self, camera: str) -> None:
        inc = self.incidents.get(camera)
        if inc is not None:
            self.incidents[camera] = FakeIncident(inc.incident_id, camera, latched=True)

    def auth_latch(self, camera: str) -> None:
        self.auth_latches.add(camera)

    def acknowledge(self, camera: str) -> None:
        self.ack_log.append(camera)
        inc = self.incidents.get(camera)
        if inc is not None:
            self.incidents[camera] = FakeIncident(
                inc.incident_id, camera, latched=False, resolved=True
            )
        self.auth_latches.discard(camera)
