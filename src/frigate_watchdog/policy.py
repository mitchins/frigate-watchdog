"""The pure decision engine.

No network, no filesystem, no sleeps, no global time. Observations, clock
values, and a read-only view of persisted safety state are passed in; the
engine returns state transitions, events, and proposed actions.

Invariants enforced here (and adversarially tested):

* No action is proposed without complete, fresh, distinct-snapshot evidence.
* No action is proposed while two or more enabled cameras are failing.
* No action is proposed without at least one stably-healthy peer.
* A latched outage never receives a second attempt; only a full
  healthy-confirmation interval or an operator acknowledgement clears it.
* Every guard is re-evaluated at dispatch time; stale proposals cancel.
"""

from __future__ import annotations

import itertools
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from .config import WatchdogConfig
from .constants import (
    DAILY_ATTEMPT_BUDGET,
    GLOBAL_MIN_ATTEMPT_INTERVAL_S,
    GROUP_INHIBITION_THRESHOLD,
    MIN_ATTEMPT_INTERVAL_S,
)
from .observations import CameraStatus, Snapshot


def peer_stability_s(post_interruption_stability_s: float) -> float:
    """A peer counts as *stably* healthy after half the stabilization window
    (30s at the default 60s window), with a 1s floor for fast deployments."""
    return max(1.0, post_interruption_stability_s / 2.0)


PREFLIGHT_BACKOFF_S = 300.0
"""After a failed dispatch preflight, wait before proposing again."""


class GlobalState(StrEnum):
    STARTING = "STARTING"
    OBSERVING = "OBSERVING"
    MONITORING = "MONITORING"
    FRIGATE_UNAVAILABLE = "FRIGATE_UNAVAILABLE"
    TELEMETRY_STALE = "TELEMETRY_STALE"
    MULTIPLE_CAMERAS_UNHEALTHY = "MULTIPLE_CAMERAS_UNHEALTHY"
    STATE_STORE_UNSAFE = "STATE_STORE_UNSAFE"


class CameraState(StrEnum):
    UNARMED = "UNARMED"
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    RECOVERY_PENDING = "RECOVERY_PENDING"
    BOOT_GRACE = "BOOT_GRACE"
    RECOVERED = "RECOVERED"
    LATCHED = "LATCHED"
    DISABLED = "DISABLED"
    UNKNOWN = "UNKNOWN"


# Stable inhibition reason codes (all applicable reasons are reported).
R_MODE_OBSERVE = "MODE_OBSERVE"
R_RECOVERY_DISABLED = "RECOVERY_DISABLED_FOR_CAMERA"
R_MAINTENANCE = "CAMERA_MAINTENANCE"
R_DISABLED = "CAMERA_DISABLED"
R_NOT_ARMED = "NOT_ARMED"
R_NOT_FAILING = "NOT_FAILING"
R_NO_ACTIVE_PROPOSAL = "NO_ACTIVE_PROPOSAL"
R_EVIDENCE_TOO_SHORT = "EVIDENCE_TOO_SHORT"
R_EVIDENCE_INSUFFICIENT = "EVIDENCE_SNAPSHOTS_INSUFFICIENT"
R_TELEMETRY_STALE = "TELEMETRY_STALE"
R_CONFIG_STALE = "CONFIG_STALE"
R_STARTUP_GRACE = "STARTUP_GRACE_ACTIVE"
R_FRIGATE_RESTART_GRACE = "FRIGATE_RESTART_GRACE_ACTIVE"
R_MONITORING_INTERRUPTED = "MONITORING_INTERRUPTED"
R_MULTIPLE_FAILING = "MULTIPLE_CAMERAS_FAILING"
R_PEER_NOT_HEALTHY = "PEER_NOT_HEALTHY"
R_NO_HEALTHY_PEER = "NO_HEALTHY_PEER"
R_OUTAGE_ATTEMPT_CONSUMED = "OUTAGE_ATTEMPT_CONSUMED"
R_COOLDOWN = "COOLDOWN_ACTIVE"
R_BUDGET = "BUDGET_EXHAUSTED"
R_GLOBAL_SPACING = "GLOBAL_SPACING_ACTIVE"
R_OPERATION_IN_FLIGHT = "OPERATION_IN_FLIGHT"
R_BOOT_GRACE = "BOOT_GRACE_ACTIVE"
R_STORE_UNSAFE = "STATE_STORE_UNSAFE"
R_PREFLIGHT_FAILED = "PREFLIGHT_FAILED"
R_PREFLIGHT_PENDING = "PREFLIGHT_NOT_RUN"
R_AUTH_LATCHED = "AUTH_LATCHED"
R_BACKOFF = "PREFLIGHT_BACKOFF"


@dataclass(frozen=True)
class GuardFailure:
    code: str
    detail: str = ""


@dataclass(frozen=True)
class Event:
    kind: str
    ts_utc: float
    ts_mono: float
    camera: str | None = None
    incident_id: str | None = None
    attempt_id: str | None = None
    reason: str | None = None
    detail: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"kind": self.kind, "ts_utc": self.ts_utc}
        for name in ("camera", "incident_id", "attempt_id", "reason", "detail"):
            value = getattr(self, name)
            if value is not None:
                out[name] = value
        return out


@dataclass(frozen=True)
class ProposedAction:
    camera: str
    nonce: str
    incident_id: str | None
    proposed_at_mono: float


@dataclass(frozen=True)
class ActionCheck:
    camera: str
    permitted: bool
    failures: tuple[GuardFailure, ...]


class IncidentView(Protocol):
    """Read-only view of a persisted incident."""

    @property
    def incident_id(self) -> str: ...
    @property
    def latched(self) -> bool: ...


class AttemptAccounting(Protocol):
    @property
    def attempts_in_window(self) -> int: ...
    @property
    def last_attempt_at(self) -> float | None: ...
    @property
    def cooldown_until(self) -> float: ...


class StoreView(Protocol):
    """The engine's read-only window into durable safety state."""

    def armed(self, camera: str) -> bool: ...
    def incident(self, camera: str) -> IncidentView | None: ...
    def accounting(self, camera: str) -> AttemptAccounting: ...
    def last_attempt_any(self) -> float | None: ...
    def auth_latched(self, camera: str) -> bool: ...


# Store mutations the engine requests; the coordinator executes them durably.
@dataclass(frozen=True)
class StoreOp:
    op: str  # mark_armed | open_incident | resolve_incident | auth_latch
    camera: str
    incident_id: str | None = None


@dataclass
class EngineDecision:
    events: list[Event] = field(default_factory=list)
    proposal: ProposedAction | None = None
    store_ops: list[StoreOp] = field(default_factory=list)


@dataclass
class CameraRuntime:
    key: str
    state: CameraState = CameraState.UNARMED
    healthy_since: float | None = None
    bad_since: float | None = None
    bad_last: float | None = None
    bad_keys: set[tuple[Any, ...]] = field(default_factory=set)
    last_status: CameraStatus | None = None
    incident_id: str | None = None
    boot_grace_until: float | None = None
    pending_since: float | None = None
    last_outcome: tuple[str, float] | None = None
    preflight_backoff_until: float | None = None
    reported_unarmed_failure: bool = False
    # Cached-snapshot bookkeeping: a re-served snapshot pauses streak clocks.
    last_key: tuple[Any, ...] | None = None
    last_poll_received: float | None = None
    # Store-sourced latches, refreshed each snapshot.
    store_latched: bool = False
    store_armed: bool = False


class DecisionEngine:
    """Owns all decisions. One instance, driven by one serialized coordinator."""

    def __init__(
        self,
        config: WatchdogConfig,
        store_view: StoreView,
        *,
        start_mono: float,
        start_utc: float,
    ) -> None:
        self.config = config
        self.store = store_view
        self.start_mono = start_mono
        self.start_utc = start_utc
        self.t = config.timings
        self._peer_stable_s = peer_stability_s(self.t["post_interruption_stability_s"])
        self._nonce_counter = itertools.count(1)
        self.cameras: Mapping[str, CameraRuntime] = {
            key: CameraRuntime(key=key) for key in config.cameras
        }
        self.in_flight: ProposedAction | None = None
        self.reservation_active = False  # a durable reservation awaits its outcome
        self.store_unsafe = False
        self.global_state = GlobalState.STARTING
        self.global_states: list[GlobalState] = [GlobalState.STARTING]
        self.monitoring_resumed_at: float | None = None
        self.restart_grace_until: float | None = None
        self.last_snapshot: Snapshot | None = None
        self.last_received_mono: float | None = None
        self.group_inhibited = False
        self.last_problem: str | None = None
        self.last_problem_at_mono: float | None = None

    # ------------------------------------------------------------------ helpers

    def _event(self, kind: str, now_mono: float, now_utc: float, **kwargs: Any) -> Event:
        return Event(kind=kind, ts_utc=now_utc, ts_mono=now_mono, **kwargs)

    def _monitored_enabled(self, key: str) -> bool:
        """Camera participates as a witness (and potentially as a target)."""
        return not self.config.cameras[key].maintenance

    def _clear_failure_evidence(self) -> None:
        for rt in self.cameras.values():
            rt.bad_since = None
            rt.bad_last = None
            rt.bad_keys.clear()

    def _clear_healthy_streaks(self) -> None:
        for rt in self.cameras.values():
            rt.healthy_since = None

    def _mark_store_unsafe(self, now_mono: float, now_utc: float, decision: EngineDecision) -> None:
        if not self.store_unsafe:
            self.store_unsafe = True
            decision.events.append(
                self._event(
                    "store_unsafe",
                    now_mono,
                    now_utc,
                    reason="STATE_STORE_UNSAFE",
                    detail="durable state unavailable; recovery inhibited until restart",
                )
            )
        self.in_flight = None
        self.reservation_active = False

    def _currently_flowing(self, rt: CameraRuntime) -> bool:
        return rt.last_status is CameraStatus.FRAMES_FLOWING

    def _failing_cameras(self) -> list[str]:
        return [
            key
            for key, rt in self.cameras.items()
            if rt.last_status is CameraStatus.NO_FRAMES and self._monitored_enabled(key)
        ]

    def _latch(
        self,
        rt: CameraRuntime,
        now_mono: float,
        now_utc: float,
        decision: EngineDecision,
        reason: str,
    ) -> None:
        rt.state = CameraState.LATCHED
        rt.boot_grace_until = None
        decision.events.append(
            self._event(
                "incident_latched",
                now_mono,
                now_utc,
                camera=rt.key,
                incident_id=rt.incident_id,
                reason=reason,
                detail="recovery attempt consumed; no further attempts this outage",
            )
        )

    # ------------------------------------------------------------------ inputs

    def on_snapshot(self, snapshot: Snapshot, now_mono: float, now_acc: float) -> EngineDecision:
        """Process one normalized poll. Proposes at most one action."""
        now_utc = snapshot.received_at_utc
        decision = EngineDecision()
        gap_detected = (
            self.last_received_mono is not None
            and snapshot.received_at - self.last_received_mono > self.t["monitoring_gap_s"]
        )
        self.last_snapshot = snapshot
        self.last_received_mono = snapshot.received_at

        if snapshot.frigate_restarted:
            self.restart_grace_until = now_mono + self.t["frigate_restart_grace_s"]
            self._clear_failure_evidence()
            self._clear_healthy_streaks()
            decision.events.append(
                self._event(
                    "frigate_restarted",
                    now_mono,
                    now_utc,
                    reason="FRIGATE_RESTART",
                    detail="Frigate instance identity changed; failure evidence discarded",
                )
            )

        if not snapshot.fresh and snapshot.problem is not None:
            self.last_problem = snapshot.problem
            self.last_problem_at_mono = now_mono
        elif snapshot.fresh:
            self.last_problem = None

        # Refresh store-sourced state; a read failure is store-unsafe.
        for rt in self.cameras.values():
            try:
                rt.store_armed = self.store.armed(rt.key)
                incident = self.store.incident(rt.key)
                rt.store_latched = bool(incident and incident.latched)
                if rt.incident_id is None and incident is not None and incident.latched:
                    rt.incident_id = incident.incident_id
            except Exception:
                self._mark_store_unsafe(now_mono, now_utc, decision)
                return self._finalize(decision, snapshot, now_mono)

        if gap_detected:
            self._clear_failure_evidence()
            self._clear_healthy_streaks()
            self.monitoring_resumed_at = now_mono
            decision.events.append(
                self._event(
                    "monitoring_interrupted",
                    now_mono,
                    now_utc,
                    reason="MONITORING_GAP",
                    detail="monitoring gap; failure evidence cleared (limits retained)",
                )
            )

        self._process_cameras(snapshot, now_mono, now_utc, now_acc, decision)
        return self._finalize(decision, snapshot, now_mono)

    def on_tick(self, now_mono: float, now_utc: float) -> EngineDecision:
        """Time-driven transitions: gaps, boot-grace expiry."""
        decision = EngineDecision()
        if (
            self.last_received_mono is not None
            and now_mono - self.last_received_mono > self.t["monitoring_gap_s"]
        ):
            decision.events.append(
                self._event("monitoring_interrupted", now_mono, now_utc, reason="MONITORING_GAP")
            )
            self._cancel_in_flight(
                GuardFailure(R_MONITORING_INTERRUPTED, "monitoring gap"),
                now_mono,
                now_utc,
                decision,
            )
            self._clear_failure_evidence()
            self._clear_healthy_streaks()
            self.monitoring_resumed_at = now_mono
            self.last_received_mono = None
        for rt in self.cameras.values():
            if (
                rt.state is CameraState.BOOT_GRACE
                and rt.boot_grace_until is not None
                and now_mono >= rt.boot_grace_until
                and not self._currently_flowing(rt)
            ):
                self._latch(rt, now_mono, now_utc, decision, reason="BOOT_GRACE_ELAPSED")
        self._finalize(decision, self.last_snapshot, now_mono)
        return decision

    # ------------------------------------------------------------------ cameras

    def _process_cameras(
        self,
        snapshot: Snapshot,
        now_mono: float,
        now_utc: float,
        now_acc: float,
        decision: EngineDecision,
    ) -> None:
        failing = [
            key
            for key in self.cameras
            if (r := snapshot.cameras.get(key)) is not None
            and r.status is CameraStatus.NO_FRAMES
            and snapshot.fresh
            and self._monitored_enabled(key)
        ]

        self._update_group_inhibition(failing, now_mono, now_utc, decision)

        for key, rt in self.cameras.items():
            cam_cfg = self.config.cameras[key]
            reading = snapshot.cameras.get(key)
            status = reading.status if reading is not None else CameraStatus.UNKNOWN
            rt.last_status = status
            # A re-served (cached) snapshot is the same observation: it must
            # not advance healthy or failure streak clocks.
            key_advanced = snapshot.key != () and snapshot.key != rt.last_key
            cached_delta = (
                0.0
                if key_advanced or rt.last_poll_received is None
                else snapshot.received_at - rt.last_poll_received
            )

            if status is CameraStatus.DISABLED or cam_cfg.maintenance:
                rt.healthy_since = None
                rt.bad_since = None
                rt.bad_last = None
                rt.bad_keys.clear()
                if rt.state not in (
                    CameraState.BOOT_GRACE,
                    CameraState.LATCHED,
                    CameraState.RECOVERED,
                ):
                    rt.state = CameraState.DISABLED
            elif status is CameraStatus.UNKNOWN:
                # Unknown resets streaks, inhibits action, and never silently
                # reduces the number of witnesses.
                rt.healthy_since = None
                rt.bad_since = None
                rt.bad_last = None
                rt.bad_keys.clear()
                if rt.state not in (
                    CameraState.BOOT_GRACE,
                    CameraState.LATCHED,
                    CameraState.RECOVERED,
                ):
                    rt.state = CameraState.UNKNOWN
            elif status is CameraStatus.FRAMES_FLOWING:
                self._process_flowing(
                    key, rt, snapshot, now_mono, now_utc, decision, key_advanced, cached_delta
                )
            else:  # NO_FRAMES
                self._process_no_frames(
                    key, rt, snapshot, now_mono, now_utc, decision, key_advanced, cached_delta
                )
            rt.last_key = snapshot.key or rt.last_key
            rt.last_poll_received = snapshot.received_at

        self._maybe_propose(snapshot, now_mono, now_utc, now_acc, decision, failing)

    def _update_group_inhibition(
        self,
        failing: list[str],
        now_mono: float,
        now_utc: float,
        decision: EngineDecision,
    ) -> None:
        if len(failing) >= GROUP_INHIBITION_THRESHOLD:
            if not self.group_inhibited:
                self.group_inhibited = True
                names = " and ".join(sorted(failing[:GROUP_INHIBITION_THRESHOLD]))
                decision.events.append(
                    self._event(
                        "inhibited",
                        now_mono,
                        now_utc,
                        reason=R_MULTIPLE_FAILING,
                        detail=f"{names} have no frames; automatic recovery inhibited",
                    )
                )
            self._cancel_in_flight(
                GuardFailure(R_MULTIPLE_FAILING, "multiple cameras failing"),
                now_mono,
                now_utc,
                decision,
            )
            return
        if self.group_inhibited and len(failing) < GROUP_INHIBITION_THRESHOLD:
            # The group episode ended; each remaining failure must pass a
            # brand-new full observation window with healthy peers.
            self.group_inhibited = False
            for key in failing:
                rt = self.cameras[key]
                rt.bad_since = None
                rt.bad_last = None
                rt.bad_keys.clear()
                decision.events.append(
                    self._event(
                        "state_changed",
                        now_mono,
                        now_utc,
                        camera=key,
                        reason="GROUP_EPISODE_ENDED",
                        detail="peer recovered; a full new observation window is required",
                    )
                )

    def _process_flowing(
        self,
        key: str,
        rt: CameraRuntime,
        snapshot: Snapshot,
        now_mono: float,
        now_utc: float,
        decision: EngineDecision,
        key_advanced: bool,
        cached_delta: float,
    ) -> None:
        # Re-adopt a durable latch discovered after a restart so the
        # healthy-confirmation exit can fire (and so it is never clobbered).
        if rt.store_latched and rt.state not in (CameraState.BOOT_GRACE, CameraState.LATCHED):
            rt.state = CameraState.LATCHED
        if key_advanced:
            if rt.healthy_since is None:
                rt.healthy_since = snapshot.received_at
        elif cached_delta and rt.healthy_since is not None:
            # Cached snapshot re-served: pause the healthy streak clock.
            rt.healthy_since += cached_delta
        rt.bad_since = None
        rt.bad_last = None
        rt.bad_keys.clear()
        rt.reported_unarmed_failure = False

        if (
            not rt.store_armed
            and rt.healthy_since is not None
            and snapshot.received_at - rt.healthy_since >= self.t["arm_healthy_s"]
        ):
            decision.store_ops.append(StoreOp("mark_armed", camera=key))
            rt.store_armed = True  # optimistic; the store is re-read every snapshot
            decision.events.append(
                self._event(
                    "armed", now_mono, now_utc, camera=key, detail="healthy baseline established"
                )
            )

        if (
            rt.healthy_since is not None
            and snapshot.received_at - rt.healthy_since >= self.t["recovery_confirm_s"]
            and rt.state in (CameraState.BOOT_GRACE, CameraState.LATCHED)
        ):
            decision.store_ops.append(
                StoreOp("resolve_incident", camera=key, incident_id=rt.incident_id)
            )
            rt.state = CameraState.RECOVERED
            rt.boot_grace_until = None  # resolution ends the boot-grace hold
            decision.events.append(
                self._event(
                    "recovery_confirmed",
                    now_mono,
                    now_utc,
                    camera=key,
                    incident_id=rt.incident_id,
                    reason="FRAMES_RESTORED",
                    detail="frames restored continuously; recovery confirmed",
                )
            )
            if self.in_flight is not None and self.in_flight.camera == key:
                self.in_flight = None
            return

        if (
            rt.incident_id is not None
            and not rt.store_latched
            and rt.state
            not in (
                CameraState.BOOT_GRACE,
                CameraState.LATCHED,
                CameraState.RECOVERY_PENDING,
                CameraState.RECOVERED,
            )
            and rt.healthy_since is not None
            and snapshot.received_at - rt.healthy_since >= self.t["recovery_confirm_s"]
        ):
            # A transient blip opened an incident but frames returned without
            # any attempt: resolve it so stale open incidents never linger.
            decision.store_ops.append(
                StoreOp("resolve_incident", camera=key, incident_id=rt.incident_id)
            )
            rt.incident_id = None
        if rt.state is CameraState.RECOVERED:
            rt.state = CameraState.HEALTHY if rt.store_armed else CameraState.UNARMED
            rt.incident_id = None
        elif rt.state not in (
            CameraState.BOOT_GRACE,
            CameraState.RECOVERY_PENDING,
            CameraState.LATCHED,
        ):
            rt.state = CameraState.HEALTHY if rt.store_armed else CameraState.UNARMED

    def _process_no_frames(
        self,
        key: str,
        rt: CameraRuntime,
        snapshot: Snapshot,
        now_mono: float,
        now_utc: float,
        decision: EngineDecision,
        key_advanced: bool,
        cached_delta: float,
    ) -> None:
        rt.healthy_since = None
        grace_done = now_mono - self.start_mono >= self.t["startup_grace_s"]
        if key_advanced:
            if rt.bad_since is None:
                rt.bad_since = snapshot.received_at
            rt.bad_last = snapshot.received_at
            if (
                grace_done
                and len(rt.bad_keys) < int(self.t["min_bad_snapshots"])
                and snapshot.fresh
            ):
                # Only a *distinct* fresh snapshot is new evidence; a cached
                # snapshot re-served is the same observation, not another
                # failure. The set is capped: only the required count matters.
                rt.bad_keys.add(snapshot.key)
        elif cached_delta and rt.bad_since is not None:
            # Cached snapshot re-served: pause the failure streak clock.
            rt.bad_since += cached_delta
            if rt.bad_last is not None:
                rt.bad_last += cached_delta

        if rt.state is CameraState.BOOT_GRACE:
            return  # attempt already consumed for this outage
        if rt.state is CameraState.RECOVERY_PENDING:
            return
        if rt.store_latched:
            rt.state = CameraState.LATCHED
            return

        if rt.store_armed:
            if rt.state is not CameraState.SUSPECT:
                rt.state = CameraState.SUSPECT
                decision.events.append(
                    self._event(
                        "frames_stopped",
                        now_mono,
                        now_utc,
                        camera=key,
                        reason="NO_FRAMES",
                        detail="frames stopped; observing",
                    )
                )
                if self.config.mode == "recover" and rt.incident_id is None:
                    decision.store_ops.append(StoreOp("open_incident", camera=key))
        else:
            rt.state = CameraState.UNARMED
            if not rt.reported_unarmed_failure:
                rt.reported_unarmed_failure = True
                decision.events.append(
                    self._event(
                        "frames_stopped",
                        now_mono,
                        now_utc,
                        camera=key,
                        reason="UNARMED",
                        detail="camera has never established a healthy baseline; "
                        "automatic recovery not permitted",
                    )
                )

    # ------------------------------------------------------------------ proposal

    def _evidence_complete(self, rt: CameraRuntime, now_mono: float) -> bool:
        if rt.bad_since is None or rt.bad_last is None:
            return False
        if now_mono - rt.bad_since < self.t["zero_frames_threshold_s"]:
            return False
        return len(rt.bad_keys) >= int(self.t["min_bad_snapshots"])

    def _boot_grace_active_anywhere(self) -> str | None:
        for key, rt in self.cameras.items():
            if rt.state is CameraState.BOOT_GRACE or rt.boot_grace_until is not None:
                return key
        return None

    def _maybe_propose(
        self,
        snapshot: Snapshot,
        now_mono: float,
        now_utc: float,
        now_acc: float,
        decision: EngineDecision,
        failing: list[str],
    ) -> None:
        # Keep any existing proposal honest: re-evaluate against this snapshot.
        if self.in_flight is not None:
            check = self.evaluate_action(
                self.in_flight.camera,
                snapshot=snapshot,
                now_mono=now_mono,
                now_acc=now_acc,
            )
            if not check.permitted:
                # Report the most specific cause, not merely the first-listed
                # guard (evidence guards routinely precede the real reason).
                specific = [
                    f
                    for f in check.failures
                    if f.code not in (R_NOT_FAILING, R_EVIDENCE_TOO_SHORT, R_EVIDENCE_INSUFFICIENT)
                ]
                self._cancel_in_flight(
                    specific[0] if specific else check.failures[0], now_mono, now_utc, decision
                )
        if self.in_flight is not None:
            return
        if self.reservation_active:
            return  # a durable reservation awaits its outcome
        if self.store_unsafe or self.config.mode != "recover":
            return
        if self._boot_grace_active_anywhere() is not None:
            return
        if len(failing) >= GROUP_INHIBITION_THRESHOLD:
            return

        for key in failing:
            rt = self.cameras[key]
            if not self._evidence_complete(rt, now_mono):
                continue
            check = self.evaluate_action(key, snapshot=snapshot, now_mono=now_mono, now_acc=now_acc)
            if check.permitted:
                action = ProposedAction(
                    camera=key,
                    nonce=f"n{next(self._nonce_counter)}",
                    incident_id=rt.incident_id,
                    proposed_at_mono=now_mono,
                )
                self.in_flight = action
                rt.state = CameraState.RECOVERY_PENDING
                rt.pending_since = now_mono
                decision.proposal = action
                decision.events.append(
                    self._event(
                        "action_proposed",
                        now_mono,
                        now_utc,
                        camera=key,
                        incident_id=rt.incident_id,
                        reason="ELIGIBLE",
                        detail="all recovery conditions satisfied",
                    )
                )
                return

    def cancel_proposed(
        self,
        camera: str,
        failure: GuardFailure,
        now_mono: float,
        now_utc: float,
    ) -> EngineDecision:
        """Cancel this camera's pending proposal (dispatch recheck failed)."""
        decision = EngineDecision()
        if self.in_flight is not None and self.in_flight.camera == camera:
            self._cancel_in_flight(failure, now_mono, now_utc, decision)
        return decision

    def _cancel_in_flight(
        self,
        failure: GuardFailure,
        now_mono: float,
        now_utc: float,
        decision: EngineDecision,
    ) -> bool:
        if self.in_flight is None:
            return False
        action, self.in_flight = self.in_flight, None
        rt = self.cameras.get(action.camera)
        if rt is not None:
            rt.pending_since = None
            if rt.state is CameraState.RECOVERY_PENDING:
                rt.state = CameraState.SUSPECT
        decision.events.append(
            self._event(
                "action_cancelled",
                now_mono,
                now_utc,
                camera=action.camera,
                incident_id=action.incident_id,
                reason=failure.code,
                detail=failure.detail,
            )
        )
        return True

    # ------------------------------------------------------------------ guards

    def evaluate_action(
        self,
        camera: str,
        *,
        snapshot: Snapshot | None,
        now_mono: float,
        now_acc: float,
        preflight_ok: bool | None = None,
        for_dispatch: bool = False,
    ) -> ActionCheck:
        """Evaluate every eligibility condition; return *all* failures.

        ``for_dispatch`` additionally requires an active proposal for this
        camera and an explicitly successful preflight.
        """
        failures: list[GuardFailure] = []

        def fail(code: str, detail: str = "") -> None:
            failures.append(GuardFailure(code, detail))

        cam_cfg = self.config.cameras.get(camera)
        rt = self.cameras.get(camera)
        if cam_cfg is None or rt is None:
            return ActionCheck(camera, False, (GuardFailure("NOT_CONFIGURED", ""),))

        reading = snapshot.cameras.get(camera) if snapshot is not None else None

        # 1. mode
        if self.config.mode != "recover":
            fail(R_MODE_OBSERVE, "running in observe mode")
        # 2. camera recovery configuration
        if cam_cfg.recovery != "onvif":
            fail(R_RECOVERY_DISABLED, "camera is not configured for onvif recovery")
        # 3. maintenance / disabled
        if cam_cfg.maintenance:
            fail(R_MAINTENANCE, "camera is flagged for maintenance")
        if reading is not None and reading.status is CameraStatus.DISABLED:
            fail(R_DISABLED, "camera is disabled in Frigate configuration")
        # 4. failure evidence
        if reading is None or reading.status is not CameraStatus.NO_FRAMES:
            fail(R_NOT_FAILING, "camera is not currently reporting zero frames")
        if not self._evidence_complete(rt, now_mono):
            if (
                rt.bad_since is not None
                and now_mono - rt.bad_since < self.t["zero_frames_threshold_s"]
            ):
                fail(
                    R_EVIDENCE_TOO_SHORT,
                    f"{now_mono - rt.bad_since:.0f}s of "
                    f"{self.t['zero_frames_threshold_s']:.0f}s required",
                )
            else:
                fail(
                    R_EVIDENCE_INSUFFICIENT,
                    f"{len(rt.bad_keys)} of {int(self.t['min_bad_snapshots'])} "
                    "distinct fresh snapshots",
                )
        # 5. baseline
        try:
            armed = self.store.armed(camera)
        except Exception:
            armed = False
            fail(R_STORE_UNSAFE, "armed state unreadable")
        if not armed:
            fail(R_NOT_ARMED, "no healthy baseline recorded")
        # 6. freshness of the whole observation
        if snapshot is None or not snapshot.fresh:
            reason = (
                R_CONFIG_STALE if snapshot is not None and snapshot.stats_ok else R_TELEMETRY_STALE
            )
            fail(reason, "latest observation is not fresh")
        # 7. startup grace
        if now_mono - self.start_mono < self.t["startup_grace_s"]:
            fail(R_STARTUP_GRACE, "watchdog startup grace active")
        # 8. frigate restart grace
        if self.restart_grace_until is not None and now_mono < self.restart_grace_until:
            fail(R_FRIGATE_RESTART_GRACE, "Frigate restarted recently")
        # 9. post-interruption stability
        if (
            self.monitoring_resumed_at is not None
            and now_mono - self.monitoring_resumed_at < self.t["post_interruption_stability_s"]
        ):
            fail(R_MONITORING_INTERRUPTED, "telemetry resumed after a gap; stabilization active")
        # 10/11/12. witnesses
        other_failing: list[str] = []
        peers_stable = 0
        peers_known_healthy = 0
        peers_total = 0
        for other_key, other_rt in self.cameras.items():
            if other_key == camera:
                continue
            other_reading = snapshot.cameras.get(other_key) if snapshot is not None else None
            other_status = (
                other_reading.status if other_reading is not None else CameraStatus.UNKNOWN
            )
            if not self._monitored_enabled(other_key) or other_status is CameraStatus.DISABLED:
                continue
            peers_total += 1
            if other_status is CameraStatus.NO_FRAMES:
                other_failing.append(other_key)
            elif other_status is CameraStatus.FRAMES_FLOWING:
                peers_known_healthy += 1
                if (
                    other_rt.healthy_since is not None
                    and now_mono - other_rt.healthy_since >= self._peer_stable_s
                ):
                    peers_stable += 1
        if len(other_failing) + 1 >= GROUP_INHIBITION_THRESHOLD:
            fail(
                R_MULTIPLE_FAILING,
                f"{len(other_failing) + 1} cameras failing: {', '.join([camera, *other_failing])}",
            )
        if peers_total == 0:
            fail(R_NO_HEALTHY_PEER, "no other monitored camera exists")
        elif peers_stable != peers_known_healthy or peers_known_healthy != (
            peers_total - len(other_failing)
        ):
            # Failing peers are already reported by MULTIPLE_FAILING; this
            # guard covers peers that are neither failing nor stably healthy.
            fail(R_PEER_NOT_HEALTHY, "not all peer cameras are known and stably healthy")
        if peers_total > 0 and peers_stable == 0 and not other_failing:
            fail(R_NO_HEALTHY_PEER, "no stably healthy peer")
        # 13. store health
        if self.store_unsafe:
            fail(R_STORE_UNSAFE, "state store is not healthy")
        # 14. outage latch
        try:
            incident = self.store.incident(camera)
        except Exception:
            incident = None
            fail(R_STORE_UNSAFE, "incident state unreadable")
        if incident is not None and incident.latched:
            fail(R_OUTAGE_ATTEMPT_CONSUMED, "this outage already consumed its attempt")
        # 15. per-camera accounting (accumulated-runtime clock)
        try:
            accounting = self.store.accounting(camera)
        except Exception:
            accounting = None
            fail(R_STORE_UNSAFE, "attempt accounting unreadable")
        if accounting is not None:
            if now_acc < accounting.cooldown_until:
                fail(
                    R_COOLDOWN,
                    f"cooldown active for {accounting.cooldown_until - now_acc:.0f}s more",
                )
            if accounting.attempts_in_window >= DAILY_ATTEMPT_BUDGET:
                fail(R_BUDGET, f"{accounting.attempts_in_window} attempts in the last 24h window")
            if (
                accounting.last_attempt_at is not None
                and now_acc - accounting.last_attempt_at < MIN_ATTEMPT_INTERVAL_S
            ):
                fail(R_COOLDOWN, "minimum interval between attempts on one camera is one hour")
        # 16. global spacing
        try:
            last_any = self.store.last_attempt_any()
        except Exception:
            last_any = None
            fail(R_STORE_UNSAFE, "global attempt history unreadable")
        if last_any is not None and now_acc - last_any < GLOBAL_MIN_ATTEMPT_INTERVAL_S:
            fail(R_GLOBAL_SPACING, "global minimum spacing between attempts is 5 minutes")
        # 17. single-flight
        if self.in_flight is not None and self.in_flight.camera != camera:
            fail(R_OPERATION_IN_FLIGHT, "another recovery proposal is pending")
        if self.reservation_active:
            fail(R_OPERATION_IN_FLIGHT, "a reserved attempt awaits its outcome")
        boot_owner = self._boot_grace_active_anywhere()
        if boot_owner is not None and boot_owner != camera:
            fail(R_OPERATION_IN_FLIGHT, f"camera '{boot_owner}' is in boot grace")
        if rt.state is CameraState.BOOT_GRACE or rt.boot_grace_until is not None:
            fail(R_BOOT_GRACE, "camera is in post-command boot grace")
        # 18. auth latch
        try:
            if self.store.auth_latched(camera):
                fail(R_AUTH_LATCHED, "camera recovery latched until acknowledged")
        except Exception:
            fail(R_STORE_UNSAFE, "auth latch unreadable")
        # 19. preflight backoff
        if rt.preflight_backoff_until is not None and now_mono < rt.preflight_backoff_until:
            fail(R_BACKOFF, "recent preflight failed; backing off")
        # 20. preflight
        if preflight_ok is False:
            fail(R_PREFLIGHT_FAILED, "ONVIF preflight failed")
        elif preflight_ok is None and for_dispatch:
            fail(R_PREFLIGHT_PENDING, "preflight has not been run")
        # 21. dispatch requires an active proposal
        if for_dispatch and (self.in_flight is None or self.in_flight.camera != camera):
            fail(R_NO_ACTIVE_PROPOSAL, "dispatch requires the engine's active proposal")

        return ActionCheck(camera, not failures, tuple(failures))

    # ------------------------------------------------------------------ outcomes

    def on_incident_opened(
        self,
        camera: str,
        incident_id: str | None,
        now_mono: float,
        now_utc: float,
    ) -> EngineDecision:
        decision = EngineDecision()
        rt = self.cameras.get(camera)
        if rt is not None and incident_id is not None:
            rt.incident_id = incident_id
            decision.events.append(
                self._event(
                    "incident_opened", now_mono, now_utc, camera=camera, incident_id=incident_id
                )
            )
        return decision

    def on_reservation_recorded(
        self,
        camera: str,
        attempt_id: str,
        now_mono: float,
        now_utc: float,
    ) -> EngineDecision:
        """The durable reservation committed; one send is now authorized."""
        decision = EngineDecision()
        self.reservation_active = True
        rt = self.cameras.get(camera)
        if rt is not None:
            rt.pending_since = None
        decision.events.append(
            self._event(
                "action_reserved",
                now_mono,
                now_utc,
                camera=camera,
                incident_id=rt.incident_id if rt else None,
                attempt_id=attempt_id,
                reason="RESERVED",
                detail="attempt durably reserved",
            )
        )
        return decision

    def on_outcome(
        self,
        camera: str,
        attempt_id: str,
        outcome: str,
        now_mono: float,
        now_utc: float,
    ) -> EngineDecision:
        """Classify a dispatched command (or a reservation with no recorded send)."""
        decision = EngineDecision()
        self.reservation_active = False
        rt = self.cameras.get(camera)
        if rt is None:
            return decision
        rt.last_outcome = (outcome, now_mono)
        if self.in_flight is not None and self.in_flight.camera == camera:
            self.in_flight = None
        decision.events.append(
            self._event(
                "action_outcome",
                now_mono,
                now_utc,
                camera=camera,
                incident_id=rt.incident_id,
                attempt_id=attempt_id,
                reason=outcome,
                detail=OUTCOME_DETAILS.get(outcome, outcome),
            )
        )
        if outcome in ("ACKNOWLEDGED", "OUTCOME_UNKNOWN"):
            rt.state = CameraState.BOOT_GRACE
            rt.boot_grace_until = now_mono + self.t["boot_grace_s"]
        else:  # AUTH_FAILED, UNSUPPORTED, UNREACHABLE, REJECTED
            if outcome in ("AUTH_FAILED", "UNSUPPORTED"):
                decision.store_ops.append(StoreOp("auth_latch", camera=camera))
            # Defense in depth: the durable latch must not depend solely on the
            # reservation having been recorded before the crash.
            decision.store_ops.append(StoreOp("latch_incident", camera=camera))
            self._latch(rt, now_mono, now_utc, decision, reason=outcome)
        return decision

    def on_preflight_failed(
        self,
        camera: str,
        code: str,
        now_mono: float,
        now_utc: float,
    ) -> EngineDecision:
        """A dispatch preflight failed *before* reservation; back off.

        A credential rejection during the read-only preflight durably
        auth-latches the camera (operator must correct and acknowledge).
        No attempt is reserved or consumed: nothing was sent.
        """
        decision = EngineDecision()
        rt = self.cameras.get(camera)
        if code == "AUTH_FAILED":
            decision.store_ops.append(StoreOp("auth_latch", camera=camera))
            decision.events.append(
                self._event(
                    "inhibited",
                    now_mono,
                    now_utc,
                    camera=camera,
                    reason=R_AUTH_LATCHED,
                    detail="credentials rejected by read-only preflight; "
                    "latched until acknowledged",
                )
            )
        if rt is not None:
            rt.preflight_backoff_until = now_mono + PREFLIGHT_BACKOFF_S
        cancelled = self._cancel_in_flight(
            GuardFailure(R_PREFLIGHT_FAILED, code), now_mono, now_utc, decision
        )
        if not cancelled:
            decision.events.append(
                self._event(
                    "action_cancelled",
                    now_mono,
                    now_utc,
                    camera=camera,
                    reason=R_PREFLIGHT_FAILED,
                    detail=code,
                )
            )
        return decision

    def on_store_op_failed(self, op: StoreOp, now_mono: float, now_utc: float) -> EngineDecision:
        decision = EngineDecision()
        self._mark_store_unsafe(now_mono, now_utc, decision)
        return decision

    def on_store_failure(self, now_mono: float, now_utc: float) -> EngineDecision:
        decision = EngineDecision()
        self._mark_store_unsafe(now_mono, now_utc, decision)
        return decision

    def on_acknowledged(self, camera: str, now_mono: float, now_utc: float) -> EngineDecision:
        """Operator acknowledged a latched incident via the local CLI."""
        decision = EngineDecision()
        rt = self.cameras.get(camera)
        if rt is not None:
            rt.store_latched = False
            rt.incident_id = None
            rt.boot_grace_until = None
            if rt.last_status is CameraStatus.FRAMES_FLOWING:
                rt.state = CameraState.HEALTHY if rt.store_armed else CameraState.UNARMED
            elif rt.last_status is CameraStatus.NO_FRAMES:
                rt.state = CameraState.SUSPECT if rt.store_armed else CameraState.UNARMED
            elif rt.last_status is CameraStatus.DISABLED:
                rt.state = CameraState.DISABLED
            else:
                rt.state = CameraState.UNKNOWN
        decision.events.append(
            self._event(
                "acknowledged",
                now_mono,
                now_utc,
                camera=camera,
                reason="OPERATOR_ACK",
                detail="incident latch cleared by operator; cooldowns and budgets still apply",
            )
        )
        return decision

    # ------------------------------------------------------------------ status

    def _finalize(
        self,
        decision: EngineDecision,
        snapshot: Snapshot | None,
        now_mono: float,
    ) -> EngineDecision:
        states: list[GlobalState] = []
        if self.store_unsafe:
            states.append(GlobalState.STATE_STORE_UNSAFE)
        if (
            self.last_problem is not None
            and self.last_problem_at_mono is not None
            and now_mono - self.last_problem_at_mono <= self.t["monitoring_gap_s"]
        ):
            states.append(GlobalState.FRIGATE_UNAVAILABLE)
        if snapshot is None or not snapshot.fresh:
            states.append(GlobalState.TELEMETRY_STALE)
        if now_mono - self.start_mono < self.t["startup_grace_s"]:
            states.append(GlobalState.STARTING)
        if len(self._failing_cameras()) >= GROUP_INHIBITION_THRESHOLD:
            states.append(GlobalState.MULTIPLE_CAMERAS_UNHEALTHY)
        if self.config.mode == "observe":
            states.append(GlobalState.OBSERVING)
        elif not states:
            states.append(GlobalState.MONITORING)
        self.global_state = states[0] if states else GlobalState.MONITORING
        self.global_states = states
        return decision

    def global_inhibition_reasons(self, now_mono: float) -> list[str]:
        reasons: list[str] = []
        if self.store_unsafe:
            reasons.append(R_STORE_UNSAFE)
        if self.config.mode != "recover":
            reasons.append(R_MODE_OBSERVE)
        if now_mono - self.start_mono < self.t["startup_grace_s"]:
            reasons.append(R_STARTUP_GRACE)
        if self.restart_grace_until is not None and now_mono < self.restart_grace_until:
            reasons.append(R_FRIGATE_RESTART_GRACE)
        if (
            self.monitoring_resumed_at is not None
            and now_mono - self.monitoring_resumed_at < self.t["post_interruption_stability_s"]
        ):
            reasons.append(R_MONITORING_INTERRUPTED)
        if self.last_snapshot is None or not self.last_snapshot.fresh:
            reasons.append(R_TELEMETRY_STALE)
        if self.in_flight is not None:
            reasons.append(R_OPERATION_IN_FLIGHT)
        if self._boot_grace_active_anywhere() is not None:
            reasons.append(R_OPERATION_IN_FLIGHT)
        if len(self._failing_cameras()) >= GROUP_INHIBITION_THRESHOLD:
            reasons.append(R_MULTIPLE_FAILING)
        monitored = [k for k in self.cameras if self._monitored_enabled(k)]
        if len(monitored) <= 1:
            reasons.append(R_NO_HEALTHY_PEER)
        return reasons

    def camera_summaries(self, now_mono: float) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for key, rt in self.cameras.items():
            out[key] = {
                "state": rt.state.value,
                "armed": rt.store_armed,
                "last_status": rt.last_status.value if rt.last_status else None,
                "frigate_name": self.config.cameras[key].frigate_name,
                "recovery": self.config.cameras[key].recovery,
                "maintenance": self.config.cameras[key].maintenance,
                "failing_since": rt.bad_since,
                "distinct_bad_snapshots": len(rt.bad_keys),
                "incident_id": rt.incident_id,
                "latched": rt.store_latched,
                "boot_grace_until": rt.boot_grace_until,
                "last_outcome": rt.last_outcome[0] if rt.last_outcome else None,
                "seconds_unhealthy": now_mono - rt.bad_since if rt.bad_since is not None else None,
            }
        return out


OUTCOME_DETAILS = {
    "ACKNOWLEDGED": "reboot request acknowledged; boot grace started",
    "OUTCOME_UNKNOWN": "reboot outcome unknown; no automatic retry",
    "AUTH_FAILED": "camera rejected credentials",
    "UNSUPPORTED": "camera does not support SystemReboot",
    "UNREACHABLE": "camera unreachable",
    "REJECTED": "camera rejected the reboot request",
}

__all__ = [
    "PREFLIGHT_BACKOFF_S",
    "ActionCheck",
    "AttemptAccounting",
    "CameraRuntime",
    "CameraState",
    "DecisionEngine",
    "EngineDecision",
    "Event",
    "GlobalState",
    "GuardFailure",
    "IncidentView",
    "ProposedAction",
    "StoreOp",
    "StoreView",
    "peer_stability_s",
]
