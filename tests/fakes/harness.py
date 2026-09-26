"""Test harness: builds configs, normalized snapshots, and drives the engine
exactly the way the service coordinator will (store ops applied, proposals
held for explicit dispatch)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import yaml

from frigate_watchdog.config import Timings, WatchdogConfig, parse_config
from frigate_watchdog.observations import FrigateNormalizer, Snapshot
from frigate_watchdog.policy import (
    DecisionEngine,
    EngineDecision,
    Event,
    ProposedAction,
)

from .clock import FakeClock
from .store import FakeStore

DEFAULT_TIMINGS = {
    "startup_grace_s": 60.0,
    "frigate_restart_grace_s": 60.0,
    "zero_frames_threshold_s": 60.0,
    "min_bad_snapshots": 3,
    "arm_healthy_s": 30.0,
    "recovery_confirm_s": 60.0,
    "boot_grace_s": 60.0,
    "post_interruption_stability_s": 30.0,
    "monitoring_gap_s": 30.0,
    "max_stats_age_s": 60.0,
    "poll_interval_s": 10.0,
}

DEFAULT_CAMERAS = {
    "porch": ("Porch", "onvif"),
    "driveway": ("Driveway", "onvif"),
    "doorbell": ("Doorbell", "none"),
}


def build_config(
    mode: str = "recover",
    cameras: Mapping[str, tuple[str, str]] | None = None,
    timings: Mapping[str, float] | None = None,
    maintenance: tuple[str, ...] = (),
) -> str:
    cameras = DEFAULT_CAMERAS if cameras is None else cameras
    merged = dict(DEFAULT_TIMINGS)
    if timings:
        merged.update(timings)
    cams_yaml = {}
    for key, (frigate_name, recovery) in cameras.items():
        block: dict[str, Any] = {"frigate_name": frigate_name, "recovery": recovery}
        if key in maintenance:
            block["maintenance"] = True
        if recovery == "onvif":
            last_octet = 40 + list(cameras).index(key)
            block["onvif"] = {
                "endpoint": f"http://192.168.10.{last_octet}/onvif/device_service",
                "username": "admin",
                "password_env": f"CAMERA_{key.upper()}_PASSWORD",
            }
        cams_yaml[key] = block
    doc = {
        "schema_version": 1,
        "mode": mode,
        "frigate": {"base_url": "http://frigate:5000", "auth": {"mode": "none"}},
        "cameras": cams_yaml,
        "timings": merged,
    }
    return yaml.safe_dump(doc, sort_keys=False)


def _env_for(cameras: Mapping[str, tuple[str, str]] | None) -> dict[str, str]:
    return {f"CAMERA_{k.upper()}_PASSWORD": f"pw-{k}" for k in (cameras or DEFAULT_CAMERAS)}


@dataclass
class PollResult:
    decision: EngineDecision
    snapshot: Snapshot


@dataclass
class Harness:
    clock: FakeClock
    store: FakeStore
    engine: DecisionEngine
    config: WatchdogConfig
    normalizer: FrigateNormalizer
    frigate_names: dict[str, str]
    uptime: float = 100.0
    frigate_utc: float = 0.0  # set in make_harness; Frigate's own wall clock
    events: list[Event] = field(default_factory=list)
    last_proposal: ProposedAction | None = None
    total_proposals: int = 0
    _produced_at: float | None = field(default=None, repr=False)

    def poll(
        self,
        fps: Mapping[str, float | None] | None = None,
        *,
        advance: float = 10.0,
        enabled: Mapping[str, bool] | None = None,
        problem: str | None = None,
        config_ok: bool = True,
        stale_age_s: float = 0.0,
        future_age_s: float = 0.0,
        restart: bool = False,
        utc_drift: float = 0.0,
        cached: bool = False,
        raw_fps: Mapping[str, Any] | None = None,
        omit_from_stats: tuple[str, ...] = (),
    ) -> PollResult:
        """One coordinator poll: advance clock, normalize, feed the engine.

        ``cached`` re-serves the previous produced timestamp/uptime so the
        snapshot is identical to the prior poll (Frigate cache behavior).
        ``raw_fps`` overrides fps values with arbitrary JSON values.
        ``utc_drift`` shifts only the *watchdog's* wall clock, simulating host
        clock skew against Frigate's own clock.
        """
        self.clock.advance(advance, utc_seconds=advance + utc_drift)
        self.store.acc += advance  # runtime accrues while the process runs
        self.frigate_utc += advance
        if restart:
            self.uptime = 1.0
        elif not cached:
            self.uptime += advance
        produced = self.frigate_utc - stale_age_s + future_age_s
        if cached and self._produced_at is not None:
            produced = self._produced_at
        else:
            self._produced_at = produced

        names_fps: dict[str, Any] = {}
        for key, name in self.frigate_names.items():
            if raw_fps is not None and key in raw_fps:
                names_fps[name] = raw_fps[key]
            else:
                names_fps[name] = fps.get(key) if fps else 5.0
        stats_cameras = {
            name: {"camera_fps": value}
            for name, value in names_fps.items()
            if value is not None and key_not_omitted(name, omit_from_stats, self.frigate_names)
        }
        stats: dict[str, Any] = {
            "service": {"last_updated": produced, "uptime": self.uptime},
            "cameras": stats_cameras,
        }
        enabled_map = None
        if config_ok:
            enabled_map = dict.fromkeys(self.frigate_names.values(), True)
            if enabled is not None:
                for key, value in enabled.items():
                    enabled_map[self.frigate_names[key]] = value

        norm = self.normalizer.normalize(
            received_at=self.clock.mono,
            received_at_utc=self.clock.utc,
            stats=None if problem is not None else stats,
            enabled_map=enabled_map,
            cameras_cfg=self.engine.config.cameras,
            problem=problem,
        )
        decision = self.engine.on_snapshot(norm.snapshot, self.clock.mono, self.store.acc)
        self.events.extend(decision.events)
        self._apply_store_ops(decision)
        if decision.proposal is not None:
            self.last_proposal = decision.proposal
            self.total_proposals += 1
        return PollResult(decision, norm.snapshot)

    def _apply_store_ops(self, decision: EngineDecision) -> None:
        for op in decision.store_ops:
            if op.op == "mark_armed":
                self.store.mark_armed(op.camera)
            elif op.op == "open_incident":
                inc = self.store.open_incident(op.camera)
                d = self.engine.on_incident_opened(
                    op.camera, inc.incident_id, self.clock.mono, self.clock.utc
                )
                self.events.extend(d.events)
            elif op.op == "resolve_incident":
                self.store.resolve_incident(op.camera)
            elif op.op == "auth_latch":
                self.store.auth_latch(op.camera)
            elif op.op == "latch_incident":
                self.store.latch_incident(op.camera)

    def tick(self) -> EngineDecision:
        decision = self.engine.on_tick(self.clock.mono, self.clock.utc)
        self.events.extend(decision.events)
        return decision

    # convenience: emulate the coordinator's dispatch steps
    def recheck(self, camera: str, preflight_ok: bool | None = None):
        return self.engine.evaluate_action(
            camera,
            snapshot=self.engine.last_snapshot,
            now_mono=self.clock.mono,
            now_acc=self.store.acc,
            preflight_ok=preflight_ok,
            for_dispatch=True,
        )

    def reserve(self, camera: str) -> str:
        attempt_id = self.store.reserve_attempt(camera)
        d = self.engine.on_reservation_recorded(camera, attempt_id, self.clock.mono, self.clock.utc)
        self.events.extend(d.events)
        return attempt_id

    def outcome(self, camera: str, attempt_id: str, outcome: str) -> None:
        self.store.record_outcome(camera, outcome)
        d = self.engine.on_outcome(camera, attempt_id, outcome, self.clock.mono, self.clock.utc)
        self.events.extend(d.events)
        self._apply_store_ops(d)

    def preflight_failed(self, camera: str, code: str = "UNREACHABLE") -> None:
        d = self.engine.on_preflight_failed(camera, code, self.clock.mono, self.clock.utc)
        self.events.extend(d.events)
        self._apply_store_ops(d)

    def acknowledge(self, camera: str) -> None:
        self.store.acknowledge(camera)
        d = self.engine.on_acknowledged(camera, self.clock.mono, self.clock.utc)
        self.events.extend(d.events)

    def events_of(self, kind: str) -> list[Event]:
        return [e for e in self.events if e.kind == kind]

    def state_of(self, camera: str) -> str:
        return self.engine.cameras[camera].state.value


def key_not_omitted(name: str, omit: tuple[str, ...], names: dict[str, str]) -> bool:
    return name not in {names[k] for k in omit}


def make_harness(
    mode: str = "recover",
    cameras: Mapping[str, tuple[str, str]] | None = None,
    timings: Mapping[str, float] | None = None,
    maintenance: tuple[str, ...] = (),
    start_mono: float = 1000.0,
    override_timings: Mapping[str, float] | None = None,
) -> Harness:
    text = build_config(mode=mode, cameras=cameras, timings=timings, maintenance=maintenance)
    config = parse_config(text, env=_env_for(cameras))
    if override_timings:
        # Post-parse override for guard-matrix scenarios that must compress a
        # single timing below operator-facing bounds (engine does not re-check).
        import dataclasses

        config = dataclasses.replace(
            config, timings=Timings({**config.timings.values, **dict(override_timings)})
        )
    clock = FakeClock(start_mono=start_mono)
    store = FakeStore()
    engine = DecisionEngine(config, store, start_mono=clock.mono, start_utc=clock.utc)
    names = {key: cfg.frigate_name for key, cfg in config.cameras.items()}
    return Harness(
        clock=clock,
        store=store,
        engine=engine,
        config=config,
        normalizer=FrigateNormalizer(max_stats_age_s=config.timings["max_stats_age_s"]),
        frigate_names=names,
        frigate_utc=clock.utc,
    )


def warm_up_healthy(h: Harness, cameras_still_zero: tuple[str, ...] = ()) -> None:
    """Advance past startup grace and arm every camera with healthy frames."""
    h.poll(advance=70)
    for _ in range(4):
        h.poll(
            advance=10, fps={k: (0.0 if k in cameras_still_zero else 5.0) for k in h.frigate_names}
        )
    for key in h.frigate_names:
        if key not in cameras_still_zero:
            assert h.store.armed(key), f"{key} should be armed after warm-up"
