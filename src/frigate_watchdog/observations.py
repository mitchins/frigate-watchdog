"""Normalization of raw Frigate API responses into observations.

Everything in this module is pure: clocks and previous snapshots are passed
in, and results are frozen dataclasses. The decision engine consumes these
structures and never touches raw HTTP payloads.

Freshness model
---------------
Frigate's stats endpoint may serve cached snapshots, so a successful HTTP
response is not evidence. A snapshot is *fresh* only when its producer
timestamp (``service.last_updated``) is recent, plausible, and has not moved
backwards, and the effective camera configuration was readable. A repeated
snapshot carries the same ``key`` so the engine can treat repeated delivery
of one cached snapshot as a single observation.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class CameraStatus(StrEnum):
    FRAMES_FLOWING = "FRAMES_FLOWING"
    NO_FRAMES = "NO_FRAMES"
    DISABLED = "DISABLED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class CameraReading:
    camera: str  # local configured key
    frigate_name: str
    status: CameraStatus
    fps: float | None = None
    detail: str | None = None  # stable sub-code, e.g. "missing_from_stats"


@dataclass(frozen=True)
class Snapshot:
    """A normalized observation of the whole monitored group at one poll."""

    received_at: float  # monotonic clock seconds
    received_at_utc: float  # epoch seconds
    produced_at: float | None  # epoch seconds of service.last_updated
    uptime: float | None
    stats_ok: bool  # stats endpoint returned a well-formed body
    config_ok: bool  # effective camera configuration was readable
    frigate_restarted: bool = False
    problem: str | None = None  # unreachable | malformed | auth | too_large | ...
    cameras: Mapping[str, CameraReading] = field(default_factory=dict)
    key: tuple[Any, ...] = ()  # distinctness identity

    @property
    def fresh(self) -> bool:
        return self.stats_ok and self.config_ok and self.problem is None

    @property
    def age(self) -> float | None:
        if self.produced_at is None:
            return None
        return self.received_at_utc - self.produced_at


def _number(value: Any) -> float | None:
    """Return a finite float, or None for anything not a real number.

    Booleans, strings, None, NaN and infinities are all rejected: a missing or
    nonsensical fps is *unknown*, never zero.
    """
    if isinstance(value, bool):
        return None
    if not isinstance(value, int | float):
        return None
    f = float(value)
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def classify_fps(fps: float | None) -> tuple[CameraStatus, float | None]:
    if fps is None:
        return CameraStatus.UNKNOWN, None
    if fps < 0:
        return CameraStatus.UNKNOWN, fps
    if fps == 0:
        return CameraStatus.NO_FRAMES, fps
    return CameraStatus.FRAMES_FLOWING, fps


@dataclass(frozen=True)
class Normalization:
    """Result of normalizing one poll; wraps a Snapshot plus freshness detail."""

    snapshot: Snapshot
    stale_reason: str | None = None  # age | future | backwards | uptime_backwards | missing


class FrigateNormalizer:
    """Stateful-but-pure normalizer.

    State (previous producer timestamp and uptime) is kept only to detect
    Frigate restarts and backwards movement; all time values are passed in by
    the caller, so this class never reads clocks itself.
    """

    FUTURE_TOLERANCE_S = 5.0

    def __init__(self, max_stats_age_s: float = 60.0) -> None:
        self.max_stats_age_s = max_stats_age_s
        self._last_produced_at: float | None = None
        self._last_uptime: float | None = None

    def reset_instance(self) -> None:
        """Forget the previous Frigate instance identity (e.g. after a long gap)."""
        self._last_produced_at = None
        self._last_uptime = None

    def normalize(
        self,
        *,
        received_at: float,
        received_at_utc: float,
        stats: Any,
        enabled_map: Mapping[str, bool] | None,
        cameras_cfg: Mapping[str, Any],
        problem: str | None = None,
    ) -> Normalization:
        """Build a Snapshot from one poll.

        ``stats`` is the parsed /api/stats JSON (or None on fetch failure);
        ``enabled_map`` maps frigate camera name -> enabled flag from /api/config
        (None when that fetch failed); ``cameras_cfg`` maps local key -> camera
        config; ``problem`` carries the fetch-level failure code.
        """
        readings: dict[str, CameraReading] = {}
        stats_ok = problem is None and isinstance(stats, dict)
        config_ok = enabled_map is not None
        produced_at: float | None = None
        uptime: float | None = None
        frigate_restarted = False
        stale_reason: str | None = problem

        service = stats.get("service") if stats_ok else None
        if isinstance(service, dict):
            produced_at = _number(service.get("last_updated"))
            uptime = _number(service.get("uptime"))
        elif stats_ok:
            stats_ok = False
            stale_reason = "malformed_service"

        if stats_ok and produced_at is None:
            stats_ok = False
            stale_reason = "missing_last_updated"

        # Plausibility checks against the wall clock passed in by the caller.
        if stats_ok and produced_at is not None:
            age = received_at_utc - produced_at
            if age < -self.FUTURE_TOLERANCE_S:
                stats_ok = False
                stale_reason = "future"
            elif age > self.max_stats_age_s:
                stats_ok = False
                stale_reason = "age"

        # Restart / backwards detection against previously seen values.
        if stats_ok:
            if (
                self._last_produced_at is not None
                and produced_at is not None
                and produced_at < self._last_produced_at - self.FUTURE_TOLERANCE_S
            ):
                frigate_restarted = True
            if (
                self._last_uptime is not None
                and uptime is not None
                and uptime + self.FUTURE_TOLERANCE_S < self._last_uptime
            ):
                frigate_restarted = True
            if produced_at is not None:
                self._last_produced_at = produced_at
                self._last_uptime = uptime

        stats_cameras: Mapping[str, Any] = {}
        if stats_ok and isinstance(stats.get("cameras"), dict):
            stats_cameras = stats["cameras"]

        for key, cam_cfg in cameras_cfg.items():
            name = cam_cfg.frigate_name
            if enabled_map is None or name not in enabled_map:
                readings[key] = CameraReading(
                    camera=key,
                    frigate_name=name,
                    status=CameraStatus.UNKNOWN,
                    detail="config_missing" if enabled_map is not None else "config_unavailable",
                )
                continue
            if not enabled_map[name]:
                readings[key] = CameraReading(
                    camera=key,
                    frigate_name=name,
                    status=CameraStatus.DISABLED,
                    detail="disabled_in_config",
                )
                continue
            cam_stats = stats_cameras.get(name)
            if not isinstance(cam_stats, dict):
                readings[key] = CameraReading(
                    camera=key,
                    frigate_name=name,
                    status=CameraStatus.UNKNOWN,
                    detail="missing_from_stats" if stats_ok else "stats_unavailable",
                )
                continue
            fps = _number(cam_stats.get("camera_fps"))
            status, fps = classify_fps(fps)
            detail = None if fps is not None else "invalid_camera_fps"
            readings[key] = CameraReading(
                camera=key, frigate_name=name, status=status, fps=fps, detail=detail
            )

        key_tuple: tuple[Any, ...] = ()
        if stats_ok and produced_at is not None:
            digest = tuple(
                sorted(
                    (r.frigate_name, None if r.fps is None else r.fps) for r in readings.values()
                )
            )
            key_tuple = (produced_at, uptime, digest)

        snapshot = Snapshot(
            received_at=received_at,
            received_at_utc=received_at_utc,
            produced_at=produced_at,
            uptime=uptime,
            stats_ok=stats_ok,
            config_ok=config_ok,
            frigate_restarted=frigate_restarted,
            problem=None if stats_ok else (stale_reason or problem),
            cameras=readings,
            key=key_tuple,
        )
        return Normalization(snapshot=snapshot, stale_reason=None if stats_ok else stale_reason)
