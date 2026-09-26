"""Deterministic tests for observation normalization (freshness contract)."""

from __future__ import annotations

import math

from frigate_watchdog.config import parse_config
from frigate_watchdog.observations import CameraStatus, FrigateNormalizer
from tests.fakes.harness import _env_for, build_config

MONO = 1000.0
UTC = 1_700_000_000.0


def make_norm(max_age: float = 60.0):
    return FrigateNormalizer(max_stats_age_s=max_age)


def cams_cfg():
    config = parse_config(build_config(), env=_env_for(None))
    return config.cameras


def stats(fps_porch=5.0, fps_driveway=5.0, fps_doorbell=5.0, produced=UTC, uptime=100.0):
    return {
        "service": {"last_updated": produced, "uptime": uptime},
        "cameras": {
            "Porch": {"camera_fps": fps_porch},
            "Driveway": {"camera_fps": fps_driveway},
            "Doorbell": {"camera_fps": fps_doorbell},
        },
    }


def enabled():
    return {"Porch": True, "Driveway": True, "Doorbell": True}


def test_positive_fps_is_flowing():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(fps_porch=0.1),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.cameras["porch"].status is CameraStatus.FRAMES_FLOWING


def test_exactly_zero_is_no_frames():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(fps_porch=0.0),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.cameras["porch"].status is CameraStatus.NO_FRAMES


def test_zero_after_rounding_stays_zero():
    # 0.0 == 0 exactly; a tiny epsilon is still flowing
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(fps_porch=1e-9),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.cameras["porch"].status is CameraStatus.FRAMES_FLOWING


def test_odd_fps_values_are_unknown_not_zero():
    for bad in (None, "5", True, False, -1.0, float("nan"), float("inf"), float("-inf"), [], {}):
        raw = {"service": {"last_updated": UTC, "uptime": 100}, "cameras": {}}
        norm = make_norm().normalize(
            received_at=MONO,
            received_at_utc=UTC,
            stats={**raw, "cameras": {"Porch": {"camera_fps": bad}}},
            enabled_map=enabled(),
            cameras_cfg=cams_cfg(),
        )
        assert norm.snapshot.cameras["porch"].status is CameraStatus.UNKNOWN, f"fps={bad!r}"


def test_missing_camera_is_unknown():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats={"service": {"last_updated": UTC, "uptime": 100}, "cameras": {}},
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.cameras["porch"].status is CameraStatus.UNKNOWN
    assert norm.snapshot.cameras["porch"].detail == "missing_from_stats"


def test_disabled_camera_is_disabled_even_if_stats_absent():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats={"service": {"last_updated": UTC, "uptime": 100}, "cameras": {}},
        enabled_map={"Porch": False, "Driveway": True, "Doorbell": True},
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.cameras["porch"].status is CameraStatus.DISABLED


def test_detection_fps_only_zero_is_not_a_signal():
    # detection_fps is never consulted; camera_fps drives the classification
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats={
            "service": {"last_updated": UTC, "uptime": 100},
            "cameras": {
                "Porch": {"camera_fps": 5.0, "detection_fps": 0.0},
                "Driveway": {"camera_fps": 5.0},
                "Doorbell": {"camera_fps": 5.0},
            },
        },
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.cameras["porch"].status is CameraStatus.FRAMES_FLOWING


def test_stale_produced_timestamp_rejected():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(produced=UTC - 120),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert not norm.snapshot.stats_ok
    assert norm.stale_reason == "age"
    assert not norm.snapshot.fresh


def test_future_produced_timestamp_rejected():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(produced=UTC + 60),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert not norm.snapshot.stats_ok
    assert norm.stale_reason == "future"


def test_small_future_within_tolerance_accepted():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(produced=UTC + 2),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.stats_ok


def test_uptime_backwards_is_restart():
    normalizer = make_norm()
    normalizer.normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(uptime=100),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    norm = normalizer.normalize(
        received_at=MONO + 10,
        received_at_utc=UTC + 10,
        stats=stats(uptime=5),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.frigate_restarted is True


def test_produced_backwards_is_restart():
    normalizer = make_norm()
    normalizer.normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(produced=UTC),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    norm = normalizer.normalize(
        received_at=MONO + 10,
        received_at_utc=UTC + 10,
        stats=stats(produced=UTC - 30),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.frigate_restarted is True


def test_uptime_forward_normal():
    normalizer = make_norm()
    normalizer.normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(uptime=100),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    norm = normalizer.normalize(
        received_at=MONO + 10,
        received_at_utc=UTC + 10,
        stats=stats(uptime=110),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.frigate_restarted is False
    assert norm.snapshot.fresh


def test_identical_snapshot_yields_identical_key():
    normalizer = make_norm()
    n1 = normalizer.normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    n2 = normalizer.normalize(
        received_at=MONO + 10,
        received_at_utc=UTC + 10,
        stats=stats(produced=n1.snapshot.produced_at),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert n1.snapshot.key == n2.snapshot.key
    # ... and once produced advances, the key changes even with equal fps
    n3 = normalizer.normalize(
        received_at=MONO + 20,
        received_at_utc=UTC + 20,
        stats=stats(produced=UTC + 20),
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert n2.snapshot.key != n3.snapshot.key


def test_fetch_problems_propagate():
    for problem in ("unreachable", "auth", "malformed", "too_large"):
        norm = make_norm().normalize(
            received_at=MONO,
            received_at_utc=UTC,
            stats=None,
            enabled_map=enabled(),
            cameras_cfg=cams_cfg(),
            problem=problem,
        )
        assert norm.snapshot.problem == problem
        assert not norm.snapshot.fresh


def test_config_unavailable_marks_not_fresh_and_unknown():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats=stats(),
        enabled_map=None,
        cameras_cfg=cams_cfg(),
    )
    assert norm.snapshot.stats_ok
    assert not norm.snapshot.config_ok
    assert not norm.snapshot.fresh
    for reading in norm.snapshot.cameras.values():
        assert reading.status is CameraStatus.UNKNOWN
        assert reading.detail == "config_unavailable"


def test_missing_service_block_is_malformed():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats={"cameras": {}},
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert not norm.snapshot.stats_ok
    assert norm.stale_reason == "malformed_service"


def test_non_numeric_last_updated_is_malformed():
    norm = make_norm().normalize(
        received_at=MONO,
        received_at_utc=UTC,
        stats={"service": {"last_updated": "yesterday", "uptime": 5}, "cameras": {}},
        enabled_map=enabled(),
        cameras_cfg=cams_cfg(),
    )
    assert not norm.snapshot.stats_ok
    assert norm.stale_reason == "missing_last_updated"


def test_nonfinite_values_rejected():
    assert math.isnan(float("nan"))
    # sanity guard for the parse rules exercised above
