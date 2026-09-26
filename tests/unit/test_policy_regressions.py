"""Regression tests for issues found by adversarial review.

Each test names the finding it pins down.
"""

from __future__ import annotations

import pytest

from frigate_watchdog.config import ConfigError, parse_config
from frigate_watchdog.policy import CameraState, DecisionEngine
from tests.fakes.harness import build_config, make_harness, warm_up_healthy

ALL = ("porch", "driveway", "doorbell")


def healthy_fps(zero: tuple[str, ...] = ()) -> dict[str, float]:
    return {k: (0.0 if k in zero else 5.0) for k in ALL}


def test_finding_1_latched_resolves_via_healthy_confirmation():
    """LATCHED must not be clobbered to HEALTHY; confirmation must resolve it."""
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    # boot grace elapses with no frames -> LATCHED
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    h.tick()
    assert h.state_of("porch") == CameraState.LATCHED.value
    assert h.store.incident("porch").latched
    # frames return; state must stay LATCHED until confirmed, then resolve
    h.poll(fps=healthy_fps())
    h.poll(fps=healthy_fps())
    assert h.state_of("porch") == CameraState.LATCHED.value, "clobbered before confirmation"
    for _ in range(6):
        h.poll(fps=healthy_fps())
    assert h.state_of("porch") in (CameraState.RECOVERED.value, CameraState.HEALTHY.value)
    assert not h.store.incident("porch").latched, "durable latch must clear on confirmation"


def test_finding_1_restart_adopts_durable_latch_and_resolves():
    """After a restart, a latched camera that recovers must clear its latch."""
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "OUTCOME_UNKNOWN")
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    h.tick()
    assert h.state_of("porch") == CameraState.LATCHED.value
    # restart: brand-new engine over the same durable state
    h.engine = DecisionEngine(h.config, h.store, start_mono=h.clock.mono, start_utc=h.clock.utc)
    # camera recovered while we were down; healthy confirmation must resolve
    for _ in range(8):
        h.poll(fps=healthy_fps())
    assert not h.store.incident("porch").latched
    assert h.state_of("porch") in (CameraState.RECOVERED.value, CameraState.HEALTHY.value)


def test_finding_3_cached_snapshots_pause_healthy_confirmation():
    """Cached snapshots must not advance the recovery-confirmation streak."""
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    # healthy flow, but Frigate serves the same cached snapshot repeatedly
    for _ in range(7):
        h.poll(fps=healthy_fps(), cached=True)
    assert h.state_of("porch") == CameraState.BOOT_GRACE.value, "cached data confirmed recovery"
    # fresh data resumes: confirmation completes on real evidence
    for _ in range(8):
        h.poll(fps=healthy_fps())
    assert h.state_of("porch") in (CameraState.RECOVERED.value, CameraState.HEALTHY.value)


def test_finding_3_cached_snapshots_pause_arming():
    h = make_harness()
    h.poll(advance=70)
    # healthy but cached: arming must not complete on repeated snapshots
    for _ in range(8):
        h.poll(fps=healthy_fps(), cached=True)
    assert not h.store.armed("porch")
    # fresh evidence arms normally
    for _ in range(4):
        h.poll(fps=healthy_fps())
    assert h.store.armed("porch")


def test_finding_3_cached_snapshots_pause_failure_interval():
    """Failure interval must not advance on cached data either."""
    h = make_harness()
    warm_up_healthy(h)
    h.poll(fps=healthy_fps(("porch",)))
    # 4 cached polls (40s wall, within the 60s staleness horizon) with only
    # the first distinct
    for _ in range(4):
        h.poll(fps=healthy_fps(("porch",)), cached=True)
    rt = h.engine.cameras["porch"]
    assert rt.bad_since is not None
    assert h.clock.mono - rt.bad_since < 25, "cached polls advanced the failure interval"
    assert len(rt.bad_keys) == 1


def test_finding_4_poll_interval_vs_monitoring_gap_crosschecked():
    doc_text = build_config(timings={"poll_interval_s": 60.0, "monitoring_gap_s": 30.0})
    with pytest.raises(ConfigError) as err:
        parse_config(doc_text, env={f"CAMERA_{k.upper()}_PASSWORD": "x" for k in ALL})
    assert any(code == "invalid_timing" for _, code, _ in err.value.problems)


def test_finding_5_failure_outcome_emits_durable_latch():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "REJECTED")
    assert h.store.incident("porch").latched, "REJECTED must durably latch the outage"


def test_finding_8_preflight_failure_single_cancel_event():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    h.preflight_failed("porch", "UNREACHABLE")
    cancels = [e for e in h.events if e.kind == "action_cancelled"]
    assert len(cancels) == 1


def test_preflight_auth_failed_latches_without_consuming_attempt():
    """Wrong ONVIF credentials at the read-only preflight must durably
    auth-latch; no reservation happens, so no attempt is consumed."""
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    h.preflight_failed("porch", "AUTH_FAILED")
    assert "porch" in h.store.auth_latches
    assert h.store.attempts == [], "preflight auth failure must not consume an attempt"
    assert not h.store.incident("porch").latched, "no send happened; no outage latch"
    check = h.engine.evaluate_action(
        "porch",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    assert any(f.code == "AUTH_LATCHED" for f in check.failures)


def test_finding_9_bad_keys_capped():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(40):
        h.poll(fps=healthy_fps(("porch",)))
    rt = h.engine.cameras["porch"]
    assert len(rt.bad_keys) <= 3  # min_bad_snapshots in test timings


def test_finding_6_evidence_requires_post_grace_start():
    """Evidence accumulated during startup grace must not count."""
    h = make_harness(timings={"startup_grace_s": 300.0})
    h.poll(fps=healthy_fps(), advance=70)  # flowing during grace; not armed yet
    # grace 300s: failures during grace accumulate no evidence
    for _ in range(10):
        h.poll(fps=healthy_fps(("porch",)))
    rt = h.engine.cameras["porch"]
    assert rt.bad_keys == set()
    assert rt.bad_since is not None  # streak clock runs, but no snapshot counts
    # after grace (t >= 1300), distinct snapshots count
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)), advance=30.0)
    assert len(rt.bad_keys) >= 3


def test_finding_7_cancel_reason_is_specific():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    # telemetry goes stale -> cancel reason should be the stale code, not
    # the incidental NOT_FAILING evidence code
    h.poll(problem="unreachable")
    cancels = [e for e in h.events if e.kind == "action_cancelled"]
    assert cancels, "proposal should cancel when telemetry is unavailable"
    assert cancels[-1].reason not in ("NOT_FAILING", "EVIDENCE_TOO_SHORT")


def test_finding_12_reservation_blocks_new_proposals():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    attempt = h.reserve("porch")
    assert h.engine.reservation_active
    # outcome never arrives (crash window); no other camera may be proposed
    for _ in range(8):
        h.poll(fps=healthy_fps(("driveway",)))
    assert h.total_proposals == 1
    check = h.engine.evaluate_action(
        "driveway",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    assert any(f.code == "OPERATION_IN_FLIGHT" for f in check.failures)
    # the outcome releases the hold
    h.outcome("porch", attempt, "OUTCOME_UNKNOWN")
    assert not h.engine.reservation_active
