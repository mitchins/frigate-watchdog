"""Deterministic adversarial tests for the decision engine.

Each test maps to a named scenario in the project brief. Hours pass in
microseconds by advancing the fake clock; no real sleeps.
"""

from __future__ import annotations

import pytest

from frigate_watchdog.policy import CameraState, GlobalState
from tests.fakes.harness import make_harness, warm_up_healthy

ALL = ("porch", "driveway", "doorbell")


def healthy_fps(zero: tuple[str, ...] = ()) -> dict[str, float]:
    return {k: (0.0 if k in zero else 5.0) for k in ALL}


def drive_failure(h, camera: str, extra_zero: tuple[str, ...] = ()):
    """Poll until the failure interval and snapshot count are complete."""
    zero = (camera, *extra_zero)
    result = None
    for _ in range(8):  # 8 * 10s = 80s > 60s threshold; > 3 snapshots
        result = h.poll(fps=healthy_fps(zero))
    return result


@pytest.fixture()
def h():
    harness = make_harness()
    warm_up_healthy(harness)
    return harness


# --------------------------------------------------------------------- basics


def test_single_zero_sample_then_recovery_no_proposal(h):
    h.poll(fps=healthy_fps(("porch",)))
    result = h.poll(fps=healthy_fps())
    assert h.total_proposals == 0
    assert h.state_of("porch") == CameraState.HEALTHY.value
    assert result.decision.proposal is None


def test_sustained_single_failure_proposes_exactly_once(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    assert h.last_proposal.camera == "porch"
    assert h.state_of("porch") == CameraState.RECOVERY_PENDING.value
    # continued failure does not produce a second proposal
    for _ in range(10):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1


def test_startup_grace_blocks_proposal(h):
    h2 = make_harness()  # fresh engine, in startup grace
    for _ in range(4):  # 40s of all-zero right after start
        h2.poll(fps=healthy_fps(ALL))
    assert h2.total_proposals == 0
    assert GlobalState.STARTING in h2.engine.global_states
    # past grace, still all failing: no healthy witness, never any recovery
    for _ in range(6):
        h2.poll(fps=healthy_fps(ALL))
    assert h2.total_proposals == 0
    assert GlobalState.MULTIPLE_CAMERAS_UNHEALTHY in h2.engine.global_states
    assert "NO_HEALTHY_PEER" in h2.engine.global_inhibition_reasons(h2.clock.mono)


def test_proposal_requires_all_guards(tmp_path):
    h = make_harness()
    warm_up_healthy(h)
    # doorbell is recovery:none: still a witness, never a target
    drive_failure(h, "doorbell")
    assert h.total_proposals == 0
    check = h.engine.evaluate_action(
        "doorbell",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    codes = {f.code for f in check.failures}
    assert "RECOVERY_DISABLED_FOR_CAMERA" in codes


def test_never_armed_camera_never_proposes(h):
    h2 = make_harness()
    h2.poll(fps=healthy_fps())  # t+10
    h2.poll(fps=healthy_fps())  # t+20 (< arm_healthy_s 30)
    # porch never armed; now it fails
    drive_failure(h2, "porch")
    assert h2.total_proposals == 0
    assert h2.state_of("porch") == CameraState.UNARMED.value
    unarmed_events = [e for e in h2.events if e.reason == "UNARMED"]
    assert unarmed_events, "an unarmed failure must be reported once"


# --------------------------------------------------------------------- group


def test_second_failure_does_not_block_the_first(h):
    # porch has been failing a while; driveway starts failing just before
    # porch's evidence completes. The doorbell is a stably healthy witness.
    for _ in range(6):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 0  # not yet 60s/3 snapshots
    for _ in range(2):
        h.poll(fps=healthy_fps(("porch", "driveway")))
    assert h.total_proposals == 1
    assert h.last_proposal.camera == "porch"  # longest-failing first
    assert h.events_of("multiple_failing"), "the multi-camera episode is reported"


def test_simultaneous_failures_recovered_one_at_a_time(h):
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch", "driveway")))
    assert h.total_proposals == 1
    first = h.last_proposal.camera
    attempt = h.reserve(first)
    h.outcome(first, attempt, "OUTCOME_UNKNOWN")
    # while the first camera is in boot grace, the second is never proposed
    for _ in range(10):
        h.poll(fps=healthy_fps(("porch", "driveway")))
    assert h.total_proposals == 1


def test_phased_recovery_waits_for_confirmation_then_spacing(h):
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch", "driveway")))
    assert h.last_proposal.camera == "porch"
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "OUTCOME_UNKNOWN")
    # porch comes back and is confirmed healthy; driveway is still dark
    for _ in range(14):
        h.poll(fps=healthy_fps(("driveway",)))
    assert h.events_of("recovery_confirmed")
    assert h.total_proposals == 1, "global spacing still holds driveway back"
    # once the global 5-minute spacing has elapsed, driveway is next
    for _ in range(20):
        h.poll(fps=healthy_fps(("driveway",)))
    assert h.total_proposals == 2
    assert h.last_proposal.camera == "driveway"


def test_phasing_halts_when_a_reboot_did_not_restore_frames(h):
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch", "driveway")))
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "OUTCOME_UNKNOWN")
    for _ in range(20):  # boot grace elapses with porch still dark
        h.poll(fps=healthy_fps(("porch", "driveway")))
        h.tick()
    assert h.state_of("porch") == CameraState.LATCHED.value
    for _ in range(40):  # well past cooldowns and spacing
        h.poll(fps=healthy_fps(("porch", "driveway")))
    assert h.total_proposals == 1, "no further phased reboots after a failed one"
    assert "PHASED_RECOVERY_HALTED" in h.engine.global_inhibition_reasons(h.clock.mono)


def test_no_recovery_when_no_camera_is_healthy(h):
    for _ in range(10):
        h.poll(fps=healthy_fps(ALL))
    assert h.total_proposals == 0
    assert "NO_HEALTHY_PEER" in h.engine.global_inhibition_reasons(h.clock.mono)


def test_evidence_survives_a_peer_recovering(h):
    for _ in range(4):
        h.poll(fps=healthy_fps(("porch", "driveway")))
    h.poll(fps=healthy_fps(("porch",)))  # driveway recovers on its own
    rt = h.engine.cameras["porch"]
    assert len(rt.bad_keys) >= 3, "porch's evidence window is not reset by a peer"


def test_pending_action_survives_a_second_failure(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    h.poll(fps=healthy_fps(("porch", "driveway")))
    assert h.engine.in_flight is not None and h.engine.in_flight.camera == "porch"
    assert not h.events_of("action_cancelled")


def test_pending_action_cancelled_when_camera_recovers(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    h.poll(fps=healthy_fps())  # porch flowing again
    assert h.engine.in_flight is None
    assert h.events_of("action_cancelled")


def test_unknown_peer_blocks_action(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    # dispatch recheck with a snapshot where driveway is unknown
    h.poll(fps=healthy_fps(("porch",)), omit_from_stats=("driveway",))
    assert h.engine.in_flight is None
    check = h.recheck("porch")
    assert not check.permitted
    assert any(f.code == "PEER_NOT_HEALTHY" for f in check.failures)


# --------------------------------------------------------------------- freshness


def test_cached_snapshot_is_one_observation(h):
    # porch fails; Frigate then serves the same cached snapshot repeatedly
    h.poll(fps=healthy_fps(("porch",)))
    rt = h.engine.cameras["porch"]
    count_after_first = len(rt.bad_keys)
    for _ in range(6):
        h.poll(fps=healthy_fps(("porch",)), cached=True)
    assert len(rt.bad_keys) == count_after_first
    assert h.total_proposals == 0


def test_cached_snapshot_eventually_becomes_stale(h):
    h.poll(fps=healthy_fps(("porch",)))
    for _ in range(7):
        result = h.poll(fps=healthy_fps(("porch",)), cached=True)
    # age grows past max_stats_age_s=60 -> not fresh
    assert not result.snapshot.fresh
    assert "TELEMETRY_STALE" in h.engine.global_inhibition_reasons(h.clock.mono)
    assert h.total_proposals == 0


def test_fresh_evidence_with_unchanged_zero_is_fine(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1  # producer timestamp advanced each poll


def test_frigate_unavailable_blocks_everything(h):
    warm = h
    drive_failure(warm, "porch")
    assert warm.total_proposals == 1
    warm.poll(problem="unreachable")
    assert GlobalState.FRIGATE_UNAVAILABLE in warm.engine.global_states
    assert warm.total_proposals == 1  # no new proposals while unreachable


def test_frigate_restart_discards_evidence_but_keeps_limits(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    # Frigate restarts
    h.poll(fps=healthy_fps(), restart=True)
    assert "FRIGATE_RESTART_GRACE_ACTIVE" in h.engine.global_inhibition_reasons(h.clock.mono)
    rt = h.engine.cameras["porch"]
    assert rt.bad_keys == set()  # evidence discarded
    assert h.store.incident("porch") is not None and h.store.incident("porch").latched
    # the latch (attempt consumed) survives the restart
    check = h.engine.evaluate_action(
        "porch",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    assert any(f.code == "OUTAGE_ATTEMPT_CONSUMED" for f in check.failures)


def test_monitoring_gap_clears_evidence_not_limits(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "OUTCOME_UNKNOWN")
    # a >30s gap in polls
    h.poll(fps=healthy_fps(("porch",)), advance=60.0)
    assert "MONITORING_INTERRUPTED" in h.engine.global_inhibition_reasons(h.clock.mono)
    rt = h.engine.cameras["porch"]
    assert rt.state in (CameraState.LATCHED, CameraState.BOOT_GRACE)
    assert h.store.incident("porch").latched


def test_wall_clock_jumps_do_not_expire_limits(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    for _ in range(8):
        h.poll(fps=healthy_fps())
    assert h.state_of("porch") in (CameraState.HEALTHY.value, CameraState.RECOVERED.value)
    # second outage while the host wall clock sprints forward (+2h per poll):
    # the accumulated-runtime cooldown must barely move
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)), advance=10.0, utc_drift=7200.0)
    assert h.total_proposals == 1, "cooldown must survive wall-clock sprints"
    acct = h.store.accounting("porch")
    remaining = acct.cooldown_until - h.store.acc
    assert remaining > 3400, remaining


def test_backward_wall_clock_makes_telemetry_stale(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    h.poll(fps=healthy_fps())  # proposal cancelled on recovery
    # host wall clock jumps back an hour: snapshots look future-dated -> stale
    result = h.poll(fps=healthy_fps(("porch",)), advance=10.0, utc_drift=-3600.0)
    assert not result.snapshot.fresh
    assert result.snapshot.problem == "future"
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)), advance=10.0, utc_drift=0.0)
    # evidence does not accumulate from stale snapshots
    assert h.total_proposals == 1


# --------------------------------------------------------------------- latch & budget


def test_camera_never_returns_no_hourly_loop(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    # boot grace (60s) elapses without frames -> LATCHED
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    h.tick()
    assert h.state_of("porch") == CameraState.LATCHED.value
    # hours pass: latched forever, no new attempts
    for _ in range(40):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    assert len(h.store.attempts) == 1


def test_brief_frame_burst_does_not_rearm(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    h.poll(fps=healthy_fps())  # one healthy poll (10s < 60s confirm)
    assert h.store.incident("porch").latched  # burst does not resolve anything
    # fails again; boot grace elapses without confirmed recovery
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    h.tick()
    assert h.state_of("porch") == CameraState.LATCHED.value
    for _ in range(10):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    assert h.store.incident("porch").latched


def test_recovery_confirmed_after_stable_frames(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    for _ in range(8):  # > boot grace 60 + confirm 60
        h.poll(fps=healthy_fps())
    assert h.state_of("porch") in (CameraState.RECOVERED.value, CameraState.HEALTHY.value)
    incident = h.store.incident("porch")
    assert incident is None or not incident.latched


def test_recovers_then_fails_within_cooldown_no_second_attempt(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    for _ in range(8):
        h.poll(fps=healthy_fps())  # recovered
    # falls again immediately; cooldown (1h acc) far from elapsed
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    assert len(h.store.attempts) == 1


def test_budget_exhaustion_after_three_attempts(h):
    for cycle in range(3):
        drive_failure(h, "porch")
        assert h.total_proposals == cycle + 1, f"no proposal on cycle {cycle}"
        if h.store.incident("porch") and h.store.incident("porch").latched:
            h.acknowledge("porch")
        attempt = h.reserve("porch")
        h.outcome("porch", attempt, "ACKNOWLEDGED")
        # recover, then wait out the 1h accumulated-clock cooldown
        for _ in range(120):
            h.poll(fps=healthy_fps(), advance=30.0)
        assert h.store.incident("porch") is None or not h.store.incident("porch").latched
    # 3 attempts consumed; a fresh outage must not propose
    drive_failure(h, "porch")
    assert len(h.store.attempts) == 3
    check = h.engine.evaluate_action(
        "porch",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    assert any(f.code == "BUDGET_EXHAUSTED" for f in check.failures)
    assert h.total_proposals >= 3
    assert len(h.store.attempts) == 3, "budget must cap attempts at 3 per 24h window"


def test_ack_clears_latch_but_not_cooldown_or_budget(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    h.acknowledge("porch")
    assert not h.store.incident("porch").latched
    check = h.engine.evaluate_action(
        "porch",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    codes = {f.code for f in check.failures}
    assert "OUTAGE_ATTEMPT_CONSUMED" not in codes
    assert "COOLDOWN_ACTIVE" in codes
    assert len(h.store.attempts) == 1


def test_reservation_without_outcome_is_consumed(h):
    # crash between reservation and send: attempt remains consumed
    drive_failure(h, "porch")
    h.reserve("porch")  # attempt id unused here
    assert h.store.attempts[-1][2] is None  # outcome never recorded
    # new engine instance over the same store (restart)
    from frigate_watchdog.policy import DecisionEngine

    engine2 = DecisionEngine(h.config, h.store, start_mono=h.clock.mono, start_utc=h.clock.utc)
    assert engine2.store.incident("porch").latched
    h.engine = engine2
    warm = h.poll(fps=healthy_fps(("porch",)))
    del warm
    check = h.engine.evaluate_action(
        "porch",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    assert any(f.code == "OUTAGE_ATTEMPT_CONSUMED" for f in check.failures)


# --------------------------------------------------------------------- auth/preflight outcomes


def test_unsupported_reboot_latches(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "UNSUPPORTED")
    assert h.state_of("porch") == CameraState.LATCHED.value
    assert h.store.auth_latched("porch")
    h.acknowledge("porch")
    check = h.engine.evaluate_action(
        "porch",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    codes = {f.code for f in check.failures}
    assert "AUTH_LATCHED" not in codes  # ack cleared it
    assert "COOLDOWN_ACTIVE" in codes


def test_auth_failed_latches_and_prevents_credential_storm(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "AUTH_FAILED")
    assert h.store.auth_latched("porch")
    for _ in range(30):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    assert len(h.store.attempts) == 1


def test_preflight_failure_cancels_and_backs_off(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    h.preflight_failed("porch", "UNREACHABLE")
    assert h.engine.in_flight is None
    # immediately eligible again by evidence, but backoff blocks re-proposal
    for _ in range(3):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    check = h.recheck("porch")
    assert any(f.code == "PREFLIGHT_BACKOFF" for f in check.failures)


def test_recheck_requires_preflight_for_dispatch(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    check = h.recheck("porch", preflight_ok=None)
    assert not check.permitted
    assert any(f.code == "PREFLIGHT_NOT_RUN" for f in check.failures)
    check = h.recheck("porch", preflight_ok=True)
    assert check.permitted, check.failures


def test_recheck_requires_active_proposal(h):
    drive_failure(h, "porch")
    h.poll(fps=healthy_fps())  # recovers; proposal cancelled
    check = h.recheck("porch", preflight_ok=True)
    assert not check.permitted
    assert any(f.code in ("NO_ACTIVE_PROPOSAL", "NOT_FAILING") for f in check.failures)


# --------------------------------------------------------------------- misc guards


def test_disabled_camera_never_targeted(h):
    drive_failure(h, "porch")
    h.poll(fps=healthy_fps(("porch",)), enabled={"porch": False})
    assert h.state_of("porch") == CameraState.DISABLED.value
    assert h.total_proposals == 1  # pre-existing; new ones blocked below
    h.preflight_failed("porch", "n/a")  # clear pending
    for _ in range(8):
        h.poll(fps=healthy_fps(), enabled={"porch": False})
    assert h.total_proposals == 1
    check = h.recheck("porch", preflight_ok=True)
    assert any(f.code == "CAMERA_DISABLED" for f in check.failures)


def test_reenabled_camera_gets_fresh_observation_no_immediate_action(h):
    drive_failure(h, "porch")
    h.preflight_failed("porch", "n/a")
    h.poll(fps=healthy_fps(), enabled={"porch": False})
    # re-enabled, still zero frames: fresh grace, no instant proposal
    for _ in range(6):
        h.poll(fps=healthy_fps(("porch",)))
    # evidence accumulates from re-enable; with default timings it eventually
    # proposes again (allowed: separate fresh outage window + latch state)
    assert h.state_of("porch") in (CameraState.SUSPECT.value, CameraState.UNARMED.value)


def test_maintenance_camera_excluded(h):
    hm = make_harness(maintenance=("porch",))
    warm_up_healthy(hm, cameras_still_zero=("porch",))
    hm.store.mark_armed("porch")  # baseline earned before maintenance began
    hm.poll(fps=healthy_fps())  # engine picks up armed state
    # porch under maintenance + failing, driveway genuinely failing:
    # maintenance porch is not a witness and never a target
    for _ in range(9):
        hm.poll(fps=healthy_fps(("porch", "driveway")))
    assert hm.total_proposals == 1
    assert hm.last_proposal.camera == "driveway"
    assert hm.state_of("porch") == CameraState.DISABLED.value


def test_single_camera_installation_never_recovers(h):
    hs = make_harness(cameras={"porch": ("Porch", "onvif")})
    warm_up_healthy(hs)
    for _ in range(9):
        hs.poll(fps={"porch": 0.0})
    assert hs.total_proposals == 0
    check = hs.engine.evaluate_action(
        "porch",
        snapshot=hs.engine.last_snapshot,
        now_mono=hs.clock.mono,
        now_acc=hs.store.acc,
    )
    assert any(f.code == "NO_HEALTHY_PEER" for f in check.failures)
    assert "NO_HEALTHY_PEER" in hs.engine.global_inhibition_reasons(hs.clock.mono)


def test_observe_mode_never_proposes_under_any_failure(h):
    ho = make_harness(mode="observe")
    warm_up_healthy(ho)
    for _ in range(9):
        ho.poll(fps=healthy_fps(ALL))  # everyone failing
    assert ho.total_proposals == 0
    for _ in range(9):
        ho.poll(fps=healthy_fps(("porch",)))
    assert ho.total_proposals == 0
    assert GlobalState.OBSERVING in ho.engine.global_states
    assert "MODE_OBSERVE" in ho.engine.global_inhibition_reasons(ho.clock.mono)


def test_store_unsafe_inhibits(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    h.poll()  # healthy again clears proposal
    d = h.engine.on_store_failure(h.clock.mono, h.clock.utc)
    h.events.extend(d.events)
    for _ in range(8):
        h.poll(fps=healthy_fps(("porch",)))
    assert h.total_proposals == 1
    assert GlobalState.STATE_STORE_UNSAFE in h.engine.global_states
    assert "STATE_STORE_UNSAFE" in h.engine.global_inhibition_reasons(h.clock.mono)


def test_global_spacing_between_cameras(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    h.acknowledge("porch")
    # porch healthy again; both cooldown and 5-min global spacing apply
    for _ in range(12):
        h.poll(fps=healthy_fps())
    # now driveway fails long enough for its own window
    for _ in range(8):
        h.poll(fps=healthy_fps(("driveway",)))
    check = h.engine.evaluate_action(
        "driveway",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    codes = {f.code for f in check.failures}
    assert "GLOBAL_SPACING_ACTIVE" in codes
    assert h.total_proposals == 1


def test_latched_peer_blocks_other_camera_via_in_flight_rules(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "OUTCOME_UNKNOWN")  # BOOT_GRACE
    # driveway fails while porch's boot grace is unresolved: blocked
    for _ in range(3):
        h.poll(fps=healthy_fps(("driveway",)))
    check = h.engine.evaluate_action(
        "driveway",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    assert any(f.code == "OPERATION_IN_FLIGHT" for f in check.failures)
    assert h.total_proposals == 1
    # once porch's recovery is *confirmed*, the hold is released
    for _ in range(11):
        h.poll(fps=healthy_fps(("driveway",)))
    assert h.state_of("porch") in (CameraState.RECOVERED.value, CameraState.HEALTHY.value)
    assert h.engine._boot_grace_active_anywhere() is None
    check = h.engine.evaluate_action(
        "driveway",
        snapshot=h.engine.last_snapshot,
        now_mono=h.clock.mono,
        now_acc=h.store.acc,
    )
    assert not any(f.code == "OPERATION_IN_FLIGHT" for f in check.failures)


def test_all_reasons_reported_not_hidden(h):
    ho = make_harness(mode="observe")
    reasons = ho.engine.global_inhibition_reasons(ho.clock.mono)
    assert "MODE_OBSERVE" in reasons
    assert "STARTUP_GRACE_ACTIVE" in reasons
    assert "TELEMETRY_STALE" in reasons  # no snapshot yet
    assert "NO_HEALTHY_PEER" not in reasons


def test_boot_grace_respected_then_recovery(h):
    drive_failure(h, "porch")
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "ACKNOWLEDGED")
    # frames return after ~30s (within boot grace 60s), confirm needs 60s
    h.poll(fps=healthy_fps())
    h.poll(fps=healthy_fps())
    h.poll(fps=healthy_fps())
    assert h.state_of("porch") == CameraState.BOOT_GRACE.value  # not confirmed yet
    for _ in range(7):
        h.poll(fps=healthy_fps())
    assert h.state_of("porch") in (
        CameraState.RECOVERED.value,
        CameraState.HEALTHY.value,
    )
    assert not h.store.incident("porch").latched


def test_event_pause_does_not_queue_stale_action(h):
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    # event loop stalls 10 minutes without any poll
    h.clock.advance(600.0)
    h.store.acc += 600.0
    h.tick()
    assert h.engine.in_flight is None or h.engine.monitoring_resumed_at is not None
    # the old proposal was cancelled by the gap; nothing stale is dispatched
    assert h.engine.in_flight is None


def test_healthy_peer_recovery_of_witness_required(h):
    # doorbell (recovery:none) is a witness: while unknown, no action for porch
    drive_failure(h, "porch")
    assert h.total_proposals == 1
    h.poll(fps=healthy_fps(("porch",)), omit_from_stats=("doorbell",))
    check = h.recheck("porch", preflight_ok=True)
    assert any(f.code == "PEER_NOT_HEALTHY" for f in check.failures)
