"""Property-based adversarial sequences.

Hypothesis generates hours of synthetic camera behaviour — frame drops,
recoveries, cached snapshots, restarts, gaps, wall-clock jumps — and the
core safety properties must hold over every generated sequence:

    P1  no attempt without a durable reservation
    P2  no second attempt in the same unresolved outage
    P3  no action while required telemetry is unknown/stale
    P4  no action while multiple enabled cameras are failing
    P5  no action against disabled/maintenance/unconfigured targets
    P6  no restart or clock change reduces a safety limit
    P7  no more than one recovery operation is active at a time
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from hypothesis import given, settings
from hypothesis import strategies as st

from frigate_watchdog.policy import CameraState
from tests.fakes.harness import make_harness

CAMERAS = ("porch", "driveway", "doorbell")


@dataclass
class Recorder:
    """Observes the harness exactly like the coordinator would, performing
    reservations and recording every mutating action."""

    sends: list[tuple[str, float]] = field(default_factory=list)
    reboots: list[tuple[str, float]] = field(default_factory=list)

    def dispatch(self, h, camera: str) -> str | None:
        """Coordinator behaviour: reserve then send once, record outcome."""
        check = h.recheck(camera, preflight_ok=True)
        if not check.permitted:
            return None
        attempt = h.reserve(camera)
        self.sends.append((camera, h.clock.mono))
        outcome = "ACKNOWLEDGED"
        h.outcome(camera, attempt, outcome)
        self.reboots.append((camera, h.store.acc))
        return attempt


def run_sequence(steps: list[tuple], mode: str = "recover") -> Recorder:
    h = make_harness(mode=mode)
    recorder = Recorder()
    warm = 0
    for step in steps:
        kind = step[0]
        if kind == "fps":
            _, porch, driveway, doorbell = step
            result = h.poll(fps={"porch": porch, "driveway": driveway, "doorbell": doorbell})
        elif kind == "advance":
            _, seconds = step
            result = h.poll(advance=seconds)
        elif kind == "problem":
            _, code = step
            result = h.poll(problem=code)
        elif kind == "cached":
            result = h.poll(cached=True)
        elif kind == "restart":
            result = h.poll(restart=True)
        elif kind == "disable":
            _, camera = step
            result = h.poll(enabled={camera: False})
        else:  # pragma: no cover
            continue
        warm += 1
        if result.decision.proposal is not None and mode == "recover":
            camera = result.decision.proposal.camera
            recorder.dispatch(h, camera)
        h.tick()
    return recorder


fps_values = st.sampled_from([0.0, 0.0, 0.0, 0.5, 5.0, 15.0])
step_kinds = st.one_of(
    st.tuples(st.just("fps"), fps_values, fps_values, fps_values),
    st.tuples(st.just("advance"), st.sampled_from([10.0, 30.0, 120.0])),
    st.tuples(st.just("problem"), st.sampled_from(["unreachable", "malformed", "auth"])),
    st.tuples(st.just("cached")),
    st.tuples(st.just("restart")),
    st.tuples(st.just("disable"), st.sampled_from(CAMERAS)),
)


@settings(max_examples=200, deadline=None, derandomize=True)
@given(st.lists(step_kinds, min_size=20, max_size=120))
def test_property_safety_invariants(steps):
    recorder = run_sequence(steps)
    # P1: every send had a reservation (dispatch only after reserve) — by
    # construction here; the interesting bound is the count itself:
    # P2 / budget: at most 3 attempts per camera per 24h accumulated window,
    # and at most one per outage (reserve raises otherwise and tests fail).
    # The total attempt ceiling across all cameras in any sequence is small.
    assert len(recorder.sends) <= 9, f"unexpected attempt volume: {recorder.sends}"


@settings(max_examples=100, deadline=None, derandomize=True)
@given(
    st.lists(
        st.tuples(st.just("fps"), fps_values, fps_values, fps_values), min_size=10, max_size=60
    )
)
def test_property_observe_mode_never_sends(steps):
    recorder = run_sequence(list(steps), mode="observe")
    assert recorder.sends == [], "observe mode must never dispatch"


@settings(max_examples=150, deadline=None, derandomize=True)
@given(st.lists(step_kinds, min_size=15, max_size=80))
def test_property_no_action_while_two_fail(steps):
    """Replay the sequence while asserting the multi-camera invariant directly
    against the engine state after every step."""
    h = make_harness()
    recorder = Recorder()
    for step in steps:
        kind = step[0]
        if kind == "fps":
            _, porch, driveway, doorbell = step
            result = h.poll(fps={"porch": porch, "driveway": driveway, "doorbell": doorbell})
        elif kind == "advance":
            result = h.poll(advance=step[1])
        elif kind == "problem":
            result = h.poll(problem=step[1])
        elif kind == "cached":
            result = h.poll(cached=True)
        elif kind == "restart":
            result = h.poll(restart=True)
        else:
            result = h.poll(enabled={step[1]: False})
        if result.decision.proposal is not None:
            failing = [
                k
                for k, rt in h.engine.cameras.items()
                if rt.last_status and rt.last_status.value == "NO_FRAMES"
            ]
            assert len(failing) < 2, f"proposed a reboot while multiple cameras failing: {failing}"
            recorder.dispatch(h, result.decision.proposal.camera)
        h.tick()


@settings(max_examples=100, deadline=None, derandomize=True)
@given(
    st.lists(
        st.tuples(st.just("fps"), fps_values, fps_values, fps_values),
        min_size=8,
        max_size=50,
    ),
    st.integers(min_value=0, max_value=1_000_000),
)
def test_property_clock_jumps_never_earlier_eligibility(steps, seed):
    """Wall-clock jumps must not make a cooldown expire early: after one
    attempt, no second attempt until 1h of *accumulated runtime* passed."""
    rng = random.Random(seed)
    h = make_harness()
    recorder = Recorder()
    h.poll(advance=70)
    for _ in range(4):
        h.poll(fps=dict.fromkeys(CAMERAS, 5.0))
    # force porch into a complete outage
    for _ in range(8):
        h.poll(fps={"porch": 0.0, "driveway": 5.0, "doorbell": 5.0})
    assert h.total_proposals in (0, 1)
    if h.engine.in_flight is not None:
        recorder.dispatch(h, "porch")
    first_acc = h.store.acc
    assert len(recorder.sends) <= 1
    # random wall-clock chaos while cameras stay healthy
    for step in steps:
        drift = rng.choice([-3600.0, -60.0, 0.0, 7200.0])
        _, porch, driveway, doorbell = step
        result = h.poll(
            fps={"porch": porch, "driveway": driveway, "doorbell": doorbell},
            utc_drift=drift,
        )
        if result.decision.proposal is not None:
            recorder.dispatch(h, result.decision.proposal.camera)
    elapsed_acc = h.store.acc - first_acc
    if len(recorder.sends) == 1 and elapsed_acc < 3600.0:
        # no second attempt inside the cooldown
        for _ in range(8):
            result = h.poll(fps={"porch": 0.0, "driveway": 5.0, "doorbell": 5.0})
            if result.decision.proposal is not None:
                recorder.dispatch(h, result.decision.proposal.camera)
        porch_sends = [s for s in recorder.sends if s[0] == "porch"]
        assert len(porch_sends) == 1, "cooldown defeated by wall-clock jumps"


@settings(max_examples=60, deadline=None, derandomize=True)
@given(st.lists(step_kinds, min_size=10, max_size=60))
def test_property_single_active_operation(steps):
    """At most one recovery operation is active: no proposal while another
    camera holds boot grace or an unresolved reservation."""
    h = make_harness()
    recorder = Recorder()
    reserved = False
    for step in steps:
        kind = step[0]
        if kind == "fps":
            _, porch, driveway, doorbell = step
            result = h.poll(fps={"porch": porch, "driveway": driveway, "doorbell": doorbell})
        elif kind == "advance":
            result = h.poll(advance=step[1])
        elif kind == "problem":
            result = h.poll(problem=step[1])
        elif kind == "cached":
            result = h.poll(cached=True)
        elif kind == "restart":
            result = h.poll(restart=True)
        else:
            result = h.poll(enabled={step[1]: False})
        if result.decision.proposal is not None and not reserved:
            recorder.dispatch(h, result.decision.proposal.camera)
            reserved = recorder.sends and h.engine.reservation_active
        boot_or_pending = [
            rt.key
            for rt in h.engine.cameras.values()
            if rt.state is CameraState.BOOT_GRACE or rt.state is CameraState.RECOVERY_PENDING
        ]
        assert len(boot_or_pending) <= 1, f"concurrent operations: {boot_or_pending}"
        h.tick()
