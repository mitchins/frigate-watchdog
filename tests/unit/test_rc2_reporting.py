"""Outage reporting, observe-mode decisions, preflight auth latching, the
effectiveness report, loop-failure health, and the real-camera clock reply.

These cover gaps found during the first deployed soak: observe mode recorded
nothing about what recover mode would have done, outage durations were never
persisted, and the probe never read a real camera's clock.
"""

from __future__ import annotations

from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from frigate_watchdog.cli import _format_report, _get
from frigate_watchdog.constants import MAX_CONSECUTIVE_ITERATION_FAILURES
from frigate_watchdog.http_api import HealthSnapshot, build_app
from frigate_watchdog.onvif import OnvifClient
from frigate_watchdog.store import Store
from tests.fakes.harness import make_harness, warm_up_healthy
from tests.fakes.onvif_fake import FakeOnvifCamera, FakeOnvifState
from tests.protocol.test_onvif_protocol import onvif_config

ALL = ("porch", "driveway", "doorbell")


def fps(zero: tuple[str, ...] = ()) -> dict[str, float]:
    return {k: (0.0 if k in zero else 5.0) for k in ALL}


# ------------------------------------------------------------ observe mode


def test_observe_mode_records_would_recover_once_per_outage():
    h = make_harness(mode="observe")
    warm_up_healthy(h)
    for _ in range(12):
        h.poll(fps=fps(("porch",)))
    would = h.events_of("would_recover")
    assert [e.camera for e in would] == ["porch"], "exactly one decision per outage"
    assert h.total_proposals == 0, "observe mode never proposes"
    h.poll(fps=fps())  # recovers by itself
    for _ in range(12):  # a second, separate outage is reported again
        h.poll(fps=fps(("porch",)))
    assert len(h.events_of("would_recover")) == 2


def test_witness_camera_outage_is_recorded_as_held_not_would_recover():
    h = make_harness(mode="observe")
    warm_up_healthy(h)
    for _ in range(12):
        h.poll(fps=fps(("doorbell",)))
    assert not h.events_of("would_recover")
    held = h.events_of("recovery_held")
    assert held and held[0].camera == "doorbell"
    assert held[0].reason == "RECOVERY_DISABLED_FOR_CAMERA"


def test_recover_mode_records_why_a_camera_is_held():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(12):
        h.poll(fps=fps(ALL))  # nobody healthy
    held = [e for e in h.events_of("recovery_held") if e.camera == "porch"]
    assert held and held[0].reason == "NO_HEALTHY_PEER"
    assert len(held) == 1, "the same reasons are not re-reported every poll"


# ------------------------------------------------------------ outage durations


def test_self_recovered_outage_records_its_duration():
    h = make_harness(mode="observe")
    warm_up_healthy(h)
    for _ in range(6):  # 60s dark
        h.poll(fps=fps(("porch",)))
    h.poll(fps=fps())
    restored = h.events_of("frames_restored")
    assert len(restored) == 1 and restored[0].camera == "porch"
    assert restored[0].reason == "SELF_RECOVERED"
    assert restored[0].detail.startswith("outage_s=60;")


def test_outage_after_a_reboot_request_is_attributed_to_it():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=fps(("porch",)))
    assert h.last_proposal and h.last_proposal.camera == "porch"
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "OUTCOME_UNKNOWN")
    h.poll(fps=fps(("porch",)))
    h.poll(fps=fps())
    restored = h.events_of("frames_restored")
    assert restored and restored[-1].reason == "AFTER_REBOOT_REQUEST"


# ------------------------------------------------------------ preflight auth


def test_preflight_auth_failure_latches_durably():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=fps(("porch",)))
    h.preflight_failed("porch", "AUTH_FAILED")
    assert h.store.auth_latched("porch")
    assert h.events_of("auth_latched")
    for _ in range(40):  # long past the preflight backoff
        h.poll(fps=fps(("porch",)))
    assert h.total_proposals == 1, "no retry storm against bad credentials"


def test_unreachable_preflight_only_backs_off():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=fps(("porch",)))
    h.preflight_failed("porch", "UNREACHABLE")
    assert not h.store.auth_latched("porch")
    assert not h.events_of("auth_latched")


# ------------------------------------------------------------ report


IDENT = "http|192.0.2.10|80|/onvif/device_service"


def test_store_report_counts_outages_and_durations(tmp_path: Path):
    store = Store(tmp_path / "state.db")
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    t = 1_700_000_000.0
    for outage_s in (120, 3600, 300):
        store.record_event("frames_stopped", t, camera="porch", reason="NO_FRAMES")
        store.record_event(
            "frames_restored",
            t + outage_s,
            camera="porch",
            reason="SELF_RECOVERED",
            detail=f"outage_s={outage_s}; frames restored",
        )
        t += 10_000
    store.record_event("would_recover", t, camera="porch", reason="MODE_OBSERVE")
    report = store.report()
    row = report["cameras"]["porch"]
    assert row["outages"] == 3
    assert row["self_recovered"] == 3
    assert row["would_recover"] == 1
    assert row["outage_median_s"] == 300
    assert row["outage_max_s"] == 3600
    assert report["events_since_utc"] == 1_700_000_000.0
    text = _format_report(report)
    assert "porch" in text and "60m" in text
    store.close()


async def test_report_endpoint(tmp_path: Path):
    store = Store(tmp_path / "state.db")
    store.open()
    app = build_app(HealthSnapshot(0.2), lambda: None, store)
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/report")
        assert r.status == 200
        assert (await r.json())["cameras"] == {}
    store.close()


# ------------------------------------------------------------ health


def test_health_turns_dead_after_repeated_iteration_failures():
    snapshot = HealthSnapshot(0.2)
    snapshot.last_loop_activity_mono = None
    assert snapshot.loop_alive()
    snapshot.consecutive_iteration_failures = MAX_CONSECUTIVE_ITERATION_FAILURES - 1
    assert snapshot.loop_alive(), "a few transient failures stay healthy"
    snapshot.consecutive_iteration_failures = MAX_CONSECUTIVE_ITERATION_FAILURES
    assert not snapshot.loop_alive()


def test_cli_reaches_an_all_interfaces_bind_over_loopback(monkeypatch):
    import dataclasses

    h = make_harness()
    config = dataclasses.replace(
        h.config, http=dataclasses.replace(h.config.http, host="0.0.0.0", port=9)
    )
    seen: list[str] = []

    def fake_urlopen(url, timeout):
        seen.append(url)
        raise OSError("stop")

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    try:
        _get(config, "/health")
    except OSError:
        pass
    assert seen == ["http://127.0.0.1:9/health"]


# ------------------------------------------------------------ clock


async def test_probe_reads_a_real_camera_clock_reply():
    """Vatilon PB1 replies with Time before Date and tt: elements."""
    cam = FakeOnvifCamera(FakeOnvifState(clock_shape="vatilon"))
    endpoint = await cam.start()
    client = OnvifClient(onvif_config(endpoint))
    try:
        result = await client.get_system_date_and_time()
    finally:
        await client.close()
        await cam.stop()
    assert result.outcome == "OK"
    assert result.detail.startswith("camera clock 20"), result.detail
    assert "?" not in result.detail


def test_flapping_hold_reasons_are_reported_once_each_per_outage():
    h = make_harness()
    warm_up_healthy(h)
    for _ in range(8):
        h.poll(fps=fps(("porch",)))
    attempt = h.reserve("porch")
    h.outcome("porch", attempt, "REJECTED")  # porch latched; driveway next
    for i in range(60):
        # the doorbell witness flaps every poll while driveway stays dark
        h.poll(fps=fps(("porch", "driveway", "doorbell") if i % 2 else ("porch", "driveway")))
    held = [e for e in h.events_of("recovery_held") if e.camera == "driveway"]
    assert 1 <= len(held) <= 4, f"hold reasons flooded history: {len(held)} events"
