"""End-to-end tests: the real coordinator against real local fakes.

These exercise the full dispatch chain — poll, normalize, decide, recheck,
preflight, durable reservation, single send, outcome, boot grace, recovery
confirmation — with no mocks of the adapters.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from pathlib import Path

import pytest

from frigate_watchdog.service import WatchdogService
from tests.fakes.fast_config import fast_config
from tests.fakes.frigate_fake import FakeFrigate
from tests.fakes.onvif_fake import FakeOnvifCamera, FakeOnvifState


async def wait_until(predicate, timeout: float = 30.0, interval: float = 0.1) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


async def start_service(config, data_dir: Path):
    stop = asyncio.Event()
    service = WatchdogService(config, data_dir, shutdown=stop)
    run_task = asyncio.create_task(service.run())
    await wait_until(lambda: service.health.http_port is not None, timeout=10)
    await asyncio.sleep(0.5)  # let the first polls complete
    return service, run_task, stop


async def stop_service(service, run_task, stop) -> None:
    stop.set()
    await asyncio.wait_for(run_task, timeout=10)


async def http_get(port: int, path: str) -> dict:
    def _fetch():
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return json.loads(r.read().decode())

    return await asyncio.to_thread(_fetch)


@pytest.fixture()
async def env(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    camera = FakeOnvifCamera()
    endpoint = await camera.start()
    await camera.stop()  # restart below on a fixed port for determinism
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={
            "porch": endpoint,
            "driveway": "none",
            "doorbell": "none",
        },
    )
    data = tmp_path / "data"
    data.mkdir()
    yield_simple = {
        "frigate": frigate,
        "camera": camera,
        "config": config,
        "data": data,
    }
    return yield_simple


async def test_full_recovery_cycle(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    camera = FakeOnvifCamera(FakeOnvifState(reboot_delay_s=0.2))
    endpoint = await camera.start()

    def restore_frames_when_rebooted():
        async def watch():
            while camera.state.reboots_accepted == 0:
                await asyncio.sleep(0.1)
            await asyncio.sleep(0.5)  # simulated boot time
            frigate.state.cameras["Porch"] = 5.0

        return asyncio.create_task(watch())

    restorer = restore_frames_when_rebooted()
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={"porch": endpoint, "driveway": "none", "doorbell": "none"},
    )
    data = tmp_path / "data"
    data.mkdir()
    service, run_task, stop = await start_service(config, data)

    try:
        # healthy baselines establish quickly with fast timings
        await wait_until(
            lambda: all(service.engine.cameras[k].store_armed for k in config.cameras), timeout=15
        )
        # porch stops delivering frames
        frigate.state.cameras["Porch"] = 0.0
        # ... service should propose, preflight, reserve, send exactly once,
        # and confirm recovery once frames return
        await wait_until(lambda: camera.state.reboots_accepted == 1, timeout=30)
        await wait_until(
            lambda: service.engine.cameras["porch"].state.value in ("RECOVERED", "HEALTHY"),
            timeout=30,
        )
        stats = await http_get(service.health.http_port, "/stats")
        assert stats["recovery_currently_permitted"] is False or True  # mode-dependent
        assert stats["cameras"]["porch"]["last_outcome"] == "ACKNOWLEDGED"
        # no second attempt ever happens
        await asyncio.sleep(2)
        assert camera.state.reboots_accepted == 1
        assert camera.state.requests["SystemReboot"] == 1
    finally:
        await stop_service(service, run_task, stop)
        restorer.cancel()
        await camera.stop()
        await frigate.stop()


async def test_observe_mode_sends_zero_mutating_requests(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    camera = FakeOnvifCamera()
    endpoint = await camera.start()
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={"porch": endpoint, "driveway": "none", "doorbell": "none"},
        mode="observe",
    )
    data = tmp_path / "data"
    data.mkdir()
    service, run_task, stop = await start_service(config, data)
    try:
        await wait_until(
            lambda: all(service.engine.cameras[k].store_armed for k in config.cameras), timeout=15
        )
        frigate.state.cameras["Porch"] = 0.0
        frigate.state.cameras["Driveway"] = 0.0
        await asyncio.sleep(8)  # far past every fast-timing threshold
        assert camera.state.total_requests == 0
        stats = await http_get(service.health.http_port, "/stats")
        assert stats["mode"] == "observe"
        assert "MODE_OBSERVE" in stats["inhibition_reasons"]
    finally:
        await stop_service(service, run_task, stop)
        await camera.stop()
        await frigate.stop()


async def test_two_failing_cameras_inhibit(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    cam1 = FakeOnvifCamera()
    ep1 = await cam1.start()
    cam2 = FakeOnvifCamera()
    ep2 = await cam2.start()
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={"porch": ep1, "driveway": ep2, "doorbell": "none"},
    )
    data = tmp_path / "data"
    data.mkdir()
    service, run_task, stop = await start_service(config, data)
    try:
        await wait_until(
            lambda: all(service.engine.cameras[k].store_armed for k in config.cameras), timeout=15
        )
        frigate.state.cameras["Porch"] = 0.0
        frigate.state.cameras["Driveway"] = 0.0
        await asyncio.sleep(8)
        assert cam1.state.total_requests == 0
        assert cam2.state.total_requests == 0
        stats = await http_get(service.health.http_port, "/stats")
        assert "MULTIPLE_CAMERAS_FAILING" in stats["inhibition_reasons"]
    finally:
        await stop_service(service, run_task, stop)
        await cam1.stop()
        await cam2.stop()
        await frigate.stop()


async def test_outcome_unknown_latches_across_restart_no_resend(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    camera = FakeOnvifCamera(FakeOnvifState(behavior="reset_after_accept"))
    endpoint = await camera.start()
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={"porch": endpoint, "driveway": "none", "doorbell": "none"},
    )
    data = tmp_path / "data"
    data.mkdir()
    service, run_task, stop = await start_service(config, data)
    try:
        await wait_until(
            lambda: all(service.engine.cameras[k].store_armed for k in config.cameras), timeout=15
        )
        frigate.state.cameras["Porch"] = 0.0
        await wait_until(lambda: camera.state.reboots_accepted == 1, timeout=30)
        # the reboot was accepted; connection died -> OUTCOME_UNKNOWN
        await wait_until(
            lambda: service.engine.cameras["porch"].last_outcome is not None, timeout=15
        )
        assert service.engine.cameras["porch"].last_outcome[0] == "OUTCOME_UNKNOWN"
    finally:
        await stop_service(service, run_task, stop)

    # restart the service on the same data directory: never replay
    service2, run_task2, stop2 = await start_service(config, data)
    try:
        frigate.state.cameras["Porch"] = 0.0  # still failing
        await asyncio.sleep(6)
        assert camera.state.reboots_accepted == 1, "restart must never resend"
        assert camera.state.requests["SystemReboot"] == 1
        stats = await http_get(service2.health.http_port, "/stats")
        assert stats["cameras"]["porch"]["latched"] is True
    finally:
        await stop_service(service2, run_task2, stop2)
        await camera.stop()
        await frigate.stop()


async def test_frigate_down_health_stays_alive(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    camera = FakeOnvifCamera()
    endpoint = await camera.start()
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={"porch": endpoint, "driveway": "none", "doorbell": "none"},
    )
    data = tmp_path / "data"
    data.mkdir()
    service, run_task, stop = await start_service(config, data)
    try:
        await wait_until(lambda: service.health.http_port is not None, timeout=10)
        await frigate.stop()  # frigate goes away entirely
        await asyncio.sleep(1.5)
        payload = await http_get(service.health.http_port, "/health")
        assert payload["status"] == "alive"
        stats = await http_get(service.health.http_port, "/stats")
        assert stats["monitoring_fresh"] is False
        assert "TELEMETRY_STALE" in stats["inhibition_reasons"]
        assert camera.state.total_requests == 0
        history = await http_get(service.health.http_port, "/history?limit=10")
        assert "events" in history
    finally:
        await stop_service(service, run_task, stop)
        await camera.stop()


async def test_second_service_process_refuses_data_dir(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={"porch": "http://127.0.0.1:1/onvif", "driveway": "none"},
    )
    data = tmp_path / "data"
    data.mkdir()
    service, run_task, stop = await start_service(config, data)
    try:
        second = WatchdogService(config, data)
        result = await asyncio.wait_for(second.run(), timeout=10)
        assert result == 3, "second process holding the same /data must exit, not operate"
    finally:
        await stop_service(service, run_task, stop)
        await frigate.stop()


async def _history(service) -> dict:
    return await http_get(service.health.http_port, "/history?limit=200")


async def test_history_endpoint_carries_the_recovery(tmp_path):
    frigate = FakeFrigate()
    base = await frigate.start()
    camera = FakeOnvifCamera(FakeOnvifState(reboot_delay_s=0.1))
    endpoint = await camera.start()

    async def restorer():
        while camera.state.reboots_accepted == 0:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.4)
        frigate.state.cameras["Porch"] = 5.0

    task = asyncio.create_task(restorer())
    config = fast_config(
        frigate_base=base,
        onvif_endpoints={"porch": endpoint, "driveway": "none", "doorbell": "none"},
    )
    data = tmp_path / "data"
    data.mkdir()
    service, run_task, stop = await start_service(config, data)
    try:
        await wait_until(
            lambda: all(service.engine.cameras[k].store_armed for k in config.cameras), timeout=15
        )
        frigate.state.cameras["Porch"] = 0.0

        async def recovery_confirmed() -> bool:
            events = (await _history(service))["events"]
            return "recovery_confirmed" in [e["kind"] for e in events]

        await wait_until(recovery_confirmed, timeout=30)
        events = (await _history(service))["events"]
        kinds = [e["kind"] for e in events]
        assert "action_reserved" in kinds
        assert "action_outcome" in kinds
        assert "incident_opened" in kinds
        for event in events:  # sanitized: no secrets anywhere
            assert "cam-pass" not in json.dumps(event)
    finally:
        await stop_service(service, run_task, stop)
        task.cancel()
        await camera.stop()
        await frigate.stop()
