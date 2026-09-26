"""CLI tests: check-config, probe, acknowledge, and status queries."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest
import yaml

from frigate_watchdog import cli
from frigate_watchdog.http_api import HealthSnapshot, start_server
from frigate_watchdog.policy import DecisionEngine
from tests.fakes.fast_config import fast_config
from tests.fakes.onvif_fake import FakeOnvifCamera, FakeOnvifState

GOOD_CONFIG = {
    "schema_version": 1,
    "mode": "observe",
    "frigate": {"base_url": "http://frigate:5000", "auth": {"mode": "none"}},
    "cameras": {
        "porch": {
            "frigate_name": "Porch",
            "recovery": "onvif",
            "onvif": {
                "endpoint": "http://192.168.10.40/onvif/device_service",
                "username": "admin",
                "password_env": "CAMERA_PORCH_PASSWORD",
            },
        },
        "doorbell": {"frigate_name": "Doorbell", "recovery": "none"},
    },
    "mqtt": {"enabled": False},
}


def write_config(tmp_path: Path, doc: dict) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(doc))
    return path


@pytest.fixture(autouse=True)
def _credentials(monkeypatch):
    monkeypatch.setenv("CAMERA_PORCH_PASSWORD", "cam-pass")  # matches FakeOnvifCamera default


@pytest.fixture()
def config_path(tmp_path: Path) -> Path:
    return write_config(tmp_path, GOOD_CONFIG)


def test_check_config_ok(config_path, capsys):
    rc = cli.main(["--config", str(config_path), "check-config"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "configuration OK" in out
    assert "porch" in out and "doorbell" in out
    assert "observe" in out


def test_check_config_invalid(tmp_path, capsys):
    bad = dict(GOOD_CONFIG)
    bad["mode"] = "yolo"
    path = write_config(tmp_path, bad)
    with pytest.raises(SystemExit) as exc:
        cli.main(["--config", str(path), "check-config"])
    assert exc.value.code == 2


def test_missing_config_file(tmp_path):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--config", str(tmp_path / "nope.yaml"), "check-config"])
    assert exc.value.code == 2


async def test_probe_read_only_against_fake(tmp_path, capsys):
    cam = FakeOnvifCamera()
    endpoint = await cam.start()
    doc = dict(GOOD_CONFIG)
    doc["cameras"]["porch"]["onvif"]["endpoint"] = endpoint
    path = write_config(tmp_path, doc)
    try:
        rc = await asyncio.to_thread(cli.main, ["--config", str(path), "probe", "porch"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "FakeCam" in out
        assert "never reboots" in out
        assert cam.state.requests["GetDeviceInformation"] >= 1
        assert "SystemReboot" not in cam.state.requests  # probe never reboots
    finally:
        await cam.stop()


async def test_probe_unknown_camera(tmp_path):
    path = write_config(tmp_path, GOOD_CONFIG)
    rc = await asyncio.to_thread(cli.main, ["--config", str(path), "probe", "ghost"])
    assert rc == 2


async def test_probe_witness_camera_has_no_onvif(tmp_path):
    path = write_config(tmp_path, GOOD_CONFIG)
    rc = await asyncio.to_thread(cli.main, ["--config", str(path), "probe", "doorbell"])
    assert rc == 2


async def test_probe_auth_failure(tmp_path):
    cam = FakeOnvifCamera(FakeOnvifState(password="other"))
    endpoint = await cam.start()
    doc = dict(GOOD_CONFIG)
    doc["cameras"]["porch"]["onvif"]["endpoint"] = endpoint
    path = write_config(tmp_path, doc)
    try:
        rc = await asyncio.to_thread(cli.main, ["--config", str(path), "probe", "porch"])
        assert rc == 1
    finally:
        await cam.stop()


def test_acknowledge_requires_stopped_service(tmp_path):
    from frigate_watchdog.store import data_dir_lock

    doc = dict(GOOD_CONFIG)
    path = write_config(tmp_path, doc)
    data = tmp_path / "data"
    data.mkdir()
    with data_dir_lock(data):
        rc = cli.main(
            [
                "--config",
                str(path),
                "--data-dir",
                str(data),
                "acknowledge",
                "porch",
                "--reason",
                "test",
            ]
        )
    assert rc == 3  # refused while the (simulated) service holds the lock


def test_acknowledge_success_clears_latch(tmp_path):
    from frigate_watchdog.store import Store, endpoint_identity

    ident = endpoint_identity("http", "192.168.10.40", 80, "/onvif/device_service")
    data = tmp_path / "data"
    data.mkdir()
    store = Store(data / "state.db")
    store.open()
    store.register_camera("porch", "Porch", ident)
    store.open_incident("porch", "Porch", ident, time.time())
    store.reserve_attempt("porch", "Porch", ident, time.time())
    store.close()

    path = write_config(tmp_path, GOOD_CONFIG)
    rc = cli.main(
        [
            "--config",
            str(path),
            "--data-dir",
            str(data),
            "acknowledge",
            "porch",
            "--reason",
            "firmware fixed",
        ]
    )
    assert rc == 0
    store = Store(data / "state.db")
    store.open()
    assert store.incident("porch") is None
    store.close()


async def test_health_and_stats_queries_live_service():
    snapshot = HealthSnapshot(0.2)
    snapshot.instance = "test"
    snapshot.mode = "observe"
    engine = DecisionEngine(
        fast_config(frigate_base="http://127.0.0.1:1", onvif_endpoints={"porch": "none"}),
        _NullStore(),
        start_mono=time.monotonic(),
        start_utc=time.time(),
    )
    runner = await start_server(snapshot, lambda: engine, None, "127.0.0.1", 0)
    port = runner.addresses[0][1]
    try:
        import urllib.request

        def get(path):
            with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
                import json

                return json.loads(r.read().decode())

        payload = await asyncio.to_thread(get, "/health")
        assert payload["status"] == "alive"
        stats = await asyncio.to_thread(get, "/stats")
        assert stats["mode"] == "observe"
    finally:
        await runner.cleanup()


class _NullStore:
    def armed(self, camera):
        return False

    def incident(self, camera):
        return None

    def accounting(self, camera):
        from frigate_watchdog.store import AccountingView

        return AccountingView(0, None, 0.0)

    def last_attempt_any(self):
        return None

    def auth_latched(self, camera):
        return False
