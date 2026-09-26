"""Read-only HTTP API tests."""

from __future__ import annotations

import time

from aiohttp.test_utils import TestClient, TestServer

from frigate_watchdog.http_api import HealthSnapshot, build_app
from frigate_watchdog.policy import DecisionEngine
from frigate_watchdog.store import AccountingView, StoreError
from tests.fakes.fast_config import fast_config


class _OkStore:
    now_acc = 0.0

    def status(self):
        return {"runtime_acc": 1.0}

    def accounting(self, camera):
        return AccountingView(0, None, 0.0)

    def auth_latched(self, camera):
        return False

    def history(self, after_id=0, limit=100):
        from frigate_watchdog.store import HistoryRow

        return [
            HistoryRow(1, 1.0, "armed", "porch", None, None, None, None),
        ]


class _FailingStore(_OkStore):
    def status(self):
        raise StoreError("full")

    def history(self, after_id=0, limit=100):
        raise StoreError("full")


def _engine():
    class _Store:
        def armed(self, camera):
            return False

        def incident(self, camera):
            return None

        def accounting(self, camera):
            return AccountingView(0, None, 0.0)

        def last_attempt_any(self):
            return None

        def auth_latched(self, camera):
            return False

    return DecisionEngine(
        fast_config(frigate_base="http://127.0.0.1:1", onvif_endpoints={"porch": "none"}),
        _Store(),
        start_mono=time.monotonic(),
        start_utc=time.time(),
    )


async def test_health_alive_even_if_cameras_are_down():
    snapshot = HealthSnapshot(0.2)
    app = build_app(snapshot, lambda: None, None)
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/health")
        assert r.status == 200
        body = await r.json()
        assert body["status"] == "alive"


async def test_stats_engine_not_ready():
    snapshot = HealthSnapshot(0.2)
    app = build_app(snapshot, lambda: None, None)
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/stats")
        assert r.status == 503


async def test_stats_with_store_and_store_error():
    snapshot = HealthSnapshot(0.2)
    engine = _engine()
    app = build_app(snapshot, lambda: engine, _OkStore())
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/stats")
        assert r.status == 200
        body = await r.json()
        assert "store" in body
        assert "cameras" in body

    snapshot.store_ok = True
    app = build_app(snapshot, lambda: engine, _FailingStore())
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/stats")
        assert r.status == 200
        body = await r.json()
        assert "store_error" in body


async def test_history_store_unavailable_and_bad_params():
    snapshot = HealthSnapshot(0.2)
    snapshot.store_ok = False
    snapshot.store_error = "corrupt"
    app = build_app(snapshot, lambda: None, None)
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/history")
        assert r.status == 503

    snapshot.store_ok = True
    app = build_app(snapshot, lambda: None, _OkStore())
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/history?after=abc")
        assert r.status == 400
        r = await client.get("/history?limit=5")
        assert r.status == 200
        body = await r.json()
        assert body["count"] == 1

    app = build_app(snapshot, lambda: None, _FailingStore())
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/history")
        assert r.status == 503


async def test_health_dead_when_loop_stopped():
    snapshot = HealthSnapshot(0.2)
    snapshot.loop_stopped = True
    app = build_app(snapshot, lambda: None, None)
    async with TestClient(TestServer(app)) as client:
        r = await client.get("/health")
        assert r.status == 503
