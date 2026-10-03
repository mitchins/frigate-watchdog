"""Read-only HTTP surface: /health, /stats, /history, /report.

/health reports whether the watchdog process and its decision loop are
functioning. It must not fail merely because a camera, Frigate, or MQTT is
offline — otherwise Docker health-driven automation becomes another restart
loop. A dead decision loop is unhealthy, and so is a loop whose iterations
keep failing internally; a functioning watchdog correctly inhibiting actions
is alive. (Frigate or camera outages never raise out of an iteration.)

No reboot endpoint, no config writes, no acknowledgement action over HTTP.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from aiohttp import web

from . import __version__
from .constants import MAX_CONSECUTIVE_ITERATION_FAILURES
from .policy import DecisionEngine
from .store import Store, StoreError


class HealthSnapshot:
    """Live process state the HTTP server reads (single event loop)."""

    def __init__(self, poll_interval_s: float) -> None:
        self.poll_interval_s = poll_interval_s
        self.started_mono = time.monotonic()
        self.started_utc = time.time()
        self.last_loop_activity_mono: float | None = None
        self.loop_stopped = False
        self.store_ok = True
        self.store_error: str | None = None
        self.instance = "default"
        self.mode = "observe"
        self.http_port: int | None = None
        self.consecutive_iteration_failures = 0

    def loop_alive(self) -> bool:
        if self.consecutive_iteration_failures >= MAX_CONSECUTIVE_ITERATION_FAILURES:
            return False
        if self.loop_stopped or self.last_loop_activity_mono is None:
            return not self.loop_stopped
        return (time.monotonic() - self.last_loop_activity_mono) < 3 * max(
            self.poll_interval_s, 10.0
        )


def build_app(
    snapshot: HealthSnapshot,
    engine_getter: Callable[[], DecisionEngine | None],
    store: Store | None,
) -> web.Application:
    async def health(_request: web.Request) -> web.Response:
        alive = snapshot.loop_alive()
        payload: dict[str, Any] = {
            "status": "alive" if alive else "dead",
            "instance": snapshot.instance,
            "mode": snapshot.mode,
            "version": __version__,
            "uptime_s": round(time.monotonic() - snapshot.started_mono, 1),
            "store_ok": snapshot.store_ok,
            "consecutive_iteration_failures": snapshot.consecutive_iteration_failures,
        }
        return web.json_response(payload, status=200 if alive else 503)

    async def stats(_request: web.Request) -> web.Response:
        engine = engine_getter()
        if engine is None:
            return web.json_response({"error": "engine not ready"}, status=503)
        now_mono = time.monotonic()
        reasons = engine.global_inhibition_reasons(now_mono)
        last = engine.last_snapshot
        payload: dict[str, Any] = {
            "instance": snapshot.instance,
            "mode": snapshot.mode,
            "version": __version__,
            "service_alive": snapshot.loop_alive(),
            "monitoring_fresh": bool(last and last.fresh),
            "observation_age_s": (
                round(
                    max(
                        0.0,
                        snapshot.started_utc
                        + (now_mono - snapshot.started_mono)
                        - last.received_at_utc,
                    ),
                    1,
                )
                if last and last.received_at_utc
                else None
            ),
            "global_state": engine.global_state.value,
            "global_states": [s.value for s in engine.global_states],
            "recovery_currently_permitted": not reasons,
            "inhibition_reasons": reasons,
            "cameras": engine.camera_summaries(now_mono),
        }
        if store is not None and snapshot.store_ok:
            try:
                payload["store"] = store.status()
                for key, summary in payload["cameras"].items():
                    acct = store.accounting(key)
                    summary["attempts_in_window"] = acct.attempts_in_window
                    summary["cooldown_remaining_s"] = max(0.0, acct.cooldown_until - store.now_acc)
                    summary["auth_latched"] = store.auth_latched(key)
            except StoreError as exc:
                payload["store_error"] = str(exc)
        else:
            payload["store_error"] = snapshot.store_error or "store unavailable"
        return web.json_response(payload)

    async def history(request: web.Request) -> web.Response:
        if store is None or not snapshot.store_ok:
            return web.json_response(
                {"error": snapshot.store_error or "store unavailable", "events": []}, status=503
            )
        try:
            after = int(request.query.get("after", 0))
            limit = int(request.query.get("limit", 100))
        except ValueError:
            raise web.HTTPBadRequest(text="after and limit must be integers") from None
        try:
            rows = store.history(after_id=after, limit=limit)
        except StoreError as exc:
            return web.json_response({"error": str(exc), "events": []}, status=503)
        return web.json_response({"events": [row.as_dict() for row in rows], "count": len(rows)})

    async def report(_request: web.Request) -> web.Response:
        if store is None or not snapshot.store_ok:
            return web.json_response(
                {"error": snapshot.store_error or "store unavailable"}, status=503
            )
        try:
            return web.json_response(store.report())
        except StoreError as exc:
            return web.json_response({"error": str(exc)}, status=503)

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/stats", stats)
    app.router.add_get("/history", history)
    app.router.add_get("/report", report)
    return app


async def start_server(
    snapshot: HealthSnapshot,
    engine_getter: Callable[[], DecisionEngine | None],
    store: Store | None,
    host: str,
    port: int,
) -> web.AppRunner:
    app = build_app(snapshot, engine_getter, store)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner
