"""A real local HTTP server that speaks the subset of Frigate's API we use.

The contract is frozen against Frigate 0.18.0 semantics:

* ``GET /api/stats`` returns ``{"service": {"last_updated": <epoch float>,
  "uptime": <float seconds>, ...}, "cameras": {"<name>": {"camera_fps": ...,
  "detection_fps": ...}}}``. Disabled cameras are omitted from ``cameras``.
  The body may be served cached (identical ``last_updated``/``uptime``).
* ``GET /api/config`` returns the running config including
  ``cameras.<name>.enabled``; credential redaction upstream means we must
  never rely on stream URLs being present.
* ``POST /login`` with JSON ``{"user","password"}`` returns a bearer token;
  wrong credentials give 401; tokens expire and then reads give 401.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web


@dataclass
class FakeFrigateState:
    cameras: dict[str, float] = field(
        default_factory=lambda: {"Porch": 5.0, "Driveway": 5.0, "Doorbell": 5.0}
    )
    enabled: dict[str, bool] = field(
        default_factory=lambda: {"Porch": True, "Driveway": True, "Doorbell": True}
    )
    auth_enabled: bool = False
    users: dict[str, str] = field(default_factory=lambda: {"watchdog": "frigate-pass"})
    token_ttl_s: float = 600.0
    # fault injection / behaviour scripting
    stats_behavior: str = "normal"  # normal | cached | stale | malformed | oversized | html | down | slow | status_500 | status_429 | reset
    config_behavior: str = "normal"  # normal | omit_camera:<name> | down | status_500
    # observability
    stats_requests: int = 0
    config_requests: int = 0
    login_requests: int = 0
    authenticated_reads: int = 0
    _last_updated: float = field(default_factory=time.time)
    _uptime: float = 1234.0
    _tokens: dict[str, float] = field(default_factory=dict)  # token -> expires_monotonic
    _token_counter: int = 0
    _serve_cached_next: bool = True
    _cached_body: str | None = None


class FakeFrigate:
    def __init__(self, state: FakeFrigateState | None = None) -> None:
        self.state = state or FakeFrigateState()
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> str:
        app = web.Application()
        app.router.add_get("/api/stats", self._stats)
        app.router.add_get("/api/config", self._config)
        app.router.add_post("/login", self._login)
        app.router.add_post("/api/login", self._login)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host, port)
        await self.site.start()
        assert self.site._server is not None
        sockaddr = self.site._server.sockets[0].getsockname()
        return f"http://{sockaddr[0]}:{sockaddr[1]}"

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()

    # ---------------------------------------------------------------- handlers

    def _check_auth(self, request: web.Request) -> bool:
        if not self.state.auth_enabled:
            return True
        header = request.headers.get("Authorization", "")
        token = header.removeprefix("Bearer ").strip() if header.startswith("Bearer ") else ""
        expiry = self.state._tokens.get(token) if token else None
        if expiry is None or time.monotonic() > expiry:
            return False
        self.state.authenticated_reads += 1
        return True

    def _stats_doc(self) -> dict[str, Any]:
        s = self.state
        cameras = {
            name: {"camera_fps": fps, "detection_fps": 2.5, "process_fps": 5.0, "skipped_fps": 0.0}
            for name, fps in s.cameras.items()
            if s.enabled.get(name, True)
        }
        return {
            "service": {
                "last_updated": s._last_updated,
                "uptime": s._uptime,
                "version": "0.18.0-fake",
                "pid": 4242,
            },
            "detection_fps": 2.5,
            "cameras": cameras,
        }

    def _advance(self) -> None:
        s = self.state
        s._last_updated = time.time()
        s._uptime += 10.0

    async def _stats(self, request: web.Request) -> web.StreamResponse:
        s = self.state
        s.stats_requests += 1
        if not self._check_auth(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        if s.stats_behavior == "down":
            return web.Response(status=503)
        if s.stats_behavior == "status_500":
            return web.Response(status=500)
        if s.stats_behavior == "status_429":
            return web.Response(status=429, text="slow down")
        if s.stats_behavior == "html":
            return web.Response(
                text="<html><body>login required</body></html>",
                content_type="text/html",
            )
        if s.stats_behavior == "malformed":
            return web.Response(text="{not json", content_type="application/json")
        if s.stats_behavior == "oversized":
            blob = {
                "service": {"last_updated": time.time(), "uptime": 1.0},
                "junk": "x" * (3 * 1024 * 1024),
            }
            return web.json_response(blob)
        if s.stats_behavior == "slow":
            await asyncio.sleep(30)
            return web.json_response(self._stats_doc())
        if s.stats_behavior == "reset":
            # Abort the TCP connection mid-response.
            if request.transport is not None:
                request.transport.abort()
            raise ConnectionResetError("fake connection reset")
        if s.stats_behavior == "cached":
            if s._cached_body is None:
                self._advance()
                s._cached_body = json.dumps(self._stats_doc())
            return web.Response(text=s._cached_body, content_type="application/json")
        if s.stats_behavior == "stale":
            s._uptime += 10.0  # uptime advances but timestamp stays old
            doc = self._stats_doc()
            doc["service"]["last_updated"] = time.time() - 300.0
            return web.json_response(doc)
        self._advance()
        return web.json_response(self._stats_doc())

    async def _config(self, request: web.Request) -> web.StreamResponse:
        s = self.state
        s.config_requests += 1
        if not self._check_auth(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        if s.config_behavior == "down":
            return web.Response(status=503)
        if s.config_behavior == "status_500":
            return web.Response(status=500)
        cameras: dict[str, Any] = {}
        for name in s.enabled:
            if (
                s.config_behavior.startswith("omit_camera:")
                and name == s.config_behavior.split(":", 1)[1]
            ):
                continue
            cameras[name] = {
                "enabled": s.enabled[name],
                "ffmpeg_inputs": [
                    {"path": f"rtsp://redacted:redacted@cam/{name}", "roles": ["detect"]}
                ],
            }
        doc = {
            "cameras": cameras,
            "mqtt": {"enabled": True},
            "record": {"enabled": False},
        }
        return web.json_response(doc)

    async def _login(self, request: web.Request) -> web.StreamResponse:
        s = self.state
        s.login_requests += 1
        if not s.auth_enabled:
            return web.Response(status=404)
        try:
            body = await request.json()
        except Exception:
            return web.Response(status=400)
        user = body.get("user")
        password = body.get("password")
        if user in s.users and s.users[user] == password:
            s._token_counter += 1
            token = f"fake-jwt-{s._token_counter}"
            s._tokens[token] = time.monotonic() + s.token_ttl_s
            return web.json_response({"token": token, "user": user})
        return web.json_response({"error": "invalid credentials"}, status=401)
