"""Frigate HTTP adapter.

I/O only: this module fetches raw JSON documents and reports problems as
stable codes. Normalization into observations happens in
:mod:`frigate_watchdog.observations`, driven by the coordinator.

Hard rules implemented here:

* Response bodies are size-capped before parsing; oversized payloads are
  rejected without reading them fully.
* Redirects are never followed, and HTML login pages are rejected, so an
  auth wall can never be mistaken for telemetry.
* Native Frigate login (POST /login, JWT) is held in memory only. One
  controlled re-authentication and read retry per request on 401; repeated
  login failures back off — no login storm.
* TLS verification stays on; an optional CA bundle is the only knob.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

from .config import FrigateConfig
from .constants import HTTP_MAX_RESPONSE_BYTES

logger = logging.getLogger("frigate_watchdog.frigate")

LOGIN_BACKOFF_BASE_S = 2.0
LOGIN_BACKOFF_MAX_S = 300.0
LOGIN_FAILURES_BEFORE_BACKOFF = 2


class FrigateClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class FetchResult:
    stats: dict[str, Any] | None
    enabled_map: dict[str, bool] | None
    problem: str | None = None  # unreachable | timeout | auth | auth_backoff |
    #                            http_<status> | redirect | too_large | malformed |
    #                            html_response


class FrigateClient:
    """Minimal authenticated reader for /api/stats and /api/config."""

    def __init__(self, config: FrigateConfig, request_deadline_s: float = 5.0) -> None:
        self.config = config
        self.deadline = request_deadline_s
        self._session: aiohttp.ClientSession | None = None
        self._token: str | None = None
        self._consecutive_login_failures = 0
        self._login_blocked_until: float = 0.0

    async def start(self) -> None:
        import ssl

        ssl_context: ssl.SSLContext | bool | aiohttp.Fingerprint = True
        if self.config.auth.ca_bundle is not None:
            ssl_context = ssl.create_default_context(cafile=str(self.config.auth.ca_bundle))
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.deadline),
            connector=aiohttp.TCPConnector(ssl=ssl_context),
        )

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    @property
    def base_url(self) -> str:
        return self.config.base_url.rstrip("/")

    # ---------------------------------------------------------------- helpers

    async def _read_json(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        token: str | None = None,
        authenticate: bool = False,
    ) -> tuple[int | None, dict[str, Any] | None, str | None]:
        """Return (status, parsed_json_or_None, problem)."""
        if self._session is None:
            return None, None, "not_started"
        url = f"{self.base_url}{path}"
        headers: dict[str, str] = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            async with self._session.request(
                method, url, json=data, headers=headers, allow_redirects=False
            ) as response:
                if 300 <= response.status < 400:
                    return response.status, None, "redirect"
                if authenticate and response.status in (401, 403):
                    return response.status, None, "auth"
                if response.status != 200:
                    return response.status, None, f"http_{response.status}"
                chunks: list[bytes] = []
                total = 0
                async for chunk in response.content.iter_chunked(65536):
                    total += len(chunk)
                    if total > HTTP_MAX_RESPONSE_BYTES:
                        return response.status, None, "too_large"
                    chunks.append(chunk)
                body = b"".join(chunks)
                ctype = response.headers.get("Content-Type", "")
                if "application/json" not in ctype.lower():
                    return (
                        response.status,
                        None,
                        "html_response" if "html" in ctype.lower() else "malformed",
                    )
                try:
                    parsed = json.loads(body)
                except (ValueError, UnicodeDecodeError):
                    return response.status, None, "malformed"
                if not isinstance(parsed, dict):
                    return response.status, None, "malformed"
                return response.status, parsed, None
        except TimeoutError:
            return None, None, "timeout"
        except (aiohttp.ClientError, OSError):
            return None, None, "unreachable"

    # ---------------------------------------------------------------- login

    async def _login(self) -> str:
        """Return a fresh bearer token, or raise FrigateClientError."""
        auth = self.config.auth
        if auth.mode != "frigate" or auth.username is None or auth.password is None:
            raise FrigateClientError("login requested but frigate auth is not configured")
        now = time.monotonic()
        if now < self._login_blocked_until:
            raise FrigateClientError("auth_backoff")
        status, body, problem = await self._read_json(
            "POST",
            "/login",
            data={"user": auth.username, "password": auth.password.value},
        )
        if status == 200 and body is not None and isinstance(body.get("token"), str):
            self._consecutive_login_failures = 0
            self._token = str(body["token"])
            return self._token
        self._consecutive_login_failures += 1
        if self._consecutive_login_failures >= LOGIN_FAILURES_BEFORE_BACKOFF:
            backoff = min(
                LOGIN_BACKOFF_MAX_S,
                LOGIN_BACKOFF_BASE_S**self._consecutive_login_failures,
            )
            self._login_blocked_until = now + backoff
            logger.warning(
                "frigate login failed (%s); backing off logins for %.0fs",
                problem or status,
                backoff,
            )
        raise FrigateClientError(f"login_failed:{problem or status}")

    async def _ensure_token(self) -> str | None:
        if self.config.auth.mode != "frigate":
            return None
        if self._token is not None:
            return self._token
        try:
            return await self._login()
        except FrigateClientError as exc:
            if "backoff" in str(exc):
                return None
            # single login attempt failure surfaces as auth problem
            return None

    # ---------------------------------------------------------------- public

    async def fetch_observations(self) -> FetchResult:
        """Fetch /api/stats and /api/config with auth handling.

        On a 401 in frigate-auth mode, performs exactly one re-login and one
        retry of the read. Anything else fails the fetch with a stable code.
        """
        token = await self._ensure_token()
        if self.config.auth.mode == "frigate" and token is None:
            return FetchResult(None, None, "auth_backoff")

        status, stats, problem = await self._read_json(
            "GET", "/api/stats", token=token, authenticate=True
        )
        if problem == "auth" and self.config.auth.mode == "frigate":
            # One controlled re-authentication, one retry.
            try:
                token = await self._login()
            except FrigateClientError:
                return FetchResult(None, None, "auth")
            status, stats, problem = await self._read_json(
                "GET", "/api/stats", token=token, authenticate=True
            )
            if problem is not None:
                return FetchResult(None, None, problem)
        elif problem is not None:
            return FetchResult(None, None, problem)
        elif self.config.auth.mode == "none" and status in (401, 403):
            return FetchResult(None, None, "auth")

        assert stats is not None
        _status2, config_doc, problem2 = await self._read_json(
            "GET", "/api/config", token=token, authenticate=True
        )
        if problem2 == "auth" and self.config.auth.mode == "frigate":
            try:
                token = await self._login()
            except FrigateClientError:
                return FetchResult(None, None, "auth")
            _status2, config_doc, problem2 = await self._read_json(
                "GET", "/api/config", token=token, authenticate=True
            )
        if problem2 is not None:
            # Stats alone cannot establish capture expectations.
            return FetchResult(None, None, problem2)

        enabled_map: dict[str, bool] = {}
        assert config_doc is not None
        cameras_cfg = config_doc.get("cameras")
        if isinstance(cameras_cfg, dict):
            for name, cam in cameras_cfg.items():
                if isinstance(cam, dict):
                    enabled_map[name] = bool(cam.get("enabled", True))
        return FetchResult(stats=stats, enabled_map=enabled_map, problem=None)

    async def wait_closed(self) -> None:
        await asyncio.sleep(0)
