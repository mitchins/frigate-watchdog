"""Configuration loading and strict validation.

Configuration is explicit-only. Every value that governs recovery safety is
either validated here or is a hardcoded constant in :mod:`frigate_watchdog.constants`.
An invalid configuration must never produce a partially-usable service, so
:meth:`load_config` either returns a fully validated :class:`WatchdogConfig`
or raises :class:`ConfigError` listing every problem found.
"""

from __future__ import annotations

import ipaddress
import os
import re
import string
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from .constants import (
    DEFAULT_TIMINGS,
    TIMING_BOUNDS,
    WATCHDOG_SCHEMA_VERSION,
)

_CAMERA_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_TIMING_KEYS = frozenset(TIMING_BOUNDS)
_SAFE_ENV_CHARS = frozenset(string.ascii_letters + string.digits + "_-./@:+, ")


class ConfigError(ValueError):
    """One or more configuration problems, each with a stable code."""

    def __init__(self, problems: list[tuple[str, str, str]]) -> None:
        self.problems = problems
        rendered = "; ".join(f"[{path}] {code}: {message}" for path, code, message in problems)
        super().__init__(f"invalid configuration ({len(problems)} problem(s)): {rendered}")

    def redacted(self) -> str:
        return str(self)


class Secret:
    """A resolved secret value. Never renders its value through repr/str."""

    __slots__ = ("_value", "env_var")

    def __init__(self, env_var: str, value: str) -> None:
        self.env_var = env_var
        self._value = value

    @property
    def value(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return f"<secret from {self.env_var}>"

    def __str__(self) -> str:
        return f"<secret from {self.env_var}>"

    def redacted(self) -> str:
        return f"<secret from {self.env_var}>"


def scrub(text: str) -> str:
    """Best-effort scrub of anything that looks like a credential from a message."""
    out: list[str] = []
    for ch in text:
        out.append(ch if ch in _SAFE_ENV_CHARS else "?")
    return "".join(out)


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys."""


def _no_duplicates(
    loader: _StrictLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    seen: set[Any] = set()
    for key_node, _value in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in seen
        except TypeError:
            duplicate = False
        if duplicate:
            line = getattr(key_node, "start_mark", None)
            where = f"line {line.line + 1}" if line else "unknown line"
            raise ConfigError(
                [(str(key) or "<empty>", "duplicate_key", f"duplicate YAML key at {where}")]
            )
        seen.add(key)
    return dict(loader.construct_mapping(node, deep=deep))


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicates)


@dataclass(frozen=True)
class OnvifEndpoint:
    """A validated ONVIF device-service endpoint. Hosts must be IP literals."""

    raw: str
    scheme: str
    host: str  # normalized IP literal
    port: int
    path: str

    @property
    def url(self) -> str:
        return self.raw

    @property
    def identity(self) -> tuple[str, str, int, str]:
        """Normalized identity used to reject duplicate targets."""
        return (self.scheme, self.host, self.port, self.path.rstrip("/") or "/")


@dataclass(frozen=True)
class OnvifConfig:
    endpoint: OnvifEndpoint
    username: str
    password_env: str
    password: Secret


@dataclass(frozen=True)
class CameraConfig:
    key: str
    frigate_name: str
    recovery: str  # "onvif" | "none"
    maintenance: bool = False
    onvif: OnvifConfig | None = None


@dataclass(frozen=True)
class FrigateAuthConfig:
    mode: str  # "none" | "frigate"
    username: str | None = None
    password_env: str | None = None
    password: Secret | None = None
    ca_bundle: Path | None = None


@dataclass(frozen=True)
class FrigateConfig:
    base_url: str
    auth: FrigateAuthConfig


@dataclass(frozen=True)
class MqttConfig:
    enabled: bool
    host: str = ""
    port: int = 1883
    username: str | None = None
    password_env: str | None = None
    password: Secret | None = None
    topic_prefix: str = "frigate-watchdog"


@dataclass(frozen=True)
class HttpConfig:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8080


@dataclass(frozen=True)
class Timings:
    values: Mapping[str, float] = field(default_factory=lambda: dict(DEFAULT_TIMINGS))

    def __getitem__(self, key: str) -> float:
        return self.values[key]


@dataclass(frozen=True)
class WatchdogConfig:
    schema_version: int
    mode: str  # "observe" | "recover"
    instance: str
    frigate: FrigateConfig
    cameras: Mapping[str, CameraConfig]
    timings: Timings
    mqtt: MqttConfig
    http: HttpConfig
    source_path: Path | None = None

    @property
    def recoveries_enabled(self) -> bool:
        return self.mode == "recover"


class _Validator:
    def __init__(self, env: Mapping[str, str]) -> None:
        self.env = env
        self.problems: list[tuple[str, str, str]] = []

    def error(self, path: str, code: str, message: str) -> None:
        self.problems.append((path, code, scrub(message)))

    def _check_keys(
        self, obj: Any, path: str, allowed: frozenset[str] | set[str]
    ) -> dict[Any, Any]:
        if not isinstance(obj, dict):
            self.error(path, "not_a_mapping", f"expected a mapping, got {type(obj).__name__}")
            return {}
        for key in obj:
            if key not in allowed:
                self.error(
                    f"{path}.{key}",
                    "unknown_key",
                    f"unknown key (allowed: {', '.join(sorted(allowed))})",
                )
        return obj

    @staticmethod
    def _is_number(value: Any) -> bool:
        return isinstance(value, int | float) and not isinstance(value, bool)

    def _validate_endpoint(self, raw: Any, path: str) -> OnvifEndpoint | None:
        if not isinstance(raw, str) or not raw:
            self.error(path, "invalid_url", "endpoint must be a non-empty string")
            return None
        try:
            parts = urlsplit(raw)
        except ValueError as exc:
            self.error(path, "invalid_url", str(exc))
            return None
        if parts.scheme not in ("http", "https"):
            self.error(path, "invalid_url", "scheme must be http or https")
            return None
        if parts.username or parts.password:
            self.error(
                path, "invalid_url", "userinfo in URL is not allowed; use username/password_env"
            )
            return None
        if parts.query or parts.fragment:
            self.error(path, "invalid_url", "query strings and fragments are not allowed")
            return None
        host = parts.hostname
        if not host:
            self.error(path, "invalid_url", "host is required")
            return None
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            self.error(
                path, "not_an_ip", "ONVIF endpoints must use an explicit IP address in v0.1.0"
            )
            return None
        normalized_host = str(ip)
        default_port = 443 if parts.scheme == "https" else 80
        try:
            port = parts.port if parts.port is not None else default_port
        except ValueError:
            self.error(path, "invalid_url", "invalid port")
            return None
        if not (1 <= port <= 65535):
            self.error(path, "invalid_url", "port out of range")
            return None
        raw_path = parts.path or "/"
        if not raw_path.startswith("/"):
            self.error(path, "invalid_url", "path must start with /")
            return None
        if ".." in raw_path or "//" in raw_path:
            self.error(path, "invalid_url", "path must be a simple path")
            return None
        return OnvifEndpoint(
            raw=raw, scheme=parts.scheme, host=normalized_host, port=port, path=raw_path
        )

    def _resolve_secret(self, env_name: str, path: str) -> Secret | None:
        value = self.env.get(env_name)
        if value is None:
            self.error(path, "missing_env", f"environment variable {env_name} is not set")
            return None
        if not value.strip():
            self.error(path, "missing_env", f"environment variable {env_name} is empty")
            return None
        return Secret(env_name, value)

    def _validate_base_url(self, raw: Any, path: str) -> str | None:
        if not isinstance(raw, str) or not raw:
            self.error(path, "invalid_url", "base_url must be a non-empty string")
            return None
        try:
            parts = urlsplit(raw)
        except ValueError as exc:
            self.error(path, "invalid_url", str(exc))
            return None
        if parts.scheme not in ("http", "https"):
            self.error(path, "invalid_url", "scheme must be http or https")
            return None
        if not parts.hostname:
            self.error(path, "invalid_url", "host is required")
            return None
        if parts.username or parts.password or parts.query or parts.fragment:
            self.error(
                path, "invalid_url", "userinfo, query, and fragment are not allowed in base_url"
            )
            return None
        if parts.path not in ("", "/"):
            self.error(path, "invalid_url", "base_url must not include a path")
            return None
        try:
            port = parts.port
        except ValueError:
            self.error(path, "invalid_url", "invalid port")
            return None
        del port
        return raw

    def _validate_timings(self, raw: Any) -> dict[str, float]:
        timings: dict[str, float] = dict(DEFAULT_TIMINGS)
        obj = self._check_keys(raw or {}, "timings", _ALLOWED_TIMING_KEYS)
        for key, value in obj.items():
            if key not in TIMING_BOUNDS:
                continue  # already reported as unknown_key
            if not self._is_number(value):
                self.error(f"timings.{key}", "invalid_timing", "must be a number")
                continue
            value = float(value)
            low, high = TIMING_BOUNDS[key]
            if not (low <= value <= high):
                self.error(
                    f"timings.{key}",
                    "invalid_timing",
                    f"must be between {low:g} and {high:g} seconds",
                )
                continue
            timings[key] = value
        # Cross-checks: a poll interval at or beyond the monitoring gap would
        # make every poll look like a monitoring interruption and permanently
        # prevent evidence from accumulating.
        if timings["monitoring_gap_s"] < 2 * timings["poll_interval_s"]:
            self.error(
                "timings.monitoring_gap_s",
                "invalid_timing",
                "monitoring_gap_s must be at least twice poll_interval_s",
            )
        return timings

    def validate(self, doc: Any) -> WatchdogConfig:
        if not isinstance(doc, dict):
            self.error("root", "not_a_mapping", "configuration root must be a mapping")
            raise ConfigError(self.problems)
        self._check_keys(
            doc,
            "root",
            frozenset(
                {
                    "schema_version",
                    "mode",
                    "instance",
                    "frigate",
                    "cameras",
                    "mqtt",
                    "http",
                    "timings",
                }
            ),
        )

        schema_version = doc.get("schema_version")
        if schema_version != WATCHDOG_SCHEMA_VERSION:
            self.error(
                "schema_version",
                "unsupported_schema",
                f"must be {WATCHDOG_SCHEMA_VERSION}, got {schema_version!r}",
            )

        mode = doc.get("mode", "observe")
        if mode not in ("observe", "recover"):
            self.error("mode", "invalid_mode", "must be 'observe' or 'recover'")

        instance = doc.get("instance", "default")
        if not isinstance(instance, str) or not _CAMERA_KEY_RE.match(instance):
            self.error("instance", "invalid_instance", "must match [A-Za-z0-9][A-Za-z0-9_-]{0,63}")

        # --- frigate ---
        frigate_raw = doc.get("frigate")
        frigate: FrigateConfig | None = None
        if not isinstance(frigate_raw, dict):
            self.error("frigate", "not_a_mapping", "frigate must be a mapping")
        else:
            self._check_keys(frigate_raw, "frigate", frozenset({"base_url", "auth"}))
            base_url = self._validate_base_url(frigate_raw.get("base_url"), "frigate.base_url")
            auth_raw = frigate_raw.get("auth", {"mode": "none"})
            auth: FrigateAuthConfig | None = None
            if not isinstance(auth_raw, dict):
                self.error("frigate.auth", "not_a_mapping", "auth must be a mapping")
            else:
                auth_mode = auth_raw.get("mode")
                if auth_mode == "none":
                    self._check_keys(auth_raw, "frigate.auth", frozenset({"mode"}))
                    auth = FrigateAuthConfig(mode="none")
                elif auth_mode == "frigate":
                    self._check_keys(
                        auth_raw,
                        "frigate.auth",
                        frozenset({"mode", "username", "password_env", "ca_bundle"}),
                    )
                    username = auth_raw.get("username")
                    if not isinstance(username, str) or not username:
                        self.error("frigate.auth.username", "missing_value", "username is required")
                        username = None
                    password_env = auth_raw.get("password_env")
                    if not isinstance(password_env, str) or not _ENV_NAME_RE.match(password_env):
                        self.error(
                            "frigate.auth.password_env",
                            "invalid_env_name",
                            "must be a valid environment variable name",
                        )
                        password_env = None
                    password = None
                    if password_env is not None:
                        password = self._resolve_secret(password_env, "frigate.auth")
                    ca_bundle = auth_raw.get("ca_bundle")
                    ca_path: Path | None = None
                    if ca_bundle is not None:
                        if not isinstance(ca_bundle, str) or not ca_bundle:
                            self.error(
                                "frigate.auth.ca_bundle",
                                "invalid_value",
                                "must be a filesystem path",
                            )
                        else:
                            ca_path = Path(ca_bundle)
                            if not ca_path.is_file():
                                self.error(
                                    "frigate.auth.ca_bundle",
                                    "missing_file",
                                    f"{ca_bundle} does not exist",
                                )
                            ca_path = None if not ca_path.is_file() else ca_path
                    auth = FrigateAuthConfig(
                        mode="frigate",
                        username=username,
                        password_env=password_env,
                        password=password,
                        ca_bundle=ca_path,
                    )
                else:
                    self.error(
                        "frigate.auth.mode", "invalid_auth_mode", "must be 'none' or 'frigate'"
                    )
            if base_url is not None and auth is not None:
                frigate = FrigateConfig(base_url=base_url, auth=auth)

        # --- cameras ---
        cameras_raw = doc.get("cameras")
        cameras: dict[str, CameraConfig] = {}
        seen_frigate_names: dict[str, str] = {}
        seen_endpoints: dict[tuple[str, str, int, str], str] = {}
        if not isinstance(cameras_raw, dict) or not cameras_raw:
            self.error("cameras", "missing_value", "at least one camera must be configured")
        else:
            for key, cam_raw in cameras_raw.items():
                cam_path = f"cameras.{key}"
                if not isinstance(key, str) or not _CAMERA_KEY_RE.match(key or ""):
                    self.error(
                        cam_path,
                        "invalid_camera_key",
                        "camera keys must match [A-Za-z0-9][A-Za-z0-9_-]{0,63}",
                    )
                    continue
                if not isinstance(cam_raw, dict):
                    self.error(cam_path, "not_a_mapping", "camera configuration must be a mapping")
                    continue
                self._check_keys(
                    cam_raw,
                    cam_path,
                    frozenset({"frigate_name", "recovery", "maintenance", "onvif"}),
                )
                frigate_name = cam_raw.get("frigate_name")
                if not isinstance(frigate_name, str) or not frigate_name:
                    self.error(
                        f"{cam_path}.frigate_name", "missing_value", "frigate_name is required"
                    )
                    continue
                prior = seen_frigate_names.get(frigate_name)
                if prior is not None:
                    self.error(
                        f"{cam_path}.frigate_name",
                        "duplicate_frigate_name",
                        f"'{frigate_name}' is already configured as camera '{prior}'",
                    )
                    continue
                seen_frigate_names[frigate_name] = key

                recovery = cam_raw.get("recovery")
                if recovery not in ("onvif", "none"):
                    self.error(
                        f"{cam_path}.recovery", "invalid_recovery", "must be 'onvif' or 'none'"
                    )
                    continue
                maintenance = cam_raw.get("maintenance", False)
                if not isinstance(maintenance, bool):
                    self.error(
                        f"{cam_path}.maintenance", "invalid_value", "maintenance must be a boolean"
                    )
                    maintenance = False

                onvif: OnvifConfig | None = None
                if recovery == "onvif":
                    onvif_raw = cam_raw.get("onvif")
                    if not isinstance(onvif_raw, dict):
                        self.error(
                            f"{cam_path}.onvif",
                            "missing_value",
                            "onvif block is required when recovery is onvif",
                        )
                        continue
                    self._check_keys(
                        onvif_raw,
                        f"{cam_path}.onvif",
                        frozenset({"endpoint", "username", "password_env"}),
                    )
                    endpoint = self._validate_endpoint(
                        onvif_raw.get("endpoint"), f"{cam_path}.onvif.endpoint"
                    )
                    username = onvif_raw.get("username")
                    if not isinstance(username, str) or not username:
                        self.error(
                            f"{cam_path}.onvif.username", "missing_value", "username is required"
                        )
                        continue
                    password_env = onvif_raw.get("password_env")
                    if not isinstance(password_env, str) or not _ENV_NAME_RE.match(password_env):
                        self.error(
                            f"{cam_path}.onvif.password_env",
                            "invalid_env_name",
                            "must be a valid environment variable name",
                        )
                        continue
                    password = self._resolve_secret(password_env, f"{cam_path}.onvif")
                    if endpoint is None or password is None:
                        continue
                    ident = endpoint.identity
                    if ident in seen_endpoints:
                        self.error(
                            f"{cam_path}.onvif.endpoint",
                            "duplicate_target",
                            f"endpoint already configured for camera '{seen_endpoints[ident]}'",
                        )
                        continue
                    seen_endpoints[ident] = key
                    onvif = OnvifConfig(
                        endpoint=endpoint,
                        username=username,
                        password_env=password_env,
                        password=password,
                    )
                elif "onvif" in cam_raw:
                    self.error(
                        f"{cam_path}.onvif",
                        "unknown_key",
                        "onvif block is only allowed when recovery is 'onvif'",
                    )
                cameras[key] = CameraConfig(
                    key=key,
                    frigate_name=frigate_name,
                    recovery=recovery,
                    maintenance=maintenance,
                    onvif=onvif,
                )

        # --- mqtt ---
        mqtt_raw = doc.get("mqtt", {"enabled": False})
        mqtt: MqttConfig
        if not isinstance(mqtt_raw, dict):
            self.error("mqtt", "not_a_mapping", "mqtt must be a mapping")
            mqtt = MqttConfig(enabled=False)
        else:
            self._check_keys(
                mqtt_raw,
                "mqtt",
                frozenset({"enabled", "host", "port", "username", "password_env", "topic_prefix"}),
            )
            enabled = mqtt_raw.get("enabled", False)
            if not isinstance(enabled, bool):
                self.error("mqtt.enabled", "invalid_value", "must be a boolean")
                enabled = False
            host = mqtt_raw.get("host", "")
            if enabled and (not isinstance(host, str) or not host):
                self.error("mqtt.host", "missing_value", "host is required when mqtt is enabled")
            port = mqtt_raw.get("port", 1883)
            if not self._is_number(port) or not (1 <= int(port) <= 65535):
                self.error("mqtt.port", "invalid_value", "port must be between 1 and 65535")
                port = 1883
            username = mqtt_raw.get("username")
            if username is not None and (not isinstance(username, str) or not username):
                self.error(
                    "mqtt.username", "invalid_value", "must be a non-empty string or omitted"
                )
                username = None
            password_env = mqtt_raw.get("password_env")
            mqtt_password: Secret | None = None
            if password_env is not None:
                if not isinstance(password_env, str) or not _ENV_NAME_RE.match(password_env):
                    self.error(
                        "mqtt.password_env",
                        "invalid_env_name",
                        "must be a valid environment variable name",
                    )
                    password_env = None
                else:
                    mqtt_password = self._resolve_secret(password_env, "mqtt")
            topic_prefix = mqtt_raw.get("topic_prefix", "frigate-watchdog")
            if (
                not isinstance(topic_prefix, str)
                or not topic_prefix
                or any(c in topic_prefix for c in "+#$\x00")
                or topic_prefix.startswith("/")
            ):
                self.error(
                    "mqtt.topic_prefix", "invalid_value", "must be a valid MQTT topic prefix"
                )
                topic_prefix = "frigate-watchdog"
            mqtt = MqttConfig(
                enabled=enabled,
                host=host if isinstance(host, str) else "",
                port=int(port),
                username=username,
                password_env=password_env,
                password=mqtt_password,
                topic_prefix=topic_prefix,
            )

        # --- http ---
        http_raw = doc.get("http", {})
        http: HttpConfig
        if not isinstance(http_raw, dict):
            self.error("http", "not_a_mapping", "http must be a mapping")
            http = HttpConfig()
        else:
            self._check_keys(http_raw, "http", frozenset({"enabled", "host", "port"}))
            enabled = http_raw.get("enabled", True)
            if not isinstance(enabled, bool):
                self.error("http.enabled", "invalid_value", "must be a boolean")
                enabled = True
            host = http_raw.get("host", "127.0.0.1")
            if not isinstance(host, str) or not host:
                self.error("http.host", "invalid_value", "must be a host or address")
                host = "127.0.0.1"
            if host not in ("127.0.0.1", "localhost", "::1", "0.0.0.0", "::"):
                self.error(
                    "http.host",
                    "insecure_bind",
                    "v0.1.0 only allows loopback or explicit all-interfaces bind",
                )
            port = http_raw.get("port", 8080)
            if not self._is_number(port) or not (1 <= int(port) <= 65535):
                self.error("http.port", "invalid_value", "port must be between 1 and 65535")
                port = 8080
            http = HttpConfig(enabled=enabled, host=host, port=int(port))

        timings = Timings(self._validate_timings(doc.get("timings", {})))

        if self.problems:
            raise ConfigError(self.problems)
        if frigate is None:
            raise ConfigError([("frigate", "missing_value", "frigate configuration is required")])

        return WatchdogConfig(
            schema_version=WATCHDOG_SCHEMA_VERSION,
            mode=mode,
            instance=instance,
            frigate=frigate,
            cameras=cameras,
            timings=timings,
            mqtt=mqtt,
            http=http,
        )


def parse_config(text: str, env: Mapping[str, str] | None = None) -> WatchdogConfig:
    """Parse and validate configuration text. ``env`` defaults to ``os.environ``."""
    env = os.environ if env is None else env
    try:
        doc = yaml.load(text, Loader=_StrictLoader)  # noqa: S506 - hardened SafeLoader subclass
    except ConfigError:
        raise
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        where = f"line {mark.line + 1}" if mark else ""
        raise ConfigError(
            [("yaml", "invalid_yaml", f"could not parse YAML {where}".strip())]
        ) from exc
    return _Validator(env).validate(doc)


def load_config(path: Path | str, env: Mapping[str, str] | None = None) -> WatchdogConfig:
    """Load and validate configuration from a file."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError([(str(p), "unreadable_file", str(exc))]) from exc
    config = parse_config(text, env=env)
    return WatchdogConfig(
        schema_version=config.schema_version,
        mode=config.mode,
        instance=config.instance,
        frigate=config.frigate,
        cameras=config.cameras,
        timings=config.timings,
        mqtt=config.mqtt,
        http=config.http,
        source_path=p,
    )
