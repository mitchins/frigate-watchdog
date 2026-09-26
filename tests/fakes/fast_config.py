"""Direct construction of WatchdogConfig objects with sub-bound timings.

The shipped configuration validator enforces minimum timings for operators.
Integration tests construct configs directly (the engine never re-validates
bounds) so a full recovery cycle completes in seconds, not minutes.
"""

from __future__ import annotations

from collections.abc import Mapping

from frigate_watchdog.config import (
    CameraConfig,
    FrigateAuthConfig,
    FrigateConfig,
    HttpConfig,
    MqttConfig,
    OnvifConfig,
    OnvifEndpoint,
    Secret,
    Timings,
    WatchdogConfig,
)

FAST_TIMINGS = {
    "poll_interval_s": 0.2,
    "request_deadline_s": 2.0,
    "max_stats_age_s": 60.0,
    "startup_grace_s": 1.0,
    "frigate_restart_grace_s": 2.0,
    "post_interruption_stability_s": 1.0,
    "zero_frames_threshold_s": 2.0,
    "min_bad_snapshots": 3.0,
    "arm_healthy_s": 1.0,
    "recovery_confirm_s": 2.0,
    "boot_grace_s": 2.0,
    "monitoring_gap_s": 1.0,
}


def _endpoint(url: str) -> OnvifEndpoint:
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    return OnvifEndpoint(
        raw=url,
        scheme=parts.scheme,
        host=parts.hostname or "127.0.0.1",
        port=parts.port or 80,
        path=parts.path or "/",
    )


def fast_config(
    *,
    frigate_base: str,
    onvif_endpoints: Mapping[str, str],
    mode: str = "recover",
    frigate_names: Mapping[str, str] | None = None,
    maintenance: tuple[str, ...] = (),
    instance: str = "test",
    http_host: str = "127.0.0.1",
    http_port: int = 0,
) -> WatchdogConfig:
    names = frigate_names or {k: k.capitalize() for k in onvif_endpoints}
    cameras = {}
    for key, endpoint in onvif_endpoints.items():
        if endpoint == "none":
            cameras[key] = CameraConfig(
                key=key,
                frigate_name=names.get(key, key.capitalize()),
                recovery="none",
                maintenance=key in maintenance,
            )
        else:
            cameras[key] = CameraConfig(
                key=key,
                frigate_name=names.get(key, key.capitalize()),
                recovery="onvif",
                maintenance=key in maintenance,
                onvif=OnvifConfig(
                    endpoint=_endpoint(endpoint),
                    username="admin",
                    password_env=f"CAMERA_{key.upper()}_PASSWORD",
                    password=Secret(f"CAMERA_{key.upper()}_PASSWORD", "cam-pass"),
                ),
            )
    return WatchdogConfig(
        schema_version=1,
        mode=mode,
        instance=instance,
        frigate=FrigateConfig(base_url=frigate_base, auth=FrigateAuthConfig(mode="none")),
        cameras=cameras,
        timings=Timings(dict(FAST_TIMINGS)),
        mqtt=MqttConfig(enabled=False),
        http=HttpConfig(enabled=True, host=http_host, port=http_port),
    )
