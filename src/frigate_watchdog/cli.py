"""The ``fwatch`` command-line interface.

    fwatch check-config          validate configuration and exit
    fwatch serve                 run the appliance
    fwatch health|stats|history  query a running instance's local HTTP API
    fwatch probe CAMERA          read-only ONVIF checks; never reboots
    fwatch acknowledge CAMERA    clear a latch (only while the service is stopped)

``acknowledge`` serializes with the running service through the exclusive
data-directory lock: if the service holds the lock, acknowledgement is
refused.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from . import __version__
from .config import ConfigError, WatchdogConfig, load_config
from .reporting import setup_console_logging

DEFAULT_CONFIG = os.environ.get("WATCHDOG_CONFIG", "/config/config.yaml")
DEFAULT_DATA = os.environ.get("WATCHDOG_DATA_DIR", "/data")


def _load_config(path: str) -> WatchdogConfig:
    try:
        return load_config(path)
    except ConfigError as exc:
        print(f"configuration invalid: {exc.redacted()}", file=sys.stderr)
        raise SystemExit(2) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fwatch", description="cautious camera-recovery appliance for Frigate"
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="path to config.yaml")
    parser.add_argument("--data-dir", default=DEFAULT_DATA, help="persistent data directory")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--version", action="version", version=f"fwatch {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check-config", help="validate configuration and exit")
    sub.add_parser("serve", help="run the watchdog service")

    p_health = sub.add_parser("health", help="query /health of a running instance")
    p_health.add_argument("--json", action="store_true")

    sub.add_parser("stats", help="query /stats of a running instance")

    p_history = sub.add_parser("history", help="query /history of a running instance")
    p_history.add_argument("--after", type=int, default=0)
    p_history.add_argument("--limit", type=int, default=50)

    p_probe = sub.add_parser("probe", help="read-only ONVIF checks for a camera")
    p_probe.add_argument("camera")

    p_ack = sub.add_parser("acknowledge", help="clear a camera's latch while stopped")
    p_ack.add_argument("camera")
    p_ack.add_argument("--reason", required=True)
    return parser


def _get(config: WatchdogConfig, path: str) -> dict[str, Any]:
    url = f"http://{config.http.host}:{config.http.port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            payload: dict[str, Any] = json.loads(response.read().decode())
            return payload
    except urllib.error.URLError as exc:
        print(f"cannot reach watchdog at {url}: {exc.reason}", file=sys.stderr)
        raise SystemExit(1) from exc
    except ValueError as exc:
        print(f"invalid response from {url}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


async def _probe(config: WatchdogConfig, camera: str) -> int:
    from .onvif import OnvifClient

    cam = config.cameras.get(camera)
    if cam is None:
        print(
            f"unknown camera '{camera}' (configured: {', '.join(config.cameras)})", file=sys.stderr
        )
        return 2
    if cam.onvif is None:
        print(f"camera '{camera}' has no onvif recovery configuration", file=sys.stderr)
        return 2
    print(f"probing {camera} at {cam.onvif.endpoint.url} (read-only; never reboots)")
    client = OnvifClient(cam.onvif)
    try:
        info = await client.probe()
        if info.outcome != "OK":
            print(f"probe failed: {info.outcome} ({info.detail})")
            return 1
        assert info.device is not None
        print(f"  device      : {info.device['manufacturer']} {info.device['model']}")
        print(f"  firmware    : {info.device['firmware']}")
        print(f"  serial      : {info.device['serial']}")
        clock = await client.get_system_date_and_time()
        print(f"  clock       : {clock.detail or clock.outcome}")
        print("probe OK; SystemReboot support is only proven by an authorized attempt")
        return 0
    finally:
        await client.close()


def _acknowledge(config: WatchdogConfig, data_dir: Path, camera: str, reason: str) -> int:
    import time

    from .store import Store, StoreError, StoreLockedError, data_dir_lock

    if camera not in config.cameras:
        print(
            f"unknown camera '{camera}' (configured: {', '.join(config.cameras)})", file=sys.stderr
        )
        return 2
    try:
        with data_dir_lock(data_dir):
            store = Store(data_dir / "state.db")
            store.open()
            cam_cfg = config.cameras[camera]
            from .store import endpoint_identity, frigate_identity

            # Acknowledge the identity already in the store. Do not rebind
            # the camera key to a new endpoint as a side effect of ack.
            if store._identity_for(camera) is None:
                if cam_cfg.onvif is not None:
                    ep = cam_cfg.onvif.endpoint
                    identity = endpoint_identity(ep.scheme, ep.host, ep.port, ep.path)
                else:
                    identity = frigate_identity(cam_cfg.frigate_name)
                store.register_camera(camera, cam_cfg.frigate_name, identity)
            store.acknowledge(camera, reason, time.time())
            print(
                f"acknowledged {camera}: latch cleared. Cooldowns, budgets, and "
                "global safety conditions still apply."
            )
            store.close()
            return 0
    except StoreLockedError:
        print(
            "the running watchdog holds the data directory; stop it first "
            "(docker stop) or acknowledge while it is stopped",
            file=sys.stderr,
        )
        return 3
    except StoreError as exc:
        print(f"state store error: {exc}", file=sys.stderr)
        return 2


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_console_logging(verbose=args.verbose)
    config = _load_config(args.config)

    if args.command == "check-config":
        print(f"configuration OK ({len(config.cameras)} camera(s), mode={config.mode})")
        for key, cam in config.cameras.items():
            target = cam.onvif.endpoint.url if cam.onvif else "-"
            print(
                f"  {key}: frigate={cam.frigate_name!r} recovery={cam.recovery} "
                f"maintenance={cam.maintenance} target={target}"
            )
        print(f"frigate: {config.frigate.base_url} (auth={config.frigate.auth.mode})")
        print(f"mqtt: {'enabled' if config.mqtt.enabled else 'disabled'}")
        return 0

    if args.command == "serve":
        from .service import run_service

        return asyncio.run(run_service(config, Path(args.data_dir)))

    if args.command == "health":
        payload = _get(config, "/health")
        print(json.dumps(payload, indent=2) if args.json else payload["status"])
        return 0 if payload.get("status") in ("alive",) else 1

    if args.command == "stats":
        print(json.dumps(_get(config, "/stats"), indent=2, default=str))
        return 0

    if args.command == "history":
        payload = _get(config, f"/history?after={args.after}&limit={args.limit}")
        for event in payload.get("events", []):
            print(
                f"{event.get('id')} {event.get('ts_utc')} {event.get('kind')} "
                f"camera={event.get('camera', '-')} reason={event.get('reason', '-')}"
            )
        return 0

    if args.command == "probe":
        return asyncio.run(_probe(config, args.camera))

    if args.command == "acknowledge":
        return _acknowledge(config, Path(args.data_dir), args.camera, args.reason)

    return 2  # pragma: no cover


if __name__ == "__main__":
    raise SystemExit(main())
