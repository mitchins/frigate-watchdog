"""Console reporting: state transitions and actions, not every poll.

Events carry only stable codes and identifiers; raw HTTP bodies, SOAP
envelopes, credentials, cookies, stream URLs, and full Frigate
configurations are never printed.
"""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

from .policy import Event

logger = logging.getLogger("frigate_watchdog.console")

_HUMAN = {
    "armed": "{camera}: healthy baseline established",
    "frames_stopped": "{camera}: frames stopped ({reason}); observing",
    "frigate_restarted": "Frigate restarted; failure evidence discarded, restart grace started",
    "monitoring_interrupted": "monitoring gap detected; failure evidence cleared (limits retained)",
    "incident_opened": "{camera}: incident {incident_id} opened",
    "inhibited": "recovery inhibited: {detail}",
    "action_proposed": "{camera}: recovery eligible; verifying before dispatch",
    "action_cancelled": "{camera}: proposed action cancelled ({reason})",
    "action_reserved": "{camera}: attempt {attempt_id} durably reserved",
    "action_outcome": "{camera}: reboot {reason}; {detail}",
    "incident_latched": (
        "{camera}: outage latched; no more attempts until acknowledged or confirmed healthy"
    ),
    "incident_resolved": "{camera}: incident resolved",
    "recovery_confirmed": "{camera}: frames restored after reboot request; recovery confirmed",
    "acknowledged": "{camera}: latch acknowledged by operator",
    "store_unsafe": "state store is not safe; automatic recovery inhibited until restart",
    "state_changed": "{camera}: {detail}",
}


def _ts(ts_utc: float) -> str:
    return datetime.fromtimestamp(ts_utc, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_event(event: Event) -> str:
    template = _HUMAN.get(event.kind, "{kind} {camera} {reason} {detail}")
    text = template.format(
        kind=event.kind,
        camera=event.camera or "-",
        reason=event.reason or "",
        detail=event.detail or "",
        incident_id=event.incident_id or "-",
        attempt_id=event.attempt_id or "-",
    )
    extras = []
    if event.incident_id and event.kind != "incident_opened":
        extras.append(f"incident={event.incident_id}")
    if event.attempt_id and event.kind not in ("action_reserved",):
        extras.append(f"attempt={event.attempt_id}")
    suffix = f" [{', '.join(extras)}]" if extras else ""
    return f"{_ts(event.ts_utc)} {text}{suffix}"


def setup_console_logging(verbose: bool = False) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger("frigate_watchdog")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    # third-party noise stays quiet
    for noisy in ("zeep", "httpx", "httpcore", "aiohttp.access", "aiomqtt"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def emit(event: Event) -> None:
    logger.info("%s", format_event(event))
