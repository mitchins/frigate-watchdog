"""Console reporting format tests (sanitized output)."""

from __future__ import annotations

from frigate_watchdog.policy import Event
from frigate_watchdog.reporting import format_event, setup_console_logging


def test_event_formatting_includes_codes_and_ids():
    event = Event(
        kind="action_outcome",
        ts_utc=1_700_000_000.0,
        ts_mono=1.0,
        camera="porch",
        incident_id="inc-1",
        attempt_id="att-1",
        reason="OUTCOME_UNKNOWN",
        detail="reboot outcome unknown; no automatic retry",
    )
    line = format_event(event)
    assert "2023-11-14T22:13:20Z" in line  # rendered UTC timestamp
    assert "porch" in line
    assert "OUTCOME_UNKNOWN" in line
    assert "inc-1" in line and "att-1" in line
    assert "no automatic retry" in line


def test_reporting_never_contains_secrets():
    event = Event(
        kind="action_outcome",
        ts_utc=0,
        ts_mono=0,
        camera="porch",
        reason="AUTH_FAILED",
        detail="camera rejected credentials",
    )
    line = format_event(event)
    for secret in ("password", "token=", "cookie", "Basic "):
        assert secret not in line


def test_unknown_kind_renders_something_stable():
    event = Event(kind="future_kind", ts_utc=0, ts_mono=0, camera="porch", reason="X")
    line = format_event(event)
    assert "future_kind" in line


def test_console_logging_setup():
    setup_console_logging(verbose=True)
    setup_console_logging(verbose=False)
