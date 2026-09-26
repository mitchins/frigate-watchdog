"""Hardcoded safety constants.

The values in this module are deliberately NOT configurable. The hourly
minimum between attempts, the three-attempt daily ceiling, single-flight
execution, and the two-camera inhibition threshold are the core loop
prevention guarantees of v0.1.0; configuration must not be able to weaken
them, so they are not configuration keys at all.
"""

from __future__ import annotations

# --- Recovery limits (NOT configurable) ---
MIN_ATTEMPT_INTERVAL_S = 3600.0
"""Minimum interval between attempts on one camera (accumulated-runtime clock)."""

DAILY_ATTEMPT_BUDGET = 3
"""Maximum attempts per camera per 24h accumulated-runtime window."""

BUDGET_WINDOW_S = 86400.0
"""Accumulated-runtime seconds covered by the daily attempt budget."""

GLOBAL_MIN_ATTEMPT_INTERVAL_S = 300.0
"""Minimum spacing between any two attempts across all cameras (constant)."""

MAX_CONCURRENT_RECOVERIES = 1
"""Exactly one recovery operation may be in flight or in boot grace at a time."""

GROUP_INHIBITION_THRESHOLD = 2
"""Number of failing enabled cameras that inhibit all automatic recovery."""

# --- Protocol limits ---
ONVIF_OPERATION_DEADLINE_S = 10.0
"""Total deadline for one ONVIF operation."""

ONVIF_MAX_RESPONSE_BYTES = 256 * 1024
"""Maximum accepted ONVIF SOAP response body (small management responses)."""

HTTP_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
"""Maximum accepted Frigate API response body."""

# --- Persistence limits ---
HISTORY_MAX_EVENTS = 5000
"""Bounded event history rows; safety tables are never pruned with it."""

SCHEMA_VERSION = 1
"""SQLite state-store schema version."""

WATCHDOG_SCHEMA_VERSION = 1
"""Configuration schema_version value accepted by v0.1.0."""

# --- Timing defaults and bounds ---
# name -> (default, min, max)
_TIMING_TABLE: dict[str, tuple[float, float, float]] = {
    "poll_interval_s": (10.0, 5.0, 60.0),
    "request_deadline_s": (5.0, 2.0, 15.0),
    "max_stats_age_s": (60.0, 15.0, 300.0),
    "startup_grace_s": (180.0, 60.0, 3600.0),
    "frigate_restart_grace_s": (180.0, 60.0, 3600.0),
    "post_interruption_stability_s": (60.0, 30.0, 600.0),
    "zero_frames_threshold_s": (120.0, 60.0, 3600.0),
    "min_bad_snapshots": (6.0, 3.0, 60.0),
    "arm_healthy_s": (60.0, 30.0, 600.0),
    "recovery_confirm_s": (120.0, 60.0, 3600.0),
    "boot_grace_s": (180.0, 60.0, 3600.0),
    "monitoring_gap_s": (30.0, 15.0, 300.0),
}

DEFAULT_TIMINGS: dict[str, float] = {
    name: default for name, (default, _lo, _hi) in _TIMING_TABLE.items()
}
TIMING_BOUNDS: dict[str, tuple[float, float]] = {
    name: (lo, hi) for name, (_d, lo, hi) in _TIMING_TABLE.items()
}
