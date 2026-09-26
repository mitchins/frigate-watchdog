"""Regression tests for persistence findings from adversarial review."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from frigate_watchdog.constants import MIN_ATTEMPT_INTERVAL_S
from frigate_watchdog.store import (
    Store,
    StoreError,
    endpoint_identity,
    frigate_identity,
)

IDENT = endpoint_identity("http", "192.168.10.40", 80, "/onvif/device_service")
IDENT2 = endpoint_identity("http", "192.168.10.50", 80, "/onvif/device_service")
TS = 1_700_000_000.0


def make_store(tmp_path: Path) -> Store:
    store = Store(tmp_path / "state.db")
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    return store


def test_identity_change_moves_camera_not_merges_history(tmp_path):
    """Finding 1: a re-bound camera key must not read a stale identity row."""
    store = make_store(tmp_path)
    store.mark_armed("porch", "Porch", IDENT, TS)
    store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    store.record_outcome("porch", attempt, "ACKNOWLEDGED", TS + 2)
    # operator re-points the key at a new endpoint (new camera hardware)
    store.register_camera("porch", "PorchCam", IDENT2)
    assert store._identity_for("porch") == IDENT2
    # baseline does not carry to a different Frigate camera
    assert not store.armed("porch")
    # the old endpoint's limits are not read through the new binding
    assert store.accounting("porch").attempts_in_window == 0
    assert store.incident("porch") is None
    # and the old endpoint keeps its own history when re-registered elsewhere
    store.register_camera("porch2", "PorchCam", IDENT)
    store.mark_armed("porch2", "PorchCam", IDENT, TS + 3)
    acct = store.accounting("porch2")
    assert acct.attempts_in_window == 1
    assert acct.cooldown_until == pytest.approx(MIN_ATTEMPT_INTERVAL_S)
    store.close()


def test_same_frigate_name_rebinding_unarms_new_endpoint(tmp_path):
    """A new ONVIF target is a different device: do not carry the baseline
    (or the old identity's latch) onto it."""
    store = make_store(tmp_path)
    store.mark_armed("porch", "Porch", IDENT, TS)
    store.open_incident("porch", "Porch", IDENT, TS)
    store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    store.register_camera("porch", "Porch", IDENT2)
    assert store._identity_for("porch") == IDENT2
    assert not store.armed("porch")
    assert store.incident("porch") is None  # latch stayed on IDENT
    assert store.accounting("porch").attempts_in_window == 0
    store.close()


def test_latched_outcome_still_blocks_second_reserve(tmp_path):
    """Finding 2: one attempt per latched outage, classified or not."""
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    store.record_outcome("porch", attempt, "ACKNOWLEDGED", TS + 2)
    with pytest.raises(StoreError, match="consumed its attempt"):
        store.reserve_attempt("porch", "Porch", IDENT, TS + 3)
    # only resolution or acknowledgement opens a new outage
    store.acknowledge("porch", "operator", TS + 4)
    store.accrue(MIN_ATTEMPT_INTERVAL_S + 1)
    store.open_incident("porch", "Porch", IDENT, TS + 5000)
    store.reserve_attempt("porch", "Porch", IDENT, TS + 5001)  # ok
    store.close()


def test_resolve_marks_unresolved_attempt_as_unknown(tmp_path):
    """Crash window: healthy-confirmation resolves the outage and the
    outcomeless attempt is classified OUTCOME_UNKNOWN (consumed, not replayed)."""
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    store.resolve_incident("porch", TS + 2)
    assert store.incident("porch") is None  # resolved
    outcome = store.conn.execute("SELECT outcome FROM attempts").fetchone()[0]
    assert outcome == "OUTCOME_UNKNOWN"
    store.close()


def test_missing_safety_table_is_not_recreated(tmp_path):
    """Finding 3: dropped tables must not come back empty."""
    store = make_store(tmp_path)
    store.close()
    conn = sqlite3.connect(tmp_path / "state.db")
    conn.execute("DROP TABLE attempts")
    conn.commit()
    conn.close()
    with pytest.raises(StoreError, match="missing safety tables"):
        Store(tmp_path / "state.db").open()


def test_zero_byte_db_with_wal_debris_refused(tmp_path):
    """Finding 4: never initialize over a truncated file with WAL sidecars."""
    store = make_store(tmp_path)
    store.record_event("tick", TS)
    store.close()  # checkpoints and truncates WAL; recreate the debris case:
    wal = tmp_path / "state.db-wal"
    wal.write_bytes(b"\x00" * 100)
    (tmp_path / "state.db").write_bytes(b"")
    with pytest.raises(StoreError):
        Store(tmp_path / "state.db").open()


def test_double_open_incident_without_register_single_row(tmp_path):
    """Finding 5: open_incident registers the camera inside the txn."""
    store = Store(tmp_path / "state.db")
    store.open()
    id1 = store.open_incident("porch", "Porch", IDENT, TS)
    id2 = store.open_incident("porch", "Porch", IDENT, TS + 1)
    assert id1 == id2
    rows = store.conn.execute(
        "SELECT COUNT(*) FROM incidents WHERE endpoint_identity=? AND state='open'", (IDENT,)
    ).fetchone()[0]
    assert rows == 1
    store.close()


def test_open_failure_leaves_no_connection(tmp_path):
    """Finding 6: a refused open must not leave a usable connection."""
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.executescript("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);PRAGMA user_version=7;")
    conn.commit()
    conn.close()
    store = Store(db)
    with pytest.raises(StoreError):
        store.open()
    assert store.conn is None
    with pytest.raises(StoreError):
        store.record_event("x", TS)


def test_latch_incident_busy_raises(tmp_path):
    """Finding 7: no silent no-ops on a locked store."""
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    other = sqlite3.connect(tmp_path / "state.db", timeout=0.2)
    other.execute("BEGIN EXCLUSIVE")
    other.execute("SELECT 1 FROM meta")
    with pytest.raises(StoreError):
        store.latch_incident("porch", TS + 1)
    other.rollback()
    other.close()
    store.close()


def test_latch_incident_unknown_camera_raises(tmp_path):
    store = make_store(tmp_path)
    with pytest.raises(StoreError, match="not registered"):
        store.latch_incident("ghost", TS)
    store.close()


class _FailingExecuteProxy:
    """Delegate everything to the connection except the guarded statement."""

    def __init__(self, conn, trigger, exc):
        self._conn = conn
        self._trigger = trigger
        self._exc = exc

    def execute(self, sql, params=()):
        if self._trigger in sql:
            raise self._exc
        return self._conn.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_crash_during_reservation_leaves_nothing(tmp_path):
    """Injected failure inside the reservation txn rolls back completely."""
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    real_conn = store.conn
    store.conn = _FailingExecuteProxy(  # type: ignore[assignment]
        real_conn,
        "INSERT INTO attempts",
        sqlite3.OperationalError("injected disk I/O error"),
    )
    with pytest.raises(StoreError):
        store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    store.conn = real_conn
    status = store.status()
    assert status["attempts_total"] == 0
    # incident exists but is not latched; a retry can reserve cleanly
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 2)
    assert attempt
    store.close()


def test_budget_window_boundary_inclusive(tmp_path):
    """Finding 12: an attempt exactly 24h old still counts (stricter)."""
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    store.accrue(86400.0)
    assert store.accounting("porch").attempts_in_window == 1
    store.accrue(0.001)
    assert store.accounting("porch").attempts_in_window == 0
    store.close()


def test_two_identities_do_not_share_budget(tmp_path):
    """Finding 13."""
    store = Store(tmp_path / "state.db")
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    store.register_camera("drive", "Drive", IDENT2)
    store.open_incident("porch", "Porch", IDENT, TS)
    store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    assert store.accounting("drive").attempts_in_window == 0
    assert store.accounting("porch").attempts_in_window == 1
    store.close()


def test_acknowledge_other_camera_does_not_clear_this_one(tmp_path):
    """Finding 15."""
    store = Store(tmp_path / "state.db")
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    store.register_camera("drive", "Drive", IDENT2)
    store.open_incident("porch", "Porch", IDENT, TS)
    store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    store.open_incident("drive", "Drive", IDENT2, TS + 2)
    store.acknowledge("drive", "wrong camera", TS + 3)
    assert store.incident("porch") is not None and store.incident("porch").latched
    assert store.accounting("porch").attempts_in_window == 1
    store.close()


def test_record_outcome_auth_latch_uses_attempt_identity(tmp_path):
    """Finding 11/16: the latch follows the attempt's endpoint."""
    store = Store(tmp_path / "state.db")
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    # camera is re-bound after the attempt; outcome must still latch IDENT
    store.register_camera("porch", "Porch2", IDENT2)
    store.record_outcome("porch", attempt, "AUTH_FAILED", TS + 2)
    row = store.conn.execute("SELECT endpoint_identity FROM auth_latches").fetchone()
    assert row[0] == IDENT
    store.close()


def test_mark_armed_updates_identity(tmp_path):
    """Finding 10/17."""
    store = make_store(tmp_path)
    store.mark_armed("porch", "Porch", IDENT2, TS)
    assert store._identity_for("porch") == IDENT2
    assert store.armed("porch")
    store.close()


def test_user_version_zero_with_tables_refused(tmp_path):
    """Finding 6 (missing tests): partial initialization is refused."""
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        "CREATE TABLE events(event_id INTEGER PRIMARY KEY, ts_utc REAL, kind TEXT);"
    )
    conn.commit()
    conn.close()
    with pytest.raises(StoreError):
        Store(db).open()


def test_missing_runtime_acc_refused(tmp_path):
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE cameras(camera_key TEXT PRIMARY KEY, frigate_name TEXT,
                             endpoint_identity TEXT, armed INTEGER, armed_at_utc REAL);
        CREATE TABLE incidents(incident_id TEXT PRIMARY KEY, endpoint_identity TEXT,
                               camera_key TEXT, state TEXT, opened_at_utc REAL,
                               resolved_at_utc REAL);
        CREATE TABLE attempts(attempt_id TEXT PRIMARY KEY, endpoint_identity TEXT,
                              camera_key TEXT, incident_id TEXT, reserved_at_acc REAL,
                              outcome TEXT, outcome_at_acc REAL);
        CREATE TABLE auth_latches(endpoint_identity TEXT PRIMARY KEY, reason TEXT,
                                  latched_at_utc REAL);
        CREATE TABLE events(event_id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc REAL,
                            kind TEXT, camera TEXT, incident_id TEXT, attempt_id TEXT,
                            reason TEXT, detail TEXT);
        INSERT INTO meta VALUES('installation_id', 'x');
        INSERT INTO meta VALUES('schema_version', '1');
        PRAGMA user_version=1;
        """
    )
    conn.commit()
    conn.close()
    with pytest.raises(StoreError, match="runtime_acc"):
        Store(db).open()


CRASH_MID_RESERVE = """
import os, sys
from frigate_watchdog.store import Store, endpoint_identity
ident = endpoint_identity("http", "192.168.10.40", 80, "/onvif/device_service")
store = Store(sys.argv[1])
store.open()
store.register_camera("porch", "Porch", ident)
store.open_incident("porch", "Porch", ident, 1700000000.0)
real = store.conn
class Boom:
    def __getattr__(self, name):
        return getattr(real, name)
    def execute(self, sql, params=()):
        if "INSERT INTO attempts" in sql:
            os._exit(9)  # die mid-transaction
        return real.execute(sql, params)
store.conn = Boom()
store.reserve_attempt("porch", "Porch", ident, 1700000010.0)
"""


def test_sigkill_mid_reservation_rolls_back(tmp_path):
    """A hard kill inside the reservation txn leaves no attempt."""
    db = tmp_path / "state.db"
    proc = subprocess.run(
        [sys.executable, "-c", CRASH_MID_RESERVE, str(db)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 9
    store = Store(db)
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    assert store.status()["attempts_total"] == 0
    assert store.incident("porch") is not None  # open, not latched
    assert not store.incident("porch").latched
    store.close()


def test_witness_camera_identity_is_distinct(tmp_path):
    store = Store(tmp_path / "state.db")
    store.open()
    ident = frigate_identity("Doorbell")
    store.register_camera("doorbell", "Doorbell", ident)
    assert store.auth_latched("doorbell") is False
    assert store.accounting("doorbell").attempts_in_window == 0
    store.close()
