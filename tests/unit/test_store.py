"""Persistence tests: durability, crash recovery, locks, limits.

Crash and lock tests use real subprocesses so WAL recovery and flock
semantics are exercised, not simulated.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from frigate_watchdog.constants import MIN_ATTEMPT_INTERVAL_S
from frigate_watchdog.store import (
    AccountingView,
    Store,
    StoreError,
    StoreLockedError,
    data_dir_lock,
    endpoint_identity,
)

IDENT = endpoint_identity("http", "192.168.10.40", 80, "/onvif/device_service")
TS = 1_700_000_000.0


def make_store(tmp_path: Path) -> Store:
    store = Store(tmp_path / "state.db")
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    return store


def test_fresh_store_initializes_and_persists_identity(tmp_path):
    store = make_store(tmp_path)
    iid = store.installation_id
    store.close()
    again = Store(tmp_path / "state.db")
    again.open()
    assert again.installation_id == iid
    again.close()


def test_reservation_is_atomic(tmp_path):
    store = make_store(tmp_path)
    incident = store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 10)
    view = store.incident("porch")
    assert view is not None and view.latched
    assert view.incident_id == incident
    # one durable event for the reservation
    kinds = [r.kind for r in store.history()]
    assert "action_reserved" in kinds
    store.record_outcome("porch", attempt, "ACKNOWLEDGED", TS + 20)
    # outcome recorded; latch persists until resolution/ack
    assert store.incident("porch").latched
    store.close()


def test_replay_protection_no_second_reserve_for_unresolved_attempt(tmp_path):
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    store.reserve_attempt("porch", "Porch", IDENT, TS + 10)
    with pytest.raises(StoreError, match="consumed its attempt"):
        store.reserve_attempt("porch", "Porch", IDENT, TS + 20)


def test_after_resolution_new_outage_gets_new_incident_and_attempt(tmp_path):
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 10)
    store.record_outcome("porch", attempt, "ACKNOWLEDGED", TS + 20)
    store.resolve_incident("porch", TS + 200)
    assert store.incident("porch") is None
    store.accrue(MIN_ATTEMPT_INTERVAL_S + 1)  # cooldown elapsed
    incident2 = store.open_incident("porch", "Porch", IDENT, TS + 4000)
    attempt2 = store.reserve_attempt("porch", "Porch", IDENT, TS + 4010)
    assert incident2 != store.history()[0].incident_id  # distinct ids
    assert attempt2 != attempt
    assert store.accounting("porch").attempts_in_window == 2


def test_budget_window_on_accumulated_clock(tmp_path):
    store = make_store(tmp_path)
    for cycle in range(3):
        store.open_incident("porch", "Porch", IDENT, TS + cycle * 4000)
        attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + cycle * 4000 + 10)
        store.record_outcome("porch", attempt, "ACKNOWLEDGED", TS + cycle * 4000 + 20)
        store.resolve_incident("porch", TS + cycle * 4000 + 30)
        store.acknowledge("porch", "test", TS + cycle * 4000 + 40)
        store.accrue(MIN_ATTEMPT_INTERVAL_S + 10)
    acct = store.accounting("porch")
    assert acct.attempts_in_window == 3
    # attempts age out of the 24h window as accumulated runtime advances
    store.accrue(86400.0)
    assert store.accounting("porch").attempts_in_window == 0


def test_accrued_clock_does_not_advance_during_downtime(tmp_path):
    store = make_store(tmp_path)
    store.accrue(100.0)
    store.accrue(50.0)
    assert store.now_acc == 150.0
    store.close()
    # "downtime" passes with no process running
    reopened = Store(tmp_path / "state.db")
    reopened.open()
    reopened.register_camera("porch", "Porch", IDENT)
    assert reopened.now_acc == 150.0
    reopened.close()


def test_camera_rename_preserves_limits(tmp_path):
    store = make_store(tmp_path)
    store.accrue(10.0)
    store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 10)
    store.record_outcome("porch", attempt, "ACKNOWLEDGED", TS + 20)
    store.close()
    # operator renames the camera key; same endpoint
    s2 = Store(tmp_path / "state.db")
    s2.open()
    s2.register_camera("porch2", "Porch", IDENT)
    acct = s2.accounting("porch2")
    assert acct.attempts_in_window == 1
    assert acct.cooldown_until == pytest.approx(10.0 + MIN_ATTEMPT_INTERVAL_S)
    s2.close()


def test_camera_removed_and_readded_preserves_limits(tmp_path):
    store = make_store(tmp_path)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS)
    store.record_outcome("porch", attempt, "REJECTED", TS + 5)
    store.close()
    s2 = Store(tmp_path / "state.db")
    s2.open()
    s2.register_camera("porch", "Porch", IDENT)
    assert s2.accounting("porch").attempts_in_window == 1
    s2.close()


def test_auth_latch_and_acknowledgement(tmp_path):
    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 10)
    store.record_outcome("porch", attempt, "UNSUPPORTED", TS + 20)
    assert store.auth_latched("porch")
    assert store.incident("porch").latched
    store.acknowledge("porch", "replaced firmware", TS + 100)
    assert not store.auth_latched("porch")
    assert store.incident("porch") is None
    # acknowledgement does not touch budgets
    assert store.accounting("porch").attempts_in_window == 1


def test_event_history_bounded_and_paginated(tmp_path):
    store = make_store(tmp_path)
    for i in range(120):
        store.record_event("tick", TS + i, detail=f"e{i}")
    rows = store.history(after_id=0, limit=1000)
    assert len(rows) == 120
    page = store.history(after_id=rows[49].event_id, limit=10)
    assert len(page) == 10
    assert page[0].event_id == rows[50].event_id
    store.close()


def test_corrupt_database_never_recreated(tmp_path):
    db = tmp_path / "state.db"
    db.write_bytes(b"this is definitely not sqlite" * 100)
    before = db.read_bytes()
    store = Store(db)
    with pytest.raises(StoreError):
        store.open()
    assert db.read_bytes() == before, "a corrupt store must never be rewritten"


def test_unsupported_schema_version_rejected(tmp_path):
    db = tmp_path / "state.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);PRAGMA user_version=99;"
    )
    conn.commit()
    conn.close()
    with pytest.raises(StoreError, match="schema version"):
        Store(db).open()


def test_readonly_store_rejected(tmp_path):
    store = make_store(tmp_path)
    store.close()
    db = tmp_path / "state.db"
    db.chmod(0o444)
    try:
        with pytest.raises(StoreError):
            ro = Store(db)
            ro.open()
            ro.record_event("x", TS)
    finally:
        db.chmod(0o644)


def test_busy_store_fails_closed(tmp_path):
    store = make_store(tmp_path)
    other = sqlite3.connect(tmp_path / "state.db", timeout=0.2)
    other.execute("BEGIN EXCLUSIVE")
    other.execute("SELECT 1 FROM meta")
    with pytest.raises(StoreError):
        store.open_incident("porch", "Porch", IDENT, TS)
    other.rollback()
    other.close()
    store.close()


def test_exclusive_data_dir_lock(tmp_path):
    data = tmp_path / "data"
    with data_dir_lock(data):
        with pytest.raises(StoreLockedError):
            with data_dir_lock(data):
                pass
    # lock released: relock works
    with data_dir_lock(data):
        pass


def test_second_process_cannot_lock(tmp_path):
    data = tmp_path / "data"
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                f"""
                import time
                from frigate_watchdog.store import data_dir_lock
                with data_dir_lock({str(data)!r}):
                    print("locked", flush=True)
                    time.sleep(30)
                """
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "locked"
        with pytest.raises(StoreLockedError):
            with data_dir_lock(data):
                pass
    finally:
        holder.kill()
        holder.wait(timeout=10)


CRASH_SCRIPT = """
import os, sys
from frigate_watchdog.store import Store, endpoint_identity
ident = endpoint_identity("http", "192.168.10.40", 80, "/onvif/device_service")
store = Store(sys.argv[1])
store.open()
store.register_camera("porch", "Porch", ident)
store.open_incident("porch", "Porch", ident, 1700000000.0)
store.accrue(42.0)
store.reserve_attempt("porch", "Porch", ident, 1700000010.0)
# crash: no outcome recorded, no clean close
os._exit(1)
"""


def test_crash_after_reservation_keeps_attempt_consumed(tmp_path):
    db = tmp_path / "state.db"
    proc = subprocess.run(
        [sys.executable, "-c", CRASH_SCRIPT, str(db)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 1
    store = Store(db)
    store.open()
    store.register_camera("porch", "Porch", IDENT)
    view = store.incident("porch")
    assert view is not None and view.latched
    acct = store.accounting("porch")
    assert acct.attempts_in_window == 1
    assert store.now_acc == pytest.approx(42.0)
    # the unresolved attempt is exactly the "outcome unknown" case
    unresolved = store.conn.execute(
        "SELECT COUNT(*) FROM attempts WHERE outcome IS NULL"
    ).fetchone()[0]
    assert unresolved == 1
    # replay protection holds across the crash
    with pytest.raises(StoreError):
        store.reserve_attempt("porch", "Porch", IDENT, 1_700_000_100.0)
    store.close()


def test_events_pruned_without_touching_safety_records(tmp_path):
    import frigate_watchdog.store as store_mod

    store = make_store(tmp_path)
    store.open_incident("porch", "Porch", IDENT, TS)
    attempt = store.reserve_attempt("porch", "Porch", IDENT, TS + 1)
    # flood events beyond the cap with a tiny patched cap
    real_max = store_mod.HISTORY_MAX_EVENTS
    store_mod.HISTORY_MAX_EVENTS = 50
    try:
        for i in range(200):
            store.record_event("tick", TS + i)
    finally:
        store_mod.HISTORY_MAX_EVENTS = real_max
    assert len(store.history(limit=1000)) <= 50 + 2  # cap + the safety events
    acct = store.accounting("porch")
    assert isinstance(acct, AccountingView)
    assert acct.attempts_in_window == 1, "pruning history must not reset limits"
    assert store.incident("porch").latched
    assert attempt
