"""Durable safety state on SQLite.

Design rules (from the recovery policy):

* A reservation commits atomically: incident latch + attempt row + event in
  one ``synchronous=FULL`` transaction. If the reservation cannot commit,
  nothing is sent. A latched outage can never reserve a second attempt.
* Safety limits are recorded on an *accumulated-runtime* clock that only
  advances while the watchdog runs. Downtime earns no credit, so a crash can
  make a cooldown longer, never shorter, and wall-clock corrections cannot
  expire limits early.
* Attempt budgets and cooldowns are keyed by endpoint identity, so renaming
  or removing/re-adding a camera does not erase its limits. A camera key
  maps to exactly one identity at a time; changing it moves the camera, it
  does not merge two cameras' histories.
* Event history is pruned independently of safety records.
* One process owns the data directory, proven with an exclusive ``flock``.

The store never deletes, recreates, or "repairs" a state file it cannot
understand: unknown schema, missing tables, or leftover WAL debris after a
truncation are all hard errors.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import sqlite3
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .constants import (
    BUDGET_WINDOW_S,
    HISTORY_MAX_EVENTS,
    MIN_ATTEMPT_INTERVAL_S,
    SCHEMA_VERSION,
)

_REQUIRED_TABLES = ("meta", "cameras", "incidents", "attempts", "auth_latches", "events")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cameras (
    camera_key TEXT PRIMARY KEY,
    frigate_name TEXT NOT NULL,
    endpoint_identity TEXT NOT NULL,
    armed INTEGER NOT NULL DEFAULT 0,
    armed_at_utc REAL
);
CREATE INDEX IF NOT EXISTS idx_cameras_endpoint ON cameras(endpoint_identity);
CREATE TABLE IF NOT EXISTS incidents (
    incident_id TEXT PRIMARY KEY,
    endpoint_identity TEXT NOT NULL,
    camera_key TEXT NOT NULL,
    state TEXT NOT NULL,            -- open | latched | resolved | acknowledged
    opened_at_utc REAL NOT NULL,
    resolved_at_utc REAL
);
CREATE INDEX IF NOT EXISTS idx_incidents_endpoint ON incidents(endpoint_identity);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    endpoint_identity TEXT NOT NULL,
    camera_key TEXT NOT NULL,
    incident_id TEXT NOT NULL,
    reserved_at_acc REAL NOT NULL,
    outcome TEXT,                   -- NULL until classified
    outcome_at_acc REAL
);
CREATE INDEX IF NOT EXISTS idx_attempts_endpoint ON attempts(endpoint_identity, reserved_at_acc);
CREATE TABLE IF NOT EXISTS auth_latches (
    endpoint_identity TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    latched_at_utc REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc REAL NOT NULL,
    kind TEXT NOT NULL,
    camera TEXT,
    incident_id TEXT,
    attempt_id TEXT,
    reason TEXT,
    detail TEXT
);
"""


class StoreError(RuntimeError):
    """The durable state store is unusable or an operation failed."""


class StoreLockedError(StoreError):
    """Another process owns the data directory."""


@dataclass(frozen=True)
class IncidentView:
    incident_id: str
    camera: str
    latched: bool


@dataclass(frozen=True)
class AccountingView:
    attempts_in_window: int
    last_attempt_at: float | None
    cooldown_until: float


@dataclass(frozen=True)
class HistoryRow:
    event_id: int
    ts_utc: float
    kind: str
    camera: str | None
    incident_id: str | None
    attempt_id: str | None
    reason: str | None
    detail: str | None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"id": self.event_id, "ts_utc": self.ts_utc, "kind": self.kind}
        for name in ("camera", "incident_id", "attempt_id", "reason", "detail"):
            value = getattr(self, name)
            if value is not None:
                out[name] = value
        return out


DATA_LOCK_FILENAME = "watchdog.lock"
DB_FILENAME = "state.db"


@contextlib.contextmanager
def data_dir_lock(data_dir: Path | str) -> Iterator[Path]:
    """Exclusively lock the data directory; a second process exits via error."""
    try:
        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        lock_path = data_dir / DATA_LOCK_FILENAME
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    except OSError as exc:
        raise StoreError(f"cannot create lock in data directory: {exc}") from exc
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise StoreLockedError(
                f"another process holds the lock on {lock_path}; refusing to start"
            ) from exc
        try:
            yield data_dir
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def endpoint_identity(scheme: str, host: str, port: int, path: str) -> str:
    """Stable identity for a recovery target; limits follow this, not names."""
    return f"{scheme}://{host}:{port}{path.rstrip('/') or '/'}"


def frigate_identity(frigate_name: str) -> str:
    """Identity for cameras without an ONVIF target (witness-only)."""
    return f"frigate:{frigate_name}"


class Store:
    """SQLite-backed store. Single-process use after :meth:`open`.

    Invariant: at most one ``cameras`` row per ``camera_key``. Re-registering
    a key against a different endpoint *moves* the camera (keeping its armed
    baseline only when the Frigate camera is the same); it never leaves a
    stale row that reads could pick up instead.
    """

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection | None = None
        self._acc = 0.0

    # ---------------------------------------------------------------- lifecycle

    def open(self) -> None:
        conn: sqlite3.Connection | None = None
        try:
            fresh = self._is_fresh()
            conn = sqlite3.connect(
                self.db_path,
                timeout=5.0,
                isolation_level=None,  # explicit transactions
                check_same_thread=False,  # all access serialized by the coordinator
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=5000")
            if fresh:
                self._initialize(conn)
            else:
                self._verify(conn)
            row = conn.execute("SELECT value FROM meta WHERE key='runtime_acc'").fetchone()
            if row is None:
                raise StoreError("state store has no runtime clock; refusing to use it")
            self._acc = float(row[0])
            self.conn = conn  # attached only after successful verification
        except StoreError:
            if conn is not None:
                conn.close()
            raise
        except sqlite3.DatabaseError as exc:
            if conn is not None:
                conn.close()
            raise StoreError(f"cannot open state store at {self.db_path}: {exc}") from exc

    def _is_fresh(self) -> bool:
        if not self.db_path.exists():
            return True
        if self.db_path.stat().st_size != 0:
            return False
        # A truncated main file with WAL debris must never be re-initialized.
        return not (
            self.db_path.with_name(self.db_path.name + "-wal").exists()
            or self.db_path.with_name(self.db_path.name + "-shm").exists()
        )

    def _initialize(self, conn: sqlite3.Connection) -> None:
        # executescript() implicitly commits, so DDL runs outside the txn
        conn.executescript(_SCHEMA)
        with self._txn(conn):
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('installation_id', ?)",
                (uuid.uuid4().hex,),
            )
            conn.execute("INSERT INTO meta(key, value) VALUES('runtime_acc', '0')")
            conn.execute(f"PRAGMA user_version={int(SCHEMA_VERSION)}")

    def _verify(self, conn: sqlite3.Connection) -> None:
        try:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        except sqlite3.DatabaseError as exc:
            raise StoreError(f"state store unreadable: {exc}") from exc
        if version != SCHEMA_VERSION:
            raise StoreError(
                f"state store schema version {version} is not supported "
                f"(expected {SCHEMA_VERSION}); refusing to modify it"
            )
        names = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        missing = [t for t in _REQUIRED_TABLES if t not in names]
        if missing:
            raise StoreError(f"state store is missing safety tables {missing}; refusing to use it")
        for key in ("installation_id", "runtime_acc"):
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            if row is None:
                raise StoreError(f"state store has no {key}; refusing to use it")

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.DatabaseError:
                pass
            self.conn.close()
            self.conn = None

    @contextlib.contextmanager
    def _txn(self, conn: sqlite3.Connection | None = None) -> Iterator[sqlite3.Connection]:
        c = conn or self.conn
        assert c is not None, "store is not open"
        try:
            c.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise StoreError(f"state store busy: {exc}") from exc
        try:
            yield c
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                c.execute("ROLLBACK")
            raise
        try:
            c.execute("COMMIT")
        except sqlite3.Error as exc:
            with contextlib.suppress(sqlite3.Error):
                c.execute("ROLLBACK")
            raise StoreError(f"state store commit failed: {exc}") from exc

    def _require_open(self) -> sqlite3.Connection:
        if self.conn is None:
            raise StoreError("state store is not open")
        return self.conn

    # ---------------------------------------------------------------- meta

    @property
    def installation_id(self) -> str:
        row = (
            self._require_open()
            .execute("SELECT value FROM meta WHERE key='installation_id'")
            .fetchone()
        )
        if row is None:
            raise StoreError("state store has no installation identity")
        return str(row[0])

    # ---------------------------------------------------------------- acc clock

    def accrue(self, elapsed_seconds: float) -> None:
        """Advance the accumulated-runtime clock (only while the process runs)."""
        if elapsed_seconds <= 0:
            return
        c = self._require_open()
        try:
            with self._txn(c) as conn:
                updated = conn.execute(
                    "UPDATE meta SET value = CAST(CAST(value AS REAL) + ? AS TEXT) "
                    "WHERE key='runtime_acc'",
                    (elapsed_seconds,),
                ).rowcount
                if updated != 1:
                    raise StoreError("runtime clock row missing")
            self._acc += elapsed_seconds
        except StoreError:
            raise
        except sqlite3.Error as exc:
            raise StoreError(f"runtime clock update failed: {exc}") from exc

    @property
    def now_acc(self) -> float:
        return self._acc

    # ---------------------------------------------------------------- identity

    def register_camera(
        self,
        camera_key: str,
        frigate_name: str,
        identity: str,
        *,
        keep_armed: bool = True,
    ) -> None:
        """Bind a camera key to one identity, replacing any previous binding."""
        c = self._require_open()
        try:
            with self._txn(c) as conn:
                if keep_armed:
                    # Same Frigate name AND same endpoint keeps the baseline.
                    # A new ONVIF target is a different device: unarm so a
                    # latched outage on the old identity cannot follow the key.
                    conn.execute(
                        "UPDATE cameras SET frigate_name=?, endpoint_identity=?, "
                        "armed=CASE WHEN endpoint_identity=? THEN armed ELSE 0 END, "
                        "armed_at_utc=CASE WHEN endpoint_identity=? "
                        "THEN armed_at_utc ELSE NULL END "
                        "WHERE camera_key=?",
                        (frigate_name, identity, identity, identity, camera_key),
                    )
                else:
                    conn.execute(
                        "UPDATE cameras SET frigate_name=?, endpoint_identity=?, armed=0, "
                        "armed_at_utc=NULL WHERE camera_key=?",
                        (frigate_name, identity, camera_key),
                    )
                conn.execute(
                    "INSERT INTO cameras(camera_key, frigate_name, endpoint_identity, armed) "
                    "VALUES(?, ?, ?, 0) ON CONFLICT(camera_key) DO UPDATE SET "
                    "frigate_name=excluded.frigate_name, "
                    "endpoint_identity=excluded.endpoint_identity",
                    (camera_key, frigate_name, identity),
                )
        except sqlite3.Error as exc:
            raise StoreError(f"camera registration failed: {exc}") from exc

    def _identity_for(self, camera_key: str) -> str | None:
        row = (
            self._require_open()
            .execute("SELECT endpoint_identity FROM cameras WHERE camera_key=?", (camera_key,))
            .fetchone()
        )
        return str(row[0]) if row else None

    # ---------------------------------------------------------------- StoreView

    def armed(self, camera_key: str) -> bool:
        row = (
            self._require_open()
            .execute("SELECT armed FROM cameras WHERE camera_key=?", (camera_key,))
            .fetchone()
        )
        return bool(row and row[0])

    def mark_armed(self, camera_key: str, frigate_name: str, identity: str, ts_utc: float) -> None:
        c = self._require_open()
        try:
            with self._txn(c) as conn:
                conn.execute(
                    "INSERT INTO cameras(camera_key, frigate_name, endpoint_identity, armed, "
                    "armed_at_utc) VALUES(?, ?, ?, 1, ?) "
                    "ON CONFLICT(camera_key) DO UPDATE SET "
                    "frigate_name=excluded.frigate_name, "
                    "endpoint_identity=excluded.endpoint_identity, "
                    "armed=1, armed_at_utc=excluded.armed_at_utc",
                    (camera_key, frigate_name, identity, ts_utc),
                )
        except sqlite3.Error as exc:
            raise StoreError(f"arming failed: {exc}") from exc

    def incident(self, camera_key: str) -> IncidentView | None:
        identity = self._identity_for(camera_key)
        if identity is None:
            return None
        return self._incident_for_identity(identity)

    def _incident_for_identity(self, identity: str) -> IncidentView | None:
        row = (
            self._require_open()
            .execute(
                "SELECT incident_id, camera_key, state FROM incidents "
                "WHERE endpoint_identity=? AND state IN ('open', 'latched') "
                "ORDER BY rowid DESC LIMIT 1",
                (identity,),
            )
            .fetchone()
        )
        if row is None:
            return None
        return IncidentView(incident_id=row[0], camera=row[1], latched=row[2] == "latched")

    def accounting(self, camera_key: str) -> AccountingView:
        identity = self._identity_for(camera_key)
        if identity is None:
            return AccountingView(0, None, 0.0)
        c = self._require_open()
        rows = c.execute(
            "SELECT reserved_at_acc FROM attempts WHERE endpoint_identity=? "
            "AND reserved_at_acc >= ?",
            (identity, self._acc - BUDGET_WINDOW_S),
        ).fetchall()
        last = c.execute(
            "SELECT MAX(reserved_at_acc) FROM attempts WHERE endpoint_identity=?",
            (identity,),
        ).fetchone()[0]
        attempts = [float(r[0]) for r in rows]
        cooldown = (float(last) + MIN_ATTEMPT_INTERVAL_S) if last is not None else 0.0
        return AccountingView(len(attempts), float(last) if last is not None else None, cooldown)

    def last_attempt_any(self) -> float | None:
        row = self._require_open().execute("SELECT MAX(reserved_at_acc) FROM attempts").fetchone()
        return float(row[0]) if row and row[0] is not None else None

    def auth_latched(self, camera_key: str) -> bool:
        identity = self._identity_for(camera_key)
        if identity is None:
            return False
        row = (
            self._require_open()
            .execute("SELECT 1 FROM auth_latches WHERE endpoint_identity=?", (identity,))
            .fetchone()
        )
        return row is not None

    # ---------------------------------------------------------------- mutations

    def open_incident(
        self, camera_key: str, frigate_name: str, identity: str, ts_utc: float
    ) -> str:
        c = self._require_open()
        try:
            with self._txn(c) as conn:
                self._ensure_camera(conn, camera_key, frigate_name, identity)
                existing = conn.execute(
                    "SELECT incident_id FROM incidents "
                    "WHERE endpoint_identity=? AND state IN ('open', 'latched') "
                    "ORDER BY rowid DESC LIMIT 1",
                    (identity,),
                ).fetchone()
                if existing is not None:
                    return str(existing[0])
                incident_id = f"inc-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    "INSERT INTO incidents(incident_id, endpoint_identity, camera_key, state, "
                    "opened_at_utc) VALUES(?, ?, ?, 'open', ?)",
                    (incident_id, identity, camera_key, ts_utc),
                )
                self._append_event(
                    conn, "incident_opened", ts_utc, camera=camera_key, incident_id=incident_id
                )
                return incident_id
        except StoreError:
            raise
        except sqlite3.Error as exc:
            raise StoreError(f"incident open failed: {exc}") from exc

    @staticmethod
    def _ensure_camera(
        conn: sqlite3.Connection,
        camera_key: str,
        frigate_name: str,
        identity: str,
    ) -> None:
        conn.execute(
            "INSERT INTO cameras(camera_key, frigate_name, endpoint_identity, armed) "
            "VALUES(?, ?, ?, 0) ON CONFLICT(camera_key) DO UPDATE SET "
            "frigate_name=excluded.frigate_name, endpoint_identity=excluded.endpoint_identity",
            (camera_key, frigate_name, identity),
        )

    def latch_incident(self, camera_key: str, ts_utc: float) -> None:
        c = self._require_open()
        identity = self._identity_for(camera_key)
        if identity is None:
            raise StoreError(f"camera '{camera_key}' is not registered")
        try:
            with self._txn(c) as conn:
                conn.execute(
                    "UPDATE incidents SET state='latched' "
                    "WHERE endpoint_identity=? AND state='open'",
                    (identity,),
                )
        except sqlite3.Error as exc:
            raise StoreError(f"incident latch failed: {exc}") from exc

    def reserve_attempt(
        self,
        camera_key: str,
        frigate_name: str,
        identity: str,
        ts_utc: float,
    ) -> str:
        """Atomically: ensure incident, latch it, reserve one attempt, record event.

        Raises :class:`StoreError` if the outage already holds an attempt
        (resolved or not — a latched outage gets exactly one) or the
        transaction cannot commit.
        """
        c = self._require_open()
        try:
            with self._txn(c) as conn:
                self._ensure_camera(conn, camera_key, frigate_name, identity)
                row = conn.execute(
                    "SELECT incident_id, state FROM incidents "
                    "WHERE endpoint_identity=? AND state IN ('open', 'latched') "
                    "ORDER BY rowid DESC LIMIT 1",
                    (identity,),
                ).fetchone()
                if row is None:
                    incident_id = f"inc-{uuid.uuid4().hex[:12]}"
                    conn.execute(
                        "INSERT INTO incidents(incident_id, endpoint_identity, camera_key, "
                        "state, opened_at_utc) VALUES(?, ?, ?, 'latched', ?)",
                        (incident_id, identity, camera_key, ts_utc),
                    )
                elif row[1] == "latched":
                    raise StoreError(
                        "refusing to reserve: this outage already consumed its attempt"
                    )
                else:  # open
                    incident_id = row[0]
                    conn.execute(
                        "UPDATE incidents SET state='latched' WHERE incident_id=?",
                        (incident_id,),
                    )
                attempt_id = f"att-{uuid.uuid4().hex[:12]}"
                conn.execute(
                    "INSERT INTO attempts(attempt_id, endpoint_identity, camera_key, "
                    "incident_id, reserved_at_acc) VALUES(?, ?, ?, ?, ?)",
                    (attempt_id, identity, camera_key, incident_id, self._acc),
                )
                self._append_event(
                    conn,
                    "action_reserved",
                    ts_utc,
                    camera=camera_key,
                    incident_id=incident_id,
                    attempt_id=attempt_id,
                    reason="RESERVED",
                )
                return attempt_id
        except StoreError:
            raise
        except sqlite3.Error as exc:
            raise StoreError(f"attempt reservation failed: {exc}") from exc

    def record_outcome(self, camera_key: str, attempt_id: str, outcome: str, ts_utc: float) -> None:
        c = self._require_open()
        try:
            with self._txn(c) as conn:
                identity_row = conn.execute(
                    "SELECT endpoint_identity FROM attempts WHERE attempt_id=?", (attempt_id,)
                ).fetchone()
                if identity_row is None:
                    raise StoreError(f"attempt {attempt_id} not found")
                identity = str(identity_row[0])
                updated = conn.execute(
                    "UPDATE attempts SET outcome=?, outcome_at_acc=? WHERE attempt_id=?",
                    (outcome, self._acc, attempt_id),
                ).rowcount
                if not updated:
                    raise StoreError(f"attempt {attempt_id} not updated")
                if outcome in ("AUTH_FAILED", "UNSUPPORTED"):
                    conn.execute(
                        "INSERT OR REPLACE INTO auth_latches(endpoint_identity, reason, "
                        "latched_at_utc) VALUES(?, ?, ?)",
                        (identity, outcome, ts_utc),
                    )
                incident_row = conn.execute(
                    "SELECT incident_id FROM attempts WHERE attempt_id=?", (attempt_id,)
                ).fetchone()
                self._append_event(
                    conn,
                    "action_outcome",
                    ts_utc,
                    camera=camera_key,
                    incident_id=incident_row[0] if incident_row else None,
                    attempt_id=attempt_id,
                    reason=outcome,
                )
        except StoreError:
            raise
        except sqlite3.Error as exc:
            raise StoreError(f"outcome recording failed: {exc}") from exc

    def resolve_incident(self, camera_key: str, ts_utc: float) -> None:
        c = self._require_open()
        identity = self._identity_for(camera_key)
        if identity is None:
            raise StoreError(f"camera '{camera_key}' is not registered")
        try:
            with self._txn(c) as conn:
                row = conn.execute(
                    "SELECT incident_id FROM incidents "
                    "WHERE endpoint_identity=? AND state IN ('open', 'latched') "
                    "ORDER BY rowid DESC LIMIT 1",
                    (identity,),
                ).fetchone()
                if row is None:
                    return
                # A confirmed-healthy outage whose attempt never got an
                # outcome recorded (crash between reservation and recording)
                # resolves as OUTCOME_UNKNOWN: frames returned, delivery
                # status will never be known. The attempt stays consumed.
                conn.execute(
                    "UPDATE attempts SET outcome='OUTCOME_UNKNOWN', outcome_at_acc=? "
                    "WHERE incident_id=? AND outcome IS NULL",
                    (self._acc, row[0]),
                )
                conn.execute(
                    "UPDATE incidents SET state='resolved', resolved_at_utc=? WHERE incident_id=?",
                    (ts_utc, row[0]),
                )
                self._append_event(
                    conn,
                    "incident_resolved",
                    ts_utc,
                    camera=camera_key,
                    incident_id=row[0],
                    reason="FRAMES_RESTORED",
                )
        except StoreError:
            raise
        except sqlite3.Error as exc:
            raise StoreError(f"incident resolution failed: {exc}") from exc

    def acknowledge(self, camera_key: str, reason: str, ts_utc: float) -> None:
        """Operator acknowledgement: clears latch and auth latch only."""
        c = self._require_open()
        identity = self._identity_for(camera_key)
        if identity is None:
            raise StoreError(f"camera '{camera_key}' is not registered")
        try:
            with self._txn(c) as conn:
                conn.execute(
                    "UPDATE incidents SET state='acknowledged', resolved_at_utc=? "
                    "WHERE endpoint_identity=? AND state IN ('open', 'latched')",
                    (ts_utc, identity),
                )
                conn.execute("DELETE FROM auth_latches WHERE endpoint_identity=?", (identity,))
                self._append_event(
                    conn,
                    "acknowledged",
                    ts_utc,
                    camera=camera_key,
                    reason="OPERATOR_ACK",
                    detail=reason,
                )
        except sqlite3.Error as exc:
            raise StoreError(f"acknowledgement failed: {exc}") from exc

    def auth_latch(self, camera_key: str, reason: str, ts_utc: float) -> None:
        c = self._require_open()
        identity = self._identity_for(camera_key)
        if identity is None:
            raise StoreError(f"camera '{camera_key}' is not registered")
        try:
            with self._txn(c) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO auth_latches(endpoint_identity, reason, "
                    "latched_at_utc) VALUES(?, ?, ?)",
                    (identity, reason, ts_utc),
                )
        except sqlite3.Error as exc:
            raise StoreError(f"auth latch failed: {exc}") from exc

    # ---------------------------------------------------------------- events

    def _append_event(
        self,
        conn: sqlite3.Connection,
        kind: str,
        ts_utc: float,
        *,
        camera: str | None = None,
        incident_id: str | None = None,
        attempt_id: str | None = None,
        reason: str | None = None,
        detail: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO events(ts_utc, kind, camera, incident_id, attempt_id, reason, detail) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (ts_utc, kind, camera, incident_id, attempt_id, reason, detail),
        )
        # Bounded history; safety tables are never pruned here.
        conn.execute(
            "DELETE FROM events WHERE event_id <= "
            "(SELECT COALESCE(MAX(event_id), 0) - ? FROM events)",
            (HISTORY_MAX_EVENTS,),
        )

    def record_event(
        self,
        kind: str,
        ts_utc: float,
        *,
        camera: str | None = None,
        incident_id: str | None = None,
        attempt_id: str | None = None,
        reason: str | None = None,
        detail: str | None = None,
    ) -> None:
        c = self._require_open()
        try:
            with self._txn(c) as conn:
                self._append_event(
                    conn,
                    kind,
                    ts_utc,
                    camera=camera,
                    incident_id=incident_id,
                    attempt_id=attempt_id,
                    reason=reason,
                    detail=detail,
                )
        except sqlite3.Error as exc:
            raise StoreError(f"event recording failed: {exc}") from exc

    def history(self, after_id: int = 0, limit: int = 100) -> list[HistoryRow]:
        limit = max(1, min(limit, 1000))
        rows = (
            self._require_open()
            .execute(
                "SELECT event_id, ts_utc, kind, camera, incident_id, attempt_id, reason, detail "
                "FROM events WHERE event_id > ? ORDER BY event_id ASC LIMIT ?",
                (after_id, limit),
            )
            .fetchall()
        )
        return [HistoryRow(*row) for row in rows]

    # ---------------------------------------------------------------- status

    def status(self) -> dict[str, Any]:
        c = self._require_open()
        incidents = c.execute(
            "SELECT COUNT(*) FROM incidents WHERE state IN ('open', 'latched')"
        ).fetchone()[0]
        attempts = c.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
        unresolved = c.execute("SELECT COUNT(*) FROM attempts WHERE outcome IS NULL").fetchone()[0]
        events = c.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return {
            "installation_id": self.installation_id,
            "open_or_latched_incidents": incidents,
            "attempts_total": attempts,
            "attempts_unresolved": unresolved,
            "events": events,
            "runtime_acc": self._acc,
        }
