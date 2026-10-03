"""The serialized coordinator.

One asyncio task owns all decisions and action reservations. Cameras never
decide to reboot themselves in concurrent tasks. The dispatch sequence is:

    recheck eligibility (fresh fetch)
    -> authenticated ONVIF preflight (read-only)
    -> recheck again with preflight result
    -> durable reservation (single SQLite transaction)
    -> exactly one SystemReboot send
    -> outcome classification recorded durably

If the reservation cannot be committed, nothing is sent. If anything breaks
between reservation and send, the attempt stays consumed and the outcome is
reported as unknown after restart — never replayed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from pathlib import Path
from typing import Any

from . import __version__
from .config import WatchdogConfig
from .frigate import FrigateClient
from .http_api import HealthSnapshot, start_server
from .mqtt import MqttReporter
from .observations import FrigateNormalizer
from .onvif import OnvifClient
from .policy import DecisionEngine, Event, ProposedAction, StoreOp, StoreView
from .reporting import emit
from .store import Store, StoreError, endpoint_identity, frigate_identity

logger = logging.getLogger("frigate_watchdog.service")

# Events worth persisting to the bounded history (state changes and actions,
# not every inhibition flutter). incident_opened, action_reserved, and
# action_outcome are recorded transactionally by Store.open_incident,
# Store.reserve_attempt, and Store.record_outcome, so persisting them here
# again would duplicate history rows.
_PERSISTED_EVENT_KINDS = frozenset(
    {
        "armed",
        "frames_stopped",
        "frames_restored",
        "multiple_failing",
        "would_recover",
        "recovery_held",
        "auth_latched",
        "frigate_restarted",
        "monitoring_interrupted",
        "inhibited",
        "action_proposed",
        "action_cancelled",
        "incident_latched",
        "incident_resolved",
        "recovery_confirmed",
        "acknowledged",
        "store_unsafe",
    }
)

_CONSOLE_ONLY_KINDS = ("state_changed",)


class _NullStoreView(StoreView):
    """StoreView used when the durable store is unusable: every read raises,
    which drives the engine into STATE_STORE_UNSAFE."""

    def armed(self, camera: str) -> bool:
        raise StoreError("state store unavailable")

    def incident(self, camera: str) -> Any:
        raise StoreError("state store unavailable")

    def accounting(self, camera: str) -> Any:
        raise StoreError("state store unavailable")

    def last_attempt_any(self) -> float | None:
        raise StoreError("state store unavailable")

    def auth_latched(self, camera: str) -> bool:
        raise StoreError("state store unavailable")


class WatchdogService:
    def __init__(
        self,
        config: WatchdogConfig,
        data_dir: Path,
        *,
        shutdown: asyncio.Event | None = None,
    ) -> None:
        self.config = config
        self.data_dir = Path(data_dir)
        self.t = config.timings
        self.shutdown = shutdown or asyncio.Event()
        self.store: Store | None = None
        self.store_error: str | None = None
        self.engine: DecisionEngine | None = None
        self.normalizer = FrigateNormalizer(max_stats_age_s=self.t["max_stats_age_s"])
        self.frigate = FrigateClient(config.frigate, self.t["request_deadline_s"])
        self.health = HealthSnapshot(self.t["poll_interval_s"])
        self.health.instance = config.instance
        self.health.mode = config.mode
        self.reporter = MqttReporter(config.mqtt, config.instance, "pending")
        self._acc_last_mono = time.monotonic()
        self._onvif_clients: dict[str, OnvifClient] = {}
        self._lock_ctx: object | None = None
        self.http_runner: object | None = None

    # ---------------------------------------------------------------- lifecycle

    async def run(self) -> int:
        from .store import StoreLockedError, data_dir_lock

        self.health.last_loop_activity_mono = time.monotonic()
        try:
            lock_ctx = data_dir_lock(self.data_dir)
            lock_ctx.__enter__()
        except StoreLockedError as exc:
            logger.error("%s", exc)
            return 3
        except StoreError as exc:
            logger.error("data directory unusable: %s", exc)
            return 2
        self._lock_ctx = lock_ctx
        try:
            return await self._run_locked()
        finally:
            lock_ctx.__exit__(None, None, None)
            self._lock_ctx = None

    async def _run_locked(self) -> int:
        exit_code = 0
        try:
            store = await asyncio.to_thread(self._open_store, self.data_dir)
        except StoreError as exc:
            logger.error("state store error: %s", exc)
            self.health.store_ok = False
            self.health.store_error = str(exc)
            self.store_error = str(exc)
            store = None
        if store is not None:
            self.store = store
            for key, cam_cfg in self.config.cameras.items():
                try:
                    store.register_camera(key, cam_cfg.frigate_name, self._identity_of(key))
                except StoreError as exc:
                    logger.error("camera registration failed for %s: %s", key, exc)
                    self.health.store_ok = False
                    self.health.store_error = str(exc)
                    self.store_error = str(exc)
                    store.close()
                    self.store = None
                    break
            if self.store is not None:
                self.reporter = MqttReporter(
                    config=self.config.mqtt,
                    instance=self.config.instance,
                    installation_id=store.installation_id,
                )
            self.engine = DecisionEngine(
                self.config, store, start_mono=time.monotonic(), start_utc=time.time()
            )
            logger.info(
                "starting frigate-watchdog %s in %s mode; %d camera(s) configured",
                __version__,
                self.config.mode,
                len(self.config.cameras),
            )
        else:
            self.engine = DecisionEngine(
                self.config, _NullStoreView(), start_mono=time.monotonic(), start_utc=time.time()
            )
            logger.error(
                "running without durable state: automatic recovery is inhibited; "
                "fix the state store and restart"
            )

        await self.frigate.start()
        self.reporter.start()

        if self.config.http.enabled:
            self.http_runner = await start_server(
                self.health,
                lambda: self.engine,
                self.store,
                self.config.http.host,
                self.config.http.port,
            )
            bound = getattr(self.http_runner, "addresses", [])
            port = self.config.http.port
            for addr in bound:
                if isinstance(addr, tuple) and len(addr) == 2:
                    port = addr[1]
                    break
            self.health.http_port = port
            logger.info("http status on http://%s:%d/health", self.config.http.host, port)

        loop = asyncio.create_task(self._run_loop(), name="coordinator")
        with contextlib.suppress(NotImplementedError):
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_running_loop().add_signal_handler(sig, self.shutdown.set)
        try:
            await self.shutdown.wait()
        finally:
            self.health.loop_stopped = True
            loop.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await loop
            await self.reporter.stop()
            await self.frigate.close()
            for client in self._onvif_clients.values():
                await client.close()
            self._onvif_clients.clear()
            if self.http_runner is not None:
                cleanup = getattr(self.http_runner, "cleanup", None)
                if cleanup is not None:
                    await cleanup()
            if self.store is not None:
                self.store.close()
        return exit_code

    @staticmethod
    def _open_store(data_dir: Path) -> Store:
        store = Store(data_dir / "state.db")
        store.open()
        return store

    # ---------------------------------------------------------------- main loop

    async def _run_loop(self) -> None:
        poll_s = self.t["poll_interval_s"]
        assert self.engine is not None
        try:
            while not self.shutdown.is_set():
                started = time.monotonic()
                try:
                    await self._iterate()
                except Exception:
                    # The loop keeps running, but /health must not keep
                    # reporting alive while every iteration fails internally.
                    self.health.consecutive_iteration_failures += 1
                    logger.exception("coordinator iteration failed")
                else:
                    self.health.consecutive_iteration_failures = 0
                self.health.last_loop_activity_mono = time.monotonic()
                elapsed = time.monotonic() - started
                await asyncio.sleep(max(0.2, poll_s - elapsed))
        except asyncio.CancelledError:
            raise
        finally:
            self.health.loop_stopped = True

    async def _iterate(self) -> None:
        now_mono = time.monotonic()
        await self._accrue_runtime(now_mono)
        await self._poll_and_process(now_mono)
        # time-driven transitions (gaps, boot-grace expiry)
        if self.engine is not None:
            decision = self.engine.on_tick(time.monotonic(), time.time())
            self._handle_events(decision.events, [])
        self._publish_state()

    async def _accrue_runtime(self, now_mono: float) -> None:
        if self.store is None:
            return
        elapsed = now_mono - self._acc_last_mono
        self._acc_last_mono = now_mono
        if elapsed > 0:
            try:
                await asyncio.to_thread(self.store.accrue, elapsed)
            except StoreError as exc:
                await self._store_failed(exc)

    async def _poll_and_process(self, now_mono: float) -> None:
        fetch = await self.frigate.fetch_observations()
        norm = self.normalizer.normalize(
            received_at=time.monotonic(),
            received_at_utc=time.time(),
            stats=fetch.stats,
            enabled_map=fetch.enabled_map,
            cameras_cfg=self.config.cameras,
            problem=fetch.problem,
        )
        assert self.engine is not None
        decision = self.engine.on_snapshot(norm.snapshot, time.monotonic(), self._now_acc())
        self._handle_events(decision.events, decision.store_ops)
        if decision.proposal is not None:
            await self._dispatch(decision.proposal)

    # ---------------------------------------------------------------- dispatch

    async def _dispatch(self, proposal: ProposedAction) -> None:
        camera = proposal.camera
        cam_cfg = self.config.cameras.get(camera)
        if cam_cfg is None or cam_cfg.onvif is None:
            return  # impossible: guards prevent this
        assert self.engine is not None

        # 1. Fetch fresh stats and effective camera state again.
        fetch = await self.frigate.fetch_observations()
        norm = self.normalizer.normalize(
            received_at=time.monotonic(),
            received_at_utc=time.time(),
            stats=fetch.stats,
            enabled_map=fetch.enabled_map,
            cameras_cfg=self.config.cameras,
            problem=fetch.problem,
        )
        decision = self.engine.on_snapshot(norm.snapshot, time.monotonic(), self._now_acc())
        self._handle_events(decision.events, decision.store_ops)
        if self.engine.in_flight is None or self.engine.in_flight.camera != camera:
            return  # re-evaluation cancelled the proposal; nothing sent

        # 2. Bounded read-only preflight against the configured target.
        client = await self._onvif_client(camera)
        probe = await client.probe()
        if not probe.ok:
            decision = self.engine.on_preflight_failed(
                camera, probe.outcome, time.monotonic(), time.time()
            )
            self._handle_events(decision.events, decision.store_ops)
            return

        # 3. Final eligibility recheck with the preflight result.
        check = self.engine.evaluate_action(
            camera,
            snapshot=self.engine.last_snapshot,
            now_mono=time.monotonic(),
            now_acc=self._now_acc(),
            preflight_ok=True,
            for_dispatch=True,
        )
        if not check.permitted:
            from .policy import GuardFailure

            decision = self.engine.cancel_proposed(
                camera,
                check.failures[0] if check.failures else GuardFailure("RECHECK_FAILED", ""),
                time.monotonic(),
                time.time(),
            )
            self._handle_events(decision.events, decision.store_ops)
            return

        # 4. Durable reservation. If this cannot commit, nothing is sent.
        if self.store is None:
            self.engine.on_store_failure(time.monotonic(), time.time())
            return
        identity = endpoint_identity(
            cam_cfg.onvif.endpoint.scheme,
            cam_cfg.onvif.endpoint.host,
            cam_cfg.onvif.endpoint.port,
            cam_cfg.onvif.endpoint.path,
        )
        try:
            attempt_id = await asyncio.to_thread(
                self.store.reserve_attempt,
                camera,
                cam_cfg.frigate_name,
                identity,
                time.time(),
            )
        except StoreError as exc:
            await self._store_failed(exc)
            return
        decision = self.engine.on_reservation_recorded(
            camera, attempt_id, time.monotonic(), time.time()
        )
        self._handle_events(decision.events, decision.store_ops)

        # 5. Exactly one send.
        outcome = await client.reboot()

        # 6. Record the response classification durably.
        try:
            await asyncio.to_thread(
                self.store.record_outcome, camera, attempt_id, outcome.outcome, time.time()
            )
        except StoreError as exc:
            await self._store_failed(exc)
            # The send happened; the engine must still see the outcome.
        decision = self.engine.on_outcome(
            camera, attempt_id, outcome.outcome, time.monotonic(), time.time()
        )
        self._handle_events(decision.events, decision.store_ops)

    async def _onvif_client(self, camera: str) -> OnvifClient:
        cam_cfg = self.config.cameras[camera]
        assert cam_cfg.onvif is not None
        client = self._onvif_clients.get(camera)
        if client is None:
            client = OnvifClient(cam_cfg.onvif)
            self._onvif_clients[camera] = client
        return client

    # ---------------------------------------------------------------- plumbing

    def _now_acc(self) -> float:
        return self.store.now_acc if self.store is not None else 0.0

    def _handle_events(self, events: list[Event], store_ops: list[StoreOp]) -> None:
        assert self.engine is not None
        for event in events:
            emit(event)
            self.reporter.publish_event(event)
            if event.kind in _PERSISTED_EVENT_KINDS and self.store is not None:
                try:
                    self.store.record_event(
                        event.kind,
                        event.ts_utc,
                        camera=event.camera,
                        incident_id=event.incident_id,
                        attempt_id=event.attempt_id,
                        reason=event.reason,
                        detail=event.detail,
                    )
                except StoreError as exc:
                    logger.error("event persistence failed: %s", exc)
        for op in store_ops:
            self._apply_store_op(op)

    def _apply_store_op(self, op: StoreOp) -> None:
        assert self.engine is not None
        if self.store is None:
            self.engine.on_store_op_failed(op, time.monotonic(), time.time())
            return
        try:
            if op.op == "mark_armed":
                cam_cfg = self.config.cameras[op.camera]
                identity = self._identity_of(op.camera)
                self.store.mark_armed(op.camera, cam_cfg.frigate_name, identity, time.time())
            elif op.op == "open_incident":
                cam_cfg = self.config.cameras[op.camera]
                identity = self._identity_of(op.camera)
                incident_id = self.store.open_incident(
                    op.camera, cam_cfg.frigate_name, identity, time.time()
                )
                d = self.engine.on_incident_opened(
                    op.camera, incident_id, time.monotonic(), time.time()
                )
                self._handle_events(d.events, d.store_ops)
            elif op.op == "resolve_incident":
                self.store.resolve_incident(op.camera, time.time())
            elif op.op == "auth_latch":
                self.store.auth_latch(op.camera, "AUTH", time.time())
            elif op.op == "latch_incident":
                self.store.latch_incident(op.camera, time.time())
        except StoreError as exc:
            d = self.engine.on_store_op_failed(op, time.monotonic(), time.time())
            self._handle_events(d.events, d.store_ops)
            logger.error("store operation %s failed: %s", op.op, exc)

    def _identity_of(self, camera: str) -> str:
        cam_cfg = self.config.cameras[camera]
        if cam_cfg.onvif is not None:
            ep = cam_cfg.onvif.endpoint
            return endpoint_identity(ep.scheme, ep.host, ep.port, ep.path)
        return frigate_identity(cam_cfg.frigate_name)

    async def _store_failed(self, exc: StoreError) -> None:
        logger.error("state store failure: %s", exc)
        self.health.store_ok = False
        self.health.store_error = str(exc)
        assert self.engine is not None
        d = self.engine.on_store_failure(time.monotonic(), time.time())
        self._handle_events(d.events, d.store_ops)

    def _publish_state(self) -> None:
        assert self.engine is not None
        if not self.config.mqtt.enabled:
            return
        summaries = self.engine.camera_summaries(time.monotonic())
        self.reporter.publish_state(
            {
                "mode": self.config.mode,
                "global_state": self.engine.global_state.value,
                "inhibition_reasons": self.engine.global_inhibition_reasons(time.monotonic()),
                "cameras": {k: v["state"] for k, v in summaries.items()},
            }
        )


async def run_service(
    config: WatchdogConfig,
    data_dir: Path,
    *,
    shutdown: asyncio.Event | None = None,
) -> int:
    """CLI entry point: run the appliance under the exclusive data-dir lock."""
    service = WatchdogService(config, data_dir, shutdown=shutdown)
    return await service.run()
