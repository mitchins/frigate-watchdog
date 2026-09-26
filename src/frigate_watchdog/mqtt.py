"""Optional outbound MQTT notifications.

Output only: no subscriptions, no command topics, no second decision input.
MQTT failure never blocks monitoring, clears a latch, or causes a reboot.

* ``<prefix>/<instance>/availability`` — retained online/offline with LWT
* ``<prefix>/<instance>/state`` — retained current summary (with timestamp,
  so consumers can see staleness)
* ``<prefix>/<instance>/events`` — non-retained event JSON with stable ids
  (duplicate delivery is possible; SQLite history stays authoritative)

A bounded queue absorbs events while disconnected; overflow is counted and
reported in the next state publish.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Any

import aiomqtt

from .config import MqttConfig
from .policy import Event

logger = logging.getLogger("frigate_watchdog.mqtt")

QUEUE_LIMIT = 100
RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 60.0


class MqttReporter:
    def __init__(self, config: MqttConfig, instance: str, installation_id: str) -> None:
        self.config = config
        self.instance = instance
        self.client_id = f"frigate-watchdog-{instance}-{installation_id[:8]}"
        base = f"{config.topic_prefix}/{instance}"
        self.topic_availability = f"{base}/availability"
        self.topic_state = f"{base}/state"
        self.topic_events = f"{base}/events"
        self.queue: asyncio.Queue[Event | dict[str, Any]] = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self.dropped_events = 0
        self.published_events = 0
        # Stable ids per logical event (same object -> same id), so broker
        # redelivery and deliberate republication stay identifiable. The event
        # itself is retained so CPython can never reuse its id() address while
        # the entry exists.
        self._event_ids: dict[int, tuple[Event, str]] = {}
        self._event_id_counter = 0
        self.connected = False
        self.last_error: str | None = None
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    # ---- producer side (called from the coordinator) ----

    def publish_event(self, event: Event) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped_events += 1

    def publish_state(self, state: dict[str, Any]) -> None:
        """Replace any queued state with the latest one."""
        kept: list[Event] = []
        while True:
            try:
                item = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, Event):
                kept.append(item)
        for item in kept:
            with contextlib.suppress(asyncio.QueueFull):
                self.queue.put_nowait(item)
        try:
            self.queue.put_nowait(state)
        except asyncio.QueueFull:
            self.dropped_events += 1

    def start(self) -> None:
        if self.config.enabled:
            self._task = asyncio.create_task(self._run(), name="mqtt-reporter")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(self._task, timeout=5)
            if not self._task.done():
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                    await asyncio.wait_for(self._task, timeout=5)
            self._task = None

    # ---- consumer side ----

    async def _run(self) -> None:
        backoff = RECONNECT_MIN_S
        while not self._stop.is_set():
            try:
                async with aiomqtt.Client(
                    hostname=self.config.host,
                    port=self.config.port,
                    username=self.config.username,
                    password=self.config.password.value if self.config.password else None,
                    identifier=self.client_id,
                    will=aiomqtt.Will(
                        topic=self.topic_availability,
                        payload=b"offline",
                        qos=1,
                        retain=True,
                    ),
                ) as client:
                    self.connected = True
                    self.last_error = None
                    backoff = RECONNECT_MIN_S
                    await client.publish(self.topic_availability, b"online", qos=1, retain=True)
                    await self._drain(client)
                self.connected = False
            except (asyncio.CancelledError, TimeoutError):
                self.connected = False
                raise
            except Exception as exc:  # reconnect on anything
                self.connected = False
                self.last_error = type(exc).__name__
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, RECONNECT_MAX_S)

    async def _drain(self, client: aiomqtt.Client) -> None:
        while not self._stop.is_set():
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            await self._publish_item(client, item)

    async def _publish_item(self, client: aiomqtt.Client, item: Event | dict[str, Any]) -> None:
        try:
            if isinstance(item, Event):
                payload = item.as_dict()
                key = id(item)
                cached = self._event_ids.get(key)
                if cached is not None and cached[0] is item:
                    event_id = cached[1]
                else:
                    self._event_id_counter += 1
                    event_id = f"{self.instance}-ev{self._event_id_counter}"
                    while len(self._event_ids) >= 1000:
                        # Evict oldest; the strong ref goes with it.
                        self._event_ids.pop(next(iter(self._event_ids)))
                    self._event_ids[key] = (item, event_id)
                payload["id"] = event_id
                await client.publish(
                    self.topic_events, json.dumps(payload, default=str).encode(), qos=1
                )
                self.published_events += 1
            else:
                body = dict(item)
                body["ts_utc"] = time.time()
                body["dropped_events"] = self.dropped_events
                await client.publish(
                    self.topic_state, json.dumps(body, default=str).encode(), qos=1, retain=True
                )
        except aiomqtt.MqttError:
            raise
        except (asyncio.CancelledError, TimeoutError):
            raise
        except Exception:  # pragma: no cover - defensive
            logger.debug("mqtt publish failed", exc_info=True)
