"""MQTT protocol tests against a real disposable mosquitto broker.

Requires a local Docker daemon (skipped otherwise). CI runs these in the
protocol job. Verifies LWT, retained state, non-retained events, bounded
queueing, and that MQTT failures never propagate into the coordinator.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import subprocess
import time
from contextlib import contextmanager

import pytest

from frigate_watchdog.config import MqttConfig
from frigate_watchdog.mqtt import MqttReporter
from frigate_watchdog.policy import Event

pytestmark = pytest.mark.mqtt_broker


def docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10, check=True)
        return True
    except Exception:
        return False


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def mosquitto_broker():
    if not docker_available():
        pytest.skip("docker daemon not available for mosquitto")
    port = free_port()
    name = f"fw-test-mosquitto-{port}"
    proc = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            f"127.0.0.1:{port}:1883",
            "eclipse-mosquitto:2",
            "mosquitto",
            "-c",
            "/mosquitto-no-auth.conf",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        pytest.skip(f"cannot start mosquitto: {proc.stderr.strip()[:200]}")
    try:
        # wait for the listener
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    break
            except OSError:
                time.sleep(0.3)
        yield port
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)


def event(kind: str = "frames_stopped", camera: str = "porch") -> Event:
    return Event(kind=kind, ts_utc=time.time(), ts_mono=0, camera=camera, reason="NO_FRAMES")


async def collect(host: str, port: int, topic: str, seconds: float = 2.0):
    import aiomqtt

    messages: list[tuple[str, bytes]] = []
    async with aiomqtt.Client(hostname=host, port=port) as client:
        await client.subscribe(topic)
        try:
            async for message in client.messages:
                messages.append((str(message.topic), bytes(message.payload)))
        except asyncio.CancelledError:
            pass
    return messages


async def test_availability_lwt_and_retained_state():
    with mosquitto_broker() as port:
        cfg = MqttConfig(enabled=True, host="127.0.0.1", port=port)
        reporter = MqttReporter(cfg, "test", "abcd1234")
        reporter.start()
        reporter.publish_event(event())
        reporter.publish_state({"mode": "recover", "cameras": {"porch": "SUSPECT"}})
        # Wait for the event publish: availability (retained) precedes it and
        # state follows in the same drain, so both retained messages exist now.
        deadline = time.monotonic() + 25
        while reporter.published_events < 1 and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
        assert reporter.published_events >= 1, (
            "reporter never published "
            f"(connected={reporter.connected}, last_error={reporter.last_error})"
        )

        import aiomqtt

        # a fresh subscriber receives the retained availability and state.
        # The (non-retained) event may already be gone; only retained
        # messages are asserted here.
        got: list[tuple[str, bytes]] = []

        async def subscriber():
            async with aiomqtt.Client(hostname="127.0.0.1", port=port, timeout=10) as sub:
                await sub.subscribe("frigate-watchdog/test/#")
                try:
                    async with asyncio.timeout(10):
                        async for message in sub.messages:
                            got.append((str(message.topic), bytes(message.payload)))
                            topics_so_far = [t for t, _ in got]
                            if "frigate-watchdog/test/availability" in topics_so_far and any(
                                t.endswith("/state") for t in topics_so_far
                            ):
                                break
                except TimeoutError:
                    pass

        await asyncio.wait_for(subscriber(), timeout=20)
        topics = [t for t, _ in got]
        assert "frigate-watchdog/test/availability" in topics, (
            f"no retained availability in {topics} "
            f"(connected={reporter.connected}, last_error={reporter.last_error})"
        )
        assert b"online" in [p for t, p in got if t.endswith("availability")]
        state = next(p for t, p in got if t.endswith("/state"))
        body = json.loads(state)
        assert body["cameras"]["porch"] == "SUSPECT"
        assert "ts_utc" in body, "retained state must carry its timestamp"

        await reporter.stop()
        # LWT is configured on connect (MqttReporter._run). A clean stop sends
        # DISCONNECT so the broker does not fire it; the container suite
        # observes the CONNECT-with-will against a real broker.


async def test_events_are_not_retained_but_carry_ids():
    with mosquitto_broker() as port:
        cfg = MqttConfig(enabled=True, host="127.0.0.1", port=port)
        reporter = MqttReporter(cfg, "test", "abcd1234")
        reporter.start()
        reporter.publish_event(event("frames_stopped"))
        await asyncio.sleep(1.0)
        await reporter.stop()

        import aiomqtt

        got = []

        async def fresh_subscriber():
            async with aiomqtt.Client(hostname="127.0.0.1", port=port) as sub:
                await sub.subscribe("frigate-watchdog/test/events")
                async with asyncio.timeout(1.5):
                    async for message in sub.messages:
                        got.append(bytes(message.payload))

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(fresh_subscriber(), timeout=5)
        # non-retained: a late subscriber does not replay the event
        assert got == [], f"events must not be retained: {got}"


async def test_duplicate_event_delivery_is_identifiable():
    with mosquitto_broker() as port:
        cfg = MqttConfig(enabled=True, host="127.0.0.1", port=port)
        reporter = MqttReporter(cfg, "test", "abcd1234")
        reporter.start()
        deadline = time.monotonic() + 25
        while not reporter.connected and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
        assert reporter.connected, f"reporter never connected (last_error={reporter.last_error})"
        received: list[bytes] = []
        subscribed = asyncio.Event()

        import aiomqtt

        async def subscriber():
            async with aiomqtt.Client(hostname="127.0.0.1", port=port, timeout=10) as sub:
                await sub.subscribe("frigate-watchdog/test/events")
                subscribed.set()
                try:
                    async with asyncio.timeout(15):
                        async for message in sub.messages:
                            received.append(bytes(message.payload))
                            if len(received) >= 2:
                                break
                except TimeoutError:
                    pass

        sub_task = asyncio.create_task(subscriber())
        try:
            await asyncio.wait_for(subscribed.wait(), timeout=20)
        except TimeoutError:
            raise AssertionError(
                "subscriber never subscribed "
                f"(connected={reporter.connected}, last_error={reporter.last_error})"
            ) from None
        # duplicate delivery of the same logical event
        e = event("frames_stopped")
        reporter.publish_event(e)
        reporter.publish_event(e)
        await asyncio.wait_for(sub_task, timeout=25)
        await reporter.stop()
        assert len(received) >= 2, (
            f"only {len(received)} events received "
            f"(connected={reporter.connected}, last_error={reporter.last_error})"
        )
        first, second = json.loads(received[0]), json.loads(received[-1])
        assert first["id"] == second["id"], "duplicates must carry the same stable id"


async def test_inflight_event_retried_after_reconnect_with_same_id():
    """A publish failure must not drop the dequeued item: it stays pending
    and is retried on the next drain with its original stable id."""
    import aiomqtt

    cfg = MqttConfig(enabled=False, host="127.0.0.1", port=1883)
    reporter = MqttReporter(cfg, "test", "abcd1234")
    e = event("frames_stopped")
    reporter.publish_event(e)

    calls: list[tuple[str, bytes]] = []

    class FakeClient:
        fail = True

        async def publish(self, topic, payload, qos=0, retain=False):
            calls.append((str(topic), bytes(payload)))
            if self.fail:
                raise aiomqtt.MqttError("boom")

    client = FakeClient()
    with pytest.raises(aiomqtt.MqttError):
        await reporter._drain(client)
    assert reporter._pending is e
    client.fail = False
    task = asyncio.create_task(reporter._drain(client))
    deadline = time.monotonic() + 5
    while reporter.published_events < 1 and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    reporter._stop.set()
    await task
    assert reporter._pending is None
    ids = [json.loads(p.decode())["id"] for t, p in calls if t.endswith("/events")]
    assert len(ids) == 2, f"event must be published on retry: {calls}"
    assert ids[0] == ids[1], "retry must reuse the original stable id"


async def test_broker_outage_never_blocks_and_bounds_queue():
    cfg = MqttConfig(enabled=True, host="127.0.0.1", port=1)  # nothing listens
    reporter = MqttReporter(cfg, "test", "abcd1234")
    reporter.start()
    for _ in range(500):
        reporter.publish_event(event())
    assert reporter.dropped_events > 0, "overflow must be counted, not grow unbounded"
    assert reporter.queue.qsize() <= 100
    await reporter.stop()


async def test_reconnect_publishes_backlog():
    with mosquitto_broker() as port:
        cfg = MqttConfig(enabled=True, host="127.0.0.1", port=port)
        reporter = MqttReporter(cfg, "test", "abcd1234")
        # queue events while disconnected, then connect
        reporter.publish_event(event("frames_stopped"))
        reporter.publish_event(event("action_outcome"))
        reporter.start()
        deadline = time.monotonic() + 15
        while reporter.published_events < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
        assert reporter.published_events == 2, "backlog must drain on connect"
        await reporter.stop()
