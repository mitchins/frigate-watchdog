"""Container-level qualification choreography.

Runs INSIDE the test network alongside the built watchdog image:

1. fake Frigate + two fake ONVIF cameras (real HTTP/SOAP) on this container
2. watch the watchdog's status API through a full recovery cycle
3. assert exactly one reboot, correct outcome classification, group
   inhibition, history sanity, and MQTT availability/state/events

Exit code 0 = qualified.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

import aiohttp

sys.path.insert(0, "/repo")

from tests.fakes.frigate_fake import FakeFrigate, FakeFrigateState
from tests.fakes.onvif_fake import FakeOnvifCamera, FakeOnvifState

WATCHDOG = "http://watchdog:8080"
MQTT_HOST = "mosquitto"
TIMEOUT_S = 420.0


def log(message: str) -> None:
    print(f"[qualify] {message}", flush=True)


async def get_json(session: aiohttp.ClientSession, path: str) -> dict:
    async with session.get(f"{WATCHDOG}{path}", timeout=aiohttp.ClientTimeout(total=10)) as r:
        r.raise_for_status()
        return await r.json()


async def wait_for(check, description: str, timeout: float = TIMEOUT_S) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            if await check():
                log(f"OK: {description}")
                return
        except Exception as exc:
            last_error = str(exc)
        await asyncio.sleep(2)
    raise SystemExit(f"FAIL: {description} (last error: {last_error})")


class MqttCollector:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes]] = []

    async def run(self) -> None:
        import aiomqtt

        backoff = 1.0
        while True:
            try:
                async with aiomqtt.Client(hostname=MQTT_HOST, port=1883) as client:
                    backoff = 1.0
                    await client.subscribe("frigate-watchdog/test/#")
                    log("MQTT subscriber connected")
                    async for message in client.messages:
                        topic = str(message.topic)
                        self.messages.append((topic, bytes(message.payload)))
                        log(f"MQTT message on {topic}")
            except Exception as exc:
                log(f"MQTT subscriber error: {type(exc).__name__}: {exc}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


async def main() -> None:
    frigate = FakeFrigate(
        FakeFrigateState(
            cameras={"Porch": 5.0, "Driveway": 5.0, "Doorbell": 5.0},
            enabled={"Porch": True, "Driveway": True, "Doorbell": True},
        )
    )
    await frigate.start("0.0.0.0", 5000)
    porch_cam = FakeOnvifCamera(FakeOnvifState(password="test-porch-pass", reboot_delay_s=5.0))
    porch_ep = await porch_cam.start("0.0.0.0", 8000)
    drive_cam = FakeOnvifCamera(FakeOnvifState(password="test-drive-pass"))
    drive_ep = await drive_cam.start("0.0.0.0", 8001)
    log(f"fakes up: frigate=0.0.0.0:5000 porch={porch_ep} driveway={drive_ep}")

    collector = MqttCollector()
    mqtt_task = asyncio.create_task(collector.run())

    async with aiohttp.ClientSession() as session:
        # 1. process health
        async def alive() -> bool:
            payload = await get_json(session, "/health")
            return payload["status"] == "alive"

        await wait_for(alive, "watchdog /health alive")

        # 2. all cameras armed (startup grace + baseline)
        async def armed() -> bool:
            stats = await get_json(session, "/stats")
            return all(cam["armed"] for cam in stats["cameras"].values())

        await wait_for(armed, "all cameras armed (baseline established)")

        # 3. single-camera outage -> exactly one reboot
        frigate.state.cameras["Porch"] = 0.0

        async def one_reboot() -> bool:
            return porch_cam.state.reboots_accepted == 1

        await wait_for(one_reboot, "exactly one SystemReboot delivered to porch camera")

        async def outcome_recorded() -> bool:
            stats = await get_json(session, "/stats")
            return stats["cameras"]["porch"]["last_outcome"] == "ACKNOWLEDGED"

        await wait_for(outcome_recorded, "ACKNOWLEDGED outcome recorded")

        # camera reboots (fake delay) -> frames return
        await asyncio.sleep(8)
        frigate.state.cameras["Porch"] = 5.0

        async def recovered() -> bool:
            stats = await get_json(session, "/stats")
            return stats["cameras"]["porch"]["state"] in ("RECOVERED", "HEALTHY")

        await wait_for(recovered, "recovery confirmed after frames returned")

        async def no_extra_reboots() -> bool:
            return porch_cam.state.reboots_accepted == 1 and porch_cam.state.total_requests == 2

        await wait_for(no_extra_reboots, "no second reboot or hidden retry (2 requests total)")

        # 4. group inhibition: two simultaneous failures, no reboots
        frigate.state.cameras["Porch"] = 0.0
        frigate.state.cameras["Driveway"] = 0.0

        async def group_inhibited() -> bool:
            stats = await get_json(session, "/stats")
            return "MULTIPLE_CAMERAS_FAILING" in stats["inhibition_reasons"]

        await wait_for(group_inhibited, "group inhibition reported for two failed cameras")
        await asyncio.sleep(20)
        assert porch_cam.state.reboots_accepted == 1, "no reboots while group-inhibited"
        assert drive_cam.state.reboots_accepted == 0, "driveway must not reboot in a group outage"

        # 5. history is bounded, sanitized, and carries the incident
        history = await get_json(session, "/history?limit=200")
        kinds = [e["kind"] for e in history["events"]]
        for expected in ("action_reserved", "action_outcome", "recovery_confirmed"):
            assert expected in kinds, f"history missing {expected}: {kinds}"
        blob = json.dumps(history)
        assert "test-porch-pass" not in blob, "porch credential leaked into history"
        assert "test-drive-pass" not in blob, "driveway credential leaked into history"

        # 6. MQTT outputs (availability/state/events)
        await asyncio.sleep(5)

        def mqtt_ok() -> bool:
            topics = [t for t, _ in collector.messages]
            return (
                "frigate-watchdog/test/availability" in topics
                and "frigate-watchdog/test/state" in topics
                and any(t.endswith("/events") for t in topics)
            )

        def _mqtt_ok_sync() -> bool:
            return mqtt_ok()

        async def mqtt_check() -> bool:
            return _mqtt_ok_sync()

        await wait_for(mqtt_check, "MQTT availability, state, and events observed")
        state_msgs = [json.loads(p) for t, p in collector.messages if t.endswith("/state")]
        assert all("ts_utc" in m for m in state_msgs), "retained state must carry timestamps"

    log("QUALIFIED: all container-level checks passed")
    mqtt_task.cancel()
    await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main())
