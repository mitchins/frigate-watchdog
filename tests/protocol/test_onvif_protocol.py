"""Protocol tests for the ONVIF adapter against a real local SOAP server.

The centerpiece is the "accepted reboot then disconnected" scenario: with the
full zeep + WSSE stack active, the fake must receive exactly ONE
SystemReboot, and the client must classify the outcome as OUTCOME_UNKNOWN.
"""

from __future__ import annotations

import pytest

from frigate_watchdog.config import OnvifConfig, Secret
from frigate_watchdog.onvif import OnvifClient
from tests.fakes.onvif_fake import FakeOnvifCamera, FakeOnvifState


def onvif_config(endpoint: str, password: str = "cam-pass") -> OnvifConfig:
    from urllib.parse import urlsplit

    from frigate_watchdog.config import OnvifEndpoint

    parts = urlsplit(endpoint)
    host = parts.hostname or "127.0.0.1"
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return OnvifConfig(
        endpoint=OnvifEndpoint(
            raw=endpoint, scheme=parts.scheme, host=host, port=port, path=parts.path or "/"
        ),
        username="admin",
        password_env="CAM_PASSWORD",
        password=Secret("CAM_PASSWORD", password),
    )


@pytest.fixture()
async def camera():
    cam = FakeOnvifCamera()
    endpoint = await cam.start()
    yield cam, endpoint
    await cam.stop()


async def test_probe_authenticates_and_reads_device_info(camera):
    cam, endpoint = camera
    client = OnvifClient(onvif_config(endpoint))
    result = await client.probe()
    await client.close()
    assert result.outcome == "OK"
    assert result.device["manufacturer"] == "FakeCam"
    assert cam.state.requests["GetDeviceInformation"] == 1


async def test_reboot_acknowledged_exactly_once(camera):
    cam, endpoint = camera
    client = OnvifClient(onvif_config(endpoint))
    result = await client.reboot()
    await client.close()
    assert result.outcome == "ACKNOWLEDGED"
    assert cam.state.reboots_accepted == 1
    assert cam.state.requests["SystemReboot"] == 1


async def test_accepted_reboot_then_disconnect_is_outcome_unknown():
    """The critical trap: accept the reboot, close without responding."""
    cam = FakeOnvifCamera(FakeOnvifState(behavior="reset_after_accept"))
    endpoint = await cam.start()
    try:
        client = OnvifClient(onvif_config(endpoint))
        result = await client.reboot()
        await client.close()
        assert result.outcome == "OUTCOME_UNKNOWN", (
            "a post-send disconnect must never be classified as safe to retry"
        )
        assert cam.state.reboots_accepted == 1
        assert cam.state.requests["SystemReboot"] == 1
        # No hidden retry happened anywhere in the zeep/httpx stack:
        assert cam.state.total_requests == 1
    finally:
        await cam.stop()


async def test_probe_then_reboot_counts_two_requests_total(camera):
    cam, endpoint = camera
    client = OnvifClient(onvif_config(endpoint))
    await client.probe()
    await client.reboot()
    await client.close()
    assert cam.state.total_requests == 2


async def test_wrong_password_auth_failed_no_retry_storm(camera):
    cam, endpoint = camera
    client = OnvifClient(onvif_config(endpoint, password="wrong"))
    result = await client.probe()
    await client.close()
    assert result.outcome == "AUTH_FAILED"
    assert cam.state.auth_failures >= 1
    assert cam.state.total_requests == 1  # single-shot, even for auth faults
    result2 = OnvifClient(onvif_config(endpoint, password="wrong"))
    r = await result2.probe()
    await result2.close()
    assert r.outcome == "AUTH_FAILED"
    assert cam.state.total_requests == 2


async def test_reboot_unsupported_fault(camera):
    cam, endpoint = camera
    cam.state.behavior = "unsupported"
    client = OnvifClient(onvif_config(endpoint))
    result = await client.reboot()
    await client.close()
    assert result.outcome == "UNSUPPORTED"
    assert cam.state.reboots_accepted == 0  # fault raised before acceptance? no:
    # the fake counts acceptance before checking behavior; assert request count
    assert cam.state.requests["SystemReboot"] == 1


async def test_generic_fault_rejected(camera):
    cam, endpoint = camera
    cam.state.behavior = "fault"
    client = OnvifClient(onvif_config(endpoint))
    result = await client.reboot()
    await client.close()
    assert result.outcome == "REJECTED"


async def test_unreachable_endpoint():
    client = OnvifClient(onvif_config("http://127.0.0.1:9/onvif/device_service"))
    result = await client.reboot()
    await client.close()
    assert result.outcome == "UNREACHABLE"


async def test_slow_response_times_out_to_unknown(camera):
    cam, endpoint = camera
    cam.state.behavior = "slow"
    client = OnvifClient(onvif_config(endpoint), deadline_s=0.5)
    result = await client.reboot()
    await client.close()
    assert result.outcome == "OUTCOME_UNKNOWN"
    assert cam.state.reboots_accepted == 1  # the camera did accept it


async def test_malformed_response_after_reboot_is_unknown(camera):
    cam, endpoint = camera
    cam.state.behavior = "malformed"
    client = OnvifClient(onvif_config(endpoint))
    result = await client.reboot()
    await client.close()
    assert result.outcome == "OUTCOME_UNKNOWN"


async def test_oversized_response_rejected_before_parsing(camera):
    cam, endpoint = camera
    cam.state.behavior = "oversized"
    client = OnvifClient(onvif_config(endpoint))
    result = await client.reboot()
    await client.close()
    assert result.outcome == "OUTCOME_UNKNOWN"


async def test_redirect_never_followed_no_second_request(camera):
    cam, endpoint = camera
    cam.state.behavior = "redirect"
    client = OnvifClient(onvif_config(endpoint))
    result = await client.reboot()
    await client.close()
    assert cam.state.total_requests == 1  # no request to the redirect target
    assert result.outcome in ("OUTCOME_UNKNOWN", "REJECTED", "UNREACHABLE")


async def test_http_401_on_reboot(camera):
    cam, endpoint = camera
    cam.state.behavior = "http_401"
    client = OnvifClient(onvif_config(endpoint))
    result = await client.reboot()
    await client.close()
    # non-SOAP transport-level rejection; zeep raises TransportError
    assert result.outcome in ("REJECTED", "OUTCOME_UNKNOWN", "UNREACHABLE")


async def test_get_system_date_and_time(camera):
    _cam, endpoint = camera
    client = OnvifClient(onvif_config(endpoint))
    result = await client.get_system_date_and_time()
    await client.close()
    assert result.outcome == "OK"
    # An actual timestamp, not merely the "no clock" fallback text.
    assert result.detail.startswith("camera clock 20"), result.detail


async def test_dtd_entity_payload_rejected_at_parse(camera):
    """Entity/DTD tricks in a response body must not be expanded."""
    from aiohttp import web as aioweb

    evil = (
        '<?xml version="1.0"?><!DOCTYPE envelope ['
        '<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        "<soap12:Envelope xmlns:soap12='http://www.w3.org/2003/05/soap-envelope'>"
        "<soap12:Body><tds:SystemRebootResponse xmlns:tds="
        "'http://www.onvif.org/ver10/device/wsdl'>"
        "<tds:Message>&xxe;</tds:Message>"
        "</tds:SystemRebootResponse></soap12:Body></soap12:Envelope>"
    )

    from tests.fakes.onvif_fake import FakeOnvifState

    cam = FakeOnvifCamera(FakeOnvifState())
    await cam.start()

    async def evil_handler(request):
        cam.state.requests["SystemReboot"] = cam.state.requests.get("SystemReboot", 0) + 1
        return aioweb.Response(text=evil, content_type="application/soap+xml")

    # route via a second app on another port
    app = aioweb.Application()
    app.router.add_post("/onvif/device_service", evil_handler)
    runner = aioweb.AppRunner(app)
    await runner.setup()
    site = aioweb.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        client = OnvifClient(onvif_config(f"http://127.0.0.1:{port}/onvif/device_service"))
        result = await client.reboot()
        await client.close()
        assert result.outcome == "OUTCOME_UNKNOWN"  # parse refused, delivery ambiguous
        assert "passwd" not in result.detail  # no expansion, no leakage
    finally:
        await runner.cleanup()
        await cam.stop()
