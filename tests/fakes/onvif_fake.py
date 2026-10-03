"""A real local ONVIF Device Service fake.

It parses actual SOAP envelopes, validates the WS-Security UsernameToken
digest (nonce + created + password), counts SystemReboot requests, and can
accept a reboot then close the connection without responding — the exact
ambiguity the client must never turn into a retry.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from xml.etree import ElementTree as ET

from aiohttp import web

SOAP_ENV = "http://www.w3.org/2003/05/soap-envelope"
WSSE_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
WSU_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
TDS_NS = "http://www.onvif.org/ver10/device/wsdl"
TT_NS = "http://www.onvif.org/ver10/schema"


@dataclass
class FakeOnvifState:
    username: str = "admin"
    password: str = "cam-pass"
    behavior: str = "ok"  # ok | wrong_password | unsupported | fault | reset_after_accept |
    #                       slow | malformed | oversized | redirect | http_401 | drop_silently
    # Shape of the GetSystemDateAndTime reply: "spec" (schema order) or
    # "vatilon" (a real camera's reply: Time before Date, CRLF, tt: elements).
    clock_shape: str = "spec"
    reboot_delay_s: float = 0.0
    requests: dict[str, int] = field(default_factory=dict)
    auth_failures: int = 0
    total_requests: int = 0
    reboots_accepted: int = 0


class FakeOnvifCamera:
    def __init__(self, state: FakeOnvifState | None = None) -> None:
        self.state = state or FakeOnvifState()
        self.runner: web.AppRunner | None = None

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> str:
        app = web.Application()
        app.router.add_post("/onvif/device_service", self._device_service)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, host, port)
        await site.start()
        assert site._server is not None
        sockaddr = site._server.sockets[0].getsockname()
        return f"http://{sockaddr[0]}:{sockaddr[1]}/onvif/device_service"

    @property
    def host(self) -> str:
        url = self._url
        return url.rsplit(":", 1)[0].removeprefix("http://")

    _url: str = ""

    async def start_at(self, host: str, port: int) -> str:
        self._url = await self.start(host, port)
        return self._url

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()

    # ---------------------------------------------------------------- helpers

    def _soap_fault(self, code_ns: str, code: str, subcode: str | None, text: str) -> web.Response:
        sub = ""
        if subcode:
            sub = f"<soap12:Subcode><soap12:Value>{subcode}</soap12:Value></soap12:Subcode>"
        envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap12:Envelope xmlns:soap12="{SOAP_ENV}"
                 xmlns:ter="http://www.onvif.org/ver10/schema">
  <soap12:Body>
    <soap12:Fault>
      <soap12:Code>
        <soap12:Value>{code_ns}:{code}</soap12:Value>
        {sub}
      </soap12:Code>
      <soap12:Reason><soap12:Text xml:lang="en">{text}</soap12:Text></soap12:Reason>
    </soap12:Fault>
  </soap12:Body>
</soap12:Envelope>"""
        return web.Response(text=envelope, content_type="application/soap+xml")

    def _check_security(self, body: bytes) -> bool:
        """Validate the UsernameToken digest exactly as ONVIF requires."""
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return False
        header = root.find(f"{{{SOAP_ENV}}}Header")
        if header is None:
            return False
        token = header.find(f"{{{WSSE_NS}}}Security/{{{WSSE_NS}}}UsernameToken")
        if token is None:
            return False
        username_el = token.find(f"{{{WSSE_NS}}}Username")
        password_el = token.find(f"{{{WSSE_NS}}}Password")
        nonce_el = token.find(f"{{{WSSE_NS}}}Nonce")
        created_el = token.find(f"{{{WSU_NS}}}Created")
        if any(
            el is None or el.text is None for el in (username_el, password_el, nonce_el, created_el)
        ):
            return False
        assert username_el is not None and username_el.text is not None
        assert password_el is not None and password_el.text is not None
        assert nonce_el is not None and nonce_el.text is not None
        assert created_el is not None and created_el.text is not None
        nonce = base64.b64decode(nonce_el.text)
        expected = base64.b64encode(
            hashlib.sha1(nonce + created_el.text.encode() + self.state.password.encode()).digest()
        ).decode()
        if not hmac.compare_digest(expected, password_el.text):
            return False
        created = datetime.fromisoformat(created_el.text.replace("Z", "+00:00"))
        now = datetime.now(UTC)
        if abs((now - created).total_seconds()) > 300:
            return False
        return username_el.text == self.state.username

    def _operation(self, body: bytes) -> str | None:
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return None
        payload = root.find(f"{{{SOAP_ENV}}}Body")[0]  # type: ignore[index]
        return payload.tag.rsplit("}", 1)[-1]

    # ---------------------------------------------------------------- handler

    async def _device_service(self, request: web.Request) -> web.StreamResponse:
        s = self.state
        s.total_requests += 1
        body = await request.read()

        if s.behavior == "redirect":
            return web.Response(
                status=302, headers={"Location": "http://198.51.100.99/onvif/other"}
            )

        operation = self._operation(body)
        if operation is None:
            if s.behavior == "malformed":
                return web.Response(text="<not-xml", content_type="application/soap+xml")
            return self._soap_fault("soap12", "Sender", None, "unparseable request")
        s.requests[operation] = s.requests.get(operation, 0) + 1

        if s.behavior == "http_401":
            return web.Response(status=401, text="unauthorized")

        if not self._check_security(body):
            s.auth_failures += 1
            return self._soap_fault(
                "ter",
                "Sender",
                "ter:NotAuthorized",
                "The action requested requires authorization",
            )

        if s.behavior == "wrong_password" and operation == "GetDeviceInformation":
            return self._soap_fault("ter", "Sender", "ter:NotAuthorized", "invalid credentials")

        if operation == "GetDeviceInformation":
            envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap12:Envelope xmlns:soap12="{SOAP_ENV}" xmlns:tds="{TDS_NS}">
  <soap12:Body>
    <tds:GetDeviceInformationResponse>
      <tds:Manufacturer>FakeCam</tds:Manufacturer>
      <tds:Model>FC-2000</tds:Model>
      <tds:FirmwareVersion>1.2.3</tds:FirmwareVersion>
      <tds:SerialNumber>SN-42</tds:SerialNumber>
      <tds:HardwareId>HW-1</tds:HardwareId>
    </tds:GetDeviceInformationResponse>
  </soap12:Body>
</soap12:Envelope>"""
            return web.Response(text=envelope, content_type="application/soap+xml")

        if operation == "GetSystemDateAndTime":
            now = datetime.now(UTC).replace(tzinfo=None)
            return web.Response(
                text=clock_envelope(now, self.state.clock_shape),
                content_type="application/soap+xml",
            )

        if operation == "SystemReboot":
            if s.behavior in (
                "ok",
                "slow",
                "malformed",
                "oversized",
                "reset_after_accept",
                "drop_silently",
            ):
                # These behaviors model a camera that actually acts on the
                # reboot command; the request HAS been accepted at this point.
                s.reboots_accepted += 1
            if s.behavior == "reset_after_accept":
                if request.transport is not None:
                    request.transport.abort()
                raise asyncio.CancelledError
            if s.behavior == "drop_silently":
                if request.transport is not None:
                    request.transport.abort()
                raise asyncio.CancelledError
            if s.behavior == "slow":
                await asyncio.sleep(30)
                return self._reboot_response()
            if s.behavior == "malformed":
                return web.Response(text=">>>garbage<<<", content_type="application/soap+xml")
            if s.behavior == "oversized":
                junk = "x" * (512 * 1024)
                return web.Response(text=f"<r>{junk}</r>", content_type="application/soap+xml")
            if s.behavior == "unsupported":
                return self._soap_fault(
                    "ter",
                    "Sender",
                    "ter:ActionNotSupported",
                    "The device does not support SystemReboot",
                )
            if s.behavior == "fault":
                return self._soap_fault("ter", "Receiver", None, "camera sad")
            await asyncio.sleep(s.reboot_delay_s)
            return self._reboot_response()

        return self._soap_fault("ter", "Sender", None, f"unknown operation {operation}")

    @staticmethod
    def _reboot_response() -> web.Response:
        envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap12:Envelope xmlns:soap12="{SOAP_ENV}" xmlns:tds="{TDS_NS}">
  <soap12:Body>
    <tds:SystemRebootResponse>
      <tds:Message>Rebooting now (2026-09-26T00:00:00Z)</tds:Message>
    </tds:SystemRebootResponse>
  </soap12:Body>
</soap12:Envelope>"""
        return web.Response(text=envelope, content_type="application/soap+xml")


def clock_envelope(now: datetime, shape: str) -> str:
    """A GetSystemDateAndTime reply in the given shape."""
    utc = (
        f"<tt:Date><tt:Year>{now.year}</tt:Year><tt:Month>{now.month}</tt:Month>"
        f"<tt:Day>{now.day}</tt:Day></tt:Date>"
        f"<tt:Time><tt:Hour>{now.hour}</tt:Hour><tt:Minute>{now.minute}</tt:Minute>"
        f"<tt:Second>{now.second}</tt:Second></tt:Time>"
    )
    if shape == "vatilon":
        # Verbatim structure of a Vatilon PB1 (V1.08.89) reply: Time precedes
        # Date, lines end in CRLF, and an empty Extension closes the element.
        body = (
            "<tds:SystemDateAndTime>\r\n<tt:DateTimeType>NTP</tt:DateTimeType>\r\n"
            "<tt:DaylightSavings>false</tt:DaylightSavings>\r\n"
            "<tt:TimeZone>\r\n<tt:TZ>EAustraliaStandardTime-10</tt:TZ>\r\n</tt:TimeZone>\r\n"
            f"<tt:UTCDateTime>\r\n<tt:Time>\r\n<tt:Hour>{now.hour}</tt:Hour>\r\n"
            f"<tt:Minute>{now.minute}</tt:Minute>\r\n<tt:Second>{now.second}</tt:Second>\r\n"
            f"</tt:Time>\r\n<tt:Date>\r\n<tt:Year>{now.year}</tt:Year>\r\n"
            f"<tt:Month>{now.month}</tt:Month>\r\n<tt:Day>{now.day}</tt:Day>\r\n"
            "</tt:Date>\r\n</tt:UTCDateTime>\r\n<tt:Extension />\r\n</tds:SystemDateAndTime>"
        )
    else:
        body = (
            "<tds:SystemDateAndTime><tt:DateTimeType>NTP</tt:DateTimeType>"
            "<tt:DaylightSavings>false</tt:DaylightSavings>"
            f"<tt:UTCDateTime>{utc}</tt:UTCDateTime></tds:SystemDateAndTime>"
        )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<soap12:Envelope xmlns:soap12="{SOAP_ENV}" xmlns:tds="{TDS_NS}" xmlns:tt="{TT_NS}">
  <soap12:Body>
    <tds:GetSystemDateAndTimeResponse>{body}</tds:GetSystemDateAndTimeResponse>
  </soap12:Body>
</soap12:Envelope>"""


def future_utc(seconds: float) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


__all__ = ["Any", "FakeOnvifCamera", "FakeOnvifState", "future_utc"]
