"""ONVIF Device Management adapter with a strictly single-shot transport.

The dangerous failure mode this module exists to prevent: a camera that
accepts ``SystemReboot`` and then drops the connection. A transport that
"helpfully" retries connection resets would then deliver a *second* reboot
command. This transport performs no retries of any kind, ever — for reads
or for writes — and the ambiguity is classified as ``OUTCOME_UNKNOWN``
(never "safe to retry").

Supported operations (bundled WSDL, no runtime downloads):

* ``GetDeviceInformation`` — authenticated read-only preflight
* ``GetSystemDateAndTime`` — diagnosis only
* ``SystemReboot`` — the single recovery action

Hardening: DTDs, entities, and external references are forbidden at the XML
parser level (zeep Settings), response bodies are size-capped before
parsing, redirects are never followed, and every operation has a total
deadline.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from zeep import Settings
from zeep.client import AsyncClient
from zeep.exceptions import Error as ZeepError
from zeep.exceptions import Fault
from zeep.proxy import AsyncServiceProxy
from zeep.transports import AsyncTransport
from zeep.wsse.username import UsernameToken

from .config import OnvifConfig
from .constants import ONVIF_MAX_RESPONSE_BYTES, ONVIF_OPERATION_DEADLINE_S

logger = logging.getLogger("frigate_watchdog.onvif")

DEVICE_WSDL = Path(__file__).parent / "wsdl" / "devicemgmt.wsdl"
DEVICE_NS = "http://www.onvif.org/ver10/device/wsdl"

OUTCOME_ACK = "ACKNOWLEDGED"
OUTCOME_AUTH = "AUTH_FAILED"
OUTCOME_UNSUPPORTED = "UNSUPPORTED"
OUTCOME_UNREACHABLE = "UNREACHABLE"
OUTCOME_REJECTED = "REJECTED"
OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


class OnvifResponseTooLarge(ZeepError):
    """Response body exceeded the configured cap before parsing."""

    def __init__(self, message: str = "response too large") -> None:
        super().__init__(message)  # type: ignore[no-untyped-call]


class SafeAsyncTransport(AsyncTransport):
    """httpx-based zeep transport with no-retry, no-redirect, size-capped I/O.

    Zeep calls ``post()`` exactly once per operation. This class adds no
    retry, reconnection, or replay logic of any kind — deliberately.
    """

    def __init__(
        self,
        *,
        deadline_s: float = ONVIF_OPERATION_DEADLINE_S,
        max_bytes: int = ONVIF_MAX_RESPONSE_BYTES,
    ) -> None:
        # Set before anything that can raise so __del__ never explodes.
        self._close_session = False
        timeout = httpx.Timeout(deadline_s)
        client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,  # camera-supplied destinations are never followed
            verify=True,
            trust_env=False,  # HTTP(S)_PROXY must not reroute or retry SOAP
        )
        wsdl_client = httpx.Client(
            timeout=deadline_s,
            verify=True,
            follow_redirects=False,
            trust_env=False,
        )
        super().__init__(client=client, wsdl_client=wsdl_client)  # type: ignore[no-untyped-call]
        self.max_bytes = max_bytes

    async def post(self, address: str, message: Any, headers: Any) -> httpx.Response:
        # Build the request explicitly so nothing can inject retry semantics.
        request = self.client.build_request("POST", address, content=message, headers=dict(headers))
        response = await self.client.send(request, stream=True)
        # Enforce the response cap before any XML parsing happens.
        content = bytearray()
        async for chunk in response.aiter_bytes():
            content.extend(chunk)
            if len(content) > self.max_bytes:
                await response.aclose()
                raise OnvifResponseTooLarge(f"response exceeded {self.max_bytes} bytes")
        await response.aclose()
        # Materialize a non-streaming response for zeep's new_response().
        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            content=bytes(content),
            request=request,
        )


@dataclass(frozen=True)
class OnvifResult:
    outcome: str  # one of the OUTCOME_* codes, or "OK" for reads
    detail: str = ""
    device: dict[str, str] | None = None

    @property
    def ok(self) -> bool:
        return self.outcome in ("OK", OUTCOME_ACK)


def _fault_text(fault: Fault) -> str:
    parts: list[str] = []
    for subcode in getattr(fault, "subcodes", None) or []:
        # zeep exposes SOAP 1.2 subcodes as lxml QName objects; ONVIF carries
        # NotAuthorized / ActionNotSupported there, not in the reason prose.
        parts.append(getattr(subcode, "localname", None) or str(subcode))
        parts.append(str(subcode))
    parts.extend(
        str(part or "")
        for part in (getattr(fault, "code", ""), fault.message, getattr(fault, "detail", ""))
    )
    return " ".join(parts)


def _classify_fault(fault: Fault) -> str:
    text = _fault_text(fault)
    lowered = text.lower().replace("-", "").replace(" ", "")
    if (
        "notauthorized" in lowered
        or "unauthorized" in lowered
        or "unauthorised" in lowered
        or "authenticationfailed" in lowered
    ):
        return OUTCOME_AUTH
    if (
        "notsupported" in lowered
        or "notsupport" in lowered
        or "actionnotsupported" in lowered
        or "unsupportedoperation" in lowered
        or "notimplemented" in lowered
    ):
        return OUTCOME_UNSUPPORTED
    return OUTCOME_REJECTED


def _classify_transport_error(exc: Exception, *, sent: bool) -> str:
    """Map transport failures conservatively.

    Anything that could have happened after the request left the wire is
    OUTCOME_UNKNOWN. Only provable never-connected failures are UNREACHABLE.
    """
    if isinstance(exc, httpx.ConnectError | httpx.ConnectTimeout):
        return OUTCOME_UNREACHABLE
    if isinstance(exc, httpx.TransportError):
        # Includes ReadTimeout, RemoteProtocolError, ReadError, WriteError...
        return OUTCOME_UNKNOWN if sent else OUTCOME_UNREACHABLE
    return OUTCOME_UNKNOWN


class OnvifClient:
    """One camera's ONVIF Device Management endpoint."""

    def __init__(
        self, config: OnvifConfig, *, deadline_s: float = ONVIF_OPERATION_DEADLINE_S
    ) -> None:
        self.config = config
        self.deadline_s = deadline_s
        self._transport: SafeAsyncTransport | None = None
        self._settings = Settings(
            strict=False,  # tolerate extra vendor elements
            forbid_dtd=True,  # no DTD processing, ever
            forbid_entities=True,  # no entity expansion, ever
            forbid_external=True,  # no external references, ever
            xml_huge_tree=False,  # bounded parse
        )
        self._client: AsyncClient | None = None
        self._service: AsyncServiceProxy | None = None

    def _ensure_client(self) -> AsyncServiceProxy:
        if self._service is None:
            if self._transport is None:
                self._transport = SafeAsyncTransport(deadline_s=self.deadline_s)
            self._client = AsyncClient(  # type: ignore[no-untyped-call]
                wsdl=str(DEVICE_WSDL),
                settings=self._settings,
                transport=self._transport,
                wsse=UsernameToken(  # type: ignore[no-untyped-call]
                    self.config.username,
                    self.config.password.value,
                    use_digest=True,  # ONVIF WS-Security password digest
                    zulu_timestamp=True,  # ONVIF cameras expect Zulu times
                ),
            )
            binding = self._client.wsdl.bindings[f"{{{DEVICE_NS}}}DeviceBinding"]
            self._service = AsyncServiceProxy(  # type: ignore[no-untyped-call]
                self._client, binding, address=self.config.endpoint.url
            )
        return self._service

    async def close(self) -> None:
        transport, self._transport = self._transport, None
        self._service = None
        self._client = None
        if transport is not None:
            await transport.aclose()  # type: ignore[no-untyped-call]

    # ---------------------------------------------------------------- reads

    async def probe(self) -> OnvifResult:
        """Authenticated read-only GetDeviceInformation preflight."""
        try:
            service = self._ensure_client()
        except Exception as exc:
            return OnvifResult(OUTCOME_UNREACHABLE, detail=f"client error: {exc}")
        try:
            async with asyncio.timeout(self.deadline_s):
                info = await service.GetDeviceInformation()
        except Fault as fault:
            outcome = _classify_fault(fault)
            return OnvifResult(outcome, detail=str(fault.message)[:200])
        except OnvifResponseTooLarge as exc:
            return OnvifResult(OUTCOME_REJECTED, detail=str(exc))
        except ZeepError as exc:
            return OnvifResult(OUTCOME_REJECTED, detail=f"soap error: {type(exc).__name__}")
        except Exception as exc:  # transport layer
            outcome = _classify_transport_error(exc, sent=True)
            return OnvifResult(outcome, detail=type(exc).__name__)
        device = {
            "manufacturer": str(getattr(info, "Manufacturer", "") or ""),
            "model": str(getattr(info, "Model", "") or ""),
            "firmware": str(getattr(info, "FirmwareVersion", "") or ""),
            "serial": str(getattr(info, "SerialNumber", "") or ""),
        }
        return OnvifResult("OK", device=device)

    async def get_system_date_and_time(self) -> OnvifResult:
        """Read the camera clock (diagnosis only; never written)."""
        try:
            service = self._ensure_client()
        except Exception as exc:
            return OnvifResult(OUTCOME_UNREACHABLE, detail=f"client error: {exc}")
        try:
            async with asyncio.timeout(self.deadline_s):
                value = await service.GetSystemDateAndTime()
        except Fault as fault:
            return OnvifResult(_classify_fault(fault), detail=str(fault.message)[:200])
        except Exception as exc:
            return OnvifResult(_classify_transport_error(exc, sent=True), detail=type(exc).__name__)
        sdt = value  # zeep unwraps the response element to SystemDateTime itself
        utc = getattr(sdt, "UTCDateTime", None) if sdt is not None else None
        if utc is None:
            return OnvifResult("OK", detail="camera reported no clock")
        time_part = getattr(utc, "Time", None)
        date_part = getattr(utc, "Date", None)

        def _field(obj: Any, name: str, width: int) -> str:
            raw = getattr(obj, name, None)
            if isinstance(raw, bool) or not isinstance(raw, int):
                return "?" * width if width <= 2 else "????"
            return f"{raw:0{width}d}"

        clock = (
            f"{_field(date_part, 'Year', 4)}-{_field(date_part, 'Month', 2)}-"
            f"{_field(date_part, 'Day', 2)}T{_field(time_part, 'Hour', 2)}:"
            f"{_field(time_part, 'Minute', 2)}:{_field(time_part, 'Second', 2)}Z"
        )
        return OnvifResult("OK", detail=f"camera clock {clock}")

    # ---------------------------------------------------------------- write

    async def reboot(self) -> OnvifResult:
        """Send SystemReboot exactly once and classify what happened.

        A disconnect or timeout after the request was written is
        OUTCOME_UNKNOWN — never a licence to resend.
        """
        try:
            service = self._ensure_client()
        except Exception as exc:
            # Nothing was sent: a construction failure is not an attempt event.
            return OnvifResult(OUTCOME_UNREACHABLE, detail=f"client error: {exc}")
        try:
            async with asyncio.timeout(self.deadline_s):
                response = await service.SystemReboot()
        except Fault as fault:
            outcome = _classify_fault(fault)
            return OnvifResult(outcome, detail=str(fault.message)[:200])
        except OnvifResponseTooLarge as exc:
            # The request may have been delivered; treat as unknown.
            return OnvifResult(OUTCOME_UNKNOWN, detail=str(exc))
        except ZeepError as exc:
            # Parse-level failure after delivery is ambiguous.
            return OnvifResult(OUTCOME_UNKNOWN, detail=f"soap error: {type(exc).__name__}")
        except Exception as exc:  # transport layer
            outcome = _classify_transport_error(exc, sent=True)
            return OnvifResult(outcome, detail=type(exc).__name__)
        message = str(getattr(response, "Message", "") or "")
        return OnvifResult(OUTCOME_ACK, detail=message[:200])
