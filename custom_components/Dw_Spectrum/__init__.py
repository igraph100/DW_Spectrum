from __future__ import annotations

import base64
import hashlib
import logging
import re
import secrets
from datetime import datetime, timezone
from typing import Any

import voluptuous as vol

from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.components.http import HomeAssistantView
from homeassistant.components import websocket_api
import aiohttp
from aiohttp import web

from .const import (
    DOMAIN,
    CONF_HOST,
    CONF_PORT,
    CONF_SSL,
    CONF_VERIFY_SSL,
    CONF_USERNAME,
    CONF_PASSWORD,
    CONF_HA_CALLBACK_URL,
    CONF_MOTION_TOKEN,
    CONF_ENABLE_MOTION_RULES,
)
from .api import DwSpectrumApi, DwSpectrumConfig
from .coordinator import DwSpectrumCoordinator
from .server_coordinator import DwSpectrumServerCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[str] = ["camera", "sensor", "switch", "select", "button"]


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


async def _digest_get(
    session: aiohttp.ClientSession,
    url: str,
    username: str,
    password: str,
    params: dict | None = None,
    ssl=None,
) -> aiohttp.ClientResponse:
    """GET with HTTP Digest authentication (RFC 2617).

    Probes the URL to get a 401 Digest challenge, then retries with the
    computed response header.  The caller must use this inside an
    async-with block on the *returned* response.
    """
    from urllib.parse import urlencode

    kw: dict = {}
    if ssl is not None:
        kw["ssl"] = ssl

    # Step 1: probe for the challenge
    async with session.get(url, params=params, allow_redirects=False, **kw) as probe:
        await probe.read()
        status = probe.status
        www_auth = probe.headers.get("WWW-Authenticate", "")

    if status != 401 or not www_auth.lower().startswith("digest"):
        # No digest challenge — plain request
        return session.get(url, params=params, **kw)

    # Parse challenge
    def _field(name: str) -> str:
        m = re.search(rf'{name}="([^"]*)"', www_auth, re.IGNORECASE)
        return m.group(1) if m else ""

    realm  = _field("realm")
    nonce  = _field("nonce")
    opaque = _field("opaque")
    qop    = "auth" if "auth" in _field("qop") else ""

    # Digest signs the full request URI including query string
    uri_path = "/" + url.split("://", 1)[-1].split("/", 1)[-1]
    if params:
        uri_path += "?" + urlencode(params)

    ha1 = _md5(f"{username}:{realm}:{password}")
    ha2 = _md5(f"GET:{uri_path}")

    if qop:
        nc     = "00000001"
        cnonce = secrets.token_hex(8)
        resp   = _md5(f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}")
        auth   = (
            f'Digest username="{username}", realm="{realm}", nonce="{nonce}", '
            f'uri="{uri_path}", qop={qop}, nc={nc}, cnonce="{cnonce}", '
            f'response="{resp}"'
        )
    else:
        resp = _md5(f"{ha1}:{nonce}:{ha2}")
        auth = (
            f'Digest username="{username}", realm="{realm}", nonce="{nonce}", '
            f'uri="{uri_path}", response="{resp}"'
        )

    if opaque:
        auth += f', opaque="{opaque}"'

    # Step 2: real request with Digest header.
    # Return the _RequestContextManager so the caller can use `async with resp`.
    return session.get(url, params=params, headers={"Authorization": auth}, **kw)


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload entry when Options (gear icon) are saved so callback rules are created/updated."""
    await hass.config_entries.async_reload(entry.entry_id)


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


class DwSpectrumThumbnailView(HomeAssistantView):
    """HA-authenticated proxy that fetches a camera thumbnail from DW and returns it.

    The Lovelace card uses this so it never makes cross-origin requests to the DW
    server for images — HA handles auth on the way out and the browser only ever
    talks to HA (same origin).
    """

    url = "/api/dw_spectrum/{entry_id}/thumbnail/{camera_id}"
    name = "api:dw_spectrum:thumbnail"
    requires_auth = False

    async def get(self, request, entry_id: str, camera_id: str) -> web.Response:
        hass: HomeAssistant = request.app["hass"]
        entry_data = hass.data.get(DOMAIN, {}).get(entry_id)
        if not entry_data:
            return web.Response(status=404)

        # Validate media token — same token the card gets from get_info
        expected = str(entry_data.get("media_token") or "")
        given    = str(request.rel_url.query.get("token") or "")
        if not expected or given != expected:
            return web.Response(status=403)

        api = entry_data.get("api")
        if not api:
            return web.Response(status=503)
        try:
            img = await api.get_device_image(camera_id)
            if not img:
                return web.Response(status=404)
            return web.Response(
                body=img,
                content_type="image/jpeg",
                headers={"Cache-Control": "no-cache"},
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Thumbnail proxy failed for camera %s: %s", camera_id, err)
            return web.Response(status=502)


class DwSpectrumMediaView(HomeAssistantView):
    """Proxy DW Spectrum archive/live video through HA.

    The browser loads <video src="/api/dw_spectrum/{entry_id}/media/{cam_id}?...">
    which hits HA (trusted cert, same origin) instead of the DW server directly.
    This avoids SSL-certificate errors with self-signed DW certs.

    Security: the URL must include a per-entry ``token`` query param that matches
    the ``media_token`` stored in hass.data (returned only via the authenticated
    WebSocket get_info call).  requires_auth=False so <video src> works without
    custom Authorization headers.
    """

    url = "/api/dw_spectrum/{entry_id}/media/{camera_id}"
    name = "api:dw_spectrum:media"
    requires_auth = False

    async def get(
        self, request: web.Request, entry_id: str, camera_id: str
    ) -> web.Response | web.StreamResponse:
        hass: HomeAssistant = request.app["hass"]
        entry_data = hass.data.get(DOMAIN, {}).get(entry_id)
        if not entry_data:
            return web.Response(status=404)

        # Validate per-entry media token (prevents unauthenticated access)
        expected = str(entry_data.get("media_token") or "")
        given    = str(request.rel_url.query.get("token") or "")
        if not expected or given != expected:
            _LOGGER.warning("Media proxy 403: entry=%s expected_token=%s given_token=%s", entry_id, expected[:8] or "EMPTY", given[:8] or "EMPTY")
            return web.Response(status=403)

        api = entry_data.get("api")
        if not api:
            return web.Response(status=503)

        cam_id = str(camera_id).strip().strip("{}")

        # type=download → REST v3 bounded clip (matches DW app behaviour)
        # (default)     → legacy /media/{cam}.mp4 continuous stream
        is_download = request.rel_url.query.get("type") in ("hls", "download")

        params: dict[str, str] = {}
        req_headers: dict[str, str] = {}

        if not is_download:
            # Streaming playback auth: DW 6.x base64(user:md5(pass)) + bearer fallback
            params["auth"] = base64.b64encode(
                f"{api._cfg.username}:{_md5(api._cfg.password)}".encode()
            ).decode()
            if api._token:
                req_headers["Authorization"] = f"Bearer {api._token}"
            if "Range" in request.headers:
                req_headers["Range"] = request.headers["Range"]
            for key in ("pos", "startTime", "duration", "stream", "hi"):
                val = request.rel_url.query.get(key)
                if val is not None:
                    if key in ("pos", "startTime", "duration"):
                        try:
                            val = str(int(float(val)))
                        except (ValueError, TypeError):
                            pass
                    params[key] = val

        try:
            if is_download:
                # Use the same REST v3 endpoint and params the DW mobile app uses:
                # GET /rest/v3/devices/{id}/media.mp4?positionMs=...&durationMs=...
                #   &download=true&rotation=auto&continuousTimestamps=true&_ticket=...
                try:
                    ticket = await api.get_webrtc_ticket()
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning("Download: failed to get ticket: %s", err)
                    return web.Response(status=502)

                pos_ms  = request.rel_url.query.get("positionMs", "0")
                dur_ms  = request.rel_url.query.get("durationMs", str(5 * 60 * 1000))
                try:
                    pos_ms = str(int(float(pos_ms)))
                    dur_ms = str(int(float(dur_ms)))
                except (ValueError, TypeError):
                    pass

                dl_url = f"{api.base_url}/rest/v3/devices/{{{cam_id}}}/media.mp4"
                dl_params = {
                    "positionMs":          pos_ms,
                    "durationMs":          dur_ms,
                    "download":            "true",
                    "rotation":            "auto",
                    "continuousTimestamps":"true",
                    "_ticket":             ticket,
                }

                async with api._session.get(
                    dl_url,
                    params=dl_params,
                    timeout=aiohttp.ClientTimeout(total=None, connect=15),
                    **api._request_kwargs(),
                ) as resp:
                    if resp.status not in (200, 206):
                        body = await resp.text()
                        _LOGGER.warning("Download: DW returned %s: %s", resp.status, body[:200])
                        return web.Response(status=resp.status)

                    content_type = resp.headers.get("Content-Type", "video/mp4")
                    ext = "mkv" if "matroska" in content_type else "mp4"
                    proxy_headers: dict[str, str] = {
                        "Content-Type": content_type,
                        "Content-Disposition": f'attachment; filename="footage_{cam_id}.{ext}"',
                        "Cache-Control": "no-cache",
                    }
                    if "Content-Length" in resp.headers:
                        proxy_headers["Content-Length"] = resp.headers["Content-Length"]

                    proxy_resp = web.StreamResponse(status=200, headers=proxy_headers)
                    await proxy_resp.prepare(request)
                    try:
                        async for chunk in resp.content.iter_chunked(65536):
                            await proxy_resp.write(chunk)
                    except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as payload_err:
                        _LOGGER.debug("Clip download stream ended: %s", payload_err)
                    return proxy_resp
            else:
                # Stream video for playback (potentially infinite / large)
                stream_url = f"{api.base_url}/media/{cam_id}.mp4"
                _LOGGER.warning("Media proxy: fetching %s params=%s", stream_url, {k: v for k, v in params.items() if k != "auth"})
                async with api._session.get(
                    stream_url,
                    params=params,
                    headers=req_headers,
                    timeout=aiohttp.ClientTimeout(total=None, connect=15),
                    **api._request_kwargs(),
                ) as resp:
                    _LOGGER.warning("Media proxy: DW responded %s for %s", resp.status, cam_id)
                    if resp.status not in (200, 206):
                        return web.Response(status=resp.status)

                    proxy_headers: dict[str, str] = {
                        "Content-Type": resp.headers.get("Content-Type", "video/mp4"),
                        "Cache-Control": "no-cache",
                        "Accept-Ranges": "bytes",
                    }
                    for h in ("Content-Length", "Content-Range"):
                        if h in resp.headers:
                            proxy_headers[h] = resp.headers[h]

                    proxy_resp = web.StreamResponse(status=resp.status, headers=proxy_headers)
                    await proxy_resp.prepare(request)
                    try:
                        async for chunk in resp.content.iter_chunked(1 << 16):
                            await proxy_resp.write(chunk)
                    except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as payload_err:
                        # DW closed the live stream (expected on disconnect/refresh)
                        _LOGGER.debug("Live stream ended: %s", payload_err)
                    return proxy_resp

        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Media proxy error for %s: %s", camera_id, err)
            return web.Response(status=502)


class DwSpectrumWebRTCProxyView(HomeAssistantView):
    """WebSocket proxy for WebRTC signaling.

    The browser can't open a WebSocket directly to the DW server because DW uses
    a self-signed SSL certificate that browsers reject.  This proxy accepts the
    browser WebSocket on the HA (trusted) origin, opens a backend WebSocket to DW
    with SSL verification disabled, and bridges all messages bidirectionally.

    URL: /api/dw_spectrum/{entry_id}/webrtc/{camera_id}?token={media_token}
    """

    url = "/api/dw_spectrum/{entry_id}/webrtc/{camera_id}"
    name = "api:dw_spectrum:webrtc"
    requires_auth = False

    async def get(self, request: web.Request, entry_id: str, camera_id: str) -> web.WebSocketResponse:
        hass: HomeAssistant = request.app["hass"]
        entry_data = hass.data.get(DOMAIN, {}).get(entry_id)
        if not entry_data:
            return web.Response(status=404)

        # Validate media token.
        expected = str(entry_data.get("media_token") or "")
        given    = str(request.rel_url.query.get("token") or "")
        if not expected or given != expected:
            return web.Response(status=403)

        api = entry_data.get("api")
        if not api:
            return web.Response(status=503)

        # Get a short-lived DW ticket for the backend WebSocket auth.
        try:
            ticket = await api.get_webrtc_ticket()
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("WebRTC proxy: failed to get ticket: %s", err)
            return web.Response(status=502)

        # Build the DW WebSocket URL — no braces around camera ID in the path.
        cam_id  = str(camera_id).strip().strip("{}")
        dw_base = api.base_url.rstrip("/")
        dw_ws   = (
            dw_base
            .replace("https://", "wss://")
            .replace("http://", "ws://")
        )
        dw_url  = f"{dw_ws}/rest/v4/devices/{cam_id}/webrtc?_ticket={ticket}"
        _LOGGER.debug("WebRTC proxy: connecting to DW at %s", dw_url.split("?")[0])

        # Upgrade the browser connection to WebSocket.
        browser_ws = web.WebSocketResponse()
        await browser_ws.prepare(request)

        import asyncio

        # Open the backend WebSocket to DW (SSL verify disabled — DW self-signed cert).
        try:
            async with aiohttp.ClientSession() as session:
                try:
                    dw_ws_conn = await session.ws_connect(dw_url, ssl=False)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning("WebRTC proxy: could not connect to DW WebSocket: %s", err)
                    await browser_ws.close()
                    return browser_ws

                _LOGGER.debug("WebRTC proxy: DW WebSocket connected for camera %s", cam_id)

                async def _browser_to_dw():
                    async for msg in browser_ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            _LOGGER.debug("WebRTC proxy browser→DW: %s", msg.data[:120])
                            await dw_ws_conn.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await dw_ws_conn.send_bytes(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                            break

                async def _dw_to_browser():
                    async for msg in dw_ws_conn:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            _LOGGER.debug("WebRTC proxy DW→browser: %s", msg.data[:120])
                            await browser_ws.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await browser_ws.send_bytes(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                            _LOGGER.debug("WebRTC proxy: DW WebSocket closed (type=%s)", msg.type)
                            break

                done, pending = await asyncio.wait(
                    [
                        asyncio.create_task(_browser_to_dw()),
                        asyncio.create_task(_dw_to_browser()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await dw_ws_conn.close()

        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("WebRTC proxy error: %s", err)

        if not browser_ws.closed:
            await browser_ws.close()
        return browser_ws


class DwSpectrumMotionCallbackView(HomeAssistantView):
    """Unauthenticated callback endpoint used only with a per-entry secret token."""

    url = "/api/dw_spectrum/motion/{entry_id}/{token}"
    name = "api:dw_spectrum:motion"
    requires_auth = False

    async def get(self, request, entry_id: str, token: str) -> web.Response:
        return await self._handle(request, entry_id, token)

    async def post(self, request, entry_id: str, token: str) -> web.Response:
        return await self._handle(request, entry_id, token)

    async def _handle(self, request, entry_id: str, token: str) -> web.Response:
        hass: HomeAssistant = request.app["hass"]
        entry_data = hass.data.get(DOMAIN, {}).get(entry_id)
        if not entry_data:
            return web.json_response({"ok": False, "error": "unknown_entry"}, status=404)

        expected = str(entry_data.get("motion_token") or "")
        if not expected or token != expected:
            return web.json_response({"ok": False, "error": "invalid_token"}, status=403)

        params = request.rel_url.query
        payload: dict[str, Any] = {}
        if request.method == "POST":
            try:
                payload = await request.json()
            except Exception:
                payload = {}

        state_raw = str(payload.get("state") or params.get("state") or "").lower().strip()
        camera_id = str(
            payload.get("camera_id")
            or payload.get("cameraId")
            or payload.get("deviceId")
            or params.get("camera_id")
            or params.get("cameraId")
            or params.get("deviceId")
            or ""
        ).strip().strip("{}")

        if state_raw in ("start", "active", "detected", "on", "true", "1"):
            state = "Detected"
            event_key = "last_motion_ms"
        elif state_raw in ("stop", "inactive", "not_detected", "off", "false", "0"):
            state = "Not Detected"
            event_key = "last_stop_ms"
        else:
            return web.json_response({"ok": False, "error": "invalid_state", "state": state_raw}, status=400)

        if not camera_id:
            return web.json_response({"ok": False, "error": "missing_camera_id"}, status=400)

        coord = entry_data.get("motion_coordinator")
        if coord is None or not hasattr(coord, "async_set_motion"):
            return web.json_response({"ok": False, "error": "motion_coordinator_not_ready"}, status=503)

        event_ms = _now_ms()
        raw = {
            "query": dict(params),
            "json": payload,
            "remote": request.remote,
            "method": request.method,
        }
        await coord.async_set_motion(
            camera_id=camera_id,
            state=state,
            event_ms=event_ms,
            event_key=event_key,
            raw=raw,
        )
        return web.json_response({"ok": True, "camera_id": camera_id, "state": state})


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    websocket_api.async_register_command(hass, _ws_get_info)
    websocket_api.async_register_command(hass, _ws_check_footage)
    websocket_api.async_register_command(hass, _ws_get_footage_periods)
    websocket_api.async_register_command(hass, _ws_get_motion_periods)
    websocket_api.async_register_command(hass, _ws_get_footage_range)
    websocket_api.async_register_command(hass, _ws_get_webrtc_ticket)
    return True


# ---------------------------------------------------------------------------
# WebSocket API — used by dw_spectrum_playback_card.js
# ---------------------------------------------------------------------------

@websocket_api.websocket_command(
    {
        vol.Required("type"): "dw_spectrum/get_info",
        vol.Optional("entry_id"): str,
    }
)
@websocket_api.async_response
async def _ws_get_info(hass: HomeAssistant, connection, msg: dict) -> None:
    """Return DW server URL, a live auth token, and camera list for each entry.

    The Lovelace playback card calls this on load so it never needs credentials
    in its own YAML config — everything comes from the existing integration setup.
    """
    requested_id = msg.get("entry_id")
    entries = hass.config_entries.async_entries(DOMAIN)
    if requested_id:
        entries = [e for e in entries if e.entry_id == requested_id]

    if not entries:
        connection.send_error(msg["id"], "not_found", "No DW Spectrum integration found")
        return

    result: list[dict[str, Any]] = []
    for entry in entries:
        entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
        api = entry_data.get("api")
        if not api:
            continue
        try:
            token = await api.ensure_token()

            # Prefer the coordinator's already-cached data to avoid an extra round-trip.
            coordinator = entry_data.get("coordinator")
            raw_cameras: list[dict[str, Any]] = (
                list(coordinator.data)
                if coordinator and coordinator.data
                else await api.get_cameras()
            )

            cameras = [
                {
                    "id": str(c.get("id", "")).strip().strip("{}"),
                    "name": str(c.get("name") or c.get("id") or ""),
                }
                for c in raw_cameras
                if c.get("id")
            ]
            cameras.sort(key=lambda c: c["name"].lower())

            # auth_param = base64(user:pass) — DW Spectrum accepts ?auth=<this>
            # on all media/HLS URLs so the browser <video> element never needs to
            # prompt for credentials.
            auth_param = base64.b64encode(
                f"{api._cfg.username}:{api._cfg.password}".encode()
            ).decode()

            result.append(
                {
                    "entry_id":   entry.entry_id,
                    "title":      entry.title,
                    "dw_url":     api.base_url,
                    "token":      token,
                    "auth_param": auth_param,
                    "media_token": entry_data.get("media_token", ""),
                    "cameras":    cameras,
                }
            )
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("WS get_info failed for entry %s: %s", entry.entry_id, err)

    connection.send_result(msg["id"], {"entries": result})


@websocket_api.websocket_command(
    {
        vol.Required("type"): "dw_spectrum/check_footage",
        vol.Required("entry_id"): str,
        vol.Required("camera_id"): str,
        vol.Required("start_ms"): int,
        vol.Required("end_ms"): int,
    }
)
@websocket_api.async_response
async def _ws_check_footage(hass: HomeAssistant, connection, msg: dict) -> None:
    """Proxy a footage-availability check through the integration's authenticated session."""
    entry_data = hass.data.get(DOMAIN, {}).get(msg["entry_id"], {})
    api = entry_data.get("api")
    if not api:
        connection.send_error(msg["id"], "not_found", "Entry not found or not yet loaded")
        return

    cam_id = str(msg["camera_id"]).strip().strip("{}")
    try:
        data = await api._request_json(
            "GET",
            f"/rest/v4/devices/{cam_id}/footage",
            params={
                "startTimeMs":   str(msg["start_ms"]),
                "endTimeMs":     str(msg["end_ms"]),
                "detailLevelMs": "-1",
                "preciseBounds": "true",
            },
        )
        has_footage = False
        if isinstance(data, list):
            has_footage = len(data) > 0
        elif isinstance(data, dict):
            for key in ("periods", "chunks", "data"):
                if isinstance(data.get(key), list) and data[key]:
                    has_footage = True
                    break
            if not has_footage and data:
                has_footage = True

        connection.send_result(msg["id"], {"has_footage": has_footage})
    except Exception as err:  # noqa: BLE001
        # Return a soft error so the card can display the message gracefully.
        connection.send_result(msg["id"], {"has_footage": False, "error": str(err)})


@websocket_api.websocket_command(
    {
        vol.Required("type"): "dw_spectrum/get_footage_periods",
        vol.Required("entry_id"): str,
        vol.Required("camera_id"): str,
        vol.Required("start_ms"): int,
        vol.Required("end_ms"): int,
    }
)
@websocket_api.async_response
async def _ws_get_footage_periods(hass: HomeAssistant, connection, msg: dict) -> None:
    """Return recorded footage periods for a camera in a time window.

    Returns a list of {startMs, endMs} objects that the timeline can render as
    recorded segments (green bars). Also returns motion periods where available.
    """
    entry_data = hass.data.get(DOMAIN, {}).get(msg["entry_id"], {})
    api = entry_data.get("api")
    if not api:
        connection.send_error(msg["id"], "not_found", "Entry not found or not yet loaded")
        return

    cam_id = str(msg["camera_id"]).strip().strip("{}")

    def _parse_raw_list(data: Any) -> list:
        """Extract a list of period dicts from various DW response shapes."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("periods", "chunks", "data", "items"):
                if isinstance(data.get(key), list):
                    return data[key]
            # Single period returned as a plain object
            if data.get("startTimeMs") is not None:
                return [data]
        return []

    def _parse_periods(raw_list: list) -> list:
        """Convert raw DW period items to {startMs, endMs} dicts.

        DW Spectrum may use endTimeMs OR durationMs (not always endTimeMs).
        """
        out = []
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            start = item.get("startTimeMs") or item.get("startMs") or item.get("start")
            end   = item.get("endTimeMs")   or item.get("endMs")   or item.get("end")
            # Many DW builds return durationMs instead of endTimeMs
            if end is None and start is not None:
                dur = item.get("durationMs") or item.get("duration")
                if dur is not None:
                    end = int(start) + int(dur)
            if start is not None and end is not None:
                out.append({"startMs": int(start), "endMs": int(end)})
        return out

    try:
        # Try the v4 REST endpoint first; fall back to the older ec2 endpoint.
        try:
            data = await api._request_json(
                "GET",
                f"/rest/v4/devices/{cam_id}/footage",
                params={
                    "startTimeMs":   str(msg["start_ms"]),
                    "endTimeMs":     str(msg["end_ms"]),
                    "detailLevelMs": "-1",
                    "preciseBounds": "true",
                },
            )
        except Exception:
            data = await api._request_json(
                "GET",
                "/ec2/recordedTimePeriods",
                params={
                    "cameraId": cam_id,
                    "startTime": str(msg["start_ms"]),
                    "endTime":   str(msg["end_ms"]),
                    "detail":    "0",
                },
            )

        periods = _parse_periods(_parse_raw_list(data))
        connection.send_result(msg["id"], {"periods": periods})
    except Exception as err:  # noqa: BLE001
        connection.send_result(msg["id"], {"periods": [], "error": str(err)})


@websocket_api.websocket_command(
    {
        vol.Required("type"): "dw_spectrum/get_motion_periods",
        vol.Required("entry_id"): str,
        vol.Required("camera_id"): str,
        vol.Required("start_ms"): int,
        vol.Required("end_ms"): int,
    }
)
@websocket_api.async_response
async def _ws_get_motion_periods(hass: HomeAssistant, connection, msg: dict) -> None:
    """Return motion event periods for a camera in a time window.

    Queries the DW events API for cameraMotionEvent entries and pairs Active/Inactive
    states into {startMs, endMs} periods for the timeline orange markers.
    Returns an empty list gracefully if the DW build doesn't support this endpoint.
    """
    entry_data = hass.data.get(DOMAIN, {}).get(msg["entry_id"], {})
    api = entry_data.get("api")
    if not api:
        connection.send_error(msg["id"], "not_found", "Entry not found or not yet loaded")
        return

    cam_id = str(msg["camera_id"]).strip().strip("{}")
    start_ms = msg["start_ms"]
    end_ms   = msg["end_ms"]

    try:
        data = await api._request_json(
            "GET",
            f"/rest/v4/devices/{cam_id}/footage",
            params={
                "periodType":      "motion",
                "detailLevelMs":   "1000",
                "keepSmallChunks": "true",
                "startTimeMs":     str(start_ms),
                "endTimeMs":       str(end_ms),
            },
        )
        raw: list = data if isinstance(data, list) else []
        periods: list[dict] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            s = item.get("startTimeMs")
            d = item.get("durationMs")
            if s is not None and d is not None:
                periods.append({"startMs": int(s), "endMs": int(s) + int(d)})
        connection.send_result(msg["id"], {"periods": periods})
    except Exception as err:  # noqa: BLE001
        connection.send_result(msg["id"], {"periods": [], "error": str(err)})


@websocket_api.websocket_command(
    {
        vol.Required("type"): "dw_spectrum/get_footage_range",
        vol.Required("entry_id"): str,
        vol.Required("camera_id"): str,
    }
)
@websocket_api.async_response
async def _ws_get_footage_range(hass: HomeAssistant, connection, msg: dict) -> None:
    """Return the earliest available footage timestamp for a camera.

    Calls /rest/v4/devices/{cam_id}/footage?detailLevelMs=-1 which returns the
    overall footage range.  The card uses this to mark calendar days green and to
    know whether a camera has any archive at all.
    """
    entry_data = hass.data.get(DOMAIN, {}).get(msg["entry_id"], {})
    api = entry_data.get("api")
    if not api:
        connection.send_error(msg["id"], "not_found", "Entry not found or not yet loaded")
        return

    cam_id = str(msg["camera_id"]).strip().strip("{}")
    try:
        data = await api._request_json(
            "GET",
            f"/rest/v4/devices/{cam_id}/footage",
            params={"detailLevelMs": "-1"},
        )

        start_ms: int | None = None
        if isinstance(data, list) and data:
            start_ms = data[0].get("startTimeMs")
        elif isinstance(data, dict):
            start_ms = data.get("startTimeMs")

        connection.send_result(msg["id"], {
            "has_footage": start_ms is not None,
            "start_ms": int(start_ms) if start_ms is not None else None,
        })
    except Exception as err:  # noqa: BLE001
        connection.send_result(msg["id"], {"has_footage": False, "start_ms": None, "error": str(err)})


@websocket_api.websocket_command(
    {
        vol.Required("type"): "dw_spectrum/get_webrtc_ticket",
        vol.Required("entry_id"): str,
    }
)
@websocket_api.async_response
async def _ws_get_webrtc_ticket(hass: HomeAssistant, connection, msg: dict) -> None:
    """Return a short-lived vmsTicket for WebRTC WebSocket auth.

    The card uses this ticket as ``?_ticket=<value>`` on the direct WebSocket
    connection to the DW server for WebRTC signaling, avoiding embedding a
    long-lived bearer token in a visible URL.
    """
    entry_data = hass.data.get(DOMAIN, {}).get(msg["entry_id"], {})
    api = entry_data.get("api")
    if not api:
        connection.send_error(msg["id"], "not_found", "Entry not found or not yet loaded")
        return
    try:
        ticket = await api.get_webrtc_ticket()
        connection.send_result(msg["id"], {"ticket": ticket})
    except Exception as err:  # noqa: BLE001
        connection.send_error(msg["id"], "webrtc_ticket_failed", str(err))


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    # Register HTTP views once globally (multiple entries may load the same domain).
    if not hass.data[DOMAIN].get("_views_registered"):
        hass.http.register_view(DwSpectrumMotionCallbackView)
        hass.http.register_view(DwSpectrumThumbnailView)
        hass.http.register_view(DwSpectrumMediaView)
        hass.http.register_view(DwSpectrumWebRTCProxyView)
        hass.data[DOMAIN]["_views_registered"] = True

    session = async_get_clientsession(hass)

    cfg = DwSpectrumConfig(
        host=entry.data[CONF_HOST],
        port=entry.data[CONF_PORT],
        ssl=entry.data[CONF_SSL],
        verify_ssl=entry.data[CONF_VERIFY_SSL],
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
    )
    api = DwSpectrumApi(session, cfg)

    cameras_coordinator = DwSpectrumCoordinator(hass, api)
    server_coordinator = DwSpectrumServerCoordinator(hass, api)

    # If a callback URL exists, make sure the entry has a token. Older entries from
    # previous test builds may have the URL but no token, which prevented rule creation.
    callback_source = entry.options if CONF_HA_CALLBACK_URL in entry.options else entry.data
    callback_url = str(callback_source.get(CONF_HA_CALLBACK_URL) or "").strip().rstrip("/")
    motion_token = str(entry.options.get(CONF_MOTION_TOKEN) or entry.data.get(CONF_MOTION_TOKEN) or "")
    if callback_url and not motion_token:
        motion_token = secrets.token_urlsafe(32)
        new_options = dict(entry.options)
        new_options[CONF_MOTION_TOKEN] = motion_token
        new_options[CONF_ENABLE_MOTION_RULES] = True
        new_options[CONF_HA_CALLBACK_URL] = callback_url
        hass.config_entries.async_update_entry(entry, options=new_options)

    # Per-entry secret used to authenticate <video src> proxy requests.
    # Generated fresh on each HA start; card JS receives it via the WS get_info call.
    media_token = secrets.token_hex(32)

    hass.data[DOMAIN][entry.entry_id] = {
        "api": api,
        "coordinator": cameras_coordinator,
        "server_coordinator": server_coordinator,
        "motion_token": motion_token,
        "media_token": media_token,
        "motion_rules_result": None,
    }
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    # If the user supplied a callback URL, try to create the DW rules automatically.
    enable_rules = bool(callback_url) and bool(motion_token)
    if enable_rules:
        try:
            await cameras_coordinator.async_config_entry_first_refresh()
            cameras = [c for c in (cameras_coordinator.data or []) if isinstance(c, dict)]
            result = await api.ensure_motion_callback_rules(
                cameras=cameras,
                callback_base_url=callback_url,
                entry_id=entry.entry_id,
                token=motion_token,
            )
            hass.data[DOMAIN][entry.entry_id]["motion_rules_result"] = result
            _LOGGER.info("DW Spectrum motion callback rule setup result: %s", result)
            if result.get("failed"):
                _LOGGER.warning("Some DW motion callback rules failed to create: %s", result.get("failed"))
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("DW Spectrum automatic motion rule setup failed: %s", err)
            hass.data[DOMAIN][entry.entry_id]["motion_rules_result"] = {"enabled": True, "error": str(err)}
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    return unload_ok
