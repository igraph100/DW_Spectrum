from __future__ import annotations

import logging
import secrets
from datetime import datetime, timezone
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.components.http import HomeAssistantView
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


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload entry when Options (gear icon) are saved so callback rules are created/updated."""
    await hass.config_entries.async_reload(entry.entry_id)


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


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
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    # Register once globally. Duplicate register attempts can happen if multiple DW entries exist.
    if not hass.data[DOMAIN].get("_motion_view_registered"):
        hass.http.register_view(DwSpectrumMotionCallbackView)
        hass.data[DOMAIN]["_motion_view_registered"] = True

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

    hass.data[DOMAIN][entry.entry_id] = {
        "api": api,
        "coordinator": cameras_coordinator,
        "server_coordinator": server_coordinator,
        "motion_token": motion_token,
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
