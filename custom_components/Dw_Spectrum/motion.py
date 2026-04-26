from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

STATE_DETECTED = "Detected"
STATE_NOT_DETECTED = "Not Detected"


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def normalize_camera_id(value: str | None) -> str:
    return str(value or "").strip().strip("{}")


def with_braces(camera_id: str) -> str:
    cid = normalize_camera_id(camera_id)
    return "{" + cid + "}" if cid else ""


class DwSpectrumMotionCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Motion state updated only by DW Spectrum rule callbacks."""

    def __init__(self, hass: HomeAssistant) -> None:
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name="dw_spectrum_motion_callbacks",
            update_interval=None,
        )
        self._states: dict[str, dict[str, Any]] = {}

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        return dict(self._states)

    @callback
    def ensure_camera(self, camera_id: str, camera_name: str | None = None) -> None:
        cid = normalize_camera_id(camera_id)
        if not cid:
            return
        if cid not in self._states:
            self._states[cid] = {
                "state": STATE_NOT_DETECTED,
                "camera_id": cid,
                "camera_name": camera_name,
                "last_motion": None,
                "last_stop": None,
                "last_event": None,
                "last_payload": None,
                "source": "dw_rule_callback",
            }
            self.async_set_updated_data(dict(self._states))

    @callback
    def receive_motion(
        self,
        *,
        camera_id: str,
        state: str,
        camera_name: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        cid = normalize_camera_id(camera_id)
        if not cid:
            _LOGGER.warning("DW Spectrum motion callback missing camera_id: %s", payload)
            return

        now = utc_now_iso()
        existing = dict(self._states.get(cid) or {})
        existing.setdefault("camera_id", cid)
        if camera_name:
            existing["camera_name"] = camera_name

        state_l = str(state or "").lower().strip()
        if state_l in ("start", "started", "active", "detected", "on", "true", "1"):
            existing["state"] = STATE_DETECTED
            existing["last_motion"] = now
            existing["last_event"] = now
        elif state_l in ("stop", "stopped", "inactive", "not_detected", "off", "false", "0"):
            existing["state"] = STATE_NOT_DETECTED
            existing["last_stop"] = now
            existing["last_event"] = now
        else:
            _LOGGER.warning("DW Spectrum motion callback has unknown state=%s payload=%s", state, payload)
            return

        existing["source"] = "dw_rule_callback"
        existing["last_payload"] = payload or {}
        self._states[cid] = existing
        self.async_set_updated_data(dict(self._states))
