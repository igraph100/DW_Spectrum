from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
import re
from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity, DataUpdateCoordinator

from .const import DOMAIN
from .coordinator import DwSpectrumCoordinator
from .server_coordinator import DwSpectrumServerCoordinator

_LOGGER = logging.getLogger(__name__)


# -----------------------
# Device info helpers
# -----------------------
def _server_device_info(entry: ConfigEntry, system_info: dict[str, Any] | None) -> dict[str, Any]:
    server_id = None
    if isinstance(system_info, dict):
        server_id = system_info.get("id") or system_info.get("systemId") or system_info.get("system_id")
    if not server_id:
        server_id = f"{entry.data.get('host')}:{entry.data.get('port')}"

    name = None
    if isinstance(system_info, dict):
        name = system_info.get("name") or system_info.get("systemName") or "DW Spectrum Server"

    return {
        "identifiers": {(DOMAIN, str(server_id))},
        "name": name or "DW Spectrum Server",
        "manufacturer": "Digital Watchdog",
        "model": "DW Spectrum Server",
    }


def _camera_device_info(entry: ConfigEntry, cam: dict[str, Any]) -> dict[str, Any]:
    cam_id = str(cam.get("id", "")).strip()
    name = cam.get("name") or cam_id
    model = cam.get("model") or "Camera"

    return {
        "identifiers": {(DOMAIN, f"camera_{cam_id}")},
        "name": name,
        "manufacturer": "Digital Watchdog",
        "model": model,
    }


def _camera_is_lpr(cam: dict[str, Any]) -> bool:
    """Best-effort detection of an LPR/ALPR/ANPR camera.

    DW/Nx analytics capabilities vary by plugin and camera vendor, so we rely on a
    tolerant heuristic here. This keeps the new entities limited to likely LPR cameras
    without forcing them onto every camera in the system.
    """
    parts: list[str] = []

    def collect(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, dict):
            for k, v in value.items():
                parts.append(str(k))
                collect(v)
            return
        if isinstance(value, list):
            for item in value:
                collect(item)
            return
        parts.append(str(value))

    for key in (
        "name",
        "model",
        "vendor",
        "type",
        "deviceType",
        "capabilities",
        "parameters",
        "options",
    ):
        collect(cam.get(key))

    haystack = " ".join(parts).lower()
    return bool(
        re.search(
            r"(?:license\s*plate|plate\s*number|plate\s*recogn|plate\s*reader|\balpr\b|\banpr\b|\blpr\b)",
            haystack,
        )
    )


class DwSpectrumLprCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Poll only LPR cameras for the latest detected plate."""

    def __init__(self, hass: HomeAssistant, api, cams_coordinator: DwSpectrumCoordinator) -> None:
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name="dw_spectrum_lpr",
            update_interval=timedelta(seconds=5),
        )
        self._api = api
        self._cams = cams_coordinator

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        cameras = [cam for cam in (self._cams.data or []) if isinstance(cam, dict) and _camera_is_lpr(cam)]
        cam_ids = [str(cam.get("id", "")).strip() for cam in cameras if cam.get("id")]
        cam_ids = [cam_id for cam_id in cam_ids if cam_id]

        if not cam_ids:
            return {}

        merged: dict[str, dict[str, Any]] = dict(self.data or {})
        sem = asyncio.Semaphore(4)

        async def fetch_one(camera_id: str) -> None:
            async with sem:
                try:
                    latest = await self._api.get_latest_lpr_hit(camera_id, limit=20)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning("DW Spectrum LPR lookup failed for %s: %s", camera_id, err)
                    return

                if latest:
                    merged[camera_id] = latest

        await asyncio.gather(*(fetch_one(cam_id) for cam_id in cam_ids))

        # Keep only currently detected LPR cameras in coordinator state.
        return {cam_id: merged[cam_id] for cam_id in cam_ids if cam_id in merged}


# -----------------------
# License helpers
# -----------------------
def _extract_license_counts(license_summary: dict[str, Any] | None) -> tuple[int | None, int | None, int | None]:
    """
    Returns (total, used, available).

    Supports server schema:
      { "digital": { "available": 24, "inUse": 22, "total": 24 } }
    plus generic fallbacks for other builds.
    """
    if not isinstance(license_summary, dict) or not license_summary:
        return (None, None, None)

    digital = license_summary.get("digital")
    if isinstance(digital, dict):
        total = digital.get("total")
        used = digital.get("inUse")
        avail = digital.get("available")

        def to_int(v):
            try:
                return int(v)
            except Exception:
                return None

        return (to_int(total), to_int(used), to_int(avail))

    # Generic fallbacks
    total = (
        license_summary.get("total")
        or license_summary.get("totalLicenses")
        or license_summary.get("licensesTotal")
    )
    used = (
        license_summary.get("used")
        or license_summary.get("usedLicenses")
        or license_summary.get("licensesUsed")
        or license_summary.get("inUse")
    )
    avail = license_summary.get("available") or license_summary.get("free") or license_summary.get("remaining")

    if total is None and isinstance(license_summary.get("summary"), dict):
        s = license_summary["summary"]
        total = s.get("total") or s.get("totalLicenses")
        used = used or s.get("used") or s.get("usedLicenses") or s.get("inUse")
        avail = avail or s.get("available") or s.get("free") or s.get("remaining")

    def to_int(v):
        try:
            return int(v)
        except Exception:
            return None

    return (to_int(total), to_int(used), to_int(avail))


# -----------------------
# Camera Status Coordinator
# -----------------------
class DwSpectrumCameraStatusCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Fetch /status for all cameras as: {camera_id: status_json}"""

    def __init__(self, hass: HomeAssistant, api, cams_coordinator: DwSpectrumCoordinator) -> None:
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name="dw_spectrum_camera_status",
            update_interval=timedelta(seconds=30),
        )
        self._api = api
        self._cams = cams_coordinator

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        cameras = self._cams.data or []
        cam_ids = [str(c.get("id", "")).strip() for c in cameras if isinstance(c, dict) and c.get("id")]
        cam_ids = [cid for cid in cam_ids if cid]

        results: dict[str, dict[str, Any]] = {}
        sem = asyncio.Semaphore(8)

        async def fetch_one(cid: str) -> None:
            async with sem:
                try:
                    data = await self._api.get_device_status(cid)
                    if isinstance(data, dict):
                        results[cid] = data
                except Exception:
                    return

        await asyncio.gather(*(fetch_one(cid) for cid in cam_ids))
        return results


# -----------------------
# Motion Coordinator
# -----------------------
class DwSpectrumMotionCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Callback-driven DW/Nx motion state per camera.

    This does not poll and does not auto-clear. A camera returns to Not Detected
    only when DW calls the stop/inactive callback rule.
    """

    def __init__(self, hass: HomeAssistant, api, cams_coordinator: DwSpectrumCoordinator) -> None:
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name="dw_spectrum_motion_callbacks",
            update_interval=None,
        )
        self._api = api
        self._cams = cams_coordinator
        self._states: dict[str, dict[str, Any]] = {}

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        cameras = self._cams.data or []
        cam_ids = [str(c.get("id", "")).strip().strip("{}") for c in cameras if isinstance(c, dict) and c.get("id")]
        cleaned: dict[str, dict[str, Any]] = {}
        for cid in cam_ids:
            existing = self._states.get(cid) or {}
            cleaned[cid] = {
                "state": existing.get("state") or "Not Detected",
                "last_motion_ms": existing.get("last_motion_ms"),
                "last_stop_ms": existing.get("last_stop_ms"),
                "last_event_ms": existing.get("last_event_ms"),
                "source": existing.get("source") or "dw_rule_callback",
                "raw": existing.get("raw"),
            }
        self._states = cleaned
        return dict(cleaned)

    async def async_set_motion(
        self,
        *,
        camera_id: str,
        state: str,
        event_ms: int,
        event_key: str,
        raw: dict[str, Any] | None = None,
    ) -> None:
        cid = str(camera_id or "").strip().strip("{}")
        if not cid:
            return
        existing = dict(self._states.get(cid) or {})
        existing["state"] = state
        existing["last_event_ms"] = event_ms
        existing["source"] = "dw_rule_callback"
        existing["raw"] = raw
        if event_key == "last_motion_ms":
            existing["last_motion_ms"] = event_ms
        elif event_key == "last_stop_ms":
            existing["last_stop_ms"] = event_ms
        self._states[cid] = existing

        data = dict(self.data or {})
        data[cid] = existing
        self.async_set_updated_data(data)


# -----------------------
# Setup
# -----------------------
async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    api = hass.data[DOMAIN][entry.entry_id]["api"]
    cams: DwSpectrumCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
    server: DwSpectrumServerCoordinator = hass.data[DOMAIN][entry.entry_id]["server_coordinator"]

    # Ensure initial data exists
    await cams.async_config_entry_first_refresh()
    await server.async_config_entry_first_refresh()

    # Create/reuse single coordinators per config entry
    status_coord: DwSpectrumCameraStatusCoordinator | None = hass.data[DOMAIN][entry.entry_id].get("status_coordinator")
    if status_coord is None:
        status_coord = DwSpectrumCameraStatusCoordinator(hass, api, cams)
        hass.data[DOMAIN][entry.entry_id]["status_coordinator"] = status_coord

    lpr_coord: DwSpectrumLprCoordinator | None = hass.data[DOMAIN][entry.entry_id].get("lpr_coordinator")
    if lpr_coord is None:
        lpr_coord = DwSpectrumLprCoordinator(hass, api, cams)
        hass.data[DOMAIN][entry.entry_id]["lpr_coordinator"] = lpr_coord

    motion_coord: DwSpectrumMotionCoordinator | None = hass.data[DOMAIN][entry.entry_id].get("motion_coordinator")
    if motion_coord is None:
        motion_coord = DwSpectrumMotionCoordinator(hass, api, cams)
        hass.data[DOMAIN][entry.entry_id]["motion_coordinator"] = motion_coord

    await status_coord.async_config_entry_first_refresh()
    await lpr_coord.async_config_entry_first_refresh()
    await motion_coord.async_config_entry_first_refresh()

    entities: list[SensorEntity] = [
        DwSpectrumCameraCountSensor(entry, cams, server),
        DwSpectrumLicenseTotalSensor(entry, server),
        DwSpectrumLicenseUsedSensor(entry, server),
        DwSpectrumLicenseAvailableSensor(entry, server),
    ]

    # Add per-camera status/LPR sensors
    created_cam_status: set[str] = set()
    created_lpr: set[str] = set()
    created_motion: set[str] = set()

    def add_camera_status_sensors() -> None:
        new_ents: list[SensorEntity] = []
        for cam in (cams.data or []):
            if not isinstance(cam, dict):
                continue
            cam_id = str(cam.get("id", "")).strip()
            if not cam_id:
                continue

            # NOTE: Primary/Secondary stream sensors REMOVED.
            # Only keep device-status sensors you actually want.
            for key, label, icon in (
                ("status", "Recording Status", "mdi:cctv"),
                ("init", "Init", "mdi:check-network"),
                ("media", "Media", "mdi:database-check"),
                ("stream", "Stream", "mdi:video-check"),
            ):
                uniq = f"{entry.entry_id}_cam_{cam_id}_devstatus_{key}"
                if uniq in created_cam_status:
                    continue
                created_cam_status.add(uniq)
                new_ents.append(
                    DwSpectrumCameraDeviceStatusSensor(
                        entry=entry,
                        coordinator=status_coord,
                        camera=cam,
                        camera_id=cam_id,
                        status_key=key,
                        name=label,
                        unique_id=uniq,
                        icon=icon,
                    )
                )

            motion_cam_id = str(cam_id or "").strip().strip("{}")
            motion_uniq = f"{entry.entry_id}_cam_{motion_cam_id}_motion"
            if motion_uniq not in created_motion:
                created_motion.add(motion_uniq)
                new_ents.append(DwSpectrumCameraMotionSensor(entry, motion_coord, cam, motion_cam_id))

            if _camera_is_lpr(cam):
                plate_uniq = f"{entry.entry_id}_cam_{cam_id}_lpr_last_plate"
                if plate_uniq not in created_lpr:
                    created_lpr.add(plate_uniq)
                    new_ents.append(DwSpectrumCameraLastPlateSensor(entry, lpr_coord, cam, cam_id))

                seen_uniq = f"{entry.entry_id}_cam_{cam_id}_lpr_last_seen"
                if seen_uniq not in created_lpr:
                    created_lpr.add(seen_uniq)
                    new_ents.append(DwSpectrumCameraLastPlateSeenSensor(entry, lpr_coord, cam, cam_id))

        if new_ents:
            async_add_entities(new_ents, update_before_add=False)

    add_camera_status_sensors()

    # Add the server-level sensors now
    async_add_entities(entities, update_before_add=False)

    @callback
    def handle_cams_update() -> None:
        add_camera_status_sensors()
        hass.async_create_task(status_coord.async_request_refresh())
        hass.async_create_task(lpr_coord.async_request_refresh())
        hass.async_create_task(motion_coord.async_request_refresh())

    cams.async_add_listener(handle_cams_update)


# -----------------------
# Server sensors
# -----------------------
class DwSpectrumCameraCountSensor(CoordinatorEntity[DwSpectrumCoordinator], SensorEntity):
    _attr_icon = "mdi:cctv"
    _attr_has_entity_name = True

    def __init__(
        self,
        entry: ConfigEntry,
        cams_coordinator: DwSpectrumCoordinator,
        server_coordinator: DwSpectrumServerCoordinator,
    ) -> None:
        super().__init__(cams_coordinator)
        self._entry = entry
        self._server_coordinator = server_coordinator

        self._attr_name = "Camera count"
        self._attr_unique_id = f"{entry.entry_id}_camera_count"

    @property
    def device_info(self) -> dict[str, Any]:
        system_info = (self._server_coordinator.data or {}).get("system_info")
        return _server_device_info(self._entry, system_info)

    @property
    def native_value(self) -> int | None:
        if self.coordinator.data is None:
            return None
        return len(self.coordinator.data or [])


class _BaseServerSensor(CoordinatorEntity[DwSpectrumServerCoordinator], SensorEntity):
    _attr_has_entity_name = True

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator) -> None:
        super().__init__(coordinator)
        self._entry = entry

    @property
    def device_info(self) -> dict[str, Any]:
        system_info = (self.coordinator.data or {}).get("system_info")
        return _server_device_info(self._entry, system_info)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {"raw": (self.coordinator.data or {}).get("license_summary")}


class DwSpectrumLicenseTotalSensor(_BaseServerSensor):
    _attr_icon = "mdi:license"

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator) -> None:
        super().__init__(entry, coordinator)
        self._attr_name = "Licenses total"
        self._attr_unique_id = f"{entry.entry_id}_licenses_total"

    @property
    def native_value(self) -> int | None:
        lic = (self.coordinator.data or {}).get("license_summary")
        total, _used, _avail = _extract_license_counts(lic)
        return total


class DwSpectrumLicenseUsedSensor(_BaseServerSensor):
    _attr_icon = "mdi:license"

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator) -> None:
        super().__init__(entry, coordinator)
        self._attr_name = "Licenses used"
        self._attr_unique_id = f"{entry.entry_id}_licenses_used"

    @property
    def native_value(self) -> int | None:
        lic = (self.coordinator.data or {}).get("license_summary")
        _total, used, _avail = _extract_license_counts(lic)
        return used


class DwSpectrumLicenseAvailableSensor(_BaseServerSensor):
    _attr_icon = "mdi:license"

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator) -> None:
        super().__init__(entry, coordinator)
        self._attr_name = "Licenses available"
        self._attr_unique_id = f"{entry.entry_id}_licenses_available"

    @property
    def native_value(self) -> int | None:
        lic = (self.coordinator.data or {}).get("license_summary")
        total, used, avail = _extract_license_counts(lic)

        # remaining = total - used
        if total is not None and used is not None:
            remaining = total - used
            return remaining if remaining >= 0 else 0

        return avail


# -----------------------
# Per-camera motion sensors
# -----------------------
class DwSpectrumCameraMotionSensor(CoordinatorEntity[DwSpectrumMotionCoordinator], SensorEntity):
    _attr_has_entity_name = True
    _attr_icon = "mdi:motion-sensor"

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumMotionCoordinator,
        camera: dict[str, Any],
        camera_id: str,
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._camera = camera
        self._camera_id = str(camera_id or "").strip().strip("{}")
        self._attr_name = "Motion"
        self._attr_unique_id = f"{entry.entry_id}_cam_{self._camera_id}_motion"

    @property
    def device_info(self) -> dict[str, Any]:
        return _camera_device_info(self._entry, self._camera)

    def _payload(self) -> dict[str, Any]:
        data = self.coordinator.data or {}
        return data.get(self._camera_id) or data.get("{" + self._camera_id + "}") or {}

    @property
    def native_value(self) -> str:
        return str(self._payload().get("state") or "Not Detected")

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        payload = self._payload()

        def as_dt(value: Any) -> str | None:
            if value is None:
                return None
            try:
                return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()
            except Exception:
                return None

        return {
            "last_motion": as_dt(payload.get("last_motion_ms")),
            "last_event": as_dt(payload.get("last_event_ms")),
            "last_event_type": payload.get("last_event_type"),
            "last_event_caption": payload.get("last_event_caption"),
            "last_event_description": payload.get("last_event_description"),
            "last_stop": as_dt(payload.get("last_stop_ms")),
            "source": payload.get("source"),
            "raw": payload.get("raw"),
        }


# -----------------------
# Per-camera status sensors
# -----------------------
class DwSpectrumCameraDeviceStatusSensor(CoordinatorEntity[DwSpectrumCameraStatusCoordinator], SensorEntity):
    _attr_has_entity_name = True

    def __init__(
        self,
        *,
        entry: ConfigEntry,
        coordinator: DwSpectrumCameraStatusCoordinator,
        camera: dict[str, Any],
        camera_id: str,
        status_key: str,
        name: str,
        unique_id: str,
        icon: str,
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._camera = camera
        self._camera_id = camera_id
        self._status_key = status_key

        self._attr_name = name
        self._attr_unique_id = unique_id
        self._attr_icon = icon

    @property
    def device_info(self) -> dict[str, Any]:
        return _camera_device_info(self._entry, self._camera)

    @property
    def native_value(self) -> str | None:
        payload = (self.coordinator.data or {}).get(self._camera_id) or {}
        val = payload.get(self._status_key)
        if val is None:
            return None
        return str(val)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        payload = (self.coordinator.data or {}).get(self._camera_id) or {}
        return {"raw": payload}


class _BaseLprSensor(CoordinatorEntity[DwSpectrumLprCoordinator], SensorEntity):
    _attr_has_entity_name = True

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumLprCoordinator,
        camera: dict[str, Any],
        camera_id: str,
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._camera = camera
        self._camera_id = camera_id

    @property
    def device_info(self) -> dict[str, Any]:
        return _camera_device_info(self._entry, self._camera)

    @property
    def available(self) -> bool:
        return super().available

    def _payload(self) -> dict[str, Any]:
        return (self.coordinator.data or {}).get(self._camera_id) or {}


class DwSpectrumCameraLastPlateSensor(_BaseLprSensor):
    _attr_icon = "mdi:card-text-outline"

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumLprCoordinator,
        camera: dict[str, Any],
        camera_id: str,
    ) -> None:
        super().__init__(entry, coordinator, camera, camera_id)
        self._attr_name = "Last plate"
        self._attr_unique_id = f"{entry.entry_id}_cam_{camera_id}_lpr_last_plate"

    @property
    def native_value(self) -> str | None:
        payload = self._payload()
        value = payload.get("plate")
        return str(value) if value is not None else None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        payload = self._payload()
        if not payload:
            return {}
        raw_attrs = payload.get("attributes") or {}
        norm = {re.sub(r"[^a-z0-9]", "", str(k or "").lower()): v for k, v in raw_attrs.items()}
        attrs: dict[str, Any] = {
            "recognized": bool(payload.get("recognized", False)),
            "last_seen": (
                datetime.fromtimestamp(int(payload["seen_ms"]) / 1000, tz=timezone.utc).isoformat()
                if payload.get("seen_ms")
                else None
            ),
            "track_id": payload.get("track_id"),
            "plate_raw": payload.get("plate_raw"),
            "caption": payload.get("caption"),
            "description": payload.get("description"),
            "object_type": payload.get("object_type"),
            "list_status": payload.get("list_status"),
            "confidence": payload.get("confidence"),
            "has_capture": payload.get("has_capture"),
            "country": norm.get("licenseplatecountry") or norm.get("country"),
            "lane": norm.get("lane"),
            "direction": norm.get("direction"),
            "vehicle_type": norm.get("type"),
            "color": norm.get("color"),
            "brand": norm.get("brand"),
            "best_shot": payload.get("best_shot"),
            "attributes": raw_attrs,
        }
        return attrs


class DwSpectrumCameraLastPlateSeenSensor(_BaseLprSensor):
    _attr_icon = "mdi:clock-outline"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumLprCoordinator,
        camera: dict[str, Any],
        camera_id: str,
    ) -> None:
        super().__init__(entry, coordinator, camera, camera_id)
        self._attr_name = "Last plate seen"
        self._attr_unique_id = f"{entry.entry_id}_cam_{camera_id}_lpr_last_seen"

    @property
    def native_value(self) -> datetime | None:
        payload = self._payload()
        seen_ms = payload.get("seen_ms")
        if seen_ms is None:
            return None
        try:
            return datetime.fromtimestamp(int(seen_ms) / 1000, tz=timezone.utc)
        except Exception:
            return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        payload = self._payload()
        if not payload:
            return {}
        return {
            "plate": payload.get("plate"),
            "recognized": bool(payload.get("recognized", False)),
            "track_id": payload.get("track_id"),
        }
