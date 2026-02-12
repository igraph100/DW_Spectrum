from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

from homeassistant.components.sensor import SensorEntity
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

    # Create/reuse a single status coordinator per config entry
    status_coord: DwSpectrumCameraStatusCoordinator | None = hass.data[DOMAIN][entry.entry_id].get("status_coordinator")
    if status_coord is None:
        status_coord = DwSpectrumCameraStatusCoordinator(hass, api, cams)
        hass.data[DOMAIN][entry.entry_id]["status_coordinator"] = status_coord

    await status_coord.async_config_entry_first_refresh()

    entities: list[SensorEntity] = [
        DwSpectrumCameraCountSensor(entry, cams, server),
        DwSpectrumLicenseTotalSensor(entry, server),
        DwSpectrumLicenseUsedSensor(entry, server),
        DwSpectrumLicenseAvailableSensor(entry, server),
    ]

    # Add per-camera status sensors
    created_cam_status: set[str] = set()

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

        if new_ents:
            async_add_entities(new_ents, update_before_add=False)

    add_camera_status_sensors()

    # Add the server-level sensors now
    async_add_entities(entities, update_before_add=False)

    @callback
    def handle_cams_update() -> None:
        add_camera_status_sensors()
        hass.async_create_task(status_coord.async_request_refresh())

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
