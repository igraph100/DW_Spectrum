from __future__ import annotations

from typing import Any

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import DwSpectrumCoordinator


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


def _schedule_mode(schedule: dict[str, Any]) -> str | None:
    tasks = schedule.get("tasks") or []
    if not isinstance(tasks, list) or not tasks:
        return None

    rec_types: set[str] = set()
    meta_types: set[str] = set()

    for t in tasks:
        if not isinstance(t, dict):
            continue
        rec_types.add(str(t.get("recordingType", "")).strip())
        meta_types.add(str(t.get("metadataTypes", "")).strip())

    if rec_types == {"always"} and meta_types == {"none"}:
        return "always"
    if rec_types == {"metadataOnly"} and meta_types == {"motion"}:
        return "motion"
    if rec_types == {"metadataAndLowQuality"} and meta_types == {"motion"}:
        return "motion_low"

    return "unknown"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    api = hass.data[DOMAIN][entry.entry_id]["api"]
    cams: DwSpectrumCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]

    created_cam_ids: set[str] = set()

    def add_camera_selects(cameras: list[dict[str, Any]]) -> None:
        new_entities: list[SelectEntity] = []
        for cam in cameras:
            cam_id = str(cam.get("id", "")).strip()
            if not cam_id or cam_id in created_cam_ids:
                continue
            created_cam_ids.add(cam_id)
            new_entities.append(DwSpectrumCameraRecordingModeSelect(entry, cams, api, cam_id))
        if new_entities:
            async_add_entities(new_entities, update_before_add=False)

    await cams.async_config_entry_first_refresh()
    add_camera_selects(cams.data or [])

    @callback
    def handle_cams_update() -> None:
        add_camera_selects(cams.data or [])

    cams.async_add_listener(handle_cams_update)


class DwSpectrumCameraRecordingModeSelect(CoordinatorEntity[DwSpectrumCoordinator], SelectEntity):
    """
    Recording mode dropdown:

    - Always Record
    - Motion + Low Res
    - Motion Only
    - Recording Disabled
    """
    _attr_icon = "mdi:record-rec"
    _attr_has_entity_name = True
    _attr_name = "Recording Mode"

    _MODE_TO_LABEL: dict[str, str] = {
        "always": "Always Record",
        "motion_low": "Motion + Low Res",
        "motion": "Motion Only",
        "disabled": "Recording Disabled",
    }
    _LABEL_TO_MODE: dict[str, str] = {v: k for k, v in _MODE_TO_LABEL.items()}

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumCoordinator, api, camera_id: str) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._camera_id = camera_id

        self._attr_unique_id = f"{entry.entry_id}_cam_{camera_id}_rec_mode_select"
        self._attr_options = list(self._LABEL_TO_MODE.keys())

    def _get_camera(self) -> dict[str, Any] | None:
        for cam in (self.coordinator.data or []):
            if str(cam.get("id", "")).strip() == self._camera_id:
                return cam
        return None

    @property
    def device_info(self) -> dict[str, Any] | None:
        cam = self._get_camera()
        return _camera_device_info(self._entry, cam) if cam else None

    @property
    def current_option(self) -> str | None:
        cam = self._get_camera() or {}
        schedule = cam.get("schedule") or {}
        if not isinstance(schedule, dict):
            return None

        if not schedule.get("isEnabled", False):
            return self._MODE_TO_LABEL["disabled"]

        mode = _schedule_mode(schedule)
        if mode in ("always", "motion", "motion_low"):
            return self._MODE_TO_LABEL[mode]

        # If Spectrum returns a schedule we don't recognize, keep the select unset
        return None

    async def async_select_option(self, option: str) -> None:
        mode_key = self._LABEL_TO_MODE.get(option)
        if mode_key is None:
            return

        if mode_key == "disabled":
            await self._api.set_camera_schedule_enabled(self._camera_id, False)
            await self.coordinator.async_request_refresh()
            return

        await self._api.set_camera_recording_mode(self._camera_id, mode_key)
        await self.coordinator.async_request_refresh()
