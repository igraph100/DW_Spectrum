from __future__ import annotations

import asyncio
import logging
import re
from datetime import timedelta
from typing import Any

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity, DataUpdateCoordinator

from .const import DOMAIN
from .coordinator import DwSpectrumCoordinator

_LOGGER = logging.getLogger(__name__)


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


def _camera_is_ptz(cam: dict[str, Any]) -> bool:
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

    for key in ("name", "model", "vendor", "type", "typeId", "deviceType", "capabilities", "parameters", "options"):
        collect(cam.get(key))

    haystack = " ".join(parts).lower()
    if re.search(r"(?:\bptz\b|pan\s*tilt\s*zoom|speed\s*dome)", haystack):
        return True

    options = cam.get("options") if isinstance(cam.get("options"), dict) else {}
    if options.get("isControlEnabled") is True:
        return True

    return False


def _position_triplet(position: dict[str, Any] | None) -> tuple[float | None, float | None, float | None]:
    if not isinstance(position, dict):
        return None, None, None

    def _to_float(v: Any) -> float | None:
        try:
            if v is None or v == "":
                return None
            return float(v)
        except (TypeError, ValueError):
            return None

    return _to_float(position.get("pan")), _to_float(position.get("tilt")), _to_float(position.get("zoom"))


def _position_is_known(position: dict[str, Any] | None) -> bool:
    pan, tilt, zoom = _position_triplet(position)
    return pan is not None or tilt is not None or zoom is not None


def _positions_match(current: dict[str, Any] | None, learned: dict[str, Any] | None) -> bool:
    cur_pan, cur_tilt, cur_zoom = _position_triplet(current)
    ref_pan, ref_tilt, ref_zoom = _position_triplet(learned)

    if ref_pan is None and ref_tilt is None and ref_zoom is None:
        return False

    def _close(a: float | None, b: float | None, tol: float) -> bool:
        if a is None or b is None:
            return a is None and b is None
        return abs(a - b) <= tol

    return (
        _close(cur_pan, ref_pan, 1.0)
        and _close(cur_tilt, ref_tilt, 1.0)
        and _close(cur_zoom, ref_zoom, 2.0)
    )


class DwSpectrumPtzCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Poll PTZ presets and current PTZ position for PTZ-capable cameras."""

    def __init__(self, hass: HomeAssistant, api, cams_coordinator: DwSpectrumCoordinator) -> None:
        super().__init__(
            hass=hass,
            logger=_LOGGER,
            name="dw_spectrum_ptz",
            update_interval=timedelta(seconds=15),
        )
        self._api = api
        self._cams = cams_coordinator
        self._last_selected: dict[str, str] = {}
        self._learned_positions: dict[str, dict[str, dict[str, Any]]] = {}

    def set_last_selected(self, camera_id: str, option: str) -> None:
        self._last_selected[camera_id] = option
        learned_by_option = self._learned_positions.setdefault(camera_id, {})
        learned_by_option.pop(option, None)

    def _ptz_camera_ids(self) -> list[str]:
        ids: list[str] = []
        for cam in (self._cams.data or []):
            if not isinstance(cam, dict) or not _camera_is_ptz(cam):
                continue
            cam_id = str(cam.get("id", "")).strip()
            if cam_id:
                ids.append(cam_id)
        return ids

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        cam_ids = self._ptz_camera_ids()
        if not cam_ids:
            return {}

        results: dict[str, dict[str, Any]] = {}
        sem = asyncio.Semaphore(4)

        async def fetch_one(camera_id: str) -> None:
            async with sem:
                try:
                    presets = await self._api.get_ptz_presets(camera_id)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.debug("DW Spectrum PTZ preset lookup failed for %s: %s", camera_id, err)
                    presets = []

                try:
                    position = await self._api.get_ptz_position(camera_id)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.debug("DW Spectrum PTZ position lookup failed for %s: %s", camera_id, err)
                    position = {}

                preset_items: list[dict[str, Any]] = []
                option_to_id: dict[str, str] = {}
                seen_labels: dict[str, int] = {}
                for preset in presets:
                    if not isinstance(preset, dict):
                        continue
                    preset_id = str(preset.get("id", "")).strip()
                    base_label = str(preset.get("name") or preset_id).strip()
                    if not base_label or not preset_id:
                        continue
                    seen_labels[base_label] = seen_labels.get(base_label, 0) + 1
                    label = base_label if seen_labels[base_label] == 1 else f"{base_label} ({seen_labels[base_label]})"
                    option_to_id[label] = preset_id
                    preset_items.append({"id": preset_id, "name": base_label, "label": label})

                position = position if isinstance(position, dict) else {}
                selected = self._last_selected.get(camera_id)
                if selected and selected not in option_to_id:
                    selected = None
                    self._last_selected.pop(camera_id, None)

                effective_selected: str | None = selected
                if selected and _position_is_known(position):
                    learned_by_option = self._learned_positions.setdefault(camera_id, {})
                    learned = learned_by_option.get(selected)
                    if learned is None:
                        learned_by_option[selected] = {
                            "pan": position.get("pan"),
                            "tilt": position.get("tilt"),
                            "zoom": position.get("zoom"),
                        }
                    elif not _positions_match(position, learned):
                        effective_selected = "Unknown"

                results[camera_id] = {
                    "presets": preset_items,
                    "option_to_id": option_to_id,
                    "position": position,
                    "selected": effective_selected,
                }

        await asyncio.gather(*(fetch_one(cam_id) for cam_id in cam_ids))
        return results


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    api = hass.data[DOMAIN][entry.entry_id]["api"]
    cams: DwSpectrumCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]

    ptz_coord: DwSpectrumPtzCoordinator | None = hass.data[DOMAIN][entry.entry_id].get("ptz_coordinator")
    if ptz_coord is None:
        ptz_coord = DwSpectrumPtzCoordinator(hass, api, cams)
        hass.data[DOMAIN][entry.entry_id]["ptz_coordinator"] = ptz_coord

    created_recording_cam_ids: set[str] = set()
    created_ptz_cam_ids: set[str] = set()

    def add_camera_selects(cameras: list[dict[str, Any]]) -> None:
        new_entities: list[SelectEntity] = []
        ptz_data = ptz_coord.data or {}
        for cam in cameras:
            cam_id = str(cam.get("id", "")).strip()
            if not cam_id:
                continue

            if cam_id not in created_recording_cam_ids:
                created_recording_cam_ids.add(cam_id)
                new_entities.append(DwSpectrumCameraRecordingModeSelect(entry, cams, api, cam_id))

            payload = ptz_data.get(cam_id) if isinstance(ptz_data, dict) else None
            has_ptz_payload = False
            if isinstance(payload, dict):
                if payload.get("option_to_id"):
                    has_ptz_payload = True
                else:
                    position = payload.get("position")
                    has_ptz_payload = isinstance(position, dict) and any(
                        position.get(k) is not None for k in ("pan", "tilt", "zoom")
                    )

            if _camera_is_ptz(cam) and has_ptz_payload and cam_id not in created_ptz_cam_ids:
                created_ptz_cam_ids.add(cam_id)
                new_entities.append(DwSpectrumCameraPtzPresetSelect(entry, ptz_coord, api, cam))

        if new_entities:
            async_add_entities(new_entities, update_before_add=False)

    await cams.async_config_entry_first_refresh()
    await ptz_coord.async_config_entry_first_refresh()
    add_camera_selects(cams.data or [])

    @callback
    def handle_cams_update() -> None:
        add_camera_selects(cams.data or [])
        hass.async_create_task(ptz_coord.async_request_refresh())

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


class DwSpectrumCameraPtzPresetSelect(CoordinatorEntity[DwSpectrumPtzCoordinator], SelectEntity):
    _attr_icon = "mdi:cctv"
    _attr_has_entity_name = True
    _attr_name = "PTZ Preset"

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumPtzCoordinator, api, camera: dict[str, Any]) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._camera = camera
        self._camera_id = str(camera.get("id", "")).strip()
        self._attr_unique_id = f"{entry.entry_id}_cam_{self._camera_id}_ptz_preset_select"
        self._attr_options = []

    def _payload(self) -> dict[str, Any]:
        return (self.coordinator.data or {}).get(self._camera_id) or {}

    @property
    def available(self) -> bool:
        return super().available and _camera_is_ptz(self._camera)

    @property
    def device_info(self) -> dict[str, Any] | None:
        return _camera_device_info(self._entry, self._camera)

    @property
    def options(self) -> list[str]:
        payload = self._payload()
        preset_options = list((payload.get("option_to_id") or {}).keys())
        return ["Unknown", *preset_options]

    @property
    def current_option(self) -> str | None:
        payload = self._payload()
        selected = payload.get("selected")
        return str(selected) if selected is not None else "Unknown"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        payload = self._payload()
        position = payload.get("position") if isinstance(payload.get("position"), dict) else {}
        return {
            "pan": position.get("pan"),
            "tilt": position.get("tilt"),
            "zoom": position.get("zoom"),
        }

    async def async_select_option(self, option: str) -> None:
        if option == "Unknown":
            return

        payload = self._payload()
        option_to_id = payload.get("option_to_id") or {}
        preset_id = option_to_id.get(option)
        if not preset_id:
            return

        await self._api.activate_ptz_preset(self._camera_id, str(preset_id))
        self.coordinator.set_last_selected(self._camera_id, option)
        await asyncio.sleep(1.5)
        await self.coordinator.async_request_refresh()
        self.async_write_ha_state()
