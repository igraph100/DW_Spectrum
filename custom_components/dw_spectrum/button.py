from __future__ import annotations

import asyncio
from typing import Any
import secrets

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, CONF_HA_CALLBACK_URL, CONF_MOTION_TOKEN, CONF_ENABLE_MOTION_RULES
from .coordinator import DwSpectrumCoordinator
from .server_coordinator import DwSpectrumServerCoordinator
from .select import DwSpectrumPtzCoordinator, _camera_device_info, _camera_is_ptz


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



class DwSpectrumCreateMotionRulesButton(CoordinatorEntity[DwSpectrumServerCoordinator], ButtonEntity):
    """Server-level button that creates/recreates HA DW motion callback rules."""

    _attr_has_entity_name = True
    _attr_name = "Create HA motion rules"
    _attr_icon = "mdi:plus-circle-outline"

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator, cameras: DwSpectrumCoordinator, api) -> None:
        super().__init__(coordinator)
        self._hass = hass
        self._entry = entry
        self._cameras = cameras
        self._api = api
        self._attr_unique_id = f"{entry.entry_id}_create_motion_rules"
        self._last_result: dict[str, Any] | None = None

    @property
    def device_info(self) -> dict[str, Any]:
        system_info = (self.coordinator.data or {}).get("system_info")
        return _server_device_info(self._entry, system_info)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {"last_result": self._last_result}

    async def async_press(self) -> None:
        callback_source = self._entry.options if CONF_HA_CALLBACK_URL in self._entry.options else self._entry.data
        callback_url = str(callback_source.get(CONF_HA_CALLBACK_URL) or "").strip().rstrip("/")
        if not callback_url:
            self._last_result = {
                "ok": False,
                "error": "missing_callback_url",
                "message": "Set the Home Assistant callback URL in integration options first.",
            }
            self.async_write_ha_state()
            return

        token = str(self._entry.options.get(CONF_MOTION_TOKEN) or self._entry.data.get(CONF_MOTION_TOKEN) or "")
        if not token:
            token = secrets.token_urlsafe(32)
            new_options = dict(self._entry.options)
            new_options[CONF_HA_CALLBACK_URL] = callback_url
            new_options[CONF_ENABLE_MOTION_RULES] = True
            new_options[CONF_MOTION_TOKEN] = token
            self._hass.config_entries.async_update_entry(self._entry, options=new_options)
            self._hass.data[DOMAIN][self._entry.entry_id]["motion_token"] = token

        await self._cameras.async_request_refresh()
        cameras = [c for c in (self._cameras.data or []) if isinstance(c, dict)]
        self._last_result = await self._api.ensure_motion_callback_rules(
            cameras=cameras,
            callback_base_url=callback_url,
            entry_id=self._entry.entry_id,
            token=token,
        )
        self._hass.data[DOMAIN][self._entry.entry_id]["motion_rules_result"] = self._last_result
        self.async_write_ha_state()

class DwSpectrumDeleteMotionRulesButton(CoordinatorEntity[DwSpectrumServerCoordinator], ButtonEntity):
    """Server-level button that deletes the HA-created DW motion callback rules."""

    _attr_has_entity_name = True
    _attr_name = "Delete HA motion rules"
    _attr_icon = "mdi:delete-alert-outline"

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator, api) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._attr_unique_id = f"{entry.entry_id}_delete_motion_rules"
        self._last_result: dict[str, Any] | None = None

    @property
    def device_info(self) -> dict[str, Any]:
        system_info = (self.coordinator.data or {}).get("system_info")
        return _server_device_info(self._entry, system_info)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {"last_result": self._last_result}

    async def async_press(self) -> None:
        self._last_result = await self._api.delete_motion_callback_rules(entry_id=self._entry.entry_id)
        self.async_write_ha_state()


class DwSpectrumRestartServerButton(CoordinatorEntity[DwSpectrumServerCoordinator], ButtonEntity):
    """Server-level button that restarts the current DW Spectrum server."""

    _attr_has_entity_name = True
    _attr_name = "Restart Server"
    _attr_icon = "mdi:restart-alert"

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator, api) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._attr_unique_id = f"{entry.entry_id}_restart_server"
        self._last_result: dict[str, Any] | None = None

    @property
    def device_info(self) -> dict[str, Any]:
        system_info = (self.coordinator.data or {}).get("system_info")
        return _server_device_info(self._entry, system_info)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {"last_result": self._last_result}

    async def async_press(self) -> None:
        try:
            # "this" tells DW/Nx to restart the server that receives the request.
            self._last_result = await self._api.restart_server("this")
        except Exception as err:  # noqa: BLE001
            self._last_result = {
                "ok": False,
                "error": type(err).__name__,
                "message": str(err),
            }
        self.async_write_ha_state()


class DwSpectrumCameraPtzStepButton(CoordinatorEntity[DwSpectrumPtzCoordinator], ButtonEntity):
    _attr_has_entity_name = True

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumPtzCoordinator,
        api,
        camera: dict[str, Any],
        *,
        name: str,
        icon: str,
        pan_delta: float = 0.0,
        tilt_delta: float = 0.0,
        zoom_delta: float = 0.0,
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._camera = camera
        self._camera_id = str(camera.get("id", "")).strip()
        self._pan_delta = pan_delta
        self._tilt_delta = tilt_delta
        self._zoom_delta = zoom_delta
        self._attr_name = name
        self._attr_icon = icon
        slug = name.lower().replace(" ", "_")
        self._attr_unique_id = f"{entry.entry_id}_cam_{self._camera_id}_ptz_{slug}_button"

    @property
    def available(self) -> bool:
        payload = (self.coordinator.data or {}).get(self._camera_id) or {}
        has_ptz_payload = False
        if isinstance(payload, dict):
            if payload.get("option_to_id"):
                has_ptz_payload = True
            else:
                position = payload.get("position")
                has_ptz_payload = isinstance(position, dict) and any(position.get(k) is not None for k in ("pan", "tilt", "zoom"))
        return super().available and _camera_is_ptz(self._camera) and has_ptz_payload

    @property
    def device_info(self) -> dict[str, Any] | None:
        return _camera_device_info(self._entry, self._camera)

    async def async_press(self) -> None:
        await self._api.move_ptz_logical_step(
            self._camera_id,
            pan_delta=self._pan_delta,
            tilt_delta=self._tilt_delta,
            zoom_delta=self._zoom_delta,
            speed=0.1,
        )
        await asyncio.sleep(0.7)
        await self.coordinator.async_request_refresh()


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    api = hass.data[DOMAIN][entry.entry_id]["api"]
    cams: DwSpectrumCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
    server: DwSpectrumServerCoordinator = hass.data[DOMAIN][entry.entry_id]["server_coordinator"]

    ptz_coord: DwSpectrumPtzCoordinator | None = hass.data[DOMAIN][entry.entry_id].get("ptz_coordinator")
    if ptz_coord is None:
        ptz_coord = DwSpectrumPtzCoordinator(hass, api, cams)
        hass.data[DOMAIN][entry.entry_id]["ptz_coordinator"] = ptz_coord

    created_cam_ids: set[str] = set()

    def add_camera_buttons(cameras: list[dict[str, Any]]) -> None:
        new_entities: list[ButtonEntity] = []
        ptz_data = ptz_coord.data or {}
        for cam in cameras:
            cam_id = str(cam.get("id", "")).strip()
            if not cam_id or cam_id in created_cam_ids or not _camera_is_ptz(cam):
                continue

            payload = ptz_data.get(cam_id) if isinstance(ptz_data, dict) else None
            has_ptz_payload = False
            if isinstance(payload, dict):
                if payload.get("option_to_id"):
                    has_ptz_payload = True
                else:
                    position = payload.get("position")
                    has_ptz_payload = isinstance(position, dict) and any(position.get(k) is not None for k in ("pan", "tilt", "zoom"))

            if not has_ptz_payload:
                continue

            created_cam_ids.add(cam_id)
            new_entities.extend([
                DwSpectrumCameraPtzStepButton(entry, ptz_coord, api, cam, name="Move Up", icon="mdi:arrow-up-bold", tilt_delta=-1.0),
                DwSpectrumCameraPtzStepButton(entry, ptz_coord, api, cam, name="Move Down", icon="mdi:arrow-down-bold", tilt_delta=1.0),
                DwSpectrumCameraPtzStepButton(entry, ptz_coord, api, cam, name="Move Left", icon="mdi:arrow-left-bold", pan_delta=-1.0),
                DwSpectrumCameraPtzStepButton(entry, ptz_coord, api, cam, name="Move Right", icon="mdi:arrow-right-bold", pan_delta=1.0),
                DwSpectrumCameraPtzStepButton(entry, ptz_coord, api, cam, name="Zoom In", icon="mdi:magnify-plus-outline", zoom_delta=5.0),
                DwSpectrumCameraPtzStepButton(entry, ptz_coord, api, cam, name="Zoom Out", icon="mdi:magnify-minus-outline", zoom_delta=-5.0),
            ])

        if new_entities:
            async_add_entities(new_entities, update_before_add=False)

    await cams.async_config_entry_first_refresh()
    await server.async_config_entry_first_refresh()
    await ptz_coord.async_config_entry_first_refresh()

    async_add_entities([
        DwSpectrumCreateMotionRulesButton(hass, entry, server, cams, api),
        DwSpectrumDeleteMotionRulesButton(entry, server, api),
        DwSpectrumRestartServerButton(entry, server, api),
    ], update_before_add=False)
    add_camera_buttons(cams.data or [])

    @callback
    def handle_cams_update() -> None:
        add_camera_buttons(cams.data or [])
        hass.async_create_task(ptz_coord.async_request_refresh())

    cams.async_add_listener(handle_cams_update)
