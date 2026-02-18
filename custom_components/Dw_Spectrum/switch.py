from __future__ import annotations

from collections.abc import Callable
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import DwSpectrumCoordinator
from .server_coordinator import DwSpectrumServerCoordinator

STORAGE_VERSION = 1
SIGNAL_STREAM_BLOCK_CHANGED = "dw_spectrum_stream_block_changed"


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


# -----------------------
# User attribute helpers
# -----------------------
def _as_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "on"):
        return True
    if s in ("false", "0", "no", "n", "off"):
        return False
    return None


def _pick(u: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        v = u.get(k)
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _infer_user_role(u: dict[str, Any]) -> str | None:
    for k in ("role", "userRole", "user_role", "type", "userType", "group", "userGroup", "accessRole"):
        v = u.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    if _as_bool(u.get("isAdmin")) is True or _as_bool(u.get("admin")) is True:
        return "admin"
    if _as_bool(u.get("isPowerUser")) is True:
        return "power_user"
    if _as_bool(u.get("isLiveViewer")) is True:
        return "live_viewer"

    v = u.get("roleId") or u.get("role_id") or u.get("accessLevel")
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        return str(v)

    return None


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

    # -----------------------
    # PERSISTED CACHE: recording mode
    # -----------------------
    store = Store(hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}.recording_mode_cache")
    persisted = await store.async_load()
    persisted = persisted if isinstance(persisted, dict) else {}

    mode_cache: dict[str, str] = hass.data[DOMAIN][entry.entry_id].setdefault("recording_mode_cache", {})
    for cam_id, mode in persisted.items():
        if isinstance(cam_id, str) and isinstance(mode, str) and mode in ("always", "motion", "motion_low"):
            mode_cache[cam_id] = mode

    async def _save_mode_cache() -> None:
        clean: dict[str, str] = {
            k: v
            for k, v in mode_cache.items()
            if isinstance(k, str) and isinstance(v, str) and v in ("always", "motion", "motion_low")
        }
        await store.async_save(clean)

    def schedule_save() -> None:
        hass.async_create_task(_save_mode_cache())

    # -----------------------
    # PERSISTED CACHE: stream block
    # -----------------------
    stream_store = Store(hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}.stream_block_cache")
    stream_persisted = await stream_store.async_load()
    stream_persisted = stream_persisted if isinstance(stream_persisted, dict) else {}

    stream_block_cache: dict[str, bool] = hass.data[DOMAIN][entry.entry_id].setdefault("stream_block_cache", {})
    for cam_id, v in stream_persisted.items():
        if isinstance(cam_id, str):
            stream_block_cache[cam_id] = bool(v)

    async def _save_stream_block_cache() -> None:
        clean: dict[str, bool] = {k: bool(v) for k, v in stream_block_cache.items() if isinstance(k, str)}
        await stream_store.async_save(clean)

    def schedule_stream_save_and_notify() -> None:
        hass.async_create_task(_save_stream_block_cache())
        async_dispatcher_send(hass, f"{SIGNAL_STREAM_BLOCK_CHANGED}_{entry.entry_id}")

    created_user_ids: set[str] = set()
    created_cam_keys: set[str] = set()

    def add_user_switches(users: list[dict[str, Any]]) -> None:
        new_entities: list[SwitchEntity] = []
        for u in users:
            user_id = str(u.get("id", "")).strip()
            if not user_id or user_id in created_user_ids:
                continue
            created_user_ids.add(user_id)
            new_entities.append(DwSpectrumUserEnabledSwitch(entry, server, api, u))
        if new_entities:
            async_add_entities(new_entities, update_before_add=False)

    def add_camera_switches(cameras: list[dict[str, Any]]) -> None:
        new_entities: list[SwitchEntity] = []
        for cam in cameras:
            cam_id = str(cam.get("id", "")).strip()
            if not cam_id:
                continue

            # NEW: Live stream blocked switch (HA-side, persisted)
            key = f"{cam_id}:stream_blocked"
            if key not in created_cam_keys:
                created_cam_keys.add(key)
                new_entities.append(
                    DwSpectrumCameraStreamBlockedSwitch(
                        entry, cams, cam_id, stream_block_cache, schedule_stream_save_and_notify
                    )
                )

            # Recording disabled switch (DW schedule)
            key = f"{cam_id}:disabled"
            if key not in created_cam_keys:
                created_cam_keys.add(key)
                new_entities.append(
                    DwSpectrumCameraRecordingDisabledSwitch(entry, cams, api, cam_id, mode_cache, schedule_save)
                )

            # Mode switches
            for mode_key, label in (
                ("always", "Always Record"),
                ("motion_low", "Motion + Low Res"),
                ("motion", "Motion Only"),
            ):
                key = f"{cam_id}:{mode_key}"
                if key in created_cam_keys:
                    continue
                created_cam_keys.add(key)
                new_entities.append(
                    DwSpectrumCameraRecordingModeSwitch(
                        entry, cams, api, cam_id, label, mode_key, mode_cache, schedule_save
                    )
                )

        if new_entities:
            async_add_entities(new_entities, update_before_add=False)

    await server.async_config_entry_first_refresh()
    await cams.async_config_entry_first_refresh()

    add_user_switches((server.data or {}).get("users") or [])
    add_camera_switches(cams.data or [])

    @callback
    def handle_server_update() -> None:
        add_user_switches((server.data or {}).get("users") or [])

    @callback
    def handle_cams_update() -> None:
        add_camera_switches(cams.data or [])

    server.async_add_listener(handle_server_update)
    cams.async_add_listener(handle_cams_update)


# -----------------------
# User enable switch (Server device) + ATTRIBUTES
# -----------------------
class DwSpectrumUserEnabledSwitch(CoordinatorEntity[DwSpectrumServerCoordinator], SwitchEntity):
    _attr_icon = "mdi:account"
    _attr_has_entity_name = True

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumServerCoordinator, api, user: dict[str, Any]) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._user_id = str(user.get("id", "")).strip()
        self._user: dict[str, Any] = user

        self._username = user.get("fullName") or user.get("name") or user.get("email") or self._user_id
        self._attr_name = self._username
        self._attr_unique_id = f"{entry.entry_id}_user_enabled_{self._user_id}"

    @property
    def device_info(self) -> dict[str, Any]:
        system_info = (self.coordinator.data or {}).get("system_info")
        return _server_device_info(self._entry, system_info)

    def _get_user(self) -> dict[str, Any] | None:
        users = (self.coordinator.data or {}).get("users") or []
        for u in users:
            if str(u.get("id", "")).strip() == self._user_id:
                return u
        return None

    def _handle_coordinator_update(self) -> None:
        u = self._get_user()
        if isinstance(u, dict):
            self._user = u
            self._username = u.get("fullName") or u.get("name") or u.get("email") or self._user_id
            self._attr_name = self._username
        super()._handle_coordinator_update()

    @property
    def is_on(self) -> bool:
        u = self._get_user()
        return bool(u.get("isEnabled", False)) if u else False

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        u = self._user if isinstance(self._user, dict) else {}

        attrs: dict[str, Any] = {
            "user_id": self._user_id,
            "full_name": _pick(u, "fullName", "name", "displayName"),
            "username": _pick(u, "username", "login", "userName"),
            "email": _pick(u, "email", "userEmail", "mail"),
            "role": _infer_user_role(u),
            "group": _pick(u, "group", "userGroup"),
            "cloud_user": _as_bool(_pick(u, "isCloud", "cloud", "cloudUser", "isCloudUser")),
            "enabled": bool(u.get("isEnabled", False)),
            "is_admin": _as_bool(_pick(u, "isAdmin", "admin")),
            "is_power_user": _as_bool(_pick(u, "isPowerUser")),
            "is_live_viewer": _as_bool(_pick(u, "isLiveViewer")),
            "created_at": _pick(u, "createdAt", "created_at"),
            "last_login": _pick(u, "lastLogin", "last_login"),
        }

        perms = _pick(u, "permissions", "permission", "access", "rights")
        if isinstance(perms, (list, dict)):
            attrs["permissions"] = perms

        return {k: v for k, v in attrs.items() if v is not None}

    async def async_turn_on(self, **kwargs: Any) -> None:
        await self._api.set_user_enabled(self._user_id, True)
        await self.coordinator.async_request_refresh()

    async def async_turn_off(self, **kwargs: Any) -> None:
        await self._api.set_user_enabled(self._user_id, False)
        await self.coordinator.async_request_refresh()


# -----------------------
# NEW: Live Stream Blocked switch (HA-side)
# -----------------------
class DwSpectrumCameraStreamBlockedSwitch(CoordinatorEntity[DwSpectrumCoordinator], SwitchEntity):
    """
    ON  = block all HA live streams + thumbnails for this camera
    OFF = allow HA live streams
    """
    _attr_icon = "mdi:cctv-off"
    _attr_has_entity_name = True
    _attr_name = "Block Live Stream in HA"

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumCoordinator,
        camera_id: str,
        cache: dict[str, bool],
        save_and_notify: Callable[[], None],
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._camera_id = camera_id
        self._cache = cache
        self._save_and_notify = save_and_notify
        self._attr_unique_id = f"{entry.entry_id}_cam_{camera_id}_stream_blocked"

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
    def is_on(self) -> bool:
        return bool(self._cache.get(self._camera_id, False))

    async def async_turn_on(self, **kwargs: Any) -> None:
        self._cache[self._camera_id] = True
        self._save_and_notify()
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        self._cache[self._camera_id] = False
        self._save_and_notify()
        self.async_write_ha_state()


# -----------------------
# Camera: Recording disabled switch (DW schedule)
# -----------------------
class DwSpectrumCameraRecordingDisabledSwitch(CoordinatorEntity[DwSpectrumCoordinator], SwitchEntity):
    _attr_icon = "mdi:record-off"
    _attr_has_entity_name = True
    _attr_name = "Recording Disabled"

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumCoordinator,
        api,
        camera_id: str,
        mode_cache: dict[str, str],
        save_cache: Callable[[], None],
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._camera_id = camera_id
        self._mode_cache = mode_cache
        self._save_cache = save_cache
        self._attr_unique_id = f"{entry.entry_id}_cam_{camera_id}_rec_disabled"

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
    def is_on(self) -> bool:
        cam = self._get_camera() or {}
        schedule = cam.get("schedule") or {}
        if not isinstance(schedule, dict):
            return False
        return not bool(schedule.get("isEnabled", False))

    async def async_turn_on(self, **kwargs: Any) -> None:
        cam = self._get_camera() or {}
        schedule = cam.get("schedule") or {}
        if isinstance(schedule, dict) and schedule.get("isEnabled", False):
            mode = _schedule_mode(schedule)
            if mode in ("always", "motion", "motion_low"):
                self._mode_cache[self._camera_id] = mode
                self._save_cache()

        await self._api.set_camera_schedule_enabled(self._camera_id, False)
        await self.coordinator.async_request_refresh()

    async def async_turn_off(self, **kwargs: Any) -> None:
        mode = self._mode_cache.get(self._camera_id, "always")
        await self._api.set_camera_recording_mode(self._camera_id, mode)
        if mode in ("always", "motion", "motion_low"):
            self._mode_cache[self._camera_id] = mode
            self._save_cache()
        await self.coordinator.async_request_refresh()


# -----------------------
# Camera recording mode switches
# -----------------------
class DwSpectrumCameraRecordingModeSwitch(CoordinatorEntity[DwSpectrumCoordinator], SwitchEntity):
    _attr_icon = "mdi:record-rec"
    _attr_has_entity_name = True

    def __init__(
        self,
        entry: ConfigEntry,
        coordinator: DwSpectrumCoordinator,
        api,
        camera_id: str,
        label: str,
        mode_key: str,
        mode_cache: dict[str, str],
        save_cache: Callable[[], None],
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._camera_id = camera_id
        self._mode_key = mode_key
        self._mode_cache = mode_cache
        self._save_cache = save_cache

        self._attr_name = label
        self._attr_unique_id = f"{entry.entry_id}_cam_{camera_id}_rec_{mode_key}"

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
    def is_on(self) -> bool:
        cam = self._get_camera() or {}
        schedule = cam.get("schedule") or {}
        if not isinstance(schedule, dict):
            return False
        if not schedule.get("isEnabled", False):
            return False
        mode = _schedule_mode(schedule)
        return mode == self._mode_key

    async def async_turn_on(self, **kwargs: Any) -> None:
        await self._api.set_camera_recording_mode(self._camera_id, self._mode_key)
        self._mode_cache[self._camera_id] = self._mode_key
        self._save_cache()
        await self.coordinator.async_request_refresh()

    async def async_turn_off(self, **kwargs: Any) -> None:
        await self.coordinator.async_request_refresh()
