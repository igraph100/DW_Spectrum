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


def _camera_audio_supported(cam: dict[str, Any]) -> bool:
    parameters = cam.get("parameters") if isinstance(cam.get("parameters"), dict) else {}
    media_caps = parameters.get("mediaCapabilities") if isinstance(parameters.get("mediaCapabilities"), dict) else {}
    options = cam.get("options") if isinstance(cam.get("options"), dict) else {}

    return bool(
        parameters.get("isAudioSupported")
        or media_caps.get("hasAudio")
        or parameters.get("audioCodec")
        or ("isAudioEnabled" in options)
    )


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


# Permission tokens from /rest/v4/users/{id}/permissions ordered by privilege level.
# The highest-privilege token present wins.
_PERMISSION_ROLE_MAP: list[tuple[str, str]] = [
    ("administrator", "Administrator"),
    ("poweruser", "Power User"),
    ("viewarchive", "Advanced Viewer"),
    ("viewbookmarks", "Viewer"),
    ("view", "Live Viewer"),
]


def _role_from_permissions(permissions_str: str) -> str | None:
    """Parse a pipe-delimited DW permissions string and return the primary role label."""
    if not permissions_str:
        return None
    tokens = {p.strip().lower() for p in permissions_str.split("|") if p.strip()}
    for token, label in _PERMISSION_ROLE_MAP:
        if token in tokens:
            return label
    return None


def _infer_user_role(u: dict[str, Any]) -> str | None:
    # 1. Group name from /rest/v4/userGroups — covers both built-in groups
    #    ("Power Users", "Administrators") and any custom roles the admin created.
    group_name = u.get("_dw_group_name")
    if isinstance(group_name, str) and group_name.strip():
        return group_name.strip()

    # 2. Parse the permissions string — fallback when no group is resolved
    #    (e.g. older server that doesn't support /rest/v4/userGroups).
    role = _role_from_permissions(str(u.get("_dw_permissions") or ""))
    if role:
        return role

    # 3. Last resort — explicit role fields only, never "type"/"userType" (those = cloud/local).
    for k in ("role", "userRole", "user_role", "userGroup", "accessRole"):
        v = u.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

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
    # PERSISTED CACHE: stream block (HA-side)
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

            audio_key = f"{cam_id}:audio_enabled"
            if audio_key not in created_cam_keys:
                created_cam_keys.add(audio_key)
                new_entities.append(DwSpectrumCameraAudioEnabledSwitch(entry, cams, api, cam_id))

            block_key = f"{cam_id}:stream_blocked"
            if block_key in created_cam_keys:
                continue
            created_cam_keys.add(block_key)

            new_entities.append(
                DwSpectrumCameraStreamBlockedSwitch(
                    entry, cams, cam_id, stream_block_cache, schedule_stream_save_and_notify
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

        # Resolved group name(s) from the /rest/v4/userGroups lookup.
        group_names: list[str] = u.get("_dw_group_names") or []
        group_name: str | None = u.get("_dw_group_name") or (group_names[0] if group_names else None)

        attrs: dict[str, Any] = {
            "user_id": self._user_id,
            "full_name": _pick(u, "fullName", "name", "displayName"),
            "username": _pick(u, "username", "login", "userName"),
            "email": _pick(u, "email", "userEmail", "mail"),
            "role": _infer_user_role(u),
            "permissions": u.get("_dw_permissions") or None,
            "group_name": group_name,
            "group_names": group_names if len(group_names) > 1 else None,
            "user_type": _pick(u, "type", "userType"),
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
# Camera audio enable switch (server-side)
# -----------------------
class DwSpectrumCameraAudioEnabledSwitch(CoordinatorEntity[DwSpectrumCoordinator], SwitchEntity):
    _attr_has_entity_name = True
    _attr_name = "Audio"

    def __init__(self, entry: ConfigEntry, coordinator: DwSpectrumCoordinator, api, camera_id: str) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._api = api
        self._camera_id = camera_id
        self._attr_unique_id = f"{entry.entry_id}_cam_{camera_id}_audio_enabled"

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
    def available(self) -> bool:
        return self._get_camera() is not None

    @property
    def icon(self) -> str:
        return "mdi:volume-high" if self.is_on else "mdi:volume-off"

    @property
    def is_on(self) -> bool:
        cam = self._get_camera() or {}
        options = cam.get("options") if isinstance(cam.get("options"), dict) else {}
        return bool(options.get("isAudioEnabled", False))

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        cam = self._get_camera() or {}
        parameters = cam.get("parameters") if isinstance(cam.get("parameters"), dict) else {}
        return {
            "camera_id": self._camera_id,
            "audio_supported": _camera_audio_supported(cam),
            "audio_codec": parameters.get("audioCodec"),
            "raw_audio_flag": (cam.get("options") or {}).get("isAudioEnabled") if isinstance(cam.get("options"), dict) else None,
        }

    async def async_turn_on(self, **kwargs: Any) -> None:
        await self._api.set_camera_audio_enabled(self._camera_id, True)
        await self.coordinator.async_request_refresh()

    async def async_turn_off(self, **kwargs: Any) -> None:
        await self._api.set_camera_audio_enabled(self._camera_id, False)
        await self.coordinator.async_request_refresh()


# -----------------------
# Live Stream Blocked switch (HA-side)
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
