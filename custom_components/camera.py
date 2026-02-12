from __future__ import annotations

from typing import Any
from urllib.parse import quote

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import CONNECTION_NETWORK_MAC
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import DwSpectrumCoordinator

SIGNAL_STREAM_BLOCK_CHANGED = "dw_spectrum_stream_block_changed"


def _strip_braces(guid: str) -> str:
    return guid.strip().strip("{").strip("}")


def _build_rtsp_url(entry: ConfigEntry, camera_id: str, stream_index: int) -> str:
    """
    DW Spectrum server-proxied RTSP streams.

    Primary:   stream=0
    Secondary: stream=1
    """
    host = entry.data.get("host")
    port = entry.data.get("port")

    username = entry.data.get("username", "")
    password = entry.data.get("password", "")

    userinfo = ""
    if username or password:
        u = quote(str(username), safe="")
        p = quote(str(password), safe="")
        userinfo = f"{u}:{p}@"

    cam_path_id = _strip_braces(str(camera_id))
    return f"rtsp://{userinfo}{host}:{port}/{cam_path_id}?stream={int(stream_index)}"


def _is_stream_blocked(hass: HomeAssistant, entry: ConfigEntry, camera_id: str) -> bool:
    data = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
    cache = data.get("stream_block_cache") or {}
    return bool(cache.get(camera_id, False))


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: DwSpectrumCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]

    # First refresh so entities are created immediately on setup.
    await coordinator.async_config_entry_first_refresh()

    created: set[str] = set()

    def add_entities_from_data() -> None:
        new_entities: list[Camera] = []

        for dev in coordinator.data or []:
            cam_id = str(dev.get("id", "")).strip()
            if not cam_id:
                continue

            # 1) Existing thumbnail camera entity (kept)
            uniq_thumb = f"{entry.entry_id}:{cam_id}:thumb"
            if uniq_thumb not in created:
                created.add(uniq_thumb)
                new_entities.append(DwSpectrumCamera(coordinator, dev, entry, hass))

            # NOTE:
            # Primary/Secondary RTSP stream camera entities REMOVED on purpose.
            # (This prevents camera.*primary_stream and camera.*secondary_stream from being created.)

        if new_entities:
            async_add_entities(new_entities)

    # Initial add
    add_entities_from_data()

    @callback
    def _handle_update() -> None:
        add_entities_from_data()

    coordinator.async_add_listener(_handle_update)


class DwSpectrumBaseCamera(CoordinatorEntity[DwSpectrumCoordinator], Camera):
    """Shared device_info + coordinator update logic for all DW Spectrum camera entities."""

    _attr_should_poll = False

    def __init__(
        self,
        coordinator: DwSpectrumCoordinator,
        dev: dict[str, Any],
        entry: ConfigEntry,
        hass: HomeAssistant,
    ) -> None:
        # Initialize CoordinatorEntity
        CoordinatorEntity.__init__(self, coordinator)
        # IMPORTANT: initialize HA Camera base so internal attrs exist (incl. _webrtc_provider)
        Camera.__init__(self)

        if not hasattr(self, "_webrtc_provider"):
            self._webrtc_provider = None

        self._hass = hass
        self._entry = entry
        self._dev = dev
        self._id = str(dev.get("id", "")).strip()
        self._name = dev.get("name") or dev.get("logicalId") or self._id

        self._unsub_dispatcher = None

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()

        # Update camera entities instantly when the stream-block switch changes
        signal = f"{SIGNAL_STREAM_BLOCK_CHANGED}_{self._entry.entry_id}"

        @callback
        def _on_change() -> None:
            self.async_write_ha_state()

        self._unsub_dispatcher = async_dispatcher_connect(self._hass, signal, _on_change)

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_dispatcher:
            self._unsub_dispatcher()
            self._unsub_dispatcher = None
        await super().async_will_remove_from_hass()

    @property
    def available(self) -> bool:
        # If stream is blocked, we still keep entity "available" so it doesn't vanish,
        # but it will not provide stream/image.
        if "isOnline" in self._dev:
            return bool(self._dev.get("isOnline"))
        return super().available

    @property
    def device_info(self) -> dict[str, Any]:
        # IMPORTANT: Match sensors/switches device identifiers so you get ONE device per camera
        identifiers = {(DOMAIN, f"camera_{self._id}")}

        mac = self._dev.get("physicalId") or self._dev.get("mac")
        connections = set()
        if mac and isinstance(mac, str) and ":" in mac:
            connections.add((CONNECTION_NETWORK_MAC, mac.lower()))

        info: dict[str, Any] = {
            "identifiers": identifiers,
            "name": self._name,
            "manufacturer": "Digital Watchdog",
            "model": self._dev.get("model") or self._dev.get("type") or "DW Spectrum Device",
        }
        if connections:
            info["connections"] = connections
        return info

    async def async_camera_image(self, width: int | None = None, height: int | None = None) -> bytes | None:
        # If blocked, no thumbnail either (prevents any “preview”)
        if _is_stream_blocked(self._hass, self._entry, self._id):
            return None
        return await self.coordinator.api.get_device_image(self._id)

    def _handle_coordinator_update(self) -> None:
        # Update local device snapshot from coordinator data
        if self.coordinator.data:
            for dev in self.coordinator.data:
                if str(dev.get("id", "")).strip() == self._id:
                    self._dev = dev
                    self._name = dev.get("name") or dev.get("logicalId") or self._id
                    break
        super()._handle_coordinator_update()


class DwSpectrumCamera(DwSpectrumBaseCamera):
    """Thumbnail-based camera entity (kept)."""

    def __init__(self, coordinator: DwSpectrumCoordinator, dev: dict[str, Any], entry: ConfigEntry, hass: HomeAssistant) -> None:
        super().__init__(coordinator, dev, entry, hass)
        self._attr_unique_id = f"{self._id}_thumb"
        self._attr_name = self._name


# Kept in file (unused now) so you don't break imports/references if anything else still references it.
# But it will NOT be created because async_setup_entry no longer adds these entities.
class DwSpectrumRtspStreamCamera(DwSpectrumBaseCamera):
    """RTSP stream camera entity (primary or secondary) for each DW camera."""

    _attr_supported_features = CameraEntityFeature.STREAM

    def __init__(
        self,
        coordinator: DwSpectrumCoordinator,
        dev: dict[str, Any],
        entry: ConfigEntry,
        hass: HomeAssistant,
        stream_index: int,
    ) -> None:
        super().__init__(coordinator, dev, entry, hass)
        self._stream_index = int(stream_index)

        suffix = "Primary Stream" if self._stream_index == 0 else "Secondary Stream"
        self._attr_unique_id = f"{self._id}_rtsp_{self._stream_index}"
        self._attr_name = f"{self._name} {suffix}"

    async def async_stream_source(self) -> str | None:
        # If blocked, HA has no stream URL to play at all
        if _is_stream_blocked(self._hass, self._entry, self._id):
            return None
        return _build_rtsp_url(self._entry, self._id, self._stream_index)
