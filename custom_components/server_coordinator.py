from __future__ import annotations

from datetime import timedelta
import logging
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import DwSpectrumApi, DwSpectrumConnectionError

_LOGGER = logging.getLogger(__name__)


class DwSpectrumServerCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator that refreshes server info, users, and license summary."""

    def __init__(self, hass: HomeAssistant, api: DwSpectrumApi, scan_interval: int = 15) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name="DW Spectrum Server",
            update_interval=timedelta(seconds=scan_interval),
        )
        self.api = api

    async def _async_update_data(self) -> dict[str, Any]:
        # Always try to fetch system info; if that fails, entry should be considered unavailable.
        try:
            system_info = await self.api.get_system_info()
        except DwSpectrumConnectionError as err:
            raise UpdateFailed(str(err)) from err

        # Users + licenses should NOT prevent the integration from loading.
        users: list[dict[str, Any]] = []
        license_summary: dict[str, Any] = {}

        try:
            users = await self.api.get_users()
        except DwSpectrumConnectionError as err:
            _LOGGER.warning("DW Spectrum: users fetch failed (continuing): %s", err)

        try:
            license_summary = await self.api.get_license_summary()
        except DwSpectrumConnectionError as err:
            _LOGGER.warning("DW Spectrum: license fetch failed (continuing): %s", err)

        return {
            "system_info": system_info,
            "users": users,
            "license_summary": license_summary,
        }
