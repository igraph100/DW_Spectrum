from __future__ import annotations

from datetime import timedelta
import logging
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import DwSpectrumApi, DwSpectrumConnectionError

_LOGGER = logging.getLogger(__name__)


class DwSpectrumCoordinator(DataUpdateCoordinator[list[dict[str, Any]]]):
    """Coordinator that refreshes camera/device inventory from DW Spectrum."""

    def __init__(self, hass: HomeAssistant, api: DwSpectrumApi, scan_interval: int = 15) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name="DW Spectrum Cameras",
            update_interval=timedelta(seconds=scan_interval),
        )
        self.api = api

    async def _async_update_data(self) -> list[dict[str, Any]]:
        try:
            return await self.api.get_cameras()
        except DwSpectrumConnectionError as err:
            raise UpdateFailed(str(err)) from err
        except Exception as err:
            raise UpdateFailed(f"Unexpected error: {err}") from err
