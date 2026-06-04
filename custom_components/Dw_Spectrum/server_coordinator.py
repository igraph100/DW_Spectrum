from __future__ import annotations

from datetime import timedelta
import logging
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import DwSpectrumApi, DwSpectrumConnectionError  # noqa: F401 (re-exported)

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

        # Users, groups, and licenses should NOT prevent the integration from loading.
        users: list[dict[str, Any]] = []
        user_groups: list[dict[str, Any]] = []
        license_summary: dict[str, Any] = {}

        try:
            users = await self.api.get_users()
        except DwSpectrumConnectionError as err:
            _LOGGER.warning("DW Spectrum: users fetch failed (continuing): %s", err)

        try:
            user_groups = await self.api.get_user_groups()
        except DwSpectrumConnectionError as err:
            _LOGGER.warning("DW Spectrum: user groups fetch failed (continuing): %s", err)

        try:
            license_summary = await self.api.get_license_summary()
        except DwSpectrumConnectionError as err:
            _LOGGER.warning("DW Spectrum: license fetch failed (continuing): %s", err)

        # Build a group id → name lookup, normalising ids (strip braces/whitespace).
        group_name_by_id: dict[str, str] = {}
        for g in user_groups:
            raw_id = str(g.get("id") or "").strip().strip("{}")
            name = str(g.get("name") or "").strip()
            if raw_id and name:
                group_name_by_id[raw_id.lower()] = name

        # groupIds and permissions are already present on the user object returned by
        # GET /rest/v3/users — no need for per-user permission endpoint calls.
        # (permissions is "none" for most users because permissions live on the group.)
        enriched_users: list[dict[str, Any]] = []
        for user in users:
            u = dict(user)

            # Raw permissions string on the user (often "none" — real perms are on the group).
            u["_dw_permissions"] = str(u.get("permissions") or "").strip()

            # Resolve group names from groupIds already in the user object.
            group_ids_raw: list[str] = []
            for ids_field in ("groupIds", "group_ids", "userGroupIds"):
                val = u.get(ids_field)
                if isinstance(val, list):
                    group_ids_raw = [str(v).strip().strip("{}") for v in val if v]
                    break

            group_names = [group_name_by_id[i.lower()] for i in group_ids_raw if i.lower() in group_name_by_id]
            u["_dw_group_names"] = group_names
            u["_dw_group_name"] = ", ".join(group_names) if group_names else None

            enriched_users.append(u)

        return {
            "system_info": system_info,
            "users": enriched_users,
            "user_groups": user_groups,
            "license_summary": license_summary,
        }


class DwSpectrumMetricsCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Poll /rest/v4/metrics/values every 30 seconds for server health data."""

    def __init__(self, hass: HomeAssistant, api: DwSpectrumApi, scan_interval: int = 30) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name="DW Spectrum Metrics",
            update_interval=timedelta(seconds=scan_interval),
        )
        self.api = api

    async def _async_update_data(self) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        alarms: dict[str, Any] = {}
        update_info: dict[str, Any] = {}
        update_status: dict[str, Any] = {}
        try:
            metrics = await self.api.get_server_metrics()
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("DW Spectrum: metrics fetch failed: %s", err)
        try:
            alarms = await self.api.get_server_alarms()
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("DW Spectrum: alarms fetch failed: %s", err)
        try:
            update_info = await self.api.get_server_update_info()
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("DW Spectrum: update info fetch failed: %s", err)
        try:
            update_status = await self.api.get_server_update_status()
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("DW Spectrum: update status fetch failed: %s", err)
        return {"metrics": metrics, "alarms": alarms, "update_info": update_info, "update_status": update_status}
