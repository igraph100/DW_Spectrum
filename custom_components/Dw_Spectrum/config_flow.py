from __future__ import annotations

import logging
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import (
    DwSpectrumApi,
    DwSpectrumConfig,
    DwSpectrumAuthError,
    DwSpectrumConnectionError,
)
from .const import (
    DOMAIN,
    CONF_HOST,
    CONF_PORT,
    CONF_SSL,
    CONF_VERIFY_SSL,
    CONF_USERNAME,
    CONF_PASSWORD,
    DEFAULT_PORT,
    DEFAULT_SSL,
    DEFAULT_VERIFY_SSL,
)

_LOGGER = logging.getLogger(__name__)

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): str,
        vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
        vol.Optional(CONF_SSL, default=DEFAULT_SSL): bool,
        vol.Optional(CONF_VERIFY_SSL, default=DEFAULT_VERIFY_SSL): bool,
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): str,
    }
)


async def _validate_input(hass: HomeAssistant, data: dict) -> None:
    session = async_get_clientsession(hass)
    cfg = DwSpectrumConfig(
        host=data[CONF_HOST],
        port=data[CONF_PORT],
        ssl=data[CONF_SSL],
        verify_ssl=data[CONF_VERIFY_SSL],
        username=data[CONF_USERNAME],
        password=data[CONF_PASSWORD],
    )
    api = DwSpectrumApi(session, cfg)
    await api.validate()


class DwSpectrumConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            unique = f"{user_input[CONF_HOST]}:{user_input[CONF_PORT]}"
            await self.async_set_unique_id(unique)
            self._abort_if_unique_id_configured()

            try:
                await _validate_input(self.hass, user_input)

            except DwSpectrumAuthError as err:
                _LOGGER.warning("DW Spectrum auth failed for %s: %s", unique, err)
                errors["base"] = "invalid_auth"

            except DwSpectrumConnectionError as err:
                # This is the important one: now you will see the real reason in logs
                _LOGGER.exception("DW Spectrum cannot_connect for %s: %s", unique, err)
                errors["base"] = "cannot_connect"

            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("DW Spectrum unknown error for %s: %s", unique, err)
                errors["base"] = "unknown"

            else:
                return self.async_create_entry(title=unique, data=user_input)

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
        )
