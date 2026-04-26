from __future__ import annotations

import logging
import secrets
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import HomeAssistant, callback
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
    CONF_HA_CALLBACK_URL,
    CONF_MOTION_TOKEN,
    CONF_ENABLE_MOTION_RULES,
)

_LOGGER = logging.getLogger(__name__)

MOTION_CALLBACK_HELP = (
    "Motion sensors are optional. To receive DW Spectrum motion detection in Home Assistant, "
    "enter a Home Assistant URL that the DW server can reach, for example "
    "http://192.168.1.50:8123 for local network use, or https://ha.example.com for remote use. "
    "When this is filled in, the integration creates private DW event rules that call Home Assistant "
    "when each camera motion starts and stops. Leave it blank to skip motion rule creation."
)


def _user_schema() -> vol.Schema:
    # Intentionally default username/password to blank. This prevents stale browser/UI
    # values from being shown when adding another DW server.
    return vol.Schema(
        {
            vol.Required(CONF_HOST, default=""): str,
            vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
            vol.Optional(CONF_SSL, default=DEFAULT_SSL): bool,
            vol.Optional(CONF_VERIFY_SSL, default=DEFAULT_VERIFY_SSL): bool,
            vol.Required(CONF_USERNAME, default=""): str,
            vol.Required(CONF_PASSWORD, default=""): str,
            vol.Optional(CONF_HA_CALLBACK_URL, default=""): str,
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


def _normalize_entry_data(data: dict, existing_token: str | None = None) -> dict:
    out = dict(data)
    callback_url = str(out.get(CONF_HA_CALLBACK_URL, "") or "").strip().rstrip("/")
    out[CONF_HA_CALLBACK_URL] = callback_url
    out[CONF_ENABLE_MOTION_RULES] = bool(callback_url)
    token = existing_token or out.get(CONF_MOTION_TOKEN)
    if callback_url and not token:
        token = secrets.token_urlsafe(32)
    if token:
        out[CONF_MOTION_TOKEN] = token
    return out


class DwSpectrumConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return DwSpectrumOptionsFlowHandler(config_entry)

    async def async_step_user(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            unique = f"{user_input[CONF_HOST]}:{user_input[CONF_PORT]}"
            await self.async_set_unique_id(unique)
            self._abort_if_unique_id_configured()

            try:
                await _validate_input(self.hass, user_input)
                user_input = _normalize_entry_data(user_input)
            except DwSpectrumAuthError as err:
                _LOGGER.warning("DW Spectrum auth failed for %s: %s", unique, err)
                errors["base"] = "invalid_auth"
            except DwSpectrumConnectionError as err:
                _LOGGER.exception("DW Spectrum cannot_connect for %s: %s", unique, err)
                errors["base"] = "cannot_connect"
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("DW Spectrum unknown error for %s: %s", unique, err)
                errors["base"] = "unknown"
            else:
                return self.async_create_entry(title=unique, data=user_input)

        return self.async_show_form(
            step_id="user",
            data_schema=_user_schema(),
            errors=errors,
            description_placeholders={"motion_callback_help": MOTION_CALLBACK_HELP},
        )

    async def async_step_reconfigure(self, user_input: dict | None = None) -> FlowResult:
        """Reconfigure an existing DW Spectrum entry without pre-filling credentials."""
        errors: dict[str, str] = {}
        entry = self._get_reconfigure_entry()
        data = dict(entry.data)

        schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default=data.get(CONF_HOST, "")): str,
                vol.Optional(CONF_PORT, default=data.get(CONF_PORT, DEFAULT_PORT)): int,
                vol.Optional(CONF_SSL, default=data.get(CONF_SSL, DEFAULT_SSL)): bool,
                vol.Optional(CONF_VERIFY_SSL, default=data.get(CONF_VERIFY_SSL, DEFAULT_VERIFY_SSL)): bool,
                vol.Required(CONF_USERNAME, default=""): str,
                vol.Required(CONF_PASSWORD, default=""): str,
                vol.Optional(
                    CONF_HA_CALLBACK_URL,
                    default=str(
                        (entry.options if CONF_HA_CALLBACK_URL in entry.options else data).get(CONF_HA_CALLBACK_URL)
                        or ""
                    ),
                ): str,
            }
        )

        if user_input is not None:
            unique = f"{user_input[CONF_HOST]}:{user_input[CONF_PORT]}"
            try:
                await _validate_input(self.hass, user_input)
                new_data = dict(data)
                new_data.update(user_input)
                new_data = _normalize_entry_data(
                    new_data,
                    existing_token=str(entry.options.get(CONF_MOTION_TOKEN) or data.get(CONF_MOTION_TOKEN) or ""),
                )
            except DwSpectrumAuthError as err:
                _LOGGER.warning("DW Spectrum reconfigure auth failed for %s: %s", unique, err)
                errors["base"] = "invalid_auth"
            except DwSpectrumConnectionError as err:
                _LOGGER.exception("DW Spectrum reconfigure cannot_connect for %s: %s", unique, err)
                errors["base"] = "cannot_connect"
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("DW Spectrum reconfigure unknown error for %s: %s", unique, err)
                errors["base"] = "unknown"
            else:
                self.hass.config_entries.async_update_entry(entry, title=unique, data=new_data)
                await self.hass.config_entries.async_reload(entry.entry_id)
                return self.async_abort(reason="reconfigure_successful")

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=schema,
            errors=errors,
            description_placeholders={"motion_callback_help": MOTION_CALLBACK_HELP},
        )



class DwSpectrumOptionsFlowHandler(config_entries.OptionsFlow):
    """Options flow used by Home Assistant's Configure button."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry

    def _connection_schema(self) -> vol.Schema:
        data = self._config_entry.data or {}
        opts = self._config_entry.options or {}
        return vol.Schema(
            {
                vol.Required(CONF_HOST, default=data.get(CONF_HOST, "")): str,
                vol.Optional(CONF_PORT, default=data.get(CONF_PORT, DEFAULT_PORT)): int,
                vol.Optional(CONF_SSL, default=data.get(CONF_SSL, DEFAULT_SSL)): bool,
                vol.Optional(CONF_VERIFY_SSL, default=data.get(CONF_VERIFY_SSL, DEFAULT_VERIFY_SSL)): bool,
                vol.Required(CONF_USERNAME, default=data.get(CONF_USERNAME, "")): str,
                vol.Optional(CONF_PASSWORD, default=""): str,
                vol.Optional(
                    CONF_HA_CALLBACK_URL,
                    default=str(
                        (opts if CONF_HA_CALLBACK_URL in opts else data).get(CONF_HA_CALLBACK_URL)
                        or ""
                    ),
                ): str,
            }
        )

    async def async_step_init(self, user_input: dict | None = None) -> FlowResult:
        """Show a Configure menu like the working YidCal options flow."""
        return self.async_show_menu(
            step_id="init",
            menu_options=["connection", "motion"],
        )

    async def async_step_connection(self, user_input: dict | None = None) -> FlowResult:
        """Edit DW Spectrum connection/login settings from Configure."""
        errors: dict[str, str] = {}
        entry = self._config_entry
        current_data = dict(entry.data)

        if user_input is not None:
            effective_input = dict(user_input)

            # Leave password blank to keep the saved password. This avoids exposing
            # the current password in the Configure form while still allowing edits
            # to host/port/SSL/username/callback URL.
            if not str(effective_input.get(CONF_PASSWORD, "") or "").strip():
                effective_input[CONF_PASSWORD] = current_data.get(CONF_PASSWORD, "")

            unique = f"{effective_input[CONF_HOST]}:{effective_input[CONF_PORT]}"

            try:
                await _validate_input(self.hass, effective_input)
                new_data = dict(current_data)
                new_data.update(effective_input)
                new_data = _normalize_entry_data(
                    new_data,
                    existing_token=str(
                        entry.options.get(CONF_MOTION_TOKEN)
                        or current_data.get(CONF_MOTION_TOKEN)
                        or ""
                    ),
                )
            except DwSpectrumAuthError as err:
                _LOGGER.warning("DW Spectrum Configure auth failed for %s: %s", unique, err)
                errors["base"] = "invalid_auth"
            except DwSpectrumConnectionError as err:
                _LOGGER.exception("DW Spectrum Configure cannot_connect for %s: %s", unique, err)
                errors["base"] = "cannot_connect"
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("DW Spectrum Configure unknown error for %s: %s", unique, err)
                errors["base"] = "unknown"
            else:
                # Keep callback URL/token in options too, because options are what
                # Home Assistant's Configure button edits and they intentionally
                # override old entry.data values, including a blank callback URL.
                new_options = dict(entry.options)
                callback_url = str(new_data.get(CONF_HA_CALLBACK_URL, "") or "").strip().rstrip("/")
                new_options[CONF_HA_CALLBACK_URL] = callback_url
                new_options[CONF_ENABLE_MOTION_RULES] = bool(callback_url)
                token = new_options.get(CONF_MOTION_TOKEN) or new_data.get(CONF_MOTION_TOKEN)
                if token:
                    new_options[CONF_MOTION_TOKEN] = token

                # Save the real connection settings on entry.data, not only options.
                self.hass.config_entries.async_update_entry(
                    entry,
                    title=unique,
                    data=new_data,
                    options=new_options,
                )
                await self.hass.config_entries.async_reload(entry.entry_id)

                return self.async_create_entry(title="", data=new_options)

        return self.async_show_form(
            step_id="connection",
            data_schema=self._connection_schema(),
            errors=errors,
            description_placeholders={"motion_callback_help": MOTION_CALLBACK_HELP},
        )

    async def async_step_motion(self, user_input: dict | None = None) -> FlowResult:
        """Edit only the HA callback URL used for automatic motion rules."""
        errors: dict[str, str] = {}
        source = self._config_entry.options if CONF_HA_CALLBACK_URL in self._config_entry.options else self._config_entry.data
        current_url = str(source.get(CONF_HA_CALLBACK_URL) or "").strip().rstrip("/")
        schema = vol.Schema({vol.Optional(CONF_HA_CALLBACK_URL, default=current_url): str})

        if user_input is not None:
            callback_url = str(user_input.get(CONF_HA_CALLBACK_URL, "") or "").strip().rstrip("/")
            opts = dict(self._config_entry.options)
            opts[CONF_HA_CALLBACK_URL] = callback_url
            opts[CONF_ENABLE_MOTION_RULES] = bool(callback_url)
            token = opts.get(CONF_MOTION_TOKEN) or self._config_entry.data.get(CONF_MOTION_TOKEN)
            if callback_url and not token:
                token = secrets.token_urlsafe(32)
            if token:
                opts[CONF_MOTION_TOKEN] = token
            return self.async_create_entry(title="", data=opts)

        return self.async_show_form(
            step_id="motion",
            data_schema=schema,
            errors=errors,
            description_placeholders={
                "motion_callback_help": (
                    MOTION_CALLBACK_HELP
                    + " Clearing this field disables future automatic rule creation, but use the server button Delete HA motion rules to remove rules that were already created."
                )
            },
        )

