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
    CONF_ENABLE_RTSP,
    CONF_RTSP_MAIN_STREAM,
    CONF_RTSP_SUB_STREAM,
    DEFAULT_ENABLE_RTSP,
    DEFAULT_RTSP_MAIN_STREAM,
    DEFAULT_RTSP_SUB_STREAM,
)

_LOGGER = logging.getLogger(__name__)

RTSP_HELP = (
    "You'll receive an MJPEG thumbnail stream regardless of whether RTSP is enabled. "
    "Only enable RTSP if you have a specific need for it — for example, to convert a camera feed "
    "to WebRTC format. When enabled, select which streams to expose: Main Stream (stream=0) is the "
    "full-resolution primary feed, Sub Stream (stream=1) is the lower-resolution secondary feed."
)

MOTION_CALLBACK_HELP = (
    "Motion sensors work automatically without any configuration — no callback URL needed. "
    "The integration polls the DW Spectrum API every 5 seconds; motion will be detected within "
    "3–15 seconds of it starting and will clear 20–30 seconds after it stops. "
    "If you need instant motion detection (under 1 second), enter a Home Assistant URL that the "
    "DW server can reach — for example http://192.168.1.50:8123 for local network use or "
    "https://ha.example.com for remote access. When filled in, the integration creates DW event "
    "rules that push motion start/stop to Home Assistant in real time. Leave blank to use the "
    "default API polling instead."
)


def _rtsp_schema(
    enable_rtsp: bool = DEFAULT_ENABLE_RTSP,
    main_stream: bool = DEFAULT_RTSP_MAIN_STREAM,
    sub_stream: bool = DEFAULT_RTSP_SUB_STREAM,
) -> vol.Schema:
    return vol.Schema(
        {
            vol.Optional(CONF_ENABLE_RTSP, default=enable_rtsp): bool,
            vol.Optional(CONF_RTSP_MAIN_STREAM, default=main_stream): bool,
            vol.Optional(CONF_RTSP_SUB_STREAM, default=sub_stream): bool,
        }
    )


def _user_schema() -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_HOST, default=""): str,
            vol.Optional(CONF_PORT, description={"suggested_value": str(DEFAULT_PORT)}): str,
            vol.Optional(CONF_SSL, default=DEFAULT_SSL): bool,
            vol.Optional(CONF_VERIFY_SSL, default=DEFAULT_VERIFY_SSL): bool,
            vol.Required(CONF_USERNAME, default=""): str,
            vol.Required(CONF_PASSWORD, default=""): str,
            vol.Optional(CONF_HA_CALLBACK_URL, default=""): str,
        }
    )


def _coerce_port_value(val):
    """Convert string/int/None to int or None."""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip()
    return int(s) if s else None


async def _validate_input(hass: HomeAssistant, data: dict) -> None:
    session = async_get_clientsession(hass)
    cfg = DwSpectrumConfig(
        host=data[CONF_HOST],
        port=data.get(CONF_PORT) or None,
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

    _pending_data: dict
    _pending_reconfigure_data: dict

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return DwSpectrumOptionsFlowHandler(config_entry)

    async def async_step_user(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            user_input[CONF_PORT] = _coerce_port_value(user_input.get(CONF_PORT))

            port = user_input[CONF_PORT]
            unique = f"{user_input[CONF_HOST]}:{port}" if port else user_input[CONF_HOST]
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
            except Exception as err:
                _LOGGER.exception("DW Spectrum unknown error for %s: %s", unique, err)
                errors["base"] = "unknown"
            else:
                self._pending_data = user_input
                return await self.async_step_rtsp()

        return self.async_show_form(
            step_id="user",
            data_schema=_user_schema(),
            errors=errors,
            description_placeholders={"motion_callback_help": MOTION_CALLBACK_HELP},
        )

    async def async_step_rtsp(self, user_input: dict | None = None) -> FlowResult:
        if user_input is not None:
            data = dict(self._pending_data)
            data[CONF_ENABLE_RTSP] = user_input.get(CONF_ENABLE_RTSP, DEFAULT_ENABLE_RTSP)
            data[CONF_RTSP_MAIN_STREAM] = user_input.get(CONF_RTSP_MAIN_STREAM, DEFAULT_RTSP_MAIN_STREAM)
            data[CONF_RTSP_SUB_STREAM] = user_input.get(CONF_RTSP_SUB_STREAM, DEFAULT_RTSP_SUB_STREAM)
            port = data[CONF_PORT]
            unique = f"{data[CONF_HOST]}:{port}" if port else data[CONF_HOST]
            return self.async_create_entry(title=unique, data=data)

        return self.async_show_form(
            step_id="rtsp",
            data_schema=_rtsp_schema(),
            description_placeholders={"rtsp_help": RTSP_HELP},
        )

    async def async_step_reconfigure(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        entry = self._get_reconfigure_entry()
        data = dict(entry.data)

        schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default=data.get(CONF_HOST, "")): str,
                vol.Optional(CONF_PORT, description={"suggested_value": str(data.get(CONF_PORT) or DEFAULT_PORT)}): str,
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
            user_input[CONF_PORT] = _coerce_port_value(user_input.get(CONF_PORT))

            port = user_input[CONF_PORT]
            unique = f"{user_input[CONF_HOST]}:{port}" if port else user_input[CONF_HOST]
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
            except Exception as err:
                _LOGGER.exception("DW Spectrum reconfigure unknown error for %s: %s", unique, err)
                errors["base"] = "unknown"
            else:
                self._pending_reconfigure_data = new_data
                return await self.async_step_reconfigure_rtsp()

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=schema,
            errors=errors,
            description_placeholders={"motion_callback_help": MOTION_CALLBACK_HELP},
        )

    async def async_step_reconfigure_rtsp(self, user_input: dict | None = None) -> FlowResult:
        entry = self._get_reconfigure_entry()
        data = entry.data

        if user_input is not None:
            final_data = dict(self._pending_reconfigure_data)
            final_data[CONF_ENABLE_RTSP] = user_input.get(CONF_ENABLE_RTSP, DEFAULT_ENABLE_RTSP)
            final_data[CONF_RTSP_MAIN_STREAM] = user_input.get(CONF_RTSP_MAIN_STREAM, DEFAULT_RTSP_MAIN_STREAM)
            final_data[CONF_RTSP_SUB_STREAM] = user_input.get(CONF_RTSP_SUB_STREAM, DEFAULT_RTSP_SUB_STREAM)
            port = final_data[CONF_PORT]
            unique = f"{final_data[CONF_HOST]}:{port}" if port else final_data[CONF_HOST]
            self.hass.config_entries.async_update_entry(entry, title=unique, data=final_data)
            await self.hass.config_entries.async_reload(entry.entry_id)
            return self.async_abort(reason="reconfigure_successful")

        return self.async_show_form(
            step_id="reconfigure_rtsp",
            data_schema=_rtsp_schema(
                enable_rtsp=data.get(CONF_ENABLE_RTSP, DEFAULT_ENABLE_RTSP),
                main_stream=data.get(CONF_RTSP_MAIN_STREAM, DEFAULT_RTSP_MAIN_STREAM),
                sub_stream=data.get(CONF_RTSP_SUB_STREAM, DEFAULT_RTSP_SUB_STREAM),
            ),
            description_placeholders={"rtsp_help": RTSP_HELP},
        )


class DwSpectrumOptionsFlowHandler(config_entries.OptionsFlow):
    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry

    def _connection_schema(self) -> vol.Schema:
        data = self._config_entry.data or {}
        opts = self._config_entry.options or {}
        return vol.Schema(
            {
                vol.Required(CONF_HOST, default=data.get(CONF_HOST, "")): str,
                vol.Optional(CONF_PORT, description={"suggested_value": str(data.get(CONF_PORT) or DEFAULT_PORT)}): str,
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
        return self.async_show_menu(
            step_id="init",
            menu_options=["connection", "motion", "rtsp"],
        )

    async def async_step_connection(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        entry = self._config_entry
        current_data = dict(entry.data)

        if user_input is not None:
            effective_input = dict(user_input)

            if not str(effective_input.get(CONF_PASSWORD, "") or "").strip():
                effective_input[CONF_PASSWORD] = current_data.get(CONF_PASSWORD, "")

            effective_input[CONF_PORT] = _coerce_port_value(effective_input.get(CONF_PORT))

            port = effective_input[CONF_PORT]
            unique = f"{effective_input[CONF_HOST]}:{port}" if port else effective_input[CONF_HOST]

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
            except Exception as err:
                _LOGGER.exception("DW Spectrum Configure unknown error for %s: %s", unique, err)
                errors["base"] = "unknown"
            else:
                new_options = dict(entry.options)
                callback_url = str(new_data.get(CONF_HA_CALLBACK_URL, "") or "").strip().rstrip("/")
                new_options[CONF_HA_CALLBACK_URL] = callback_url
                new_options[CONF_ENABLE_MOTION_RULES] = bool(callback_url)
                token = new_options.get(CONF_MOTION_TOKEN) or new_data.get(CONF_MOTION_TOKEN)
                if token:
                    new_options[CONF_MOTION_TOKEN] = token

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

    async def async_step_rtsp(self, user_input: dict | None = None) -> FlowResult:
        entry = self._config_entry
        opts = entry.options or {}
        data = entry.data or {}

        current_enable = opts.get(CONF_ENABLE_RTSP, data.get(CONF_ENABLE_RTSP, DEFAULT_ENABLE_RTSP))
        current_main = opts.get(CONF_RTSP_MAIN_STREAM, data.get(CONF_RTSP_MAIN_STREAM, DEFAULT_RTSP_MAIN_STREAM))
        current_sub = opts.get(CONF_RTSP_SUB_STREAM, data.get(CONF_RTSP_SUB_STREAM, DEFAULT_RTSP_SUB_STREAM))

        if user_input is not None:
            new_opts = dict(opts)
            new_opts[CONF_ENABLE_RTSP] = user_input.get(CONF_ENABLE_RTSP, DEFAULT_ENABLE_RTSP)
            new_opts[CONF_RTSP_MAIN_STREAM] = user_input.get(CONF_RTSP_MAIN_STREAM, DEFAULT_RTSP_MAIN_STREAM)
            new_opts[CONF_RTSP_SUB_STREAM] = user_input.get(CONF_RTSP_SUB_STREAM, DEFAULT_RTSP_SUB_STREAM)

            new_data = dict(data)
            new_data[CONF_ENABLE_RTSP] = new_opts[CONF_ENABLE_RTSP]
            new_data[CONF_RTSP_MAIN_STREAM] = new_opts[CONF_RTSP_MAIN_STREAM]
            new_data[CONF_RTSP_SUB_STREAM] = new_opts[CONF_RTSP_SUB_STREAM]

            self.hass.config_entries.async_update_entry(entry, data=new_data, options=new_opts)
            await self.hass.config_entries.async_reload(entry.entry_id)
            return self.async_create_entry(title="", data=new_opts)

        return self.async_show_form(
            step_id="rtsp",
            data_schema=_rtsp_schema(current_enable, current_main, current_sub),
            description_placeholders={"rtsp_help": RTSP_HELP},
        )

    async def async_step_motion(self, user_input: dict | None = None) -> FlowResult:
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
