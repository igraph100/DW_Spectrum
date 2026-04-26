from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import asyncio
import json
import re
import uuid
import time
from datetime import datetime, timezone

import aiohttp
import logging


_LOGGER = logging.getLogger(__name__)


class DwSpectrumAuthError(Exception):
    """Authentication failed."""


class DwSpectrumConnectionError(Exception):
    """Connection-level failure (DNS, routing, port, TLS, HTTP error, etc.)."""


@dataclass
class DwSpectrumConfig:
    host: str
    port: int
    ssl: bool
    verify_ssl: bool
    username: str
    password: str
    runtime_guid: str | None = None


class DwSpectrumApi:
    """REST v3 client for DW Spectrum / Network Optix-based VMS."""

    def __init__(self, session: aiohttp.ClientSession, cfg: DwSpectrumConfig) -> None:
        self._session = session
        self._cfg = cfg

        if not self._cfg.runtime_guid:
            self._cfg.runtime_guid = f"ha-{uuid.uuid4()}"

        self._token: str | None = None
        self._web_cookies: dict[str, str] = {}

    @property
    def base_url(self) -> str:
        scheme = "https" if self._cfg.ssl else "http"
        return f"{scheme}://{self._cfg.host}:{self._cfg.port}"

    def _request_kwargs(self) -> dict[str, Any]:
        if self._cfg.ssl:
            return {"ssl": (None if self._cfg.verify_ssl else False)}
        return {}

    def _default_headers(self) -> dict[str, str]:
        headers = {"accept": "application/json"}
        if self._cfg.runtime_guid:
            headers["x-runtime-guid"] = self._cfg.runtime_guid
        return headers

    def _web_cookie_header(self) -> str:
        return "; ".join(f"{k}={v}" for k, v in self._web_cookies.items() if v)

    def _normalize_dw_id(self, value: str | None) -> str:
        """Normalize DW/Nx identifiers by trimming whitespace and optional braces."""
        return str(value or "").strip().strip("{}")

    def _store_response_cookies(self, resp: aiohttp.ClientResponse) -> None:
        try:
            for name, morsel in resp.cookies.items():
                value = getattr(morsel, "value", None) or str(morsel)
                if value:
                    self._web_cookies[name] = value
        except Exception:
            pass

    async def _parse_jsonish_response(self, resp: aiohttp.ClientResponse) -> Any:
        """Return JSON when present, otherwise text/None.

        Some DW web REST write calls respond with an empty body or non-JSON body even on success.
        """
        if resp.status == 204:
            return None

        text_body = await resp.text()
        if not text_body or not text_body.strip():
            return None

        try:
            return json.loads(text_body)
        except Exception:
            return text_body

    async def _parse_token(self, resp: aiohttp.ClientResponse) -> str:
        content_type = (resp.headers.get("Content-Type") or "").lower()

        if "json" in content_type:
            try:
                data = await resp.json(content_type=None)
                if isinstance(data, dict) and data.get("token"):
                    return str(data["token"])
                if isinstance(data, str) and data.strip():
                    return data.strip()
            except Exception:
                pass

        text_body = (await resp.text()).strip()
        if not text_body:
            raise DwSpectrumConnectionError("Empty response body; no token returned")

        try:
            parsed = json.loads(text_body)
            if isinstance(parsed, str) and parsed.strip():
                return parsed.strip()
            if isinstance(parsed, dict) and parsed.get("token"):
                return str(parsed["token"])
        except Exception:
            pass

        return text_body.strip().strip('"')

    async def login(self, set_cookie: bool = False) -> str:
        url = f"{self.base_url}/rest/v3/login/sessions"
        payload = {"username": self._cfg.username, "password": self._cfg.password, "setCookie": bool(set_cookie)}

        try:
            async with self._session.post(
                url,
                json=payload,
                headers=self._default_headers(),
                timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=False,
                **self._request_kwargs(),
            ) as resp:
                if resp.status in (301, 302, 307, 308):
                    loc = resp.headers.get("Location", "")
                    raise DwSpectrumConnectionError(f"Redirect ({resp.status}) to {loc or 'unknown'}")

                if resp.status in (401, 403):
                    raise DwSpectrumAuthError(f"Unauthorized ({resp.status})")

                if resp.status >= 400:
                    body = (await resp.text()).strip()
                    raise DwSpectrumConnectionError(f"HTTP {resp.status}: {body}")

                if set_cookie:
                    self._store_response_cookies(resp)

                token: str | None = None
                try:
                    token = await self._parse_token(resp)
                except DwSpectrumConnectionError:
                    if not set_cookie:
                        raise

                if token:
                    self._token = token
                    return token

                if set_cookie:
                    # Some web REST calls rely on the aiohttp session cookie rather than a bearer token.
                    return self._token or ""

                raise DwSpectrumConnectionError("Login succeeded but no token returned")

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise DwSpectrumConnectionError(str(err)) from err


    async def _request_json_web(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        retry_on_401: bool = True,
        use_bearer: bool = True,
    ) -> Any:
        """Request helper for /web/rest endpoints.

        These endpoints appear to work with bearer auth on some systems and with a session cookie on others.
        We try bearer first, then re-login with a cookie and retry *without* Authorization if needed.
        """
        headers = self._default_headers()
        cookie_header = self._web_cookie_header()
        if cookie_header:
            headers["Cookie"] = cookie_header
        if use_bearer:
            token = await self.ensure_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        url = f"{self.base_url}{path}"

        try:
            async with self._session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=aiohttp.ClientTimeout(total=25),
                **self._request_kwargs(),
            ) as resp:
                if resp.status in (401, 403) and retry_on_401:
                    await self.login(set_cookie=True)
                    return await self._request_json_web(
                        method,
                        path,
                        params=params,
                        json_body=json_body,
                        retry_on_401=False,
                        use_bearer=False,
                    )

                if resp.status >= 400:
                    body = (await resp.text()).strip()
                    raise DwSpectrumConnectionError(f"{method} {path} -> HTTP {resp.status}: {body}")

                self._store_response_cookies(resp)
                return await self._parse_jsonish_response(resp)

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise DwSpectrumConnectionError(str(err)) from err

    async def _request_json_any_auth(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        """Try bearer auth first, then cookie/session auth.

        Some DW analytics endpoints succeed from the browser session but behave
        differently with bearer-only auth. For those endpoints, fall back to the
        same cookie/session style auth the web client uses.
        """
        try:
            return await self._request_json(method, path, params=params, json_body=json_body)
        except DwSpectrumConnectionError as first_err:
            try:
                return await self._request_json_web(
                    method,
                    path,
                    params=params,
                    json_body=json_body,
                    retry_on_401=True,
                    use_bearer=True,
                )
            except DwSpectrumConnectionError:
                raise first_err


    async def _request_jsonish_any_auth(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        """Try bearer auth first, then cookie/session auth, while allowing empty bodies."""
        try:
            return await self._request_json(method, path, params=params, json_body=json_body)
        except DwSpectrumConnectionError as first_err:
            try:
                return await self._request_json_web(
                    method,
                    path,
                    params=params,
                    json_body=json_body,
                    retry_on_401=True,
                    use_bearer=True,
                )
            except DwSpectrumConnectionError:
                raise first_err

    async def ensure_token(self) -> str:
        if self._token:
            return self._token
        return await self.login()

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        retry_on_401: bool = True,
    ) -> Any:
        token = await self.ensure_token()
        headers = {**self._default_headers(), "Authorization": f"Bearer {token}"}
        url = f"{self.base_url}{path}"

        try:
            async with self._session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=aiohttp.ClientTimeout(total=25),
                **self._request_kwargs(),
            ) as resp:
                if resp.status in (401, 403) and retry_on_401:
                    self._token = None
                    await self.login()
                    return await self._request_json(
                        method,
                        path,
                        params=params,
                        json_body=json_body,
                        retry_on_401=False,
                    )

                if resp.status >= 400:
                    body = (await resp.text()).strip()
                    raise DwSpectrumConnectionError(f"{method} {path} -> HTTP {resp.status}: {body}")

                return await self._parse_jsonish_response(resp)

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise DwSpectrumConnectionError(str(err)) from err

    # -----------------------
    # Devices / Cameras
    # -----------------------
    async def get_devices(self) -> list[dict[str, Any]]:
        """Basic REST v3 inventory fallback."""
        params = {"_with": "id,name,deviceType,type,model,physicalId,logicalId,isOnline,status,schedule"}
        data = await self._request_json("GET", "/rest/v3/devices", params=params)

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("items", "data", "devices"):
                if isinstance(data.get(key), list):
                    return data[key]

        raise DwSpectrumConnectionError("Unexpected /rest/v3/devices response shape")

    async def get_web_devices(self, device_id: str | None = None) -> list[dict[str, Any]]:
        """Camera inventory using the same web REST shape the Spectrum UI reads.

        This is used so per-camera switches can see fields like options.isAudioEnabled.
        Falls back to the v3 inventory if the web endpoint is unavailable.
        """
        params = {
            "_keepDefault": "true",
            "_with": (
                "id,name,vendor,model,physicalId,url,serverId,status,typeId,capabilities,deviceType,"
                "options.isAudioEnabled,options.isControlEnabled,options.isDualStreamingDisabled,"
                "parameters.isAudioSupported,parameters.mediaCapabilities.hasAudio,parameters.audioCodec,"
                "parameters.overrideAr,parameters.rotation,"
                "motion.mask,motion.type,"
                "schedule.isEnabled,schedule.tasks.dayOfWeek,schedule.tasks.endTime,"
                "schedule.tasks.fps,schedule.tasks.metadataTypes,schedule.tasks.recordingType,"
                "schedule.tasks.startTime,schedule.tasks.streamQuality"
            ),
        }
        if device_id:
            params["id"] = device_id

        try:
            data = await self._request_json_web("GET", "/web/rest/v2/devices", params=params)
        except DwSpectrumConnectionError:
            if device_id:
                legacy = await self._request_json(
                    "GET",
                    f"/rest/v3/devices/{device_id}",
                    params={"_with": "id,name,deviceType,type,model,physicalId,logicalId,isOnline,status,schedule"},
                )
                return [legacy] if isinstance(legacy, dict) else []
            return await self.get_devices()

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("items", "data", "devices"):
                if isinstance(data.get(key), list):
                    return data[key]

        raise DwSpectrumConnectionError("Unexpected /web/rest/v2/devices response shape")

    def _looks_like_camera(self, dev: dict[str, Any]) -> bool:
        """Best-effort camera detection, including grouped/virtual camera entries.

        Some DW installations expose grouped cameras differently across the web and v3
        inventory endpoints. We treat a device as camera-like when the usual camera
        fields match, and we also accept common virtual/grouped camera identifiers.
        """
        parts: list[str] = []

        def collect(value: Any) -> None:
            if value is None:
                return
            if isinstance(value, dict):
                for k, v in value.items():
                    parts.append(str(k))
                    collect(v)
                return
            if isinstance(value, list):
                for item in value:
                    collect(item)
                return
            parts.append(str(value))

        for key in (
            "deviceType",
            "type",
            "typeId",
            "name",
            "model",
            "vendor",
            "capabilities",
            "parameters",
            "options",
            "url",
        ):
            collect(dev.get(key))

        haystack = " ".join(parts).lower()
        if "camera" in haystack:
            return True
        if any(token in haystack for token in ("virtual camera", "camera group", "grouped camera", "multisensor")):
            return True

        # Some grouped cameras still look like camera channels even when the type string
        # does not literally contain "camera".
        if dev.get("physicalId") and dev.get("schedule") is not None:
            return True

        return False

    async def get_cameras(self) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}

        try:
            for dev in await self.get_devices():
                if not isinstance(dev, dict):
                    continue
                dev_id = str(dev.get("id", "")).strip()
                if dev_id:
                    merged[dev_id] = dict(dev)
        except DwSpectrumConnectionError:
            pass

        for dev in await self.get_web_devices():
            if not isinstance(dev, dict):
                continue
            dev_id = str(dev.get("id", "")).strip()
            if not dev_id:
                continue
            if dev_id in merged:
                merged[dev_id].update(dev)
            else:
                merged[dev_id] = dict(dev)

        cams: list[dict[str, Any]] = []
        for dev in merged.values():
            if self._looks_like_camera(dev):
                cams.append(dev)
        return cams

    async def get_device(self, device_id: str) -> dict[str, Any]:
        # Prefer the web REST shape because it includes audio fields used by the new switch.
        try:
            devices = await self.get_web_devices(device_id)
            if devices and isinstance(devices[0], dict):
                return devices[0]
        except DwSpectrumConnectionError:
            pass

        data = await self._request_json(
            "GET",
            f"/rest/v3/devices/{device_id}",
            params={"_with": "id,name,schedule"},
        )
        if isinstance(data, dict):
            return data
        raise DwSpectrumConnectionError("Unexpected /rest/v3/devices/{id} response shape")

    async def get_device_status(self, device_id: str) -> dict[str, Any]:
        data = await self._request_json("GET", f"/rest/v3/devices/{device_id}/status")
        if isinstance(data, dict):
            return data
        raise DwSpectrumConnectionError("Unexpected /rest/v3/devices/{id}/status response shape")

    async def get_device_image(self, device_id: str) -> bytes | None:
        token = await self.ensure_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "accept": "image/jpeg,image/png,*/*",
            **({"x-runtime-guid": self._cfg.runtime_guid} if self._cfg.runtime_guid else {}),
        }
        url = f"{self.base_url}/rest/v3/devices/{device_id}/image"

        try:
            async with self._session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=25),
                **self._request_kwargs(),
            ) as resp:
                if resp.status in (401, 403):
                    self._token = None
                    await self.login()
                    return await self.get_device_image(device_id)

                if resp.status >= 400:
                    body = (await resp.text()).strip()
                    raise DwSpectrumConnectionError(
                        f"GET /rest/v3/devices/{device_id}/image -> HTTP {resp.status}: {body}"
                    )

                return await resp.read()

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise DwSpectrumConnectionError(str(err)) from err

    async def patch_device(self, device_id: str, body: dict[str, Any]) -> dict[str, Any]:
        data = await self._request_json("PATCH", f"/rest/v3/devices/{device_id}", json_body=body)
        return data if isinstance(data, dict) else {"raw": data}

    async def set_camera_schedule_enabled(self, device_id: str, enabled: bool) -> None:
        # This is the supported REST v3 “start/stop recording” mechanism.
        await self.patch_device(device_id, {"schedule": {"isEnabled": enabled}})

    def _clone_task(self, t: dict[str, Any]) -> dict[str, Any]:
        nt = dict(t)
        nt.setdefault("metadataTypes", "none")
        nt.setdefault("fps", 0)
        nt.setdefault("bitrateKbps", 0)
        nt.setdefault("streamQuality", "highest")
        nt.setdefault("startTime", 0)
        nt.setdefault("endTime", 86400)
        nt.setdefault("dayOfWeek", 1)
        return nt

    async def set_camera_recording_mode(self, device_id: str, mode: str) -> None:
        dev = await self.get_device(device_id)
        schedule = dev.get("schedule") or {}
        tasks_raw = schedule.get("tasks") if isinstance(schedule, dict) else None
        tasks: list[dict[str, Any]] = [t for t in (tasks_raw or []) if isinstance(t, dict)]

        if not tasks:
            tasks = []
            for dow in range(1, 8):
                tasks.append(
                    {
                        "bitrateKbps": 0,
                        "dayOfWeek": dow,
                        "endTime": 86400,
                        "fps": 24,
                        "metadataTypes": "none",
                        "recordingType": "always",
                        "startTime": 0,
                        "streamQuality": "highest",
                    }
                )

        def rewrite(recording_type: str, metadata_types: str) -> list[dict[str, Any]]:
            new_tasks: list[dict[str, Any]] = []
            for t in tasks:
                nt = self._clone_task(t)
                nt["recordingType"] = recording_type
                nt["metadataTypes"] = metadata_types
                new_tasks.append(nt)
            return new_tasks

        if mode == "always":
            new_tasks = rewrite("always", "none")
        elif mode == "motion":
            new_tasks = rewrite("metadataOnly", "motion")
        elif mode == "motion_low":
            new_tasks = rewrite("metadataAndLowQuality", "motion")
        else:
            raise DwSpectrumConnectionError(f"Unknown recording mode: {mode}")

        await self.patch_device(device_id, {"schedule": {"isEnabled": True, "tasks": new_tasks}})


    async def set_camera_audio_enabled(self, device_id: str, enabled: bool) -> None:
        """Enable/disable camera audio using the same web REST call the UI makes.

        The UI sends a partial device payload to /web/rest/v1/devices/{id}. We mirror that
        request shape here because simple partial patches were not confirmed for this flag.
        """
        dev = await self.get_device(device_id)

        motion = dev.get("motion") if isinstance(dev.get("motion"), dict) else {}
        params = dev.get("parameters") if isinstance(dev.get("parameters"), dict) else {}
        schedule = dev.get("schedule") if isinstance(dev.get("schedule"), dict) else {}

        body: dict[str, Any] = {
            "id": device_id,
            "name": dev.get("name") or device_id,
            "scheduleEnabled": bool(schedule.get("isEnabled", False)),
            "options": {"isAudioEnabled": bool(enabled)},
            "motion": {
                "mask": motion.get("mask", ""),
                "type": motion.get("type", "software"),
            },
            "parameters": {
                "overrideAr": str(params.get("overrideAr", "")),
                "rotation": str(params.get("rotation", "0")),
            },
        }

        try:
            await self._request_json_web("PATCH", f"/web/rest/v1/devices/{device_id}", json_body=body)
            return
        except DwSpectrumConnectionError:
            # Fallback in case this server also accepts the option through REST v3.
            await self.patch_device(device_id, {"options": {"isAudioEnabled": bool(enabled)}})


    async def get_ptz_presets(self, device_id: str) -> list[dict[str, Any]]:
        paths = [
            (f"/rest/v4/devices/{device_id}/ptz/presets", {"id": device_id, "_format": "JSON"}),
            (f"/rest/v3/devices/{device_id}/ptz/presets", {"id": device_id, "_format": "JSON"}),
            (f"/rest/v3/devices/{device_id}/ptz/presets", None),
        ]

        last_err: Exception | None = None
        for path, params in paths:
            try:
                data = await self._request_jsonish_any_auth("GET", path, params=params)
            except DwSpectrumConnectionError as err:
                last_err = err
                continue

            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
            if isinstance(data, dict):
                for key in ("items", "data", "presets"):
                    if isinstance(data.get(key), list):
                        return [item for item in data[key] if isinstance(item, dict)]
                return []
            if data is None:
                return []

        if last_err is not None:
            raise DwSpectrumConnectionError(str(last_err)) from last_err
        return []

    async def get_ptz_position(self, device_id: str) -> dict[str, Any]:
        paths = [
            (f"/rest/v4/devices/{device_id}/ptz/position", None),
            (f"/rest/v3/devices/{device_id}/ptz/position", None),
        ]

        last_err: Exception | None = None
        for path, params in paths:
            try:
                data = await self._request_jsonish_any_auth("GET", path, params=params)
            except DwSpectrumConnectionError as err:
                last_err = err
                continue

            if isinstance(data, dict):
                return data
            if data is None:
                return {}

        if last_err is not None:
            raise DwSpectrumConnectionError(str(last_err)) from last_err
        return {}

    async def activate_ptz_preset(self, device_id: str, preset_id: str) -> None:
        """Activate a PTZ preset.

        DW/Nx PTZ commands are most reliable via /api/ptz with a JSON POST body.
        Some systems return ids wrapped in braces, so we try both normalized and raw
        values before falling back to REST preset-activate routes.
        """

        last_err: Exception | None = None

        raw_device_id = str(device_id or "").strip()
        raw_preset_id = str(preset_id or "").strip()
        norm_device_id = self._normalize_dw_id(raw_device_id)
        norm_preset_id = self._normalize_dw_id(raw_preset_id)

        device_variants = [v for v in [norm_device_id, raw_device_id, f"{{{norm_device_id}}}" if norm_device_id else ""] if v]
        preset_variants = [v for v in [raw_preset_id, norm_preset_id, f"{{{norm_preset_id}}}" if norm_preset_id else ""] if v]

        tried: set[tuple[str, str]] = set()

        # 1) Primary path: POST /api/ptz with JSON body (per Nx/DW PTZ docs).
        for dev_id in device_variants:
            for pre_id in preset_variants:
                key = (dev_id, pre_id)
                if key in tried:
                    continue
                tried.add(key)
                body = {
                    "cameraId": dev_id,
                    "command": "ActivatePresetPtzCommand",
                    "presetId": pre_id,
                    "speed": 1.0,
                }
                try:
                    await self._request_jsonish_any_auth("POST", "/api/ptz", json_body=body)
                    return
                except DwSpectrumConnectionError as err:
                    last_err = err

        # 2) Compatibility path: GET /api/ptz query string on some builds.
        tried.clear()
        for dev_id in device_variants:
            for pre_id in preset_variants:
                key = (dev_id, pre_id)
                if key in tried:
                    continue
                tried.add(key)
                params = {
                    "cameraId": dev_id,
                    "command": "ActivatePresetPtzCommand",
                    "presetId": pre_id,
                    "speed": "1.0",
                }
                try:
                    await self._request_jsonish_any_auth("GET", "/api/ptz", params=params)
                    return
                except DwSpectrumConnectionError as err:
                    last_err = err

        # 3) Fallback: REST preset activation routes using normalized ids.
        rest_device = norm_device_id or raw_device_id
        for rest_preset in [norm_preset_id, raw_preset_id]:
            if not rest_preset:
                continue
            paths = [
                f"/rest/v3/devices/{rest_device}/ptz/presets/{rest_preset}/activate",
                f"/rest/v4/devices/{rest_device}/ptz/presets/{rest_preset}/activate",
            ]
            for path in paths:
                for json_body in ({}, None):
                    try:
                        await self._request_jsonish_any_auth("POST", path, json_body=json_body)
                        return
                    except DwSpectrumConnectionError as err:
                        last_err = err
                        continue

        if last_err is not None:
            raise DwSpectrumConnectionError(str(last_err)) from last_err
        raise DwSpectrumConnectionError("Unable to activate PTZ preset")


    async def move_ptz_logical_step(
        self,
        device_id: str,
        *,
        pan_delta: float = 0.0,
        tilt_delta: float = 0.0,
        zoom_delta: float = 0.0,
        speed: float = 0.1,
    ) -> dict[str, Any]:
        """Move a PTZ camera one step from its current position.

        Some DW/Nx cameras expose their current PTZ position in absolute coordinates
        and fail when asked to resolve a logical position. We therefore prefer the
        same position type the camera already returns, defaulting to ``absolute``
        when the type is omitted from the GET response.
        """

        position = await self.get_ptz_position(device_id)
        if not isinstance(position, dict):
            position = {}

        def _to_float(value: Any, default: float = 0.0) -> float:
            try:
                if value is None or value == "":
                    return default
                return float(value)
            except (TypeError, ValueError):
                return default

        def _clamp(value: float, low: float | None = None, high: float | None = None) -> float:
            if low is not None:
                value = max(low, value)
            if high is not None:
                value = min(high, value)
            return value

        current_pan = _to_float(position.get("pan"), 0.0)
        current_tilt = _to_float(position.get("tilt"), 0.0)
        current_zoom = _to_float(position.get("zoom"), 0.0)

        reported_type = str(position.get("type") or "").strip().lower()
        preferred_type = reported_type if reported_type in {"absolute", "logical"} else "absolute"
        candidate_types: list[str] = [preferred_type]
        if preferred_type != "logical":
            candidate_types.append("logical")
        if preferred_type != "absolute":
            candidate_types.append("absolute")

        def _make_body(position_type: str) -> dict[str, Any]:
            if position_type == "logical":
                pan = _clamp(current_pan + pan_delta, -180.0, 180.0)
                tilt = _clamp(current_tilt + tilt_delta, -180.0, 180.0)
                zoom = _clamp(current_zoom + zoom_delta, 0.0, 180.0)
            else:
                pan = current_pan + pan_delta
                tilt = current_tilt + tilt_delta
                zoom = _clamp(current_zoom + zoom_delta, 0.0, None)

            body: dict[str, Any] = {
                "type": position_type,
                "pan": pan,
                "tilt": tilt,
                "zoom": zoom,
                "speed": float(speed),
            }
            api_name = position.get("api")
            if api_name not in (None, ""):
                body["api"] = api_name
            return body

        paths = [
            f"/rest/v4/devices/{self._normalize_dw_id(device_id)}/ptz/position",
            f"/rest/v3/devices/{self._normalize_dw_id(device_id)}/ptz/position",
        ]

        last_err: Exception | None = None
        for position_type in candidate_types:
            body = _make_body(position_type)
            for path in paths:
                try:
                    data = await self._request_jsonish_any_auth("POST", path, json_body=body)
                    if isinstance(data, dict):
                        return data
                    return body
                except DwSpectrumConnectionError as err:
                    last_err = err
                    continue

        if last_err is not None:
            raise DwSpectrumConnectionError(str(last_err)) from last_err
        raise DwSpectrumConnectionError("Unable to move PTZ position")

    # -----------------------
    # Server / Users / Licenses
    # -----------------------
    async def get_system_info(self) -> dict[str, Any]:
        data = await self._request_json("GET", "/rest/v3/system/info")
        if isinstance(data, dict):
            return data
        raise DwSpectrumConnectionError("Unexpected /rest/v3/system/info response shape")

    async def restart_server(self, server_id: str = "this") -> dict[str, Any]:
        """Restart a DW Spectrum server using REST v4.

        The DW endpoint accepts "this" as the path id for the server handling the
        request, which is safer here than guessing whether /system/info returned
        a site id or an actual server id.
        """
        raw_server_id = str(server_id or "this").strip()
        path_server_id = "this" if raw_server_id.lower() == "this" else self._normalize_dw_id(raw_server_id)
        if not path_server_id:
            path_server_id = "this"

        data = await self._request_jsonish_any_auth("POST", f"/rest/v4/servers/{path_server_id}/restart")
        if isinstance(data, dict):
            result = dict(data)
        elif data is None:
            result = {}
        else:
            result = {"response": data}

        result.setdefault("ok", True)
        result.setdefault("server_id", path_server_id)
        return result

    async def get_users(self) -> list[dict[str, Any]]:
        params = {"_with": "id,name,fullName,email,type,isEnabled,permissions,attributes"}
        data = await self._request_json("GET", "/rest/v3/users", params=params)

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("items", "data", "users"):
                if isinstance(data.get(key), list):
                    return data[key]

        raise DwSpectrumConnectionError("Unexpected /rest/v3/users response shape")

    async def set_user_enabled(self, user_id: str, enabled: bool) -> None:
        _ = await self._request_json("PATCH", f"/rest/v3/users/{user_id}", json_body={"isEnabled": enabled})

    async def get_license_summary(self) -> dict[str, Any]:
        data = await self._request_json("GET", "/rest/v3/licenses/*/summary")
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return {"items": data}
        return {"raw": data}

    # -----------------------
    # LPR / Analytics helpers
    # -----------------------
    def _flatten_items(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            for key in ("items", "data", "objects", "tracks", "reply", "results", "rules", "eventRules"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
            return [data]
        return []

    def _coerce_epoch_ms(self, value: Any) -> int | None:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            iv = int(value)
            if iv <= 0:
                return None
            if iv >= 10**15:
                return iv // 1000
            if iv >= 10**12:
                return iv
            if iv >= 10**9:
                return iv * 1000
            return None

        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            if raw.isdigit():
                return self._coerce_epoch_ms(int(raw))
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return None

        return None

    def _attrs_to_dict(self, attrs: Any) -> dict[str, Any]:
        if isinstance(attrs, dict):
            return dict(attrs)
        if not isinstance(attrs, list):
            return {}

        out: dict[str, Any] = {}
        for item in attrs:
            if not isinstance(item, dict):
                continue
            key = (
                item.get("name")
                or item.get("key")
                or item.get("attribute")
                or item.get("type")
                or item.get("caption")
            )
            if not key:
                continue
            value = item.get("value")
            if value is None:
                value = item.get("text")
            if value is None:
                value = item.get("description")
            out[str(key)] = value
        return out

    def _extract_list_status(self, description: str, attrs: dict[str, Any]) -> str | None:
        attrs_norm = {re.sub(r"[^a-z0-9]", "", str(k or "").lower()): v for k, v in attrs.items()}
        for key in ("listStatus", "list_status", "plateListStatus", "status", "List", "list"):
            value = attrs.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().lower()
        for key in ("liststatus", "plateliststatus", "allowdeny", "allowdenyliststatus", "list"):
            value = attrs_norm.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().lower()

        text = description.lower()
        for marker in ("allow", "deny", "denied", "other", "unknown"):
            if marker in text:
                return marker
        return None

    def _extract_confidence(self, item: dict[str, Any], attrs: dict[str, Any]) -> float | int | None:
        attrs_norm = {re.sub(r"[^a-z0-9]", "", str(k or "").lower()): v for k, v in attrs.items()}
        candidates = [
            item.get("confidence"),
            item.get("score"),
            attrs.get("confidence"),
            attrs.get("score"),
            attrs.get("probability"),
            attrs_norm.get("confidence"),
            attrs_norm.get("score"),
            attrs_norm.get("probability"),
        ]
        for value in candidates:
            if value is None:
                continue
            try:
                num = float(value)
                if num.is_integer():
                    return int(num)
                return num
            except Exception:
                continue
        return None

    def _clean_plate(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None

        upper = text.upper().strip()
        if upper in {"PLATE NUMBER", "LICENSE PLATE", "PLATE", "NUMBER", "VEHICLE", "CAR"}:
            return None

        compact = re.sub(r"\s+", "", upper)
        compact = compact.strip("-_./")
        if not compact:
            return None

        if len(compact) < 2:
            return None

        return compact

    def _extract_plate(self, item: dict[str, Any], attrs: dict[str, Any]) -> tuple[str | None, str | None]:
        def norm_key(key: Any) -> str:
            return re.sub(r"[^a-z0-9]", "", str(key or "").lower())

        attrs_norm = {norm_key(k): v for k, v in attrs.items()}

        candidates: list[Any] = [
            # Exact DW objectTracks fields first.
            attrs.get("License Plate.Number"),
            attrs.get("Number"),
            attrs.get("License Plate Number"),
            attrs_norm.get("licenseplatenumber"),
            attrs_norm.get("number"),
            item.get("plate"),
            item.get("plateNumber"),
            item.get("licensePlate"),
            item.get("license_plate"),
            item.get("plateText"),
            item.get("plate_text"),
            item.get("numberPlate"),
            item.get("number_plate"),
            item.get("recognizedText"),
            item.get("recognized_text"),
            item.get("text"),
            item.get("caption"),
            attrs.get("plate"),
            attrs.get("plateNumber"),
            attrs.get("plate_number"),
            attrs.get("licensePlate"),
            attrs.get("license_plate"),
            attrs.get("plateText"),
            attrs.get("plate_text"),
            attrs.get("numberPlate"),
            attrs.get("number_plate"),
            attrs.get("recognizedText"),
            attrs.get("recognized_text"),
            attrs.get("number"),
            attrs.get("text"),
            attrs.get("value"),
            attrs_norm.get("platenumber"),
            attrs_norm.get("licenseplate"),
            attrs_norm.get("licensenumber"),
            attrs_norm.get("numberplate"),
            attrs_norm.get("recognizedtext"),
            attrs_norm.get("platetext"),
        ]

        raw_value: str | None = None
        cleaned: str | None = None
        for value in candidates:
            if value is None:
                continue
            raw_value = str(value).strip() or raw_value
            cleaned = self._clean_plate(value)
            if cleaned:
                return cleaned, raw_value
        return None, raw_value

    def _normalize_lpr_hit(self, item: dict[str, Any], camera_id: str) -> dict[str, Any] | None:
        attrs = self._attrs_to_dict(item.get("attributes") or item.get("attributeValues") or item.get("details"))
        description = str(item.get("description") or item.get("eventDescription") or "").strip()
        object_type = str(item.get("objectType") or item.get("objectTypeId") or item.get("typeId") or item.get("eventType") or item.get("type") or "").strip()
        caption = str(item.get("caption") or item.get("name") or item.get("text") or "").strip()

        marker_blob = " ".join(
            [
                object_type,
                caption,
                description,
                " ".join(str(k) for k in attrs.keys()),
                " ".join(str(v) for v in attrs.values() if v is not None),
            ]
        ).lower()
        looks_like_lpr = any(
            token in marker_blob
            for token in (
                "license plate",
                "licenseplate",
                "plate number",
                "plate recogn",
                "plate reader",
                "alpr",
                "anpr",
                "lpr",
            )
        )
        explicit_plate_event = any(
            token in marker_blob
            for token in (
                "license plate",
                "licenseplate",
                "plate number",
                "recognized text",
                "plate text",
            )
        )

        plate, plate_raw = self._extract_plate(item, attrs)
        if not looks_like_lpr and plate is None:
            return None
        if plate is None and not explicit_plate_event:
            return None

        seen_ms = None
        for key in (
            "timestampMs",
            "timeMs",
            "endTimeMs",
            "startTimeMs",
            "timestampUs",
            "timeUs",
            "endTimeUs",
            "startTimeUs",
            "createdTimeMs",
            "createdAtMs",
            "utcTimeMs",
            "timestamp",
            "time",
            "startTime",
            "endTime",
            "createdTime",
            "createdAt",
        ):
            seen_ms = self._coerce_epoch_ms(item.get(key))
            if seen_ms is not None:
                break
        if seen_ms is None:
            attrs_time = attrs.get("timestampMs") or attrs.get("timeMs") or attrs.get("timestamp") or attrs.get("time")
            seen_ms = self._coerce_epoch_ms(attrs_time)
        if seen_ms is None:
            return None

        track_id = (
            item.get("objectTrackId")
            or item.get("trackId")
            or item.get("id")
            or item.get("objectId")
            or f"{camera_id}:{seen_ms}:{plate or 'unknown'}"
        )

        list_status = self._extract_list_status(description, attrs)
        confidence = self._extract_confidence(item, attrs)
        recognized = plate is not None

        return {
            "camera_id": camera_id,
            "track_id": str(track_id),
            "seen_ms": int(seen_ms),
            "plate": plate or "UNKNOWN",
            "plate_raw": plate_raw,
            "recognized": recognized,
            "description": description or None,
            "object_type": object_type or None,
            "caption": caption or None,
            "list_status": list_status,
            "confidence": confidence,
            "attributes": attrs,
            "best_shot": item.get("bestShot") if isinstance(item.get("bestShot"), dict) else None,
            "has_capture": bool(item.get("bestShot") or track_id),
        }

    def _sort_lpr_hits(self, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def score(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
            plate = str(row.get("plate") or "")
            object_type = str(row.get("object_type") or "").lower()
            attrs = row.get("attributes") or {}
            has_plate_number_attr = any(
                key in attrs for key in ("License Plate.Number", "Number", "License Plate Number")
            )
            attr_keys_norm = {re.sub(r"[^a-z0-9]", "", str(k or "").lower()) for k in attrs.keys()}
            has_vehicle_detail = 1 if any(
                key in attr_keys_norm
                for key in ("lane", "direction", "brand", "color", "type", "licensplatecountry", "country", "licenseplatecountry")
            ) else 0
            return (
                1 if plate and plate != "UNKNOWN" else 0,
                has_vehicle_detail,
                1 if ("licenseplate" in object_type or has_plate_number_attr) else 0,
                int(row.get("seen_ms") or 0),
                str(row.get("track_id") or ""),
            )

        return sorted(hits, key=score, reverse=True)

    def _merge_lpr_hits(self, primary: dict[str, Any], others: list[dict[str, Any]]) -> dict[str, Any]:
        """Merge nearby LPR hits so the chosen result keeps the rich vehicle attributes.

        Many DW servers emit separate object tracks for the vehicle and the plate.
        The plate object often has Number/Country, while the vehicle object carries
        lane, direction, color, brand, and type. For Home Assistant we want one
        combined latest result.
        """
        merged = dict(primary)
        merged_attrs = dict(primary.get("attributes") or {})
        merged_best_shot = primary.get("best_shot") if isinstance(primary.get("best_shot"), dict) else None

        primary_plate = str(primary.get("plate") or "").strip().upper()
        primary_seen = int(primary.get("seen_ms") or 0)

        def normalized_plate(hit: dict[str, Any]) -> str:
            value = str(hit.get("plate") or hit.get("plate_raw") or "").strip().upper()
            return "" if value == "UNKNOWN" else value

        for other in others:
            if other is primary:
                continue
            other_seen = int(other.get("seen_ms") or 0)
            if primary_seen and other_seen and abs(primary_seen - other_seen) > 3000:
                continue
            other_plate = normalized_plate(other)
            if primary_plate and other_plate and other_plate != primary_plate:
                continue

            other_attrs = other.get("attributes") or {}
            for key, value in other_attrs.items():
                if value is None:
                    continue
                existing = merged_attrs.get(key)
                if existing is None or str(existing).strip() == "":
                    merged_attrs[key] = value

            if not merged.get("description") and other.get("description"):
                merged["description"] = other.get("description")
            if not merged.get("caption") and other.get("caption"):
                merged["caption"] = other.get("caption")
            if not merged.get("list_status") and other.get("list_status"):
                merged["list_status"] = other.get("list_status")
            if merged.get("confidence") is None and other.get("confidence") is not None:
                merged["confidence"] = other.get("confidence")
            if not merged_best_shot and isinstance(other.get("best_shot"), dict):
                merged_best_shot = other.get("best_shot")
            if not merged.get("recognized") and other.get("recognized"):
                merged["recognized"] = True
            if (merged.get("plate") in (None, "", "UNKNOWN")) and other.get("plate") not in (None, "", "UNKNOWN"):
                merged["plate"] = other.get("plate")
                merged["plate_raw"] = other.get("plate_raw")

        merged["attributes"] = merged_attrs
        merged["best_shot"] = merged_best_shot
        merged["has_capture"] = bool(merged_best_shot or merged.get("track_id"))
        return merged

    def _norm_resource_id(self, value: Any) -> str:
        return str(value or "").strip().strip("{}").lower()

    def _timestamp_from_object_track(self, item: dict[str, Any]) -> int | None:
        for key in ("startTimeMs", "endTimeMs", "timestampMs", "timeMs", "startTime", "endTime", "timestamp"):
            value = self._coerce_epoch_ms(item.get(key))
            if value is not None:
                return value
        best_shot = item.get("bestShot")
        if isinstance(best_shot, dict):
            for key in ("timestampMs", "timestampUs", "timestamp"):
                value = self._coerce_epoch_ms(best_shot.get(key))
                if value is not None:
                    return value
        return None

    def _extract_simple_objecttrack_lpr_hit(self, item: dict[str, Any], camera_id: str) -> dict[str, Any] | None:
        attrs = self._attrs_to_dict(item.get("attributes") or [])
        attrs_norm = {re.sub(r"[^a-z0-9]", "", str(k or "").lower()): v for k, v in attrs.items()}

        raw_plate = None
        for key in ("License Plate.Number", "Number", "License Plate Number"):
            value = attrs.get(key)
            if value is not None and str(value).strip():
                raw_plate = str(value).strip()
                break
        if raw_plate is None:
            for key in ("licenseplatenumber", "number"):
                value = attrs_norm.get(key)
                if value is not None and str(value).strip():
                    raw_plate = str(value).strip()
                    break

        plate = self._clean_plate(raw_plate) if raw_plate is not None else None

        object_type = str(item.get("objectTypeId") or item.get("objectType") or item.get("type") or "").strip()
        object_type_norm = re.sub(r"[^a-z0-9]", "", object_type.lower())
        attr_keys_blob = " ".join(str(k) for k in attrs.keys()).lower()
        is_probable_plate_track = (
            "licenseplate" in object_type_norm
            or attrs.get("License Plate") is not None
            or "license plate" in attr_keys_blob
            or "plate.number" in attr_keys_blob
        )

        if plate is None and not is_probable_plate_track:
            return None

        seen_ms = self._timestamp_from_object_track(item)
        if seen_ms is None:
            return None

        return {
            "camera_id": camera_id,
            "track_id": str(item.get("id") or item.get("trackId") or f"{camera_id}:{seen_ms}:{plate or 'unknown'}"),
            "seen_ms": int(seen_ms),
            "plate": plate or "UNKNOWN",
            "plate_raw": raw_plate,
            "recognized": plate is not None,
            "description": None,
            "object_type": object_type or None,
            "caption": None,
            "list_status": self._extract_list_status("", attrs),
            "confidence": self._extract_confidence(item, attrs),
            "attributes": attrs,
            "best_shot": item.get("bestShot") if isinstance(item.get("bestShot"), dict) else None,
            "has_capture": isinstance(item.get("bestShot"), dict),
        }


    def _extract_motion_event(self, item: dict[str, Any], camera_id: str) -> dict[str, Any] | None:
        """Normalize one DW/Nx event-log row into a motion start/stop event.

        Different DW/Nx builds expose event rows with slightly different field names.
        This parser accepts the common /api/getEvents, /ec2/getEvents, and newer
        REST event shapes, then returns only rows that look like camera motion events
        for the requested camera.
        """
        params = item.get("eventParams") if isinstance(item.get("eventParams"), dict) else {}
        attrs = self._attrs_to_dict(item.get("attributes") or item.get("details") or [])

        def first(*values: Any) -> Any:
            for value in values:
                if value is not None and str(value).strip() != "":
                    return value
            return None

        event_type = str(first(
            item.get("eventType"),
            item.get("type"),
            item.get("event"),
            item.get("name"),
            params.get("eventType"),
            params.get("type"),
            attrs.get("eventType"),
        ) or "")
        caption = str(first(item.get("caption"), item.get("title"), item.get("name"), params.get("caption")) or "")
        description = str(first(
            item.get("description"),
            item.get("eventDescription"),
            item.get("text"),
            params.get("description"),
        ) or "")

        blob = " ".join(
            str(v)
            for v in [event_type, caption, description, item.get("actionType"), item.get("reasonCode"), params.get("reasonCode")]
            if v is not None
        ).lower()
        if "motion" not in blob and "cameraMotionEvent" not in event_type:
            return None

        item_cam = first(
            item.get("deviceId"),
            item.get("cameraId"),
            item.get("resourceId"),
            item.get("sourceId"),
            item.get("sourceResourceId"),
            params.get("deviceId"),
            params.get("cameraId"),
            params.get("resourceId"),
            attrs.get("deviceId"),
            attrs.get("cameraId"),
        )
        want_id = self._norm_resource_id(camera_id)
        if item_cam and self._norm_resource_id(item_cam) != want_id:
            return None

        ts_ms: int | None = None
        for key in (
            "eventTimestampMs", "timestampMs", "timeMs", "eventTimeMs", "createdTimeMs",
            "eventTimestamp", "timestamp", "time", "eventTime", "createdTime", "dateTime", "date",
        ):
            ts_ms = self._coerce_epoch_ms(item.get(key))
            if ts_ms is not None:
                break
        if ts_ms is None:
            for key in ("timestampMs", "timeMs", "timestamp", "time", "eventTimestamp"):
                ts_ms = self._coerce_epoch_ms(params.get(key) or attrs.get(key))
                if ts_ms is not None:
                    break
        if ts_ms is None:
            return None

        state_blob = " ".join(
            str(v)
            for v in [
                item.get("state"), item.get("eventState"), item.get("status"), item.get("reasonCode"),
                params.get("state"), params.get("eventState"), params.get("status"), params.get("reasonCode"),
                attrs.get("state"), attrs.get("status"), event_type, caption, description,
            ]
            if v is not None
        ).lower()

        stop_markers = ("stop", "stopped", "ends", "ended", "inactive", "false", "off", "finished", "cleared")
        start_markers = ("start", "started", "starts", "detected", "active", "true", "on", "motion")

        is_active: bool | None = None
        if any(marker in state_blob for marker in stop_markers):
            is_active = False
        elif any(marker in state_blob for marker in start_markers):
            is_active = True

        if is_active is None:
            return None

        return {
            "camera_id": camera_id,
            "timestamp_ms": int(ts_ms),
            "active": bool(is_active),
            "event_type": event_type or None,
            "caption": caption or None,
            "description": description or None,
            "raw": item,
        }

    async def get_recent_motion_events(self, camera_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent motion start/stop events for one camera.

        Newer DW/Nx builds expose event-log style data through REST routes, while
        older builds commonly expose /api/getEvents or /ec2/getEvents. We try the
        modern routes first and keep the parser tolerant so this works across more
        DW Spectrum versions.
        """
        norm_camera_id = self._normalize_dw_id(camera_id)
        raw_camera_id = str(camera_id or "").strip()
        limit_s = str(max(1, min(int(limit or 50), 200)))

        queries: list[tuple[str, dict[str, str]]] = []
        for cid in [norm_camera_id, raw_camera_id, f"{{{norm_camera_id}}}" if norm_camera_id else ""]:
            if not cid:
                continue
            queries.extend([
                ("/rest/v4/events", {"deviceId": cid, "limit": limit_s}),
                ("/rest/v4/events", {"cameraId": cid, "limit": limit_s}),
                ("/rest/v4/events", {"resourceId": cid, "limit": limit_s}),
                ("/api/getEvents", {"cameraId": cid, "limit": limit_s, "eventsOnly": "true"}),
                ("/ec2/getEvents", {"cameraId": cid, "limit": limit_s, "eventsOnly": "true"}),
            ])

        last_err: Exception | None = None
        seen: set[tuple[int, bool, str | None]] = set()
        out: list[dict[str, Any]] = []

        for path, params in queries:
            try:
                data = await self._request_jsonish_any_auth("GET", path, params=params)
            except DwSpectrumConnectionError as err:
                last_err = err
                continue

            rows = self._flatten_items(data)
            for item in rows:
                if not isinstance(item, dict):
                    continue
                ev = self._extract_motion_event(item, norm_camera_id or raw_camera_id)
                if ev is None:
                    continue
                key = (int(ev["timestamp_ms"]), bool(ev["active"]), ev.get("event_type"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(ev)

            if out:
                out.sort(key=lambda ev: int(ev.get("timestamp_ms") or 0), reverse=True)
                return out[: int(limit_s)]

        if last_err is not None:
            raise DwSpectrumConnectionError(str(last_err)) from last_err
        return []


    async def get_event_rules(self) -> list[dict[str, Any]]:
        """Return DW/Nx event rules from both REST v4 and legacy endpoints."""
        last_err: Exception | None = None
        out: list[dict[str, Any]] = []
        seen: set[str] = set()

        for path in ("/rest/v4/events/rules", "/ec2/getEventRules"):
            try:
                data = await self._request_json_any_auth("GET", path)
            except Exception as err:  # noqa: BLE001
                last_err = err
                continue
            for rule in self._flatten_items(data):
                if not isinstance(rule, dict):
                    continue
                rid = self._normalize_dw_id(str(rule.get("id") or ""))
                key = rid or f"{path}:{len(out)}:{rule.get('comment')}:{self._rule_action_url(rule)}"
                if key in seen:
                    continue
                seen.add(key)
                out.append(rule)

        if out:
            return out
        if last_err:
            raise DwSpectrumConnectionError(str(last_err)) from last_err
        return []

    async def create_event_rule(self, rule: dict[str, Any]) -> Any:
        """Create an event rule.

        DW/Nx rule APIs differ between builds. Try REST v4 first using the body as
        supplied, then try the legacy ec2 shape as a fallback.
        """
        try:
            return await self._request_json_any_auth("POST", "/rest/v4/events/rules", json_body=rule)
        except Exception as first_err:  # noqa: BLE001
            _LOGGER.debug("REST v4 event rule create failed, trying legacy saveEventRule: %s", first_err)

        # Legacy endpoint generally accepts the flattened event rule object.
        legacy = dict(rule)
        if "event" in legacy or "action" in legacy:
            legacy = self._flatten_v4_rule_for_legacy(rule)
        try:
            return await self._request_json_any_auth("POST", "/ec2/saveEventRule", json_body=legacy)
        except Exception as second_err:  # noqa: BLE001
            raise DwSpectrumConnectionError(
                f"Unable to create event rule via REST v4 or legacy API: {second_err}"
            ) from second_err

    def _flatten_v4_rule_for_legacy(self, rule: dict[str, Any]) -> dict[str, Any]:
        event = rule.get("event") if isinstance(rule.get("event"), dict) else {}
        action = rule.get("action") if isinstance(rule.get("action"), dict) else {}
        action_params = action.get("params") if isinstance(action.get("params"), dict) else {}
        event_condition = event.get("condition") if isinstance(event.get("condition"), dict) else {}
        return {
            "actionParams": json.dumps(action_params),
            "actionResourceIds": action.get("resourceIds") or [],
            "actionType": action.get("type") or action.get("actionType") or "execHttpRequestAction",
            "aggregationPeriod": int(rule.get("aggregationPeriod") or 0),
            "comment": rule.get("comment") or "",
            "disabled": not bool(rule.get("enabled", True)),
            "eventCondition": json.dumps(event_condition or {"eventTimestampUsec": "0", "eventType": "undefinedEvent", "metadata": {"allUsers": False, "level": ""}, "omitDbLogging": False, "progress": 0, "reasonCode": "none"}),
            "eventResourceIds": event.get("resourceIds") or [],
            "eventState": event.get("state") or event.get("eventState") or "Undefined",
            "eventType": event.get("type") or event.get("eventType") or "undefinedEvent",
            "schedule": rule.get("schedule") or "",
            "system": False,
        }

    async def delete_event_rule(self, rule_id: str) -> Any:
        """Delete one DW/Nx event rule by id using REST v4."""
        rid = self._normalize_dw_id(rule_id)
        if not rid:
            raise DwSpectrumConnectionError("Missing event rule id")
        return await self._request_jsonish_any_auth("DELETE", f"/rest/v4/events/rules/{rid}")

    def _rule_action_url(self, rule: dict[str, Any]) -> str:
        """Best-effort extraction of an HTTP action URL from old or REST v4 rule shapes."""
        action = rule.get("action") if isinstance(rule.get("action"), dict) else {}
        params = action.get("params") if isinstance(action.get("params"), dict) else {}
        if params.get("url"):
            return str(params.get("url") or "")

        raw_params = rule.get("actionParams")
        if isinstance(raw_params, str) and raw_params.strip():
            try:
                parsed = json.loads(raw_params)
                if isinstance(parsed, dict) and parsed.get("url"):
                    return str(parsed.get("url") or "")
            except Exception:
                return raw_params
        elif isinstance(raw_params, dict) and raw_params.get("url"):
            return str(raw_params.get("url") or "")
        return ""

    def _is_ha_motion_callback_rule(self, rule: dict[str, Any], entry_id: str | None = None) -> bool:
        """Return True only for HA-created DW motion callback rules."""
        comment = str(rule.get("comment") or "")
        url = self._rule_action_url(rule)
        has_callback_url = "/api/dw_spectrum/motion/" in url
        has_entry_url = bool(entry_id and f"/api/dw_spectrum/motion/{entry_id}/" in url)
        has_our_comment = comment.startswith("Home Assistant DW Spectrum motion ")

        if entry_id:
            return has_entry_url or (has_our_comment and not url) or (has_our_comment and has_callback_url)
        return has_callback_url or has_our_comment

    async def delete_motion_callback_rules(self, *, entry_id: str | None = None) -> dict[str, Any]:
        """Delete all Home Assistant motion callback rules created for this entry."""
        rules = await self.get_event_rules()
        targets = [r for r in rules if isinstance(r, dict) and self._is_ha_motion_callback_rule(r, entry_id)]
        deleted: list[str] = []
        failed: list[dict[str, str]] = []
        skipped_no_id: list[str] = []

        for rule in targets:
            rid = str(rule.get("id") or "").strip()
            comment = str(rule.get("comment") or rid or "unknown")
            if not rid:
                skipped_no_id.append(comment)
                continue
            try:
                await self.delete_event_rule(rid)
                deleted.append(comment)
                _LOGGER.info("Deleted DW motion callback rule: %s", comment)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("Could not delete DW motion callback rule %s: %s", comment, err)
                failed.append({"id": rid, "comment": comment, "error": str(err)})

        return {
            "scanned": len(rules),
            "found": len(targets),
            "deleted": deleted,
            "failed": failed,
            "skipped_no_id": skipped_no_id,
        }

    async def ensure_motion_callback_rules(
        self,
        *,
        cameras: list[dict[str, Any]],
        callback_base_url: str,
        entry_id: str,
        token: str,
    ) -> dict[str, Any]:
        """Create missing per-camera motion start/stop callback rules.

        We intentionally create one start and one stop rule per camera. This avoids
        depending on DW placeholder variables for camera_id in the HTTP action.
        """
        existing = await self.get_event_rules()
        existing_by_comment: dict[str, list[dict[str, Any]]] = {}
        for r in existing:
            if isinstance(r, dict):
                existing_by_comment.setdefault(str(r.get("comment") or ""), []).append(r)
        created: list[str] = []
        skipped: list[str] = []
        replaced: list[str] = []
        failed: list[dict[str, str]] = []
        base = str(callback_base_url or "").strip().rstrip("/")
        if not base:
            return {"created": created, "skipped": skipped, "replaced": replaced, "failed": failed, "enabled": False}

        for cam in cameras:
            if not isinstance(cam, dict) or not cam.get("id"):
                continue
            cam_id = self._normalize_dw_id(str(cam.get("id")))
            cam_name = str(cam.get("name") or cam_id)
            braced_id = f"{{{cam_id}}}"
            for state, label in (("Active", "start"), ("Inactive", "stop")):
                comment = f"Home Assistant DW Spectrum motion {label} {cam_id}"
                url = f"{base}/api/dw_spectrum/motion/{entry_id}/{token}?state={label}&camera_id={cam_id}"

                # If HA was removed/re-added, DW may still have rules with the same
                # comment but the old entry_id/token in the callback URL. Do not skip
                # stale rules; delete and recreate them with the current URL.
                stale_rules = []
                already_ok = False
                for existing_rule in existing_by_comment.get(comment, []):
                    existing_url = self._rule_action_url(existing_rule)
                    if existing_url == url:
                        already_ok = True
                    else:
                        stale_rules.append(existing_rule)
                if already_ok and not stale_rules:
                    skipped.append(comment)
                    continue

                stale_delete_failed = False
                for stale in stale_rules:
                    stale_id = str(stale.get("id") or "").strip()
                    if not stale_id:
                        failed.append({"camera_id": cam_id, "state": label, "comment": comment, "error": "stale_rule_has_no_id"})
                        stale_delete_failed = True
                        continue
                    try:
                        await self.delete_event_rule(stale_id)
                        replaced.append(comment)
                    except Exception as err:  # noqa: BLE001
                        failed.append({"camera_id": cam_id, "state": label, "comment": comment, "error": f"delete_stale_failed: {err}"})
                        stale_delete_failed = True
                if stale_delete_failed:
                    continue
                action_params = {
                    "allUsers": False,
                    "authType": "authBasicAndDigest",
                    "durationMs": 5000,
                    "forced": True,
                    "fps": 10,
                    "needConfirmation": False,
                    "playToClient": True,
                    "recordAfter": 0,
                    "recordBeforeMs": 1000,
                    "streamQuality": "highest",
                    "url": url,
                    "useSource": False,
                }
                event_condition = {
                    "eventTimestampUsec": "0",
                    "eventType": "undefinedEvent",
                    "metadata": {"allUsers": False, "level": ""},
                    "omitDbLogging": False,
                    "progress": 0,
                    "reasonCode": "none",
                }
                legacy_rule = {
                    "actionParams": json.dumps(action_params),
                    "actionResourceIds": [],
                    "actionType": "execHttpRequestAction",
                    "aggregationPeriod": 0,
                    "comment": comment,
                    "disabled": False,
                    "eventCondition": json.dumps(event_condition),
                    "eventResourceIds": [braced_id],
                    "eventState": state,
                    "eventType": "cameraMotionEvent",
                    "schedule": "",
                    "system": False,
                }
                v4_rule = {
                    "event": {
                        "type": "cameraMotionEvent",
                        "eventType": "cameraMotionEvent",
                        "state": state,
                        "eventState": state,
                        "resourceIds": [braced_id],
                        "condition": event_condition,
                    },
                    "action": {
                        "type": "execHttpRequestAction",
                        "actionType": "execHttpRequestAction",
                        "resourceIds": [],
                        "params": action_params,
                    },
                    "enabled": True,
                    "schedule": [],
                    "comment": comment,
                    "aggregationPeriod": 0,
                    # Include legacy fields too; some v4 builds accept/ignore extras.
                    **legacy_rule,
                }
                try:
                    await self.create_event_rule(v4_rule)
                    existing_by_comment[comment] = []
                    created.append(comment)
                    _LOGGER.info("Created DW motion callback rule for %s (%s): %s", cam_name, cam_id, label)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning("Could not create DW motion callback rule for %s (%s) %s: %s", cam_name, cam_id, label, err)
                    failed.append({"camera_id": cam_id, "state": label, "error": str(err)})
        return {"created": created, "skipped": skipped, "replaced": replaced, "failed": failed, "enabled": True}

    async def get_latest_lpr_hit(self, camera_id: str, limit: int = 20) -> dict[str, Any] | None:
        """Fetch the latest LPR hit for a camera from /rest/v4/analytics/objectTracks.

        This uses the exact endpoint shape confirmed by the user and keeps the
        logic intentionally simple: request the newest object tracks for the
        camera, parse the attributes array, and return the newest plate hit.
        """
        params = {"deviceId": camera_id, "limit": str(max(limit, 2))}
        data = await self._request_json_any_auth("GET", "/rest/v4/analytics/objectTracks", params=params)

        rows = self._flatten_items(data)
        _LOGGER.debug("DW Spectrum objectTracks camera=%s rows=%s", camera_id, len(rows))

        exact_hits: list[dict[str, Any]] = []
        fallback_hits: list[dict[str, Any]] = []
        want_id = self._norm_resource_id(camera_id)

        for item in rows:
            if not isinstance(item, dict):
                continue

            item_device_id = self._norm_resource_id(item.get("deviceId") or item.get("cameraId") or item.get("resourceId"))
            if item_device_id and item_device_id != want_id:
                continue

            direct_hit = self._extract_simple_objecttrack_lpr_hit(item, camera_id)
            if direct_hit is not None:
                exact_hits.append(direct_hit)
                continue

            normalized = self._normalize_lpr_hit(item, camera_id)
            if normalized is not None:
                fallback_hits.append(normalized)

        if exact_hits:
            exact_hits = self._sort_lpr_hits(exact_hits)
            return self._merge_lpr_hits(exact_hits[0], exact_hits[1:])

        if fallback_hits:
            fallback_hits = self._sort_lpr_hits(fallback_hits)
            return self._merge_lpr_hits(fallback_hits[0], fallback_hits[1:])

        return None

    async def get_recent_lpr_hits(self, camera_id: str, since_ms: int | None = None, limit: int = 25) -> list[dict[str, Any]]:
        """Compatibility wrapper returning the latest parsed LPR hit.

        The LPR sensor only needs the latest plate. Keep this wrapper for existing
        call sites, but simplify it to the reliable latest-hit behavior.
        """
        latest = await self.get_latest_lpr_hit(camera_id, limit=limit)
        if latest is None:
            return []
        return [latest]

    async def logout(self) -> None:
        if not self._token:
            return

        token = self._token
        self._token = None
        self._web_cookies = {}

        url = f"{self.base_url}/rest/v3/login/sessions/{token}"
        headers = {**self._default_headers(), "Authorization": f"Bearer {token}"}

        try:
            async with self._session.delete(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
                **self._request_kwargs(),
            ) as resp:
                _ = resp.status
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return

    async def validate(self) -> None:
        token = await self.login()
        self._token = token
        await self.logout()
