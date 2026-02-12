from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import asyncio
import json
import uuid

import aiohttp


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

    async def login(self) -> str:
        url = f"{self.base_url}/rest/v3/login/sessions"
        payload = {"username": self._cfg.username, "password": self._cfg.password, "setCookie": False}

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

                token = await self._parse_token(resp)
                if not token:
                    raise DwSpectrumConnectionError("Login succeeded but no token returned")

                self._token = token
                return token

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise DwSpectrumConnectionError(str(err)) from err

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

                return await resp.json(content_type=None)

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise DwSpectrumConnectionError(str(err)) from err

    # -----------------------
    # Devices / Cameras
    # -----------------------
    async def get_devices(self) -> list[dict[str, Any]]:
        params = {"_with": "id,name,deviceType,type,model,physicalId,logicalId,isOnline,status,schedule"}
        data = await self._request_json("GET", "/rest/v3/devices", params=params)

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("items", "data", "devices"):
                if isinstance(data.get(key), list):
                    return data[key]

        raise DwSpectrumConnectionError("Unexpected /rest/v3/devices response shape")

    async def get_cameras(self) -> list[dict[str, Any]]:
        devices = await self.get_devices()
        cams: list[dict[str, Any]] = []
        for d in devices:
            dt = str(d.get("deviceType", "")).lower()
            typ = str(d.get("type", "")).lower()
            if dt == "camera" or "camera" in typ:
                cams.append(d)
        return cams

    async def get_device(self, device_id: str) -> dict[str, Any]:
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

    # -----------------------
    # Server / Users / Licenses
    # -----------------------
    async def get_system_info(self) -> dict[str, Any]:
        data = await self._request_json("GET", "/rest/v3/system/info")
        if isinstance(data, dict):
            return data
        raise DwSpectrumConnectionError("Unexpected /rest/v3/system/info response shape")

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

    async def logout(self) -> None:
        if not self._token:
            return

        token = self._token
        self._token = None

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
