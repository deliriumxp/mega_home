"""Thin HTTP client for the Mega Manager home-config endpoints."""

from __future__ import annotations

from typing import Any

from urllib.parse import quote

import aiohttp

from .const import (
    API_APP_FILE,
    API_APP_MANIFEST,
    API_CONFIG,
    API_ICON,
    API_ROOM_PHOTO,
    API_VERSION,
    ICON_SIZE,
    REQUEST_TIMEOUT,
)


class ManagerError(Exception):
    """The manager could not be reached or answered with an error."""


class ManagerAuthError(ManagerError):
    """The object token was rejected (401)."""


class ManagerClient:
    """Talks to one Mega Manager on behalf of one object.

    Authentication is the object's own webhook token, the same secret the
    router's netwatch hooks already use. Nothing here is object-specific
    otherwise: the manager resolves the object from the token, so a token that
    was moved to another object simply starts returning that object's home.
    """

    def __init__(
        self, session: aiohttp.ClientSession, base_url: str, token: str
    ) -> None:
        self._session = session
        self._base = base_url.rstrip("/")
        self._token = token

    @property
    def base_url(self) -> str:
        """Manager base URL, without a trailing slash."""
        return self._base

    async def async_version(self) -> str:
        """Return the current config version hash."""
        payload = await self._get_json(API_VERSION)
        version = payload.get("version")
        if not isinstance(version, str) or not version:
            raise ManagerError("manager returned no config version")
        return version

    async def async_config(self) -> dict[str, Any]:
        """Return the full home config."""
        payload = await self._get_json(API_CONFIG)
        if not isinstance(payload.get("tiles"), list):
            raise ManagerError("manager returned a config without tiles")
        return payload

    async def async_icon(self, icon: str, size: str = ICON_SIZE) -> bytes:
        """Return one scenario icon as PNG bytes."""
        return await self._get_bytes(f"{API_ICON}/{icon}?size={size}")

    async def async_room_photo(self, room_id: str) -> bytes:
        """Return the installer's background for one room, as JPEG bytes."""
        return await self._get_bytes(f"{API_ROOM_PHOTO}/{quote(room_id)}")

    async def _get_bytes(self, path: str) -> bytes:
        try:
            async with self._session.get(
                f"{self._base}{path}",
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as response:
                self._raise_for_status(response.status)
                return await response.read()
        except aiohttp.ClientError as err:
            raise ManagerError(str(err)) from err

    async def async_app_manifest(self) -> dict[str, Any]:
        """Return the manifest of the resident app bundle."""
        return await self._get_json(API_APP_MANIFEST)

    async def async_app_file(self, path: str) -> bytes:
        """Return one file of the bundle, as bytes."""
        return await self._get_bytes(f"{API_APP_FILE}?path={quote(path)}")

    async def _get_json(self, path: str) -> dict[str, Any]:
        try:
            async with self._session.get(
                f"{self._base}{path}",
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as response:
                self._raise_for_status(response.status)
                # The manager always answers JSON here, but a reverse proxy in
                # front of it may not (a captive portal or an error page), so
                # the content type is not trusted.
                payload = await response.json(content_type=None)
        except aiohttp.ClientError as err:
            raise ManagerError(str(err)) from err
        except ValueError as err:
            raise ManagerError("manager answered with non-JSON content") from err
        if not isinstance(payload, dict):
            raise ManagerError("manager answered with an unexpected payload")
        return payload

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    @staticmethod
    def _raise_for_status(status: int) -> None:
        if status in (401, 403):
            raise ManagerAuthError(f"manager rejected the object token ({status})")
        if status >= 400:
            raise ManagerError(f"manager answered with HTTP {status}")
