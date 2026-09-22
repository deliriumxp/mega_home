"""Thin HTTP client for the Mega Manager home-config endpoints."""

from __future__ import annotations

from typing import Any

from urllib.parse import quote

import aiohttp

from .const import (
    API_AGENT,
    API_APP_FILE,
    API_ASSET,
    API_APP_MANIFEST,
    API_CONFIG,
    API_ICON,
    API_INTEGRATION_FILE,
    API_INTEGRATION_MANIFEST,
    API_RELAY,
    API_VERSION,
    ICON_SIZE,
    RELAY_TIMEOUT,
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

    async def async_asset(self, key: str) -> bytes:
        """Return ONE file the manager named in the config manifest.

        ⚠ One method for every kind of file on purpose (`assets.py`): the
        manager decides what the key means, the home only carries the bytes.
        """
        return await self._get_bytes(f"{API_ASSET}/{quote(key, safe='')}")

    async def async_agent(
        self, version: str | None, reports: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Отдать отчёты сторожа и забрать его правила ОДНИМ запросом.

        ⚠ Один обход, а не два: дом на узком канале ходит сюда раз в четверть
        часа, и «правила отдельно, отчёты отдельно» стоило бы второго обхода
        ради тех же байтов. Совпавшая версия избавляет от тела правил.

        ⚠ Что означают правила, дом не знает и знать не должен (`agent.py`).
        """
        payload: dict[str, Any] = {"reports": reports}
        if version:
            payload["version"] = version
        _, answer = await self._request("POST", API_AGENT, json=payload)
        return _object(answer)

    async def async_relay(self, payload: dict[str, Any]) -> tuple[int, Any]:
        """Ask the manager something on behalf of the app; return status and answer.

        ⚠ Deliberately opaque: the home does not read the question and does not
        interpret the answer. That is what keeps a future feature — the AI chat
        first of all — from needing a release of this integration.
        """
        return await self._request("POST", API_RELAY, json=payload, timeout=RELAY_TIMEOUT, strict=False)

    async def _get_bytes(self, path: str) -> bytes:
        _, body = await self._request("GET", path, raw=True)
        return body

    async def async_app_manifest(self) -> dict[str, Any]:
        """Return the manifest of the resident app bundle."""
        return await self._get_json(API_APP_MANIFEST)

    async def async_app_file(self, path: str) -> bytes:
        """Return one file of the bundle, as bytes."""
        return await self._get_bytes(f"{API_APP_FILE}?path={quote(path)}")

    async def async_integration_manifest(self) -> dict[str, Any]:
        """Manifest of the integration code the manager serves (`ha_update.py`)."""
        return await self._get_json(API_INTEGRATION_MANIFEST)

    async def async_integration_file(self, path: str) -> bytes:
        """One file of the integration code, as bytes."""
        return await self._get_bytes(f"{API_INTEGRATION_FILE}?path={quote(path)}")

    async def _get_json(self, path: str) -> dict[str, Any]:
        _, payload = await self._request("GET", path)
        return _object(payload)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        timeout: float = REQUEST_TIMEOUT,
        raw: bool = False,
        strict: bool = True,
    ) -> tuple[int, Any]:
        """Один запрос к менеджеру. ЛЮБОЙ отказ дороги — `ManagerError`.

        ⚠ Таймаут тоже: у aiohttp это `TimeoutError`, а не `ClientError`, и до
        0.5.6 он проходил мимо — опрос конфига падал «неожиданной ошибкой» с
        трейсбеком и без отступа, синхронизация бандла (обещано «не бросает»)
        бросала, а сторож ронял весь цикл. Недоступный менеджер для дома — норма.

        `strict=False` — статус не отказ, а часть ответа (перенос `relay`).
        """
        try:
            async with self._session.request(
                method,
                f"{self._base}{path}",
                headers={"Authorization": f"Bearer {self._token}"},
                json=json,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if strict and response.status in (401, 403):
                    raise ManagerAuthError(f"manager rejected the object token ({response.status})")
                if strict and response.status >= 400:
                    raise ManagerError(f"manager answered with HTTP {response.status}")
                # The manager always answers JSON here, but a reverse proxy in
                # front of it may not (a captive portal or an error page), so
                # the content type is not trusted.
                body = await response.read() if raw else await response.json(content_type=None)
                return response.status, body
        except (aiohttp.ClientError, TimeoutError) as err:
            raise ManagerError(str(err) or "manager did not answer in time") from err
        except ValueError as err:
            raise ManagerError("manager answered with non-JSON content") from err


def _object(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ManagerError("manager answered with an unexpected payload")
    return payload
