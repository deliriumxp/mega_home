"""Яндекс Музыка: одна привязка и каталог для каждого дома."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from time import time
from typing import Any, NoReturn
from urllib.parse import urlsplit

from ya_passport_auth import (
    ClientConfig, Credentials, DeviceCodeSession, DeviceCodeTimeoutError,
    InvalidCredentialsError, PassportClient, SecretStr, YaPassportError,
)
from yandex_music import ClientAsync
from yandex_music.exceptions import NotFoundError, UnauthorizedError, YandexMusicError
from yandex_music.utils.request_async import Request

from ..core.const import LOGGER
from ..core.host import Host, KeyStore
from ..core.ops_base import OpError
from .music_catalog import catalog, favorite_ids, queue_ids, set_favorite, track_details
from .music_stream import stream as stream_track


@dataclass
class Attempt:
    status: dict[str, Any]
    task: asyncio.Task | None = None


class MusicForbidden(YandexMusicError):
    status_code = 403


class MusicUnauthorized(UnauthorizedError):
    status_code = 401


class MusicRequest(Request):
    def _handle_error_response(self, status_code: int, content: bytes) -> NoReturn:
        # SDK сводит 401 и 403 к одному исключению. Только 401 требует обновления токена.
        if status_code == 401:
            raise MusicUnauthorized()
        if status_code == 403:
            raise MusicForbidden()
        return super()._handle_error_response(status_code, content)


class MusicService:
    def __init__(self, env: Host, home_key: str) -> None:
        self._env = env
        self._store = env.store(f"mega_home_yandex_music.{home_key}", private=True)
        # Под блокировкой — запись привязки и обмен токенов. Каталог и поток читаются
        # параллельно, иначе долгий поиск блокирует запуск трека у всей семьи.
        self._lock = asyncio.Lock()
        self._revision = 0
        self._attempt: Attempt | None = None

    def stop(self) -> None:
        self._cancel()

    def _cancel(self) -> None:
        attempt, self._attempt = self._attempt, None
        if attempt and attempt.task:
            attempt.task.cancel()

    async def call(self, can_manage: bool, request: dict) -> dict:
        action = request.get("action")
        if action not in ("status", "start", "cancel", "disconnect", "search", "liked", "playlists", "playlist",
                          "album", "artist", "stream", "track_details", "favorite_ids", "set_favorite"):
            raise OpError("Неизвестное действие Яндекс Музыки", 404)
        if action == "set_favorite" and not can_manage:
            raise OpError("Избранное Яндекс Музыки меняют жильцы с учётной записью дома", 403)
        if action in ("start", "cancel", "disconnect") and not can_manage:
            raise OpError("Аккаунтом Яндекс Музыки управляют жильцы дома", 403)
        try:
            async with asyncio.timeout(26):
                async with self._lock:
                    if action == "disconnect":
                        self._cancel()
                        await self._store.async_save({})
                        self._revision += 1
                        return {"state": "disconnected", "canManage": True}
                    credentials = _load(await self._store.async_load())
                    if action == "cancel":
                        self._cancel()
                    if action in ("status", "cancel") or (action == "start" and credentials):
                        return self._status(credentials, can_manage)
                    if action == "start":
                        return await self._start()
                    if credentials is None:
                        raise OpError("Подключите аккаунт Яндекс Музыки", 409)
                    revision = self._revision
                answer = await self._catalog(credentials, revision, action, request)
                if revision != self._revision:
                    raise OpError("Привязка Яндекс Музыки изменилась. Повторите запрос", 412)
                return answer
        except NotFoundError:
            raise OpError("В Яндекс Музыке ничего не найдено", 404) from None
        except MusicForbidden as error:
            _log_failure(action, error)
            message = "Яндекс Музыка не разрешила воспроизведение этого трека" if action == "stream" else "Яндекс Музыка ограничила доступ к этому разделу"
            raise OpError(message, 503) from None
        except (YaPassportError, YandexMusicError, TimeoutError, ValueError, OSError) as error:
            _log_failure(action, error)
            message = "Каталог Яндекс Музыки недоступен из сети дома" if _restricted(error) else "Яндекс Музыка не ответила. Попробуйте ещё раз"
            raise OpError(message, 503) from None

    async def with_passport(self, use: Callable[[Credentials | None], Awaitable[dict]]) -> dict:
        # Выпуск и отключение привязки сериализованы с передачей учётки в HA.
        async with self._lock:
            return await use(_load(await self._store.async_load()))

    def _status(self, credentials: Credentials | None, can_manage: bool) -> dict:
        if credentials:
            return {"state": "connected", "canManage": can_manage, "canEditFavorites": can_manage,
                    "login": credentials.display_login if can_manage else None}
        if can_manage and self._attempt:
            return {**self._attempt.status, "canManage": True}
        return {"state": "disconnected", "canManage": can_manage}

    async def _start(self) -> dict:
        previous = self._attempt
        if previous and previous.status["state"] == "pending" and previous.status["expiresAt"] > time() * 1000:
            return self._status(None, True)
        self._cancel()
        async with PassportClient.create(config=ClientConfig()) as passport:
            device = await passport.start_device_login(device_name="Mega Home")
        url = urlsplit(device.verification_url)
        if url.scheme != "https" or url.hostname not in {"ya.ru", "yandex.ru", "oauth.yandex.ru"}:
            raise ValueError("Адрес подтверждения Яндекса изменился")
        attempt = Attempt({
            "state": "pending", "code": device.user_code, "url": device.verification_url,
            "expiresAt": int((time() + device.expires_in) * 1000),
        })
        self._attempt = attempt
        attempt.task = self._env.spawn(self._finish(attempt, device), "Яндекс Музыка: вход")
        return self._status(None, True)

    async def _finish(self, attempt: Attempt, device: DeviceCodeSession) -> None:
        try:
            async with PassportClient.create(config=ClientConfig()) as passport:
                async with asyncio.timeout(device.expires_in + 20):
                    credentials = await passport.poll_device_until_confirmed(device)
            if credentials.music_token is None:
                raise ValueError("Яндекс не выдал музыкальный токен")
            async with self._lock:
                if self._attempt is attempt:
                    await _save(self._store, credentials)
                    self._revision += 1
                    self._attempt = None
        except (DeviceCodeTimeoutError, TimeoutError):
            attempt.status = {"state": "expired"}
        except InvalidCredentialsError:
            attempt.status = {"state": "denied"}
        except (YaPassportError, ValueError, OSError) as error:
            _log_failure("confirm", error)
            attempt.status = {"state": "failed"}

    async def _catalog(self, credentials: Credentials, revision: int, action: str, request: dict) -> dict:
        async def read(current: Credentials) -> dict:
            if current.music_token is None:
                raise UnauthorizedError("Нет музыкального токена")
            client = await ClientAsync(current.music_token.get_secret(), request=MusicRequest()).init()
            if action == "stream":
                return await stream_track(client, request)
            if action == "track_details":
                return await track_details(client, queue_ids(request.get("ids")))
            if action == "favorite_ids":
                return await favorite_ids(client)
            if action == "set_favorite":
                return await set_favorite(client, request)
            return await catalog(client, action, request)

        try:
            return await read(credentials)
        except UnauthorizedError:
            pass
        refreshed = await self._refresh(credentials, revision)
        try:
            return await read(refreshed)
        except UnauthorizedError as error:
            _log_failure(action, error)
            raise OpError("Яндекс Музыка отклонила запрос. Проверьте доступ в аккаунте Яндекса", 503) from None

    async def _refresh(self, rejected: Credentials, revision: int) -> Credentials:
        async with self._lock:
            current = _load(await self._store.async_load())
            if current is None or revision != self._revision:
                raise OpError("Привязка Яндекс Музыки изменилась. Повторите запрос", 412)
            if current != rejected:
                return current
            try:
                async with PassportClient.create(config=ClientConfig()) as passport:
                    try:
                        token = await passport.refresh_music_token(current.x_token)
                        refreshed = replace(current, music_token=token)
                    except InvalidCredentialsError:
                        refreshed = await passport.refresh_credentials(current)
            except InvalidCredentialsError as error:
                # Обмен токена Музыки может отказать даже после выдачи нового токена Паспорта.
                # Сохраняем привязку дома до явного отключения жильцом.
                _log_failure("refresh", error)
                raise OpError("Яндекс не обновил сеанс Музыки. Переподключите аккаунт в настройках", 503) from None
            await _save(self._store, refreshed)
            return refreshed


def _load(data: Any) -> Credentials | None:
    if not isinstance(data, dict) or not data.get("x_token"):
        return None
    return Credentials(
        x_token=SecretStr(data["x_token"]),
        music_token=SecretStr(data["music_token"]) if data.get("music_token") else None,
        refresh_token=SecretStr(data["refresh_token"]) if data.get("refresh_token") else None,
        display_login=data.get("login"),
    )


async def _save(store: KeyStore, credentials: Credentials) -> None:
    await store.async_save({
        "x_token": credentials.x_token.get_secret(),
        "music_token": credentials.music_token.get_secret() if credentials.music_token else None,
        "refresh_token": credentials.refresh_token.get_secret() if credentials.refresh_token else None,
        "login": credentials.display_login,
    })


def _restricted(error: Exception) -> bool:
    return getattr(error, "status_code", None) == 451 or (isinstance(error, YandexMusicError) and "(451)" in str(error))


def _log_failure(action: str, error: Exception) -> None:
    # В исключениях сторонних библиотек бывают тела ответов с деталями учётки.
    LOGGER.warning("Яндекс Музыка: действие %s, ошибка %s, HTTP %s", action, type(error).__name__, 451 if _restricted(error) else getattr(error, "status_code", None))
