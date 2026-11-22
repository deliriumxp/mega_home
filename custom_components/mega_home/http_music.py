"""Медиапорт для музыкальных плееров локальной сети."""

from __future__ import annotations

import ipaddress
import shutil
from collections.abc import Callable
from http import HTTPStatus
from typing import Any
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientTimeout, web

from homeassistant.core import HomeAssistant

from .core.const import LOGGER, URL_PREFIX
from .core.ops_base import OpError
from .http_door import _MegaHomeView, _coordinator
from .providers.music_flow import FlowSession, ffmpeg_decoder, ffmpeg_encoder
from .providers.music_pick import stream_content_type
from .providers.music_proxy import MusicStreamLinks

# Поток сессии: MP3 неизвестной длины — объявлять нечего, кэшировать нельзя (адрес — пропуск).
FLOW_HEADERS = {"Content-Type": "audio/mpeg", "Cache-Control": "no-store"}

# Поток трека: подключение к CDN и молчание в середине. Общего срока нет —
# трек идёт минутами, а `Range` заставляет плеер ходить несколько раз.
UPSTREAM_CONNECT_S = 10
UPSTREAM_READ_S = 30
STREAM_CHUNK = 64 * 1024

class MegaHomeMusicStreamView(_MegaHomeView):
    """`/mega-home/stream/<токен>` — короткий адрес трека Яндекс Музыки для плеера LAN.

    ⚠ Без ключа устройства и без куки: адрес забирает не браузер, а плеер
    (Sonos), и учётки у него нет. Пропуск — сам токен (`providers/music_proxy.py`):
    он случайный и живёт в памяти дома. Ссылку Яндекса дом разрешает в момент
    запроса — она длиннее предела плеера и истекает, поэтому хранить её в адресе
    нельзя (замер obj68, 2026-11-19: 1023 из 1024 знаков, `GET` → 404).
    """

    url = f"{URL_PREFIX}/stream/{{token}}"
    name = "mega_home:music_stream"

    async def get(self, request: web.Request, token: str) -> web.StreamResponse:
        return await self._stream(request, token, body=True)

    async def head(self, request: web.Request, token: str) -> web.StreamResponse:
        """Заголовки без тела: часть плееров сначала спрашивает размер и тип."""
        return await self._stream(request, token, body=False)

    async def _stream_flow(
        self, request: web.Request, session: FlowSession, coordinator: Any, *, body: bool,
    ) -> web.StreamResponse:
        """Поток сессии плеера: один адрес на всю сессию, элементы сменяет дом.

        ⚠ Плеер забирает такой адрес ОДИН раз и держит соединение: смена элемента приходит внутрь
        этого же ответа (`FlowSession.switch`). Так играет и плеер, который новый `media_url` поверх
        живого потока не берёт (прошитый Amazon Echo, живой случай 2026-11-22).
        """
        if not body:
            # HEAD: у потока неизвестной длины объявлять нечего, тело не открываем.
            return web.Response(status=HTTPStatus.OK, headers=FLOW_HEADERS)
        binary = await _ffmpeg(coordinator)
        if not binary:
            return self.json_message(
                "В этом доме нет ffmpeg — поток отдать нечем", HTTPStatus.SERVICE_UNAVAILABLE
            )
        response = web.StreamResponse(status=HTTPStatus.OK, headers=FLOW_HEADERS)
        await response.prepare(request)

        async def write(chunk: bytes) -> None:
            await response.write(chunk)

        try:
            await session.serve(write, lambda url: ffmpeg_decoder(binary, url), lambda out: ffmpeg_encoder(binary, out))
        except (ConnectionError, OSError):
            # Плеер закрыл поток (пауза, смена источника, выключение) — это норма, не ошибка.
            response.force_close()
            return response
        try:
            await response.write_eof()
        except (ConnectionError, OSError):
            response.force_close()
        return response

    async def _stream(self, request: web.Request, token: str, *, body: bool) -> web.StreamResponse:
        coordinator = _coordinator(request.app["hass"])
        # Сессия потока узнаётся ПЕРВОЙ: у неё тот же адрес, но за ним не трек, а живой поток
        # (`providers/music_flow.py`) — элементов в нём столько, сколько скажет очередь дома.
        playback = getattr(coordinator, "music_playback", None)
        session = playback.flow.find(token) if playback is not None else None
        if session is not None:
            return await self._stream_flow(request, session, coordinator, body=body)
        links = getattr(coordinator, "music_links", None)
        track = links.track(token) if links is not None else None
        if track is None:
            return self.json_message("Ссылка на трек устарела", HTTPStatus.NOT_FOUND)
        music = getattr(coordinator, "music", None)
        if music is None:
            return self.json_message(
                "Яндекс Музыка не подключена в этом доме", HTTPStatus.SERVICE_UNAVAILABLE
            )
        lock = links.lock(token)
        ready = links.resolved(token)
        if ready is None:
            async with lock:
                # Пока ждали, соседний запрос мог уже разрешить ссылку.
                ready = links.resolved(token)
                if ready is None:
                    ready = await _resolve_track(music, track, self, links, token)
                    if isinstance(ready, web.Response):
                        return ready
                    if ready is None:
                        return self.json_message(
                            "Яндекс Музыка не отдала поток трека", HTTPStatus.BAD_GATEWAY
                        )
                    links.remember(token, *ready)
        url, content_type = ready
        session = _client_session(request.app["hass"])
        headers = {}
        if range_header := request.headers.get("Range"):
            headers["Range"] = range_header
        # Общего таймаута нет: трек идёт минутами. Сторожим подключение и молчание CDN.
        timeout = ClientTimeout(
            total=None, sock_connect=UPSTREAM_CONNECT_S, sock_read=UPSTREAM_READ_S
        )
        for attempt in (0, 1):
            try:
                async with session.get(
                    url, headers=headers, timeout=timeout, allow_redirects=True
                ) as upstream:
                    if upstream.status in (401, 403, 404, 410) and attempt == 0:
                        # Ссылка истекла между выдачей и запросом: разрешаем заново,
                        # а не отдаём плееру отказ на ровном месте.
                        async with lock:
                            links.forget(token, expected_url=url)
                            refreshed = links.resolved(token)
                            if refreshed is None:
                                refreshed = await _resolve_track(music, track, self, links, token)
                                if isinstance(refreshed, web.Response):
                                    return refreshed
                                if refreshed is None:
                                    return self.json_message(
                                        "Яндекс Музыка не отдала поток трека",
                                        HTTPStatus.BAD_GATEWAY,
                                    )
                                links.remember(token, *refreshed)
                        url, content_type = refreshed
                        continue
                    if upstream.status == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                        return _range_refusal(upstream)
                    if upstream.status not in (200, 206):
                        LOGGER.warning(
                            "Яндекс Музыка: поток трека ответил %s", upstream.status
                        )
                        return self.json_message(
                            "Поток трека недоступен", HTTPStatus.BAD_GATEWAY
                        )
                    return await _pipe(request, upstream, content_type, body, lambda: links.opened(token))
            except (ClientError, TimeoutError, OSError) as err:
                LOGGER.warning("Яндекс Музыка: поток трека не открылся: %s", err)
                return self.json_message("Поток трека недоступен", HTTPStatus.BAD_GATEWAY)
        return self.json_message("Поток трека недоступен", HTTPStatus.BAD_GATEWAY)


async def _resolve_track(
    music: object, track: str, view: _MegaHomeView, links: MusicStreamLinks, token: str
) -> tuple[str, str] | web.Response | None:
    """Ссылка Яндекса и её `Content-Type` — тем же действием, что у приложения."""
    try:
        answer = await music.call(False, {"action": "stream", "id": track})  # type: ignore[attr-defined]
    except OpError as err:
        return view.json_message(err.message, err.status)
    chosen = links.pick(token, answer.get("infos") if isinstance(answer, dict) else None)
    if chosen is None:
        return None
    return chosen["url"], stream_content_type(chosen.get("codec"))


async def _ffmpeg(coordinator: Any) -> str | None:
    """Бинарь ffmpeg дома: им же живут go2rtc и запись камер — в образе Home Assistant он есть."""
    env = getattr(coordinator, "env", None)
    if env is None:
        return None
    return await env.run(shutil.which, "ffmpeg")


def _client_session(hass: HomeAssistant) -> object:
    """HTTP-сессия Home Assistant — её SSL и прокси; тесты подменяют эту функцию."""
    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    return async_get_clientsession(hass)


def _range_refusal(upstream: object) -> web.Response:
    """`416` — конец файла, а не «сервер сломан»: заголовок CDN отдаём как есть."""
    headers = {"Cache-Control": "no-store"}
    value = upstream.headers.get("Content-Range")  # type: ignore[attr-defined]
    if value:
        headers["Content-Range"] = value
    return web.Response(status=HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE, headers=headers)


async def _pipe(
    request: web.Request, upstream: object, content_type: str, body: bool, opened: Callable[[], None] | None
) -> web.StreamResponse:
    """Ответ плееру кусками — как пришло от CDN, включая `Range` и размер.

    ⚠ Ошибки ПОСЛЕ `prepare()` не превращаются в новый ответ: aiohttp записал бы вторую
    строку статуса в уже начатое тело. Обрыв CDN или ушедший клиент — это обрыв потока:
    закрываем соединение и возвращаем уже подготовленный ответ.
    """
    headers = {
        "Content-Type": content_type,
        # Адрес — пропуск, кэшировать его нельзя.
        "Cache-Control": "no-store",
    }
    # Тип от CDN главнее нашего: если он уже `audio/*`, значит знает контейнер точнее.
    upstream_type = upstream.headers.get("Content-Type") or ""  # type: ignore[attr-defined]
    if upstream_type.lower().startswith("audio/"):
        headers["Content-Type"] = upstream_type
    for name in ("Content-Length", "Content-Range", "Accept-Ranges", "Last-Modified", "ETag"):
        value = upstream.headers.get(name)  # type: ignore[attr-defined]
        if value:
            headers[name] = value
    response = web.StreamResponse(status=upstream.status, headers=headers)  # type: ignore[attr-defined]
    await response.prepare(request)
    if body:
        try:
            async for chunk in upstream.content.iter_chunked(STREAM_CHUNK):  # type: ignore[attr-defined]
                try:
                    await response.write(chunk)
                    if opened and chunk:
                        opened()
                        opened = None
                except ConnectionError:
                    # Плеер закрыл поток: пауза, перемотка, смена трека. Это норма.
                    response.force_close()
                    return response
        except (ClientError, TimeoutError, OSError) as err:
            # CDN оборвался уже после начала ответа — новый статус в тело не пишем.
            LOGGER.warning("Яндекс Музыка: поток оборвался после начала ответа: %s", err)
            response.force_close()
            return response
    try:
        await response.write_eof()
    except (ConnectionError, OSError):
        response.force_close()
    return response


def stream_origin(hass: HomeAssistant, config: dict | None = None) -> str | None:
    """Адрес дома, по которому плеер локальной сети заберёт поток.

    ⚠ Сперва `localUrl` конфига — адрес приложения в доме, который менеджер
    собирает тем же `haTarget`, что локальная дверь: хост со схемой и порт уже
    разобраны, HA-токен не нужен. Дальше — `house.ha.url` (адрес менеджера),
    `internal_url` («как зайти изнутри») и адрес, которым HA назвал себя.
    `external_url` не берём: это публичное имя, а плеер стоит в той же сети.
    Адреса localhost/loopback отбрасываем — плеер по ним не достучится.
    HTTPS без внутреннего адреса не предлагаем: сертификат выписан на внешнее
    имя, и плеер его не примет.
    """
    if isinstance(config, dict):
        local = _origin(config.get("localUrl"))
        if local:
            return local
        house = config.get("house")
        managed = house.get("ha") if isinstance(house, dict) else None
        url = managed.get("url") if isinstance(managed, dict) else None
        origin = _origin(url)
        if origin:
            return origin
    origin = _origin(getattr(hass.config, "internal_url", None))
    if origin:
        return origin
    api = getattr(hass.config, "api", None)
    if api is None or getattr(api, "use_ssl", False):
        return None
    ip = getattr(api, "local_ip", None)
    port = getattr(api, "port", None)
    if not isinstance(ip, str) or not port or _unreachable(ip):
        return None
    host = f"[{ip}]" if ":" in ip else ip
    return f"http://{host}:{port}"


def _origin(value: Any) -> str | None:
    """`http://host:port/путь` → `http://host:port`; пусто/криво/loopback — `None`."""
    if not isinstance(value, str) or not value.strip():
        return None
    parts = urlsplit(value.strip())
    if parts.scheme not in ("http", "https") or not parts.hostname or _unreachable(parts.hostname):
        return None
    return f"{parts.scheme}://{parts.netloc}"


def _unreachable(host: str) -> bool:
    """Адрес, по которому плеер в локальной сети не достучится до дома."""
    name = host.strip("[]").lower()
    if name in ("localhost", "0.0.0.0", "::", "::1", "127.0.0.1"):
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False
