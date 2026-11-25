"""Короткие адреса трека: токен, трек, кэш ссылки и сам потоковый маршрут.

Адрес нужен плееру, у которого есть предел длины `mediaUrl` (Sonos — 1024
символа, ссылка Яндекса ~1100): дом отдаёт свой короткий адрес, а ссылку Яндекса
разрешает в момент запроса — ровно так делает стрим-сервер Music Assistant.
"""

from __future__ import annotations

import asyncio
import shutil
from types import SimpleNamespace
from time import monotonic

import aiohttp
import pytest
from aiohttp import web

from mega_home import http as mega_http, http_music
from mega_home.core.host import PlainHost
from mega_home.providers import music_proxy
from mega_home.providers.music_flow import FlowStreams
from mega_home.providers.music_proxy import MusicStreamLinks

BODY = bytes(range(256)) * 4


def _token(address: str) -> str:
    return address.rsplit("/", 1)[1]


def test_короткий_адрес_ведёт_на_маршрут_дома_и_помнит_трек():
    links = MusicStreamLinks("http://192.168.88.102:8123/")

    address = links.address("123:456")

    assert address is not None
    assert address.startswith("http://192.168.88.102:8123/mega-home/stream/")
    assert links.track(_token(address)) == "123:456"


def test_без_своего_адреса_короткой_ссылки_нет():
    links = MusicStreamLinks(None)

    assert links.address("1") is None
    assert links.with_address("1", {"infos": []}) == {"infos": []}


def test_ответ_stream_получает_короткий_адрес_и_тёплую_ссылку_рядом_с_прямыми():
    links = MusicStreamLinks("http://home:8123")
    infos = [{"url": "https://cdn/1.mp3", "codec": "mp3", "bitrate": 320}]

    answer = links.with_address("7", {"infos": infos, "url": "https://cdn/1.mp3", "format": "mp3"})

    assert answer["infos"] == infos
    assert answer["url"] == "https://cdn/1.mp3"
    assert answer["proxy"].startswith("http://home:8123/mega-home/stream/")
    # Прогрев: плеер придёт через секунды, и второго запроса к Яндексу быть не должно.
    assert links.resolved(_token(answer["proxy"])) == ("https://cdn/1.mp3", "audio/mpeg")


def test_тот_же_трек_получает_тот_же_адрес():
    links = MusicStreamLinks("http://home:8123")

    first = links.address("1")
    second = links.address("1")

    assert first == second
    assert links.address("2") != first


def test_короткий_адрес_несёт_расширение_и_media_source_id():
    links = MusicStreamLinks("http://home:8123")
    infos = [{"url": "https://cdn/1.mp3", "codec": "mp3", "bitrate": 320}]

    answer = links.with_address("7", {"infos": infos})

    assert answer["proxy"].endswith(".mp3")
    assert answer["mediaSource"].startswith("media-source://mega_home/track/")
    token = answer["mediaSource"].rsplit("/", 1)[1]
    assert links.public(token) == answer["proxy"]


def test_mp4_кодек_отдаётся_как_audio_mp4_и_m4a():
    from mega_home.providers.music_pick import stream_content_type, stream_extension

    assert stream_content_type("aac-mp4") == "audio/mp4"
    assert stream_extension("aac-mp4") == ".m4a"
    assert stream_content_type("mp3") == "audio/mpeg"
    assert stream_extension("mp3") == ".mp3"


def test_неизвестный_токен_трек_не_называет():
    assert MusicStreamLinks("http://home:8123").track("выдуманный") is None


def test_ссылка_яндекса_кэшируется_на_время_трека_и_забывается():
    links = MusicStreamLinks("http://home:8123")
    token = _token(links.address("1") or "")

    assert links.resolved(token) is None
    links.remember(token, "https://cdn/1.mp3", "audio/mpeg")
    assert links.resolved(token) == ("https://cdn/1.mp3", "audio/mpeg")
    # Отказ CDN (401/403/404/410) — повод разрешить заново, а не отдать старую ссылку.
    links.forget(token)
    assert links.resolved(token) is None


def test_просроченный_токен_и_просроченная_ссылка_не_отдаются():
    links = MusicStreamLinks("http://home:8123")
    token = _token(links.address("1") or "")
    links._links[token] = music_proxy._Link("1", monotonic() - music_proxy.LINK_TTL_S - 1)

    assert links.track(token) is None
    assert links.resolved(token) is None


def test_потолок_вытесняет_только_при_выдаче_и_не_рвёт_чтение():
    """⚠ Потолок в `_expire` удалял бы токен, который сейчас читают."""
    links = MusicStreamLinks("http://home:8123")
    tokens = [_token(links.address(str(i)) or "") for i in range(music_proxy.MAX_LINKS)]

    assert links.track(tokens[0]) == "0"
    assert links.address("новый") is not None
    assert links.track(tokens[0]) is None
    assert links.track(tokens[1]) == "1"


def test_адрес_дома_берётся_из_конфига_дома_и_не_отдаёт_loopback():
    hass = SimpleNamespace(config=SimpleNamespace(internal_url=None, api=None))

    # localUrl — тот же адрес, что у локальной двери: порт и схема уже разобраны.
    assert http_music.stream_origin(hass, {"localUrl": "http://192.168.88.102:8123/mega-home/"}) == (
        "http://192.168.88.102:8123"
    )
    # house.ha.url — запасной, если localUrl нет.
    assert http_music.stream_origin(hass, {"house": {"ha": {"url": "https://ha.example:8443/"}}}) == (
        "https://ha.example:8443"
    )
    # localhost из конфига — не адрес для плеера: идём дальше по цепочке.
    named = SimpleNamespace(config=SimpleNamespace(internal_url="http://home.local:8123/", api=None))
    assert http_music.stream_origin(named, {"localUrl": "http://localhost:8123/mega-home/"}) == "http://home.local:8123"

    own = SimpleNamespace(
        config=SimpleNamespace(
            internal_url=None,
            api=SimpleNamespace(use_ssl=False, local_ip="192.168.88.102", port=8123),
        )
    )
    assert http_music.stream_origin(own) == "http://192.168.88.102:8123"

    # Порт — тот, на котором HA реально слушает (новые версии — и на 80), а не константа.
    plain = SimpleNamespace(
        config=SimpleNamespace(
            internal_url=None, api=SimpleNamespace(use_ssl=False, local_ip="192.168.88.102", port=80)
        )
    )
    assert http_music.stream_origin(plain) == "http://192.168.88.102:80"

    # IPv6 — в скобках, иначе адрес не разобрать.
    v6 = SimpleNamespace(
        config=SimpleNamespace(
            internal_url=None, api=SimpleNamespace(use_ssl=False, local_ip="fe80::1", port=8123)
        )
    )
    assert http_music.stream_origin(v6) == "http://[fe80::1]:8123"

    # HTTPS без внутреннего имени не предлагаем: сертификат выписан на внешнее имя,
    # и плеер в локальной сети его не примет.
    secure = SimpleNamespace(
        config=SimpleNamespace(
            internal_url=None, api=SimpleNamespace(use_ssl=True, local_ip="192.168.88.102", port=8123)
        )
    )
    assert http_music.stream_origin(secure) is None

    # `internal_url`, записанный с localhost, тоже не годится.
    loopback = SimpleNamespace(
        config=SimpleNamespace(
            internal_url="http://127.0.0.1:8123/",
            api=SimpleNamespace(use_ssl=False, local_ip="192.168.88.102", port=8123),
        )
    )
    assert http_music.stream_origin(loopback) == "http://192.168.88.102:8123"


class FakeEntries:
    def __init__(self, coordinator: object) -> None:
        self._coordinator = coordinator

    def async_loaded_entries(self, _domain: str) -> list[object]:
        return [SimpleNamespace(runtime_data=self._coordinator)]


def _hass(coordinator: object) -> SimpleNamespace:
    return SimpleNamespace(config_entries=FakeEntries(coordinator))


class FakeMusic:
    def __init__(self, url: str) -> None:
        self.url = url
        self.calls: list[tuple[bool, dict]] = []

    async def call(self, can_manage: bool, request: dict) -> dict:
        self.calls.append((can_manage, request))
        return {"infos": [{"url": self.url, "codec": "mp3", "bitrate": 320}]}


async def _serve(app: web.Application) -> tuple[web.AppRunner, int]:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr,index]
    return runner, port


async def _upstream(request: web.Request) -> web.StreamResponse:
    headers = {"Content-Type": "application/octet-stream", "Accept-Ranges": "bytes"}
    value = request.headers.get("Range")
    if not value:
        return web.Response(body=BODY, headers={**headers, "Content-Length": str(len(BODY))})
    first, _, last = value.removeprefix("bytes=").partition("-")
    start = int(first or 0)
    end = min(int(last) if last else len(BODY) - 1, len(BODY) - 1)
    if start >= len(BODY):
        return web.Response(
            status=416, headers={**headers, "Content-Range": f"bytes */{len(BODY)}"}
        )
    return web.Response(
        status=206,
        body=BODY[start : end + 1],
        headers={
            **headers,
            "Content-Range": f"bytes {start}-{end}/{len(BODY)}",
            "Content-Length": str(end - start + 1),
        },
    )


async def _broken(request: web.Request) -> web.StreamResponse:
    """CDN отдал заголовки и половину тела — потом оборвался."""
    response = web.StreamResponse(
        headers={"Content-Type": "audio/mpeg", "Content-Length": str(len(BODY))}
    )
    await response.prepare(request)
    await response.write(BODY[:10])
    request.transport.close()  # type: ignore[union-attr]
    return response


def _view_app(coordinator: object) -> tuple[web.Application, http_music.MegaHomeMusicStreamView]:
    app = web.Application()
    app["hass"] = _hass(coordinator)
    view = http_music.MegaHomeMusicStreamView()
    # Заглушка `HomeAssistantView` в conftest отдаёт свой объект; здесь нужен
    # настоящий ответ aiohttp, иначе сервер не сможет его отправить.
    view.json_message = lambda message, status=400: web.json_response(  # type: ignore[method-assign]
        {"message": message}, status=status
    )
    return app, view


async def _mount(app: web.Application, view: http_music.MegaHomeMusicStreamView) -> None:
    path = "/mega-home/stream/{token}"

    async def stream(request: web.Request) -> web.StreamResponse:
        # HA передаёт части пути аргументами (`handler(request, **match_info)`);
        # в голом aiohttp это делает обёртка.
        return await view.get(request, **request.match_info)

    async def head(request: web.Request) -> web.StreamResponse:
        return await view.head(request, **request.match_info)

    # `add_get` сам добавляет HEAD на GET-обработчик — здесь маршруты разные.
    app.router.add_route("HEAD", path, head)
    app.router.add_route("GET", path, stream)


async def _case() -> None:
    upstream_app = web.Application()
    upstream_app.router.add_get("/track.mp3", _upstream)
    upstream_runner, port = await _serve(upstream_app)

    links = MusicStreamLinks("http://home:8123")
    token = _token(links.address("1") or "")
    music = FakeMusic(f"http://127.0.0.1:{port}/track.mp3")
    coordinator = SimpleNamespace(music=music, music_links=links)

    app, view = _view_app(coordinator)
    await _mount(app, view)
    runner, home_port = await _serve(app)

    session = aiohttp.ClientSession()
    original = http_music._client_session
    http_music._client_session = lambda _hass: session
    base = f"http://127.0.0.1:{home_port}/mega-home/stream"
    try:
        async with session.get(f"{base}/{token}") as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "audio/mpeg"
            assert response.headers["Cache-Control"] == "no-store"
            assert await response.read() == BODY

        async with session.get(f"{base}/{token}", headers={"Range": "bytes=0-9"}) as response:
            assert response.status == 206
            assert response.headers["Content-Range"] == f"bytes 0-9/{len(BODY)}"
            assert await response.read() == BODY[:10]

        async with session.get(f"{base}/{token}", headers={"Range": "bytes=999999-"}) as response:
            assert response.status == 416
            assert response.headers["Content-Range"] == f"bytes */{len(BODY)}"

        async with session.head(f"{base}/{token}") as response:
            assert response.status == 200
            assert response.headers["Content-Length"] == str(len(BODY))
            assert await response.read() == b""

        async with session.get(f"{base}/чужой") as response:
            assert response.status == 404

        # Тёплая ссылка: первый GET разрешил, дальше Range/HEAD к Яндексу не ходят.
        assert len(music.calls) == 1
    finally:
        http_music._client_session = original
        await session.close()
        await runner.cleanup()
        await upstream_runner.cleanup()


def test_маршрут_отдаёт_поток_range_head_416_и_не_пускает_чужой_токен():
    asyncio.run(_case())


async def _broken_case() -> None:
    upstream_app = web.Application()
    upstream_app.router.add_get("/broken.mp3", _broken)
    upstream_runner, port = await _serve(upstream_app)

    links = MusicStreamLinks("http://home:8123")
    token = _token(links.address("1") or "")
    coordinator = SimpleNamespace(music=FakeMusic(f"http://127.0.0.1:{port}/broken.mp3"), music_links=links)

    app, view = _view_app(coordinator)
    await _mount(app, view)
    runner, home_port = await _serve(app)

    session = aiohttp.ClientSession()
    original = http_music._client_session
    http_music._client_session = lambda _hass: session
    try:
        async with session.get(f"http://127.0.0.1:{home_port}/mega-home/stream/{token}") as response:
            streamed = bytearray()
            try:
                async for chunk in response.content.iter_chunked(1024):
                    streamed.extend(chunk)
            except aiohttp.ClientPayloadError:
                pass
            # Обрыв CDN — короткое тело, а не второй HTTP-статус внутри аудио.
            assert response.status == 200
            assert streamed == BODY[:10]
            assert b"HTTP/1.1" not in streamed
    finally:
        http_music._client_session = original
        await session.close()
        await runner.cleanup()
        await upstream_runner.cleanup()


def test_обрыв_cdn_посреди_трека_не_пишет_второй_статус():
    asyncio.run(_broken_case())


async def _media_source_case() -> None:
    links = MusicStreamLinks("http://home:8123")
    answer = links.with_address("7", {"infos": [{"url": "https://cdn/1.mp3", "codec": "mp3", "bitrate": 320}]})
    token = answer["mediaSource"].rsplit("/", 1)[1]
    links.forget(token)  # ссылку ещё не разрешали для этого токена
    music = FakeMusic("https://cdn/1.mp3")

    ready = await music_proxy.resolve_for_media_source(music, links, token)

    assert ready == (answer["proxy"], "audio/mpeg")
    assert len(music.calls) == 1
    assert links.public(token) == answer["proxy"]


def test_media_source_разрешает_ссылку_тем_же_действием_что_приложение():
    asyncio.run(_media_source_case())


def test_маршрут_зарегистрирован_до_раздачи_бандла():
    names = [view.__name__ for view in mega_http.VIEWS]

    assert "MegaHomeMusicStreamView" in names
    # `/mega-home/{path:.*}` отвечает первым подошедшим — наш путь обязан быть раньше.
    assert names.index("MegaHomeMusicStreamView") < names.index("MegaHomeAppView")
