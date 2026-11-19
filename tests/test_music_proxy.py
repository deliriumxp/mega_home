"""Короткие адреса трека: токен, трек, кэш ссылки и сам потоковый маршрут.

Адрес нужен плееру, у которого есть предел длины `mediaUrl` (Sonos — 1024
символа, ссылка Яндекса ~1100): дом отдаёт свой короткий адрес, а ссылку Яндекса
разрешает в момент запроса — ровно так делает стрим-сервер Music Assistant.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from time import monotonic

import aiohttp
from aiohttp import web

from mega_home import http as mega_http
from mega_home.providers import music_proxy
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


def test_ответ_stream_получает_короткий_адрес_рядом_с_прямыми():
    links = MusicStreamLinks("http://home:8123")
    infos = [{"url": "https://cdn/1.mp3", "codec": "mp3", "bitrate": 320}]

    answer = links.with_address("7", {"infos": infos, "url": "https://cdn/1.mp3", "format": "mp3"})

    assert answer["infos"] == infos
    assert answer["url"] == "https://cdn/1.mp3"
    assert answer["proxy"].startswith("http://home:8123/mega-home/stream/")


def test_неизвестный_токен_трек_не_называет():
    assert MusicStreamLinks("http://home:8123").track("выдуманный") is None


def test_ссылка_яндекса_кэшируется_на_время_трека_и_забывается():
    links = MusicStreamLinks("http://home:8123")
    token = _token(links.address("1") or "")

    assert links.resolved(token) is None
    links.remember(token, "https://cdn/1.mp3", "mp3")
    assert links.resolved(token) == ("https://cdn/1.mp3", "mp3")
    # Отказ CDN (403/404) — повод разрешить заново, а не отдать старую ссылку.
    links.forget(token)
    assert links.resolved(token) is None


def test_просроченный_токен_и_просроченная_ссылка_не_отдаются():
    links = MusicStreamLinks("http://home:8123")
    token = _token(links.address("1") or "")
    links._links[token] = music_proxy._Link("1", monotonic() - music_proxy.LINK_TTL_S - 1)

    assert links.track(token) is None
    assert links.resolved(token) is None


def test_адрес_дома_берётся_из_внутреннего_имени_или_своего_лица():
    # Адрес, которым в дом ходит менеджер, — первый: в контейнере local_ip бывает мостом.
    managed = SimpleNamespace(config=SimpleNamespace(internal_url="http://home.local:8123", api=None))
    assert mega_http.stream_origin(managed, {"ha": {"url": "http://192.168.88.102:8123/"}}) == "http://192.168.88.102:8123"

    named = SimpleNamespace(config=SimpleNamespace(internal_url="http://home.local:8123/", api=None))
    assert mega_http.stream_origin(named) == "http://home.local:8123"

    own = SimpleNamespace(
        config=SimpleNamespace(
            internal_url=None,
            api=SimpleNamespace(use_ssl=False, local_ip="192.168.88.102", port=8123),
        )
    )
    assert mega_http.stream_origin(own) == "http://192.168.88.102:8123"

    # Порт — тот, на котором HA реально слушает (новые версии — и на 80), а не константа.
    plain = SimpleNamespace(
        config=SimpleNamespace(
            internal_url=None, api=SimpleNamespace(use_ssl=False, local_ip="192.168.88.102", port=80)
        )
    )
    assert mega_http.stream_origin(plain) == "http://192.168.88.102:80"

    # HTTPS без внутреннего имени не предлагаем: сертификат выписан на внешнее имя,
    # и плеер в локальной сети его не примет.
    secure = SimpleNamespace(
        config=SimpleNamespace(
            internal_url=None, api=SimpleNamespace(use_ssl=True, local_ip="192.168.88.102", port=8123)
        )
    )
    assert mega_http.stream_origin(secure) is None


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
    return web.Response(
        status=206,
        body=BODY[start : end + 1],
        headers={
            **headers,
            "Content-Range": f"bytes {start}-{end}/{len(BODY)}",
            "Content-Length": str(end - start + 1),
        },
    )


async def _case() -> None:
    upstream_app = web.Application()
    upstream_app.router.add_get("/track.mp3", _upstream)
    upstream_runner, port = await _serve(upstream_app)

    links = MusicStreamLinks("http://home:8123")
    token = _token(links.address("1") or "")
    coordinator = SimpleNamespace(music=FakeMusic(f"http://127.0.0.1:{port}/track.mp3"), music_links=links)
    hass = _hass(coordinator)

    app = web.Application()
    app["hass"] = hass
    view = mega_http.MegaHomeMusicStreamView()
    # Заглушка `HomeAssistantView` в conftest отдаёт свой объект; здесь нужен
    # настоящий ответ aiohttp, иначе сервер не сможет его отправить.
    view.json_message = lambda message, status=400: web.json_response(  # type: ignore[method-assign]
        {"message": message}, status=status
    )

    async def stream(request: web.Request) -> web.StreamResponse:
        # HA передаёт части пути аргументами (`handler(request, **match_info)`);
        # в голом aiohttp это делает обёртка.
        return await view.get(request, **request.match_info)

    app.router.add_get("/mega-home/stream/{token}", stream)
    runner, home_port = await _serve(app)

    session = aiohttp.ClientSession()
    original = mega_http._client_session
    mega_http._client_session = lambda _hass: session
    try:
        async with session.get(f"http://127.0.0.1:{home_port}/mega-home/stream/{token}") as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "audio/mpeg"
            assert response.headers["Cache-Control"] == "no-store"
            assert await response.read() == BODY

        async with session.get(
            f"http://127.0.0.1:{home_port}/mega-home/stream/{token}",
            headers={"Range": "bytes=0-9"},
        ) as response:
            assert response.status == 206
            assert response.headers["Content-Range"] == f"bytes 0-9/{len(BODY)}"
            assert await response.read() == BODY[:10]

        async with session.get(f"http://127.0.0.1:{home_port}/mega-home/stream/чужой") as response:
            assert response.status == 404
    finally:
        mega_http._client_session = original
        await session.close()
        await runner.cleanup()
        await upstream_runner.cleanup()


def test_маршрут_отдаёт_поток_с_range_и_не_пускает_чужой_токен():
    asyncio.run(_case())


def test_маршрут_зарегистрирован_до_раздачи_бандла():
    names = [view.__name__ for view in mega_http.VIEWS]

    assert "MegaHomeMusicStreamView" in names
    # `/mega-home/{path:.*}` отвечает первым подошедшим — наш путь обязан быть раньше.
    assert names.index("MegaHomeMusicStreamView") < names.index("MegaHomeAppView")
