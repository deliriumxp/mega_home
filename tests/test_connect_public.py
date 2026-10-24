"""Ось «адресат»: `connect` по ИМЕНИ хоста — в интернет (решение заказчика 2026-10-20).

⚠ Предмет — граница: литеральный адрес по-прежнему только частный, имя проверяется по
адресам, куда ведёт, и соединение идёт ровно на проверенные адреса (без второго DNS).
"""

from __future__ import annotations

import asyncio

import pytest
from aiohttp import web

from mega_home.core import connect as connect_mod
from mega_home.core.ops_base import OpError


def fake_dns(monkeypatch, *addresses: str) -> list[str]:
    asked: list[str] = []

    async def getaddrinfo(host, port, family):
        asked.append(host)
        return [(2, 1, 6, "", (address, port)) for address in addresses]

    monkeypatch.setattr(connect_mod, "_getaddrinfo", getaddrinfo)
    return asked


def test_имя_принимается_литерал_публичный_нет():
    assert connect_mod.resolve_address({"host": "WWW.Radiorecord.ru", "port": 443}) == ("www.radiorecord.ru", 443)
    with pytest.raises(OpError, match="по имени хоста"):
        connect_mod.resolve_address({"host": "8.8.8.8", "port": 443})
    with pytest.raises(OpError, match="частный IPv4"):
        connect_mod.resolve_address({"host": "router", "port": 80})


@pytest.mark.parametrize("address", ["127.0.0.1", "169.254.169.254", "0.0.0.0", "::1", "fe80::1"])
def test_имя_на_служебный_адрес_отказ(monkeypatch, address):
    fake_dns(monkeypatch, "93.184.216.34", address)
    with pytest.raises(OpError, match="служебный адрес"):
        asyncio.run(connect_mod.pinned("example.com", 443))


def test_имя_на_частный_адрес_пускается(monkeypatch):
    # До частного адреса вызывающий и так достаёт литералом — имя тут ничего не открывает.
    fake_dns(monkeypatch, "192.168.1.10")
    assert asyncio.run(connect_mod.pinned("nas.example.com", 80))[0]["host"] == "192.168.1.10"


def test_соединение_идёт_на_проверенный_адрес_с_именем_в_host(monkeypatch):
    """Имя уходит в Host (и в SNI у TLS), а сокет — на адрес, который проверил `pinned`."""

    async def scenario():
        seen: dict[str, str] = {}

        async def handler(request):
            seen["host"] = request.headers.get("Host", "")
            return web.json_response({"ok": True})

        app = web.Application()
        app.router.add_get("/api/stations/", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.2", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        # Проверку адреса здесь обходим подменой `pinned`: петля — единственный наш сервер.
        async def pinned(host, port_, family=0):
            return [{"hostname": host, "host": "127.0.0.2", "port": port_, "family": 2, "proto": 6, "flags": 0}]

        monkeypatch.setattr(connect_mod, "pinned", pinned)
        try:
            answer = await connect_mod.perform(
                {"kind": "http", "host": "www.radiorecord.ru", "port": port, "path": "/api/stations/"}
            )
        finally:
            await runner.cleanup()
        return answer, seen

    answer, seen = asyncio.run(scenario())
    assert answer["status"] == 200
    assert seen["host"].startswith("www.radiorecord.ru")


def test_ресурс_браузеру_не_из_интернета():
    """⚠ Ответ ресурса браузер открывает в origin двери: чужая страница исполнила бы там скрипт."""
    with pytest.raises(OpError) as refused:
        asyncio.run(connect_mod.resource({"kind": "http", "host": "evil.example.com", "port": 443, "tls": True, "path": "/x"}))
    assert refused.value.status == 403


def test_метка_кэша_ресурса_своя_у_службы_записи_и_двери_у_прочих(monkeypatch):
    """Ревью 2026-10-24: дверь подменяла `Cache-Control` службы записи на 30 с — закрытый спрайт
    (`max-age=3600`) и кадр под меткой контроллер собирал заново. Служба с `own_cache` — её метка;
    без метки в ответе и у прочих служб и устройств — дверная (кадр «сейчас»)."""
    from mega_home.core import services

    answers: list[dict] = []

    async def fake_http(payload, plan=None):
        return answers.pop(0)

    monkeypatch.setattr(connect_mod, "_http", fake_http)
    services.register("recorder", 1, own_cache=True)
    services.register("go2rtc", 2)
    try:
        for host, headers, cache in (
            ("recorder", {"Cache-Control": "private, max-age=3600"}, "private, max-age=3600"),
            ("recorder", {"cache-control": "no-store"}, "no-store"),
            ("recorder", {}, connect_mod.RESOURCE_CACHE),
            ("go2rtc", {"Cache-Control": "max-age=86400"}, connect_mod.RESOURCE_CACHE),
            ("192.168.1.9", {"Cache-Control": "max-age=86400"}, connect_mod.RESOURCE_CACHE),
        ):
            answers.append({"status": 200, "headers": {"Content-Type": "image/jpeg", **headers}, "body": "x"})
            got = asyncio.run(connect_mod.resource({"kind": "http", "host": host, "port": 80, "path": "/preview"}))
            assert got[3] == cache, host
    finally:
        services.unregister("recorder")
        services.unregister("go2rtc")


def test_ws_не_идёт_по_перенаправлению(monkeypatch):
    """⚠ Перенаправление на литерал aiohttp соединяет мимо резолвера: имя увело бы дом на loopback."""

    async def scenario():
        hits: list[str] = []
        reached: list[str] = []

        async def secret(request):
            reached.append(request.path)
            return web.Response(text="секрет")

        inner = web.Application()
        inner.router.add_get("/secret", secret)
        inner_runner = web.AppRunner(inner)
        await inner_runner.setup()
        inner_site = web.TCPSite(inner_runner, "127.0.0.1", 0)
        await inner_site.start()
        inner_port = inner_site._server.sockets[0].getsockname()[1]

        async def bounce(request):
            raise web.HTTPFound(f"http://127.0.0.1:{inner_port}/secret")

        app = web.Application()
        app.router.add_get("/ws", bounce)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.2", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        async def pinned(host, port_, family=0):
            hits.append(host)
            return [{"hostname": host, "host": "127.0.0.2", "port": port_, "family": 2, "proto": 6, "flags": 0}]

        monkeypatch.setattr(connect_mod, "pinned", pinned)
        try:
            with pytest.raises(OpError):
                await connect_mod.perform({"kind": "ws", "host": "ws.example.com", "port": port, "path": "/ws", "timeout": 5})
        finally:
            await runner.cleanup()
            await inner_runner.cleanup()
        return hits, reached

    hits, reached = asyncio.run(scenario())
    assert hits == ["ws.example.com"]
    assert reached == []


def test_по_имени_сертификат_проверяется(monkeypatch):
    fake_dns(monkeypatch, "93.184.216.34")

    async def scenario():
        connector = await connect_mod.connector_for("example.com", 443, True)
        try:
            # `ssl=True` у aiohttp — проверенный контекст; у устройства LAN — `False`.
            return connector._ssl
        finally:
            await connector.close()

    assert asyncio.run(scenario()) is True
