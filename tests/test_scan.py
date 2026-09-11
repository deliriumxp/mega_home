"""Обход локальной сети объекта (`scan.py`).

⚠ Предмет — ГРАНИЦЫ примитива: что считаем подсетью, как понимаем «хост жив» и
что попадает в ответ. Толкование «какой это вендор и что открывать» живёт в
менеджере; сюда оно не приезжает никогда, иначе обход менялся бы релизом HACS.

⚠ Обход проверяется НАСТОЯЩИМ сокетом на localhost: примитив существует ради
сети, и мок вернул бы то, что мы сами придумали.
"""

from __future__ import annotations

import asyncio
import sys

import aiohttp

from mega_home import scan


def run(coro):
    return asyncio.run(coro)


class _Hass:
    """Заглушка: обход зовёт executor только за ARP, там сети нет."""

    async def async_add_executor_job(self, func, *args):
        return func(*args)


async def _serve(handler, host: str = "127.0.0.1"):
    server = await asyncio.start_server(handler, host, 0)
    return server.sockets[0].getsockname()[1], server


def _use_real_session(monkeypatch):
    """Отдать обходу НАСТОЯЩУЮ сессию aiohttp.

    ⚠ В `conftest.py` подмена Home Assistant отдаёт `None`: ему там нужен только
    импорт. Но ONVIF спрашивается HTTP-запросом, и подделка сессии проверяла бы
    нашу же выдумку вместо ответа живого сервера — поэтому в тестах обхода
    сессия настоящая. Закрывать её обязан вызвавший.
    """
    session = aiohttp.ClientSession()
    monkeypatch.setattr(
        sys.modules["homeassistant.helpers.aiohttp_client"],
        "async_get_clientsession",
        lambda *a, **k: session,
    )
    return session


def test_network_must_be_private_and_small():
    # Публичную сеть не обходим: иначе дом стал бы сканером интернета.
    assert scan._parse_network("8.8.8.0/24") is None
    # /16 — это уже не квартира, и поток соединений лёг бы на чужой роутер.
    assert scan._parse_network("192.168.0.0/16") is None
    assert scan._parse_network("nonsense") is None
    assert scan._parse_network(None) is None
    assert str(scan._parse_network("192.168.1.0/24")) == "192.168.1.0/24"


def test_title_is_trimmed_and_capped():
    assert scan._title("<HTML><TITLE> RouterOS\n </TITLE></html>") == "RouterOS"
    assert scan._title("<title>" + "x" * 200 + "</title>") == "x" * 80
    assert scan._title("no title here") == ""


def test_is_open_answers_for_live_and_closed_port():
    async def scenario():
        port, server = await _serve(lambda r, w: None)
        try:
            assert await scan._is_open("127.0.0.1", port) is True
            # Отказ (RST) — это НЕ открытый порт, но хост живой (`_reachable`).
            assert await scan._is_open("127.0.0.1", 1) is False
            assert await scan._reachable("127.0.0.1", 1) is True
        finally:
            server.close()
            await server.wait_closed()

    run(scenario())


def test_run_reports_host_with_web_port_and_title(monkeypatch):
    async def scenario():
        async def answer(reader, writer):
            await reader.read(1024)
            body = b"<html><title>Hello</title></html>"
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s"
                % (len(body), body)
            )
            await writer.drain()
            writer.close()

        port, server = await _serve(answer)
        monkeypatch.setattr(scan, "WEB_PORTS", (port,))
        monkeypatch.setattr(scan, "DISCOVERY_PORTS", (port,))
        monkeypatch.setattr(scan, "TLS_PORTS", frozenset())

        async def fake_fetch(session, ip, p, tls):
            return {"status": 200, "server": "Test", "title": "Hello"}

        session = _use_real_session(monkeypatch)
        monkeypatch.setattr(scan, "_fetch", fake_fetch)
        try:
            result = await scan.run(_Hass(), {"subnet": "127.0.0.1/32"})
        finally:
            await session.close()
            server.close()
            await server.wait_closed()

        assert result["subnet"] == "127.0.0.1/32"
        assert len(result["hosts"]) == 1
        host = result["hosts"][0]
        assert host["ip"] == "127.0.0.1"
        assert host["ports"] == [
            {"port": port, "tls": False, "status": 200, "server": "Test", "title": "Hello"}
        ]

    run(scenario())


def test_probe_video_marks_rtsp_and_onvif(monkeypatch):
    """Отметка встаёт по СОСТОЯВШЕМУСЯ диалогу, а не по открытому порту.

    ⚠ Предмет здесь — «это видеонаблюдение», а не «камера»: по RTSP и ONVIF
    одинаково отвечают камеры, регистраторы и вызывные панели домофонии, и по
    протоколу они неразличимы. Назвать вызывную панель камерой — соврать.

    ⚠ Оба ответа настоящие (сокет и HTTP на localhost): подделка проверяла бы
    нашу же выдумку, а правило опознания тонкое — веб-морда камеры со словом
    «ONVIF» на странице прошла бы наивную проверку по подстроке.
    """

    async def scenario():
        async def rtsp(reader, writer):
            writer.write(b"RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n")
            await writer.drain()
            writer.close()

        async def soap(reader, writer):
            await reader.read(4096)
            body = (
                b'<?xml version="1.0"?><s:Envelope xmlns:s="http://www.w3.org/2003'
                b'/05/soap-envelope"><s:Body><tds:GetSystemDateAndTimeResponse/></s:Body>'
                b"</s:Envelope>"
            )
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/soap+xml\r\n"
                b"Content-Length: %d\r\n\r\n%s" % (len(body), body)
            )
            await writer.drain()
            writer.close()

        async def html(reader, writer):
            await reader.read(4096)
            # Веб-морда, которая про ONVIF только РАССКАЗЫВАЕТ.
            body = b"<html><title>Camera</title><p>ONVIF settings</p></html>"
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n"
                b"Content-Length: %d\r\n\r\n%s" % (len(body), body)
            )
            await writer.drain()
            writer.close()

        rtsp_port, rtsp_server = await _serve(rtsp)
        soap_port, soap_server = await _serve(soap)
        html_port, html_server = await _serve(html)
        session = _use_real_session(monkeypatch)
        monkeypatch.setattr(scan, "RTSP_PORTS", ((rtsp_port, False),))
        hosts = [
            {"ip": "127.0.0.1", "ports": [{"port": soap_port, "tls": False, "status": 200}]},
            {"ip": "127.0.0.1", "ports": []},
        ]
        try:
            await scan._probe_video(_Hass(), hosts)
            # Видеонаблюдение опознано и подписано признаком, по которому менеджер
            # и нарисует отметку. Порты отсортированы по номеру — строка таблицы
            # читается сверху вниз одним порядком при каждом обходе.
            by_port = {p["port"]: p for p in hosts[0]["ports"]}
            assert by_port[soap_port] == {
                "port": soap_port, "tls": False, "status": 200, "video": "onvif",
            }
            assert by_port[rtsp_port] == {"port": rtsp_port, "tls": False, "video": "rtsp"}
            assert [p["port"] for p in hosts[0]["ports"]] == sorted(by_port)
            # Устройство без веб-портов тоже опознаётся — по одному RTSP.
            assert hosts[1]["ports"] == [{"port": rtsp_port, "tls": False, "video": "rtsp"}]

            # Молчащий на RTSP порт отметки не даёт: «554 открыт» одинаково
            # выглядит у камеры и у чужого сервиса, вставшего на тот же номер.
            monkeypatch.setattr(scan, "RTSP_PORTS", ((html_port, False),))
            quiet = {"ip": "127.0.0.1", "ports": []}
            await scan._probe_video(_Hass(), [quiet])
            assert quiet["ports"] == []
        finally:
            await session.close()
            for server in (rtsp_server, soap_server, html_server):
                server.close()
                await server.wait_closed()

    run(scenario())


def test_looks_like_onvif_needs_a_conversation():
    # Ответ службы и отказ службы — признак; обычная страница — нет.
    assert scan._looks_like_onvif(
        200, "<s:Envelope><tds:GetSystemDateAndTimeResponse/></s:Envelope>", {}
    )
    # Отказ приходит тем же конвертом: это ONVIF-служба, отказавшая в доступе,
    # а не веб-сервер, ответивший по этому адресу.
    assert scan._looks_like_onvif(500, "<soap:Envelope><soap:Fault/></soap:Envelope>", {})
    assert scan._looks_like_onvif(
        401, "", {"WWW-Authenticate": 'Digest realm="ONVIF", nonce="x"'}
    )
    # ⚠ Слово «ONVIF» на странице — НЕ признак: так рассказывают о себе обычные
    # веб-морды, и отметка встала бы на устройство, которое об ONVIF только пишет.
    assert not scan._looks_like_onvif(200, "<html>ONVIF settings</html>", {})
    assert not scan._looks_like_onvif(200, "", {"Server": "RouterOS httpd"})
    assert not scan._looks_like_onvif(401, "", {"WWW-Authenticate": "Basic realm=RouterOS"})
