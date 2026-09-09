"""Обход локальной сети объекта (`scan.py`).

⚠ Предмет — ГРАНИЦЫ примитива: что считаем подсетью, как понимаем «хост жив» и
что попадает в ответ. Толкование «какой это вендор и что открывать» живёт в
менеджере; сюда оно не приезжает никогда, иначе обход менялся бы релизом HACS.

⚠ Обход проверяется НАСТОЯЩИМ сокетом на localhost: примитив существует ради
сети, и мок вернул бы то, что мы сами придумали.
"""

from __future__ import annotations

import asyncio

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

        monkeypatch.setattr(scan, "_fetch", fake_fetch)
        try:
            result = await scan.run(_Hass(), {"subnet": "127.0.0.1/32"})
        finally:
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
