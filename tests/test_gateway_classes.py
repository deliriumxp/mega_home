"""Шлюз дома: закрытый список возможностей для ЛЮБОГО вендора.

⚠ Предмет — то, что каждая строка списка (`docs/home-gateway.md` в менеджере)
действительно исполняется ДАННЫМИ описания, без вендорского кода. Сокеты
настоящие, на localhost: мок подтвердил бы только выдуманное.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from mega_home.core import access as access_mod
from mega_home.core.access import descriptor_of
from mega_home.core.access_secrets import SecretBook
from mega_home.core.device_events import BUFFER_TTL_S, EventHub
from mega_home.core.gateway import SCOPE_MANAGER, AccessDenied, AccessGateway


def test_новое_описание_несёт_всё_данными() -> None:
    d = descriptor_of(
        {
            "id": "panel", "kind": "http", "host": "192.168.88.90", "scheme": "http", "port": 80,
            "auth": {"type": "digest", "userField": "apiUser", "passField": "apiPass"},
            "secret": "fp1", "deny": ["/api/config/set"], "managerOnly": ["/api/relay"],
            "methods": ["get", "post", "put", "bogus"], "timeout": 5,
            "longPoll": {"paths": ["/wait"], "timeout": 90},
            "media": {"main": "rtsp://{secret.rtspUser|url}:{secret.rtspPass|url}@{host}/live/ch00_0"},
            "events": [{"type": "webhook", "id": "action"}, "мусор"],
        }
    )
    assert d is not None
    assert (d.auth.type, d.auth.user_field, d.secret) == ("digest", "apiUser", "fp1")
    assert d.methods == ("GET", "POST", "PUT")
    assert (d.deny, d.manager_only, d.long_poll, d.long_poll_timeout) == (
        ("/api/config/set",), ("/api/relay",), ("/wait",), 90.0
    )
    assert d.events == [{"type": "webhook", "id": "action"}]


def test_прежнее_описание_получает_прежние_списки() -> None:
    """Совместимость: менеджер до 2026-09-19 запреты и длинный опрос не присылал."""
    d = descriptor_of({"host": "10.0.0.5", "login": "/login"})
    assert d is not None
    assert "/settings" in d.deny and d.is_long_poll("/archive_events")
    assert d.auth.type == "session" and d.login_path == "/login"
    # Новое описание со своим (даже пустым) списком прежних не получает.
    fresh = descriptor_of({"host": "10.0.0.5", "deny": [], "longPoll": {"paths": []}})
    assert fresh is not None and fresh.deny == () and not fresh.is_long_poll("/archive_events")


def test_только_менеджер() -> None:
    d = descriptor_of({"host": "10.0.0.5", "deny": ["/x"], "managerOnly": ["/api/config"]})
    assert d is not None
    with pytest.raises(AccessDenied):
        AccessGateway.check(d, "GET", "/api/config/set")
    assert AccessGateway.check(d, "GET", "/api/config/set", SCOPE_MANAGER) == "/api/config/set"
    with pytest.raises(AccessDenied):
        AccessGateway.check(d, "GET", "/a/../x/y", SCOPE_MANAGER)


# --- HTTP: авторизация ставится домом ---------------------------------------


async def _server(handler: Any) -> tuple[int, web.AppRunner]:
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return site._server.sockets[0].getsockname()[1], runner  # noqa: SLF001


def _door(port: int, auth: dict[str, Any], fields: dict[str, str], **extra: Any) -> AccessGateway:
    async def fetch(_access: str) -> dict[str, str]:
        return fields

    door = AccessGateway(secrets_fetch=fetch)
    door.apply([{"id": "dev", "host": "127.0.0.1", "scheme": "http", "port": port, "deny": [],
                 "auth": auth, "secret": "fp", **extra}])
    return door


@pytest.mark.parametrize("kind", ["basic", "bearer"])
def test_basic_и_bearer(kind: str) -> None:
    async def scenario() -> str:
        seen: list[str] = []

        async def handler(request: web.Request) -> web.Response:
            seen.append(request.headers.get("Authorization", ""))
            return web.json_response({"ok": 1})

        port, runner = await _server(handler)
        door = _door(port, {"type": kind}, {"username": "u", "password": "p", "token": "T"})
        try:
            # ⚠ Свою авторизацию бандл подсунуть не может — её ставит дом.
            await door.call("dev", "GET", "/x", headers={"Authorization": "Bearer чужой"})
        finally:
            await door.async_close()
            await runner.cleanup()
        return seen[0]

    got = asyncio.run(scenario())
    expected = "Bearer T" if kind == "bearer" else "Basic " + base64.b64encode(b"u:p").decode()
    assert got == expected


def test_digest() -> None:
    """Digest по RFC 7616 — штатный режим HTTP API панелей домофона."""
    if not hasattr(aiohttp, "DigestAuthMiddleware"):
        pytest.skip("aiohttp без DigestAuthMiddleware")

    async def scenario() -> int:
        realm, nonce = "dev", "abc123"

        async def handler(request: web.Request) -> web.Response:
            header = request.headers.get("Authorization", "")
            if not header.startswith("Digest "):
                return web.Response(status=401, headers={
                    "WWW-Authenticate": f'Digest realm="{realm}", nonce="{nonce}", qop="auth", algorithm=MD5'})
            parts = dict(item.strip().split("=", 1) for item in header[7:].split(","))
            parts = {k: v.strip('"') for k, v in parts.items()}
            ha1 = hashlib.md5(f"u:{realm}:p".encode()).hexdigest()
            ha2 = hashlib.md5(f"{request.method}:{parts['uri']}".encode()).hexdigest()
            want = hashlib.md5(
                f"{ha1}:{nonce}:{parts['nc']}:{parts['cnonce']}:{parts['qop']}:{ha2}".encode()
            ).hexdigest()
            return web.json_response({"ok": 1}, status=200 if parts["response"] == want else 403)

        port, runner = await _server(handler)
        door = _door(port, {"type": "digest"}, {"username": "u", "password": "p"})
        try:
            status, _, _ = await door.call("dev", "GET", "/api/relay/trig", {"num": "1"})
        finally:
            await door.async_close()
            await runner.cleanup()
        return status

    assert asyncio.run(scenario()) == 200


@pytest.mark.parametrize("place", ["query", "header", "cookie"])
def test_сессия_подставляется_куда_сказано(place: str) -> None:
    async def scenario() -> dict[str, str]:
        seen: dict[str, str] = {}

        async def handler(request: web.Request) -> web.Response:
            if request.path == "/auth":
                return web.json_response({"token": "S1"})
            seen.update(q=request.query.get("t", ""), h=request.headers.get("t", ""), c=request.cookies.get("t", ""))
            return web.json_response({"ok": 1})

        port, runner = await _server(handler)
        auth = {"type": "session", "session": {"path": "/auth", "params": {"u": "{secret.username}"},
                                                "field": "token", "place": place, "name": "t"}}
        door = _door(port, auth, {"username": "u"})
        try:
            await door.call("dev", "GET", "/data")
        finally:
            await door.async_close()
            await runner.cleanup()
        return seen

    seen = asyncio.run(scenario())
    assert seen[{"query": "q", "header": "h", "cookie": "c"}[place]] == "S1"


def test_заголовки_ответа_и_методы_из_описания() -> None:
    async def scenario() -> tuple[Any, ...]:
        async def handler(request: web.Request) -> web.Response:
            return web.Response(text="x", headers={"Location": "/next", "Server": "dev"})

        port, runner = await _server(handler)
        door = _door(port, {"type": "none"}, {}, methods=["GET", "DELETE"])
        try:
            deleted = await door.call_full("dev", "DELETE", "/item/1")
            with pytest.raises(AccessDenied):
                await door.call("dev", "PUT", "/item/1")
        finally:
            await door.async_close()
            await runner.cleanup()
        return deleted

    status, _, _, headers = asyncio.run(scenario())
    assert status == 200 and headers == {"content-type": "text/plain; charset=utf-8", "location": "/next"}


# --- учётки ------------------------------------------------------------------


def test_учётка_перечитывается_только_по_отпечатку() -> None:
    calls: list[str] = []
    answers = [{"password": "1"}, {"password": "2"}]

    async def fetch(access: str) -> dict[str, str]:
        calls.append(access)
        return answers[len(calls) - 1]

    async def scenario() -> list[str]:
        book = SecretBook(fetch)
        first = await book.get("a", "fp1")
        again = await book.get("a", "fp1")
        changed = await book.get("a", "fp2")
        return [first["password"], again["password"], changed["password"]]

    assert asyncio.run(scenario()) == ["1", "1", "2"]
    assert calls == ["a", "a"]


def test_сессия_и_учётка_драйвера_только_своему_доступу() -> None:
    """⚠ Второй доступ с входом по сессии не получает ни сессию, ни пароль Trassir."""
    used: list[str] = []

    async def driver_sid(_fresh: bool) -> str:
        used.append("sid")
        return "DRIVER"

    async def trassir_creds() -> tuple[str, str]:
        used.append("creds")
        return "admin", "secret"

    door = AccessGateway(credentials=trassir_creds, sid_provider=driver_sid, provider_access="trassir")
    door.apply([{"id": "trassir", "host": "10.0.0.5", "login": "/login"},
                {"id": "other", "host": "10.0.0.6", "deny": [],
                 "auth": {"type": "session", "session": {"path": "/in"}}}])
    other = door.descriptor("other")
    assert asyncio.run(door.secrets.get("other", "")) == {}
    assert asyncio.run(door._http._sid(door.descriptor("trassir"))) == "DRIVER"  # noqa: SLF001
    assert used == ["sid"]
    assert other is not None and other.auth.session is not None


def test_шаблон_медиа_кодирует_учётку() -> None:
    async def fetch(_access: str) -> dict[str, str]:
        return {"rtspUser": "admin", "rtspPass": "p@ss/1"}

    door = AccessGateway(secrets_fetch=fetch)
    door.apply([{"id": "panel", "host": "192.168.88.90", "secret": "fp",
                 "media": {"main": "rtsp://{secret.rtspUser|url}:{secret.rtspPass|url}@{host}/live/{camera}"}}])
    url = asyncio.run(door.media_url("panel", "main", {"camera": "ch00_0", "secret.rtspPass": "подлог"}))
    assert url == "rtsp://admin:p%40ss%2F1@192.168.88.90/live/ch00_0"


# --- TCP и UDP -----------------------------------------------------------------


def test_tcp_обмен_до_разделителя() -> None:
    async def scenario() -> dict[str, Any]:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.read(100)
            writer.write(b"PWR=ON\r\nextra")
            await writer.drain()
            await asyncio.sleep(1)
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        door = AccessGateway()
        door.apply([{"id": "proj", "kind": "tcp", "host": "127.0.0.1", "port": port}])
        try:
            return await door.exchange("proj", {"send": base64.b64encode(b"PWR?\r").decode(),
                                                "until": base64.b64encode(b"\r\n").decode(), "timeout": 3})
        finally:
            server.close()

    answer = asyncio.run(scenario())
    assert base64.b64decode(answer["data"]).startswith(b"PWR=ON\r\n")


def test_udp_датаграмма_и_ответы() -> None:
    async def scenario() -> dict[str, Any]:
        loop = asyncio.get_running_loop()

        class Echo(asyncio.DatagramProtocol):
            def connection_made(self, transport: Any) -> None:
                self.transport = transport

            def datagram_received(self, data: bytes, addr: Any) -> None:
                self.transport.sendto(b"re:" + data, addr)

        transport, _ = await loop.create_datagram_endpoint(Echo, local_addr=("127.0.0.1", 0))
        port = transport.get_extra_info("sockname")[1]
        door = AccessGateway()
        door.apply([{"id": "dev", "kind": "udp", "host": "127.0.0.1", "port": port}])
        try:
            return await door.exchange("dev", {"send": base64.b64encode(b"ping").decode(), "timeout": 0.3})
        finally:
            transport.close()

    datagrams = asyncio.run(scenario())["datagrams"]
    assert [base64.b64decode(d["data"]) for d in datagrams] == [b"re:ping"]


# --- MQTT -----------------------------------------------------------------------


async def _broker(received: list[tuple[str, bytes]]) -> tuple[int, Any]:
    """Брокер-заглушка по 3.1.1: CONNACK, SUBACK, PUBACK и эхо публикаций подписчикам."""
    from mega_home.core import mqtt

    writers: list[asyncio.StreamWriter] = []

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                head, body = await mqtt.read_packet(reader)
                kind = head & 0xF0
                if kind == mqtt.CONNECT:
                    writer.write(mqtt.packet(mqtt.CONNACK, b"\x00\x00"))
                elif kind == 0x80:
                    writers.append(writer)
                    writer.write(mqtt.packet(mqtt.SUBACK, body[:2] + b"\x01"))
                elif kind == mqtt.PUBLISH:
                    topic, data, qos, packet_id = mqtt.parse_publish(head & 0x0F, body)
                    received.append((topic, data))
                    if qos:
                        writer.write(mqtt.packet(mqtt.PUBACK, packet_id.to_bytes(2, "big")))
                    for other in writers:
                        other.write(mqtt.publish_packet(topic, data, 0, False, 0))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server.sockets[0].getsockname()[1], server


def test_mqtt_публикация_и_подписка() -> None:
    async def scenario() -> tuple[list[Any], list[Any]]:
        received: list[tuple[str, bytes]] = []
        port, server = await _broker(received)
        door = AccessGateway()
        door.apply([{"id": "bus", "kind": "mqtt", "host": "127.0.0.1", "port": port, "deny": ["sys/"]}])
        got: list[tuple[str, bytes]] = []
        descriptor = door.descriptor("bus")
        sub = await door.mqtt_client(descriptor, lambda t, d: got.append((t, d)))
        await sub.subscribe(["home/#"])
        try:
            await door.exchange("bus", {"topic": "home/light", "payload": {"on": True}, "qos": 1})
            with pytest.raises(AccessDenied):
                await door.exchange("bus", {"topic": "sys/reboot", "payload": "1"})
            await asyncio.sleep(0.1)
        finally:
            await sub.close()
            await door.async_close()
            server.close()
        return received, got

    received, got = asyncio.run(scenario())
    assert received == [("home/light", b'{"on": true}')]
    assert got == [("home/light", b'{"on": true}')]


# --- события устройств ------------------------------------------------------------


def test_буфер_событий_на_время_обрыва() -> None:
    hub = EventHub()
    local: list[dict[str, Any]] = []
    hub.subscribe(local.append)
    # `local=True` — как ставит SIP-мост: звонок обязана увидеть панель без интернета.
    hub.publish("intercom", "sip-bridge", "call", {"caller": "192.168.88.90"}, local=True)
    old = hub.publish("intercom", "sip-bridge", "cancel", local=True)
    old["at"] -= BUFFER_TTL_S + 1
    sent: list[dict[str, Any]] = []
    fresh = hub.attach(lambda frame: sent.append(frame) or True)
    # Локальный поток получил всё сразу; менеджеру — только свежее из буфера.
    assert [f["event"] for f in local] == ["call", "cancel"]
    assert [f["event"] for f in fresh] == ["call"]
    hub.publish("intercom", "sip-bridge", "answered")
    assert [f["event"] for f in sent] == ["answered"]


def test_вебхук_только_с_адреса_устройства(monkeypatch: pytest.MonkeyPatch) -> None:
    from mega_home.core import listeners as listeners_mod
    from mega_home.core.listeners import Listeners

    from fake_host import FakeHost

    async def scenario() -> tuple[list[dict[str, Any]], int, int]:
        probe = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = probe.sockets[0].getsockname()[1]
        probe.close()
        await probe.wait_closed()
        monkeypatch.setattr(listeners_mod, "HOOK_PORT", port)
        hub = EventHub()
        got: list[dict[str, Any]] = []
        hub.subscribe(got.append)
        door = AccessGateway()
        door.apply([
            {"id": "panel", "host": "127.0.0.1", "events": [{"type": "webhook", "id": "action", "local": True}]},
            {"id": "other", "host": "10.9.9.9", "events": [{"type": "webhook", "id": "action"}]},
        ])
        sources = Listeners(FakeHost(), door, hub)
        await sources._restart(door.descriptors())  # noqa: SLF001 — FakeHost задачи не запускает
        await sources._hook_server()  # noqa: SLF001
        async with aiohttp.ClientSession() as http:
            async with http.post(f"http://127.0.0.1:{port}/hook/panel/action?code=1", data=b"ring") as ok:
                good = ok.status
            async with http.post(f"http://127.0.0.1:{port}/hook/other/action", data=b"x") as bad:
                refused = bad.status
        await sources.stop()
        return got, good, refused

    got, good, refused = asyncio.run(scenario())
    assert (good, refused) == (200, 404)
    assert len(got) == 1 and got[0]["access"] == "panel" and got[0]["data"]["text"] == "ring"
    assert got[0]["data"]["query"] == {"code": "1"}
