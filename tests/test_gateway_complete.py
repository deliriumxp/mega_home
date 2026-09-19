"""Шлюз после ревью 2026-09-19: исправленные дыры и закрытые пробелы списка.

⚠ Каждая спека здесь — сценарий отказа из ревью или класс устройств, который
без неё потребовал бы релиза интеграции (`docs/home-gateway.md`).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from typing import Any

import pytest
from aiohttp import web

from mega_home.core.access import descriptor_of
from mega_home.core.device_events import EventHub
from mega_home.core.gateway import AccessDenied, AccessGateway
from mega_home.core.templating import pick, render


async def _server(handler: Any, ws: Any = None) -> tuple[int, web.AppRunner]:
    app = web.Application()
    if ws is not None:
        app.router.add_get("/ws", ws)
    app.router.add_route("*", "/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return site._server.sockets[0].getsockname()[1], runner  # noqa: SLF001


def _door(port: int, extra: dict[str, Any], fields: dict[str, str] | None = None) -> AccessGateway:
    async def fetch(_access: str) -> dict[str, str]:
        return fields or {}

    door = AccessGateway(secrets_fetch=fetch)
    door.apply([{"id": "dev", "host": "127.0.0.1", "scheme": "http", "port": port, "secret": "fp",
                 "deny": [], **extra}])
    return door


# --- дыры ревью -----------------------------------------------------------------


@pytest.mark.parametrize("path", ["/a/%252e%252e/settings/", "/a/.%252e/settings", "/%2525"])
def test_двойное_кодирование_пути_отказ(path: str) -> None:
    d = descriptor_of({"host": "10.0.0.5", "auth": {"type": "none"}, "deny": ["/settings"]})
    with pytest.raises(AccessDenied):
        AccessGateway.check(d, "GET", path)


def test_запрет_без_учёта_регистра() -> None:
    d = descriptor_of({"host": "10.0.0.5", "auth": {"type": "none"}, "deny": ["/settings"]})
    with pytest.raises(AccessDenied):
        AccessGateway.check(d, "GET", "/Settings/x")


def test_значения_бандла_не_подменяют_хост_и_учётку() -> None:
    async def fetch(_a: str) -> dict[str, str]:
        return {"password": "p"}

    door = AccessGateway(secrets_fetch=fetch)
    door.apply([{"id": "cam", "host": "192.168.88.90", "secret": "fp", "auth": {"type": "none"},
                 "media": {"m": "rtsp://u:{secret.password}@{host}/{camera}"}}])
    url = asyncio.run(door.media_url("cam", "m", {"host": "6.6.6.6", "camera": "{secret.password}", "pass": "x"}))
    assert url == "rtsp://u:p@192.168.88.90/%7Bsecret.password%7D"


def test_новое_описание_без_deny_не_получает_запреты_регистратора() -> None:
    d = descriptor_of({"host": "10.0.0.5", "auth": {"type": "digest"}})
    assert d.deny == () and not d.is_long_poll("/events")


def test_stream_open_не_пускает_на_0000() -> None:
    from mega_home.core.stream import Streams

    refuse = Streams(object())._refuse  # noqa: SLF001
    assert refuse({"host": "0.0.0.0", "port": 1985})
    assert refuse({"host": "224.0.0.1", "port": 80})


def test_подтверждение_событий_и_порядок_повтора() -> None:
    hub = EventHub()
    local: list[str] = []
    hub.subscribe(lambda f: local.append(f["event"]))
    first = hub.publish("panel", "hook", "call", local=True)
    hub.publish("dev", "poll", "response", {"secret": 1}, local=False)
    sent: list[str] = []
    again = hub.attach(lambda f: sent.append(f["event"]) or True)
    assert [f["event"] for f in again] == ["call", "response"], "порядок сохраняется"
    assert local == ["call"], "событие уровня менеджера в локальный поток не идёт"
    hub.ack(first["id"])
    assert [f["event"] for f in hub.attach(lambda f: True)] == ["response"]


def test_закрытая_дверь_новых_соединений_не_заводит() -> None:
    from mega_home.core.access_http import AccessUnreachable

    door = AccessGateway()
    door.apply([{"id": "dev", "host": "127.0.0.1", "scheme": "http", "port": 1}])
    asyncio.run(door.async_close())
    with pytest.raises(AccessUnreachable):
        asyncio.run(door.call("dev", "GET", "/x"))


# --- шаблоны: вход с вычислением -------------------------------------------------


def test_шаблон_dahua_md5_и_onvif_wsse() -> None:
    values = {"secret.username": "admin", "secret.password": "pw", "challenge.realm": "R"}
    live = {"nonce": b"\x01\x02", "created": "2026-09-19T00:00:00.000Z", "ts": "1", "tsMs": "1"}
    dahua = render("{=[secret.username]:[challenge.realm]:[secret.password]|md5|hex|upper}", values, live)
    assert dahua == hashlib.md5(b"admin:R:pw").hexdigest().upper()
    wsse = render("{=[nonce][created][secret.password]|sha1|base64}", values, live)
    assert wsse == base64.b64encode(hashlib.sha1(b"\x01\x022026-09-19T00:00:00.000Zpw").digest()).decode()
    assert render("{nonce|base64}", values, live) == base64.b64encode(b"\x01\x02").decode()
    assert render("{unknown} {secret.password|nope}", values, live) == "{unknown} {secret.password|nope}"
    assert pick({"result": {"session": 7}}, "result.session") == "7"
    assert pick({"list": [{"id": "x"}]}, "list.0.id") == "x"


def test_вход_в_два_шага_и_свой_заголовок() -> None:
    """Вызов-подготовка даёт realm, вход — хэш; сессия по пути JSON — в заголовок."""

    async def scenario() -> dict[str, str]:
        seen: dict[str, str] = {}

        async def handler(request: web.Request) -> web.Response:
            if request.path == "/challenge":
                return web.json_response({"params": {"realm": "R"}})
            if request.path == "/login":
                body = await request.json()
                ok = body["hash"] == hashlib.md5(b"admin:R:pw").hexdigest()
                return web.json_response({"result": {"session": "S1" if ok else ""}})
            seen.update(auth=request.headers.get("X-Session", ""), key=request.headers.get("X-Key", ""))
            return web.json_response({"ok": 1})

        port, runner = await _server(handler)
        auth = {
            "type": "session",
            "headers": {"X-Key": "{secret.key}"},
            "session": {
                "challenge": {"path": "/challenge", "fields": {"realm": "params.realm"}},
                "path": "/login", "method": "POST",
                "body": '{"hash": "{=[secret.username]:[challenge.realm]:[secret.password]|md5|hex}"}',
                "contentType": "application/json",
                "fields": {"sid": "result.session"},
                "place": "header", "name": "X-Session", "template": "Session {sid}",
            },
        }
        door = _door(port, {"auth": auth}, {"username": "admin", "password": "pw", "key": "K"})
        try:
            await door.call("dev", "GET", "/data")
        finally:
            await door.async_close()
            await runner.cleanup()
        return seen

    assert asyncio.run(scenario()) == {"auth": "Session S1", "key": "K"}


def test_обёртка_тела_описанием() -> None:
    """ONVIF: бандл шлёт тело SOAP, дом оборачивает его заголовком безопасности."""

    async def scenario() -> str:
        got: list[str] = []

        async def handler(request: web.Request) -> web.Response:
            got.append(await request.text())
            return web.Response(text="ok")

        port, runner = await _server(handler)
        door = _door(port, {"auth": {"type": "none", "wrap": "<E><U>{secret.username}</U>{body}</E>"}},
                     {"username": "admin"})
        try:
            await door.call("dev", "POST", "/onvif", body=b"<GetProfiles/>")
        finally:
            await door.async_close()
            await runner.cleanup()
        return got[0]

    assert asyncio.run(scenario()) == "<E><U>admin</U><GetProfiles/></E>"


# --- WebSocket, поток событий -------------------------------------------------------


def test_websocket_вызов() -> None:
    async def scenario() -> dict[str, Any]:
        async def ws(request: web.Request) -> web.WebSocketResponse:
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            async for message in socket:
                await socket.send_str(json.dumps({"echo": message.data}))
                break
            return socket

        async def other(_request: web.Request) -> web.Response:
            return web.Response()

        port, runner = await _server(other, ws)
        door = AccessGateway()
        door.apply([{"id": "sh", "kind": "ws", "host": "127.0.0.1", "scheme": "http", "port": port}])
        try:
            return await door.exchange("sh", {"path": "/ws", "send": [{"method": "Shelly.GetStatus"}], "timeout": 2})
        finally:
            await door.async_close()
            await runner.cleanup()

    messages = asyncio.run(scenario())["messages"]
    assert json.loads(messages[0]["text"])["echo"] == '{"method": "Shelly.GetStatus"}'


def test_поток_событий_multipart() -> None:
    from mega_home.core import listeners_out
    from mega_home.core.listeners import Listeners

    from fake_host import FakeHost

    async def scenario() -> list[str]:
        async def handler(request: web.Request) -> web.StreamResponse:
            response = web.StreamResponse(headers={"Content-Type": "multipart/mixed; boundary=bnd"})
            await response.prepare(request)
            for text in ("alarm=1", "alarm=0"):
                await response.write(f"--bnd\r\n{text}\r\n".encode())
            await response.write_eof()
            return response

        port, runner = await _server(handler)
        hub = EventHub()
        got: list[str] = []
        hub.subscribe(lambda f: got.append(f["data"]["text"].strip()))
        door = _door(port, {"auth": {"type": "none"}})
        spec = {"type": "stream", "id": "alerts", "path": "/alertStream", "local": True}
        sources = Listeners(FakeHost(), door, hub)
        try:
            await listeners_out.stream(sources, door.descriptor("dev"), "alerts", spec)
        finally:
            await door.async_close()
            await runner.cleanup()
        return got

    assert asyncio.run(scenario()) == ["alarm=1", "alarm=0"]


# --- MQTT: текущее значение ------------------------------------------------------------


def test_mqtt_get_читает_retained() -> None:
    from mega_home.core import mqtt

    async def scenario() -> dict[str, Any]:
        async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while True:
                    head, body = await mqtt.read_packet(reader)
                    kind = head & 0xF0
                    if kind == mqtt.CONNECT:
                        writer.write(mqtt.packet(mqtt.CONNACK, b"\x00\x00"))
                    elif kind == 0x80:
                        writer.write(mqtt.packet(mqtt.SUBACK, body[:2] + b"\x00"))
                        writer.write(mqtt.publish_packet("/devices/wb/controls/t", b"21.5", 0, True, 0))
                    await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        door = AccessGateway()
        door.apply([{"id": "wb", "kind": "mqtt", "host": "127.0.0.1", "port": port}])
        try:
            return await door.exchange("wb", {"verb": "get", "topic": "/devices/wb/controls/t", "timeout": 2})
        finally:
            await door.async_close()
            server.close()

    assert asyncio.run(scenario()) == {"topic": "/devices/wb/controls/t", "found": True, "text": "21.5"}


# --- повторное ревью ------------------------------------------------------------------


def test_подделка_callerid_не_обходит_паузу() -> None:
    from mega_home.core.sip_calls import DoorCalls

    class Calls(DoorCalls):
        def __init__(self) -> None:
            super().__init__(None, 8188, "x", lambda kind, data: self.events.append(kind))  # type: ignore[arg-type]
            self.events: list[str] = []

        async def _request(self, method: str, path: str, params: Any = None) -> Any:
            return {}

    async def scenario() -> list[str]:
        calls = Calls()
        for i in range(3):
            await calls.handle({"type": "StasisStart", "args": ["panel", "192.168.88.90:5060"],
                                "channel": {"id": f"c{i}", "caller": {"number": f"fake{i}"}}})
        return calls.events

    assert asyncio.run(scenario()) == ["call"], "новый номер с того же адреса — всё равно занято"


def test_строка_запроса_уходит_как_есть() -> None:
    d = descriptor_of({"host": "10.0.0.5", "auth": {"type": "none"}})
    assert AccessGateway.check(d, "GET", "/find?q=a%26b") == "/find?q=a%26b"
    with pytest.raises(AccessDenied):
        AccessGateway.check(d, "GET", "/x%3Fsecret")


def test_ошибка_без_адреса_с_учёткой() -> None:
    from mega_home.core.access_http import reason

    text = reason(RuntimeError("0, message='Bad', url='http://h/x?pwd=SECRET'"))
    assert "SECRET" not in text


def test_опрос_с_курсором_и_шагом_подписки() -> None:
    from mega_home.core import listeners_out
    from mega_home.core.listeners import Listeners

    from fake_host import FakeHost

    async def scenario() -> list[str]:
        seen: list[str] = []

        async def handler(request: web.Request) -> web.Response:
            if request.path == "/subscribe":
                return web.json_response({"sub": "S9"})
            seen.append(f"{request.query.get('sub')}:{request.query.get('since')}")
            if len(seen) >= 3:
                raise asyncio.CancelledError
            return web.json_response({"next": len(seen)})

        port, runner = await _server(handler)
        door = _door(port, {"auth": {"type": "none"}, "longPoll": {"paths": ["/updates"]}})
        spec = {"type": "poll", "id": "p", "setup": {"path": "/subscribe", "carry": {"sub": "sub"}},
                "path": "/updates", "params": {"sub": "{carry.sub}", "since": "{carry.next}"},
                "carry": {"next": "next"}}
        sources = Listeners(FakeHost(), door, EventHub())
        try:
            await asyncio.wait_for(listeners_out.poll(sources, door.descriptor("dev"), "p", spec), 3)
        except Exception:  # noqa: BLE001 — сервер обрывает третий запрос
            pass
        finally:
            await door.async_close()
            await runner.cleanup()
        return seen

    assert asyncio.run(scenario())[:3] == ["S9:{carry.next}", "S9:1", "S9:2"]


def test_вход_внутри_websocket_шагами() -> None:
    from mega_home.core import listeners_out
    from mega_home.core.listeners import Listeners

    from fake_host import FakeHost

    async def scenario() -> list[str]:
        async def ws(request: web.Request) -> web.WebSocketResponse:
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            hello = await socket.receive_str()
            await socket.send_str(json.dumps({"nonce": "N1"}))
            login = await socket.receive_str()
            await socket.send_str(json.dumps({"event": "ok", "got": [hello, login]}))
            await socket.close()
            return socket

        async def other(_r: web.Request) -> web.Response:
            return web.Response()

        port, runner = await _server(other, ws)
        hub = EventHub()
        got: list[str] = []
        hub.subscribe(lambda f: got.append(f["data"]["text"]))
        door = _door(port, {"kind": "ws", "auth": {"type": "none"}}, {"password": "pw"})
        spec = {"type": "ws", "id": "rpc", "path": "/ws", "local": True, "steps": [
            {"send": "hello", "capture": {"nonce": "nonce"}},
            {"send": "{=[capture.nonce][secret.password]|sha256|hex}"},
        ]}
        sources = Listeners(FakeHost(), door, hub)
        try:
            await listeners_out.ws(sources, door.descriptor("dev"), "rpc", spec)
        finally:
            await door.async_close()
            await runner.cleanup()
        return got

    got = json.loads(asyncio.run(scenario())[0])["got"]
    assert got == ["hello", hashlib.sha256(b"N1pw").hexdigest()]
