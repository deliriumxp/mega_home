"""WS-Security UsernameToken у общего клиента дома (`core/wsse.py`, `connect.http_send`).

⚠ Настоящий сервер, а не подмена: проверяется то, что уходит по проводу, — подпись по часам
УСТРОЙСТВА. Камера без NTP живёт в своём времени, и подпись по часам дома она отвергает так же,
как неверный пароль (`ter:NotAuthorized`, стенд `.90.147`).
"""

from __future__ import annotations

import asyncio
import base64
import calendar
import hashlib
import re
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from mega_home.core import connect, wsse
from mega_home.core import listeners_out as lo
from mega_home.core.devices import AuthSpec, DeviceDescriptor

USER, PASSWORD = "support", "smart&home<43>"
CLOCK = {
    "path": "/device",
    "body": "<s:Envelope xmlns:s='x'><s:Body><GetTime/></s:Body></s:Envelope>",
    "utc": {
        "year": "Envelope.Body.TimeResponse.Utc.Date.Year",
        "month": "Envelope.Body.TimeResponse.Utc.Date.Month",
        "day": "Envelope.Body.TimeResponse.Utc.Date.Day",
        "hour": "Envelope.Body.TimeResponse.Utc.Time.Hour",
        "minute": "Envelope.Body.TimeResponse.Utc.Time.Minute",
        "second": "Envelope.Body.TimeResponse.Utc.Time.Second",
    },
}
REQUEST = b'<?xml version="1.0"?><env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope"><env:Body><Pull/></env:Body></env:Envelope>'


@pytest.fixture(autouse=True)
def _fresh_offsets() -> None:
    wsse._offsets.clear()  # noqa: SLF001 — память сдвигов общая на процесс


class _Camera:
    """Устройство со своими часами: подпись принимается в пределах 5 с ЕГО времени."""

    def __init__(self, skew: float) -> None:
        self.skew = skew
        self.clock_calls = 0

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/device", self.clock)
        app.router.add_post("/events", self.events)
        return app

    async def clock(self, _request: web.Request) -> web.Response:
        self.clock_calls += 1
        t = time.gmtime(time.time() + self.skew)
        return web.Response(
            text=(
                "<e:Envelope xmlns:e='x'><e:Body><TimeResponse><Utc>"
                f"<Date><Year>{t.tm_year}</Year><Month>{t.tm_mon}</Month><Day>{t.tm_mday}</Day></Date>"
                f"<Time><Hour>{t.tm_hour}</Hour><Minute>{t.tm_min}</Minute><Second>{t.tm_sec}</Second></Time>"
                "</Utc></TimeResponse></e:Body></e:Envelope>"
            )
        )

    async def events(self, request: web.Request) -> web.Response:
        body = await request.text()
        if "<env:Header><wsse:Security" not in body or body.index("<env:Header>") > body.index("<env:Body>"):
            return web.Response(status=400, text="NotAuthorized: нет шапки")
        found = {k: re.search(f"<{k}(?:\\s[^>]*)?>([^<]*)<", body) for k in ("wsse:Username", "wsse:Password", "wsse:Nonce", "wsu:Created")}
        if not all(found.values()):
            return web.Response(status=400, text="NotAuthorized: неполная подпись")
        user, digest, nonce, created = (m.group(1) for m in found.values())  # type: ignore[union-attr]
        expected = base64.b64encode(hashlib.sha1(base64.b64decode(nonce) + created.encode() + PASSWORD.encode()).digest()).decode()
        at = calendar.timegm(time.strptime(created, "%Y-%m-%dT%H:%M:%SZ"))
        if user != "support" or digest != expected or abs(at - (time.time() + self.skew)) > 5:
            return web.Response(status=400, text="NotAuthorized")
        return web.Response(text="<ok/>")


def _descriptor(port: int, clock: dict | None = CLOCK) -> DeviceDescriptor:
    return DeviceDescriptor(id="cam", host="127.0.0.1", port=port, auth=AuthSpec(type="wsse", user=USER, password=PASSWORD, clock=clock))


async def _pull(port: int, clock: dict | None = CLOCK) -> int:
    descriptor = _descriptor(port, clock)
    async with lo._session(descriptor) as session:  # noqa: SLF001
        status, _kind, _body = await lo._request(session, descriptor, {"method": "POST", "path": "/events", "body": REQUEST.decode()}, {})  # noqa: SLF001
        return status


def test_подпись_по_часам_устройства_принимается_и_сдвиг_помнится() -> None:
    camera = _Camera(skew=-3 * 3600)  # камера без NTP: три часа назад

    async def scenario() -> tuple[int, int]:
        async with TestServer(camera.app()) as server:
            return await _pull(server.port), await _pull(server.port)

    assert asyncio.run(scenario()) == (200, 200)
    assert camera.clock_calls == 1


def test_без_запроса_часов_подпись_по_часам_дома() -> None:
    async def scenario() -> tuple[int, int]:
        async with TestServer(_Camera(skew=0).app()) as synced, TestServer(_Camera(skew=600).app()) as drifted:
            return await _pull(synced.port, clock=None), await _pull(drifted.port, clock=None)

    assert asyncio.run(scenario()) == (200, 400)


def test_отказ_забывает_сдвиг_и_следующий_запрос_спрашивает_часы_заново() -> None:
    """Камера перезагрузилась и потеряла время: один отказ, дальше подпись снова сходится."""
    camera = _Camera(skew=0)

    async def scenario() -> list[int]:
        async with TestServer(camera.app()) as server:
            first = await _pull(server.port)
            camera.skew = -50 * 365 * 86400.0
            return [first, await _pull(server.port), await _pull(server.port)]

    assert asyncio.run(scenario()) == [200, 400, 200]
    assert camera.clock_calls == 2


def test_одиночный_connect_подписывает_тем_же_клиентом() -> None:
    camera = _Camera(skew=120)

    async def scenario() -> int:
        async with TestServer(camera.app()) as server:
            call = connect.prepare_http({
                "kind": "http", "host": "192.168.1.5", "port": server.port, "method": "POST", "path": "/events",
                "body": REQUEST.decode(), "auth": {"type": "wsse", "user": USER, "pass": PASSWORD, "clock": CLOCK},
            })
            # Петлю `connect` не пускает (адресат — устройство LAN): адрес — на тестовый сервер.
            call["url"] = call["url"].replace("192.168.1.5", "127.0.0.1")
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with await connect.http_send(session, call, aiohttp.ClientTimeout(total=5)) as answer:
                    return answer.status

    assert asyncio.run(scenario()) == 200


def test_подпись_встаёт_в_шапку_конверта() -> None:
    credentials = wsse.Credentials("u", "p")
    added = wsse.sign(REQUEST, credentials, 0)
    assert b"<env:Header><wsse:Security" in added and added.index(b"<env:Header>") < added.index(b"<env:Body>")
    existing = wsse.sign(b"<s:Envelope><s:Header><a/></s:Header><s:Body/></s:Envelope>", credentials, 0)
    assert existing.count(b"<s:Header>") == 1 and existing.index(b"<wsse:Security") < existing.index(b"<a/>")
    assert wsse.sign(b"plain text", credentials, 0) == b"plain text"


def test_дайджест_по_формуле_стандарта() -> None:
    nonce, now = b"0123456789abcdef", 1_761_220_000.0
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + 30))
    element = wsse.token("a<b", "pw", 30, nonce=nonce, now=now)
    digest = base64.b64encode(hashlib.sha1(nonce + created.encode() + b"pw").digest()).decode()
    assert f">{digest}</wsse:Password>" in element and f"<wsu:Created>{created}</wsu:Created>" in element
    assert "<wsse:Username>a&lt;b</wsse:Username>" in element


def test_неизвестный_тип_учётки_называет_все_схемы() -> None:
    with pytest.raises(connect.OpError, match="basic, digest или wsse"):
        connect.prepare_http({"kind": "http", "host": "192.168.1.5", "port": 80, "auth": {"type": "bearer"}})
