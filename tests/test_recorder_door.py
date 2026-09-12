"""Универсальная дверь к регистратору (`recorder.py`).

⚠ Предмет: дом НЕ знает ни одного вендора и не разбирает ни одного ответа. Он
исполняет запрос, описанный в конфиге объекта, подставляет сессию и отдаёт ответ
как есть. Всё, что здесь заперто, — это границы двери и то, что через неё НЕ
проходит.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from mega_home.recorder import (
    RecorderCall,
    RecorderDenied,
    descriptor_of,
)

TRASSIR = {
    "id": "trassir",
    "vendor": "trassir",
    "host": "192.168.1.50",
    "port": 8080,
    "rtspPort": 555,
    "login": "/login",
    "loginParams": {"username": "{user}", "password": "{pass}"},
    "sessionField": "sid",
    "streamPath": "/get_video",
    "streamParams": {"channel": "{camera}", "stream": "{quality}", "container": "rtsp"},
    "streamField": "token",
    "streamUrl": "rtsp://{host}:{rtspPort}/{token}",
}


def door(**patch: Any) -> RecorderCall:
    call = RecorderCall(credentials=lambda: _creds())
    blocks = [{**TRASSIR, **patch}]
    call.apply(blocks)
    return call


async def _creds() -> tuple[str, str]:
    return "megahome", "s3cret"


def test_описание_собирается_из_конфига() -> None:
    """Вендор приезжает ДАННЫМИ: новый регистратор — не релиз, а блок в конфиге."""
    descriptor = descriptor_of(TRASSIR)

    assert descriptor is not None
    assert (descriptor.id, descriptor.host, descriptor.port) == ("trassir", "192.168.1.50", 8080)
    assert descriptor.stream_url == "rtsp://{host}:{rtspPort}/{token}"
    # Мусор — «описания нет», а не падение: конфиг может быть от менеджера постарше.
    assert descriptor_of(None) is None
    assert descriptor_of({"host": "  "}) is None
    assert descriptor_of({"id": "x"}) is None


def test_учётки_через_дверь_не_ходят() -> None:
    """⚠ Пароли подставляет ДОМ. Телефон жильца знает пути, но не учётки."""
    call = door()
    descriptor = call.descriptor(None)
    assert descriptor is not None
    assert "{user}" in descriptor.login_params["username"]


@pytest.mark.parametrize(
    "method,path",
    [
        ("DELETE", "/get_video"),
        ("PUT", "/get_video"),
        ("POST", "/login"),
        ("GET", "/settings/webserver"),
        ("GET", "/objects/abc"),
        ("GET", "/users"),
        ("GET", "get_video"),
    ],
)
def test_границы_двери(method: str, path: str) -> None:
    """Вход, настройки, дерево объектов и запись — через дверь не ходят."""
    descriptor = door().descriptor(None)
    assert descriptor is not None
    with pytest.raises(RecorderDenied):
        RecorderCall.check(descriptor, method, path)


def test_воспроизведение_проходит() -> None:
    """А всё, что про просмотр, — проходит: список команд не ведём."""
    descriptor = door().descriptor(None)
    assert descriptor is not None
    for path in ("/archive_status", "/archive_events", "/screenshot/IAtwTYwK", "/get_video"):
        RecorderCall.check(descriptor, "GET", path)


def test_чужой_регистратор_это_отказ() -> None:
    """Адресат — только из конфига объекта: «сходи по LAN» дверью не выражается."""
    call = door()
    assert call.descriptor("соседний") is None
    with pytest.raises(RecorderDenied):
        asyncio.run(call.call("соседний", "GET", "/channels"))


def test_сессия_подставляется_домом(monkeypatch: pytest.MonkeyPatch) -> None:
    """⚠ Дом входит сам и подставляет `sid`: бандл его не видит и не хранит."""
    call = door()
    seen: list[dict[str, Any]] = []

    class Body:
        """Тело ответа aiohttp: читается с потолком, как в двери."""

        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def read(self, _limit: int = 0) -> bytes:
            return self._payload

    class Answer:
        def __init__(self, payload: bytes) -> None:
            self.status = 200
            self.content_type = "application/json"
            self.content = Body(payload)

        async def __aenter__(self) -> "Answer":
            return self

        async def __aexit__(self, *_: Any) -> bool:
            return False

    class Client:
        """Шов тот же, что у клиента драйвера: подменяем сессию aiohttp."""

        closed = False

        def request(self, method: str, url: str, **kwargs: Any) -> Answer:
            seen.append({"метод": method, "url": url, "params": kwargs.get("params")})
            return Answer(b'{"success": 1}')

        def get(self, url: str, **kwargs: Any) -> Answer:
            seen.append({"вход": url, "params": kwargs.get("params")})
            return Answer(b'{"sid": "abc"}')

    call._session = Client()  # noqa: SLF001 — шов тот же, что у клиента драйвера
    asyncio.run(call.call(None, "GET", "/archive_status", {"type": "timeline"}))

    login = next(item for item in seen if "вход" in item)
    assert login["params"] == {"username": "megahome", "password": "s3cret"}
    ask = next(item for item in seen if "метод" in item)
    assert ask["params"]["sid"] == "abc", "сессию подставляет дом"
    assert ask["url"].endswith("/archive_status")
