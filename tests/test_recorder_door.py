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
    # ⚠ Схема — данные: у Trassir SDK на HTTPS, у другого регистратора может
    # быть иначе. По http:// этот молча рвёт соединение (замер стенда).
    "scheme": "https",
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
    assert descriptor.scheme == "https", "схема — тоже данные описания"
    # Мусора нет — умолчание то же: SDK живёт на HTTPS.
    assert descriptor_of({"host": "1.2.3.4"}).scheme == "https"


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


def test_дверь_через_ops_выполняет_описанный_вызов(monkeypatch: pytest.MonkeyPatch) -> None:
    """⚠ Дверь живёт в `ops.recorder_call`, и её зовут ОБА транспорта (домашний
    вид и перенос через менеджера). Здесь ловим Wiring: неожиданное исключение
    на этом пути менеджер отдаёт жильцу как «Дом не смог выполнить запрос» —
    то есть живой отчёт 2026-09-12 про перемотку начинается ровно отсюда."""
    from mega_home import ops
    from mega_home.recorder import RecorderCall

    class Clips:
        _clips: dict[str, Any] = {}

    class Gateway:
        def __init__(self) -> None:
            self.clips = Clips()
            self.recorders = RecorderCall(credentials=lambda: _creds())

    class Coordinator:
        def __init__(self) -> None:
            self.trassir = Gateway()

    coordinator = Coordinator()
    coordinator.trassir.recorders.apply([TRASSIR])

    async def fake_call(*args: Any, **kwargs: Any) -> tuple[int, str, bytes]:
        return 200, "application/json", b'{"success": 1, "num": 3}'

    monkeypatch.setattr(coordinator.trassir.recorders, "call", fake_call)

    answer = asyncio.run(
        ops.recorder_call(
            coordinator,
            {"method": "GET", "path": "/archive_command", "params": {"command": "seek"}},
        )
    )

    assert answer == {"success": 1, "num": 3}


def test_дверь_без_описания_это_404_а_не_отказ() -> None:
    """⚠ Дома с дверью, но БЕЗ описания регистратора (конфиг ещё не приехал)
    обязаны отвечать «двери нет» — 404. Иначе бандл считает это отказом, не
    переходит на прежние пути, и перемотка у жильца падает с ошибкой."""
    from http import HTTPStatus

    from mega_home import ops

    class Gateway:
        def __init__(self) -> None:
            from mega_home.recorder import RecorderCall

            self.clips = type("Clips", (), {"_clips": {}})()
            self.recorders = RecorderCall()  # описаний нет вовсе

    coordinator = type("C", (), {"trassir": Gateway()})()

    with pytest.raises(ops.OpError) as err:
        asyncio.run(ops.recorder_call(coordinator, {"method": "GET", "path": "/channels"}))
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_ошибка_связи_это_отказ_а_не_исключение() -> None:
    """⚠ Живой отчёт 2026-09-12: перемотка падала с «Дом не смог выполнить
    запрос». Причина — здесь: обрыв соединения вылетал из двери наружу, а
    менеджер отдаёт неожиданное исключение жильцу именно этой фразой. Отказ
    должен быть ВНЯТНЫМ, иначе его не видно ни в интерфейсе, ни в журнале."""
    import aiohttp

    call = door()

    class Broken:
        closed = False

        def get(self, *_: Any, **__: Any) -> Any:
            raise aiohttp.ClientConnectionError("Server disconnected")

    call._session = Broken()  # noqa: SLF001 — шов тот же, что у клиента драйвера
    with pytest.raises(RecorderDenied) as err:
        asyncio.run(call.call(None, "GET", "/channels"))

    assert "Server disconnected" in str(err.value)


def test_сертификат_регистратора_не_проверяется() -> None:
    """⚠ ЗАМОК на решение заказчика от 2026-09-12: сертификат регистратора
    самоподписанный, и доверие держится не на нём, а на том, что адрес взят из
    КОНФИГА объекта, а не из запроса приложения.

    ⚠ Так же поступает драйвер (`trassir_client.py`, `ssl=False`): дверь и
    драйвер говорят с ОДНИМ И ТЕМ ЖЕ регистратором, и разная строгость означала
    бы, что дверь не подключается там, где драйвер работает (живой отчёт
    2026-09-12: «Дом не смог выполнить запрос»).

    Вернуть проверку можно только ВМЕСТЕ с пином отпечатка в описании
    регистратора — иначе дверь замолчит на всех объектах сразу.
    """
    call = door()
    session = asyncio.run(call._client())

    assert session.connector._ssl is False  # noqa: SLF001 — замок на решение
    asyncio.run(call.async_close())
