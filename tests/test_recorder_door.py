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
    RecorderUnreachable,
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
        """Тело ответа aiohttp — КУСКАМИ, как в жизни.

        ⚠ Заглушка отдавала всё одним `read()`, и ровно поэтому юнит-тесты
        пропустили беду, которую нашёл живой прогон: настоящий поток отдаёт
        первый кусок, а не тело целиком.
        """

        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def iter_chunked(self, _size: int) -> Any:
            for at in range(0, len(self._payload), 8):
                yield self._payload[at : at + 8]

        async def read(self) -> bytes:
            # ⚠ Без аргумента — это «до конца», и так его зовёт только вход.
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


def test_ошибка_связи_это_НЕДОСТУПНОСТЬ_а_не_отказ_политики() -> None:
    """⚠ Живой отчёт 2026-09-12: перемотка падала с «Дом не смог выполнить
    запрос». Причина — здесь: обрыв соединения вылетал из двери наружу, а
    менеджер отдаёт неожиданное исключение жильцу именно этой фразой. Отказ
    должен быть ВНЯТНЫМ, иначе его не видно ни в интерфейсе, ни в журнале.

    ⚠ И это НЕ отказ политики (ревизия 2026-09-13). Разница видна снаружи: по
    отказу политики бандл уходит на прежние именованные пути, а при
    недоступности регистратора идти туда некуда — там тот же регистратор,
    только другой дорогой. Пока обе беды были одним исключением и одним кодом,
    жилец читал «Дом не смог выполнить запрос», и поломку шли искать в доме.
    """
    import aiohttp

    call = door()

    class Broken:
        closed = False

        def get(self, *_: Any, **__: Any) -> Any:
            raise aiohttp.ClientConnectionError("Server disconnected")

    call._session = Broken()  # noqa: SLF001 — шов тот же, что у клиента драйвера
    with pytest.raises(RecorderUnreachable) as err:
        asyncio.run(call.call(None, "GET", "/channels"))

    assert "Server disconnected" in str(err.value)
    # ⚠ Наследник общей беды, а не политики: `except RecorderDenied` его НЕ
    # ловит — иначе разделение осталось бы только в названии.
    assert not isinstance(err.value, RecorderDenied)


def test_недоступность_регистратора_отдаётся_502_а_не_403() -> None:
    """⚠ Код ответа — это и есть диагноз, который читает бандл.

    403 значит «дверь не пустила» и разрешает уйти на прежние пути; 502 значит
    «регистратор не ответил», и уходить некуда. Пока всё было 403, приложение
    считало недоступный регистратор за «двери нет».
    """
    import aiohttp

    from mega_home import ops

    call = door()

    class Broken:
        closed = False

        def get(self, *_: Any, **__: Any) -> Any:
            raise aiohttp.ClientConnectionError("Server disconnected")

        def request(self, *_: Any, **__: Any) -> Any:
            raise aiohttp.ClientConnectionError("Server disconnected")

    call._session = Broken()  # noqa: SLF001

    class _Clips:
        @staticmethod
        def token_of(_clip: str) -> str:
            return ""

    class _Gateway:
        recorders = call
        clips = _Clips()

    class _Coordinator:
        trassir = _Gateway()

    with pytest.raises(ops.OpError) as err:
        asyncio.run(ops.recorder_call(_Coordinator(), {"method": "GET", "path": "/channels"}))

    assert err.value.status == 502


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


@pytest.mark.parametrize(
    "path",
    [
        "/a/../settings/webserver/",
        "/./settings/webserver/",
        "/%2e%2e/settings/webserver/",
        "//settings/webserver/",
        "/x/y/../../login",
    ],
)
def test_обход_запрета_точками_не_проходит(path: str) -> None:
    """⚠ Проверять надо ТО, ЧТО УЙДЁТ В СЕТЬ, а не то, что прислали.

    Замер стенда 2026-09-13: `/a/../settings/webserver/` проезжал мимо запрета
    целиком — префикс `/settings` в нём не первый, а нормализацию делает уже
    клиент, ПОСЛЕ проверки. Регистратор отвечал 200, и на том же стенде
    `sdk_settings_write = 1`, то есть той же дырой менялись бы его НАСТРОЙКИ.
    В локальном контуре дома аутентификации нет вовсе — значит любой в Wi-Fi
    объекта.
    """
    descriptor = door().descriptor(None)
    assert descriptor is not None
    with pytest.raises(RecorderDenied):
        RecorderCall.check(descriptor, "GET", path)


@pytest.mark.parametrize(
    "path",
    ["/ptz", "/archive_export", "/export_archive", "/jit-export-create-task"],
)
def test_действие_на_объекте_дверью_не_ходит(path: str) -> None:
    """⚠ Политика двери обещает пускать ВОСПРОИЗВЕДЕНИЕ, а не действие.

    PTZ физически крутит камеру, экспорт пишет файл на диск регистратора и
    занимает его очередь — локальная и удалённая задачи блокируют друг друга
    (`docs/docs-trassir/sdk-archive-export.md`). Обещание было в заголовке
    `recorder.py`, а запрета не было.
    """
    descriptor = door().descriptor(None)
    assert descriptor is not None
    with pytest.raises(RecorderDenied):
        RecorderCall.check(descriptor, "GET", path)


def test_дверь_говорит_сессией_драйвера() -> None:
    """⚠ ГЛАВНОЕ про дверь: сессия у дома ОДНА.

    Замер стенда 2026-09-13 (`TRASSIR-4.8.2.0`, две сессии одного и того же
    `Admin`): поток, открытый ПЕРВОЙ сессией, ВТОРАЯ не видит вовсе —
    `archive_status` при `type=state|timeline|calendar` отдаёт пустой список, а
    `archive_events` приходит без `CalendarEvent` и `TimelineEvent`. То есть
    дверь со своей сессией НИКОГДА не получит ни календаря, ни шкалы суток по
    записи, открытой драйвером, — а выглядит это как «регистратор не отдаёт
    дни» (живой отчёт 2026-09-13).

    Вторая причина та же по цене: вход чаще раза в 5 секунд Trassir считает по
    АДРЕСУ и банит его (`docs/docs-trassir/sdk-session.md`), а два независимых
    входа гоняются именно в этот запрет.
    """
    call = RecorderCall(credentials=_creds, sid_provider=_driver_sid)
    call.apply([TRASSIR])
    seen: list[dict[str, Any]] = []

    class Body:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def iter_chunked(self, _size: int) -> Any:
            for at in range(0, len(self._payload), 8):
                yield self._payload[at : at + 8]

        async def read(self) -> bytes:
            # ⚠ Без аргумента — это «до конца», и так его зовёт только вход.
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
        closed = False

        def request(self, method: str, url: str, **kwargs: Any) -> Answer:
            seen.append({"метод": method, "params": kwargs.get("params")})
            return Answer(b"[]")

        def get(self, url: str, **kwargs: Any) -> Answer:
            seen.append({"вход": url})
            return Answer('{"sid": "своя"}'.encode("utf-8"))

    call._session = Client()  # noqa: SLF001
    asyncio.run(call.call(None, "GET", "/archive_status", {"type": "calendar"}))

    assert not [item for item in seen if "вход" in item], (
        "своего входа у двери быть не должно, пока жива сессия драйвера"
    )
    ask = next(item for item in seen if "метод" in item)
    assert ask["params"]["sid"] == "драйверская"


async def _driver_sid(fresh: bool = False) -> str:
    return "драйверская"


async def _creds() -> tuple[str, str]:
    return "megahome", "s3cret"


def test_падение_драйверской_сессии_это_отказ_двери_а_не_500() -> None:
    """⚠ РЕГРЕССИЯ 0.2.49, найдена на живом объекте 2026-09-13.

    Дверь стала брать сессию у драйвера, а драйвер падает СВОИМИ исключениями
    (`TrassirError` при недоступном регистраторе, `TrassirAuthError` при
    неверной учётке). До провайдера дверь входила сама и отвечала на это
    отказом; с провайдером исключение полетело МИМО обработчиков
    `ops.recorder_call` и стало неперехваченным 500. Для жильца это выглядело
    как «не показывает ни архив, ни календарь» всякий раз, когда регистратор
    просто медленно отвечает.

    Беда регистратора обязана оставаться вердиктом двери — и именно
    «недоступен», чтобы приложение назвало причину, а не ушло на прежние пути.
    """

    async def падает(fresh: bool = False) -> str:
        raise RuntimeError("Trassir не отвечает: нет ответа за 15 с")

    call = RecorderCall(credentials=_creds, sid_provider=падает)
    call.apply([TRASSIR])

    with pytest.raises(RecorderUnreachable) as err:
        asyncio.run(call.call(None, "GET", "/archive_status", {"type": "calendar"}))

    assert "нет ответа" in str(err.value)


def test_протухшая_сессия_перевходит_а_не_уезжает_пустотой() -> None:
    """⚠ Регистратор отвечает на мёртвую сессию ОБЫЧНЫМ 200.

    Замер стенда 2026-09-13: `archive_status?sid=<чужой>` — HTTP 200,
    `content-type: application/json`, тело `{"error_code":"no session",
    "success":0}`. По коду ответа беду не отличить, и дверь отдавала это тело
    наружу как есть: бандл не находил своего токена и показывал ПУСТОЙ календарь
    и пустую шкалу, ничего не сообщая. Теперь дом перевходит и повторяет вызов —
    ровно один раз, по маркеру ИЗ ОПИСАНИЯ (у другого вендора слова другие).
    """
    свежесть: list[bool] = []

    async def sid(fresh: bool = False) -> str:
        свежесть.append(fresh)
        return "мёртвая" if not fresh else "живая"

    call = RecorderCall(credentials=_creds, sid_provider=sid)
    call.apply([{**TRASSIR, "sessionExpired": "no session"}])
    ответы: list[dict[str, Any]] = []

    class Body:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def iter_chunked(self, _size: int) -> Any:
            for at in range(0, len(self._payload), 8):
                yield self._payload[at : at + 8]

        async def read(self) -> bytes:
            # ⚠ Без аргумента — это «до конца», и так его зовёт только вход.
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
        closed = False

        def request(self, method: str, url: str, **kwargs: Any) -> Answer:
            sid_used = kwargs.get("params", {}).get("sid")
            ответы.append({"sid": sid_used})
            if sid_used == "мёртвая":
                return Answer(b'{"error_code":"no session","success":0}')
            return Answer(b'[{"token":"t","calendar":["2026-09-13"]}]')

    call._session = Client()  # noqa: SLF001
    status, _, payload = asyncio.run(
        call.call(None, "GET", "/archive_status", {"type": "calendar"})
    )

    assert свежесть == [False, True], "второй заход обязан просить СВЕЖУЮ сессию"
    assert [item["sid"] for item in ответы] == ["мёртвая", "живая"]
    assert b"calendar" in payload, "наружу уходит ответ живой сессии, а не отказ"


def test_без_маркера_повтора_нет() -> None:
    """Маркер — данные конфига. Не прислали — дверь не выдумывает вендорских слов."""

    async def sid(fresh: bool = False) -> str:
        return "любая"

    call = RecorderCall(credentials=_creds, sid_provider=sid)
    call.apply([TRASSIR])  # без `sessionExpired`
    заходы: list[int] = []

    class Body:
        async def iter_chunked(self, _size: int) -> Any:
            yield b'{"error_code":"no session"'
            yield b',"success":0}'

        async def read(self) -> bytes:
            return '{"sid":"живая"}'.encode("utf-8")

    class Answer:
        status = 200
        content_type = "application/json"
        content = Body()

        async def __aenter__(self) -> "Answer":
            return self

        async def __aexit__(self, *_: Any) -> bool:
            return False

    class Client:
        closed = False

        def request(self, *_: Any, **__: Any) -> Answer:
            заходы.append(1)
            return Answer()

    call._session = Client()  # noqa: SLF001
    asyncio.run(call.call(None, "GET", "/archive_status", {"type": "calendar"}))

    assert len(заходы) == 1


def test_ответ_читается_ЦЕЛИКОМ_а_не_первым_куском() -> None:
    """⚠ ГЛАВНАЯ беда двери, найденная живым прогоном 2026-09-13.

    `content.read(N)` НЕ читает N байт — он отдаёт то, что уже лежит в буфере, а
    на потоковом ответе это ПЕРВЫЙ КУСОК. Настоящий код против настоящего
    регистратора вернул на `/archive_status?type=calendar` ровно два байта —
    `[\\n`. Дальше бандл честно разбирал этот огрызок, не находил своего токена и
    показывал жильцу пустой календарь и пустую шкалу.

    ⚠ Беда ПЛАВАЮЩАЯ, и потому её так долго не видели: короткий ответ успевает
    прийти одним куском, и тогда всё работает; длинный (124 дня календаря, полсотни
    участков шкалы) — нет. Отсюда же «то показывает, то нет».
    """
    call = door()
    целое = b'[{"token":"t","calendar":["2026-09-12","2026-09-13"]}]'

    class Body:
        """Поток, отдающий тело КУСКАМИ, — как настоящий aiohttp."""

        async def read(self) -> bytes:
            # Вход читается до конца — это другой путь, не предмет спеки.
            return b'{"sid":"door"}'

        async def iter_chunked(self, _size: int) -> Any:
            for at in range(0, len(целое), 8):
                yield целое[at : at + 8]

    class Answer:
        status = 200
        content_type = "application/json"
        content = Body()

        async def __aenter__(self) -> "Answer":
            return self

        async def __aexit__(self, *_: Any) -> bool:
            return False

    class Client:
        closed = False

        def request(self, *_: Any, **__: Any) -> Answer:
            return Answer()

        def get(self, *_: Any, **__: Any) -> Answer:
            return Answer()

    call._session = Client()  # noqa: SLF001
    _, _, payload = asyncio.run(call.call(None, "GET", "/archive_status", {"type": "calendar"}))

    assert payload == целое, "тело обязано приехать целиком, а не первым куском"


def test_потолок_ответа_считается_ПО_ХОДУ() -> None:
    """Потолок остаётся потолком — но не ценой порчи всех остальных ответов."""
    from mega_home.recorder import MAX_RESPONSE_BYTES

    call = door()

    class Body:
        async def read(self) -> bytes:
            return b'{"sid":"door"}'

        async def iter_chunked(self, _size: int) -> Any:
            послано = 0
            while послано <= MAX_RESPONSE_BYTES + 1024:
                послано += 65536
                yield b"x" * 65536

    class Answer:
        status = 200
        content_type = "application/octet-stream"
        content = Body()

        async def __aenter__(self) -> "Answer":
            return self

        async def __aexit__(self, *_: Any) -> bool:
            return False

    class Client:
        closed = False

        def request(self, *_: Any, **__: Any) -> Answer:
            return Answer()

        def get(self, *_: Any, **__: Any) -> Answer:
            return Answer()

    call._session = Client()  # noqa: SLF001
    with pytest.raises(RecorderDenied):
        asyncio.run(call.call(None, "GET", "/screenshot/cam"))
