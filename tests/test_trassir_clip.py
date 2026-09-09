"""Просмотр записи события: тем же WebRTC, что и живая камера.

Здесь заперты вещи, каждая из которых ломает просмотр молча:

* **порядок**: токен → потребитель открыл поток → `archive_command`. Команда,
  отданная раньше, получает от регистратора `stream is expired` — текст читается
  как таймаут и им не является;
* **команда — ОДНА на соединение**: повторный `play` по открытому потоку
  стенд запретил (со второй-третьей команды данные встают). Поэтому старт ждёт
  готовность телефона, а перемотка — это переоткрытие, а не повтор команды;
* **метка события уходит в окно КАК ЕСТЬ**: она в шкале самого Trassir, и
  «починка» её нашими часами сдвинула бы запись на пояс сервера;
* **уборка**: забытый клип держит соединение с регистратором и поток в go2rtc, а
  предела соединений на объекте может не быть вовсе.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from mega_home import ops
from mega_home.trassir_clip import CLIP_PREFIX, ClipSessions

EVENT = {
    "id": "e1",
    "type": "Motion Start",
    "guid": "cam1",
    "cameraName": "Вход",
    "timestampUs": 1_788_960_000_000_000,
}


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.tokens = 0

    async def async_get_video(self, guid: str, stream: str, container: str) -> str:
        self.calls.append(("get_video", {"guid": guid, "stream": stream, "container": container}))
        # ⚠ Каждый клип — свой токен: стенд запретил делить соединение, и спека
        # обязана видеть то же, что прод.
        self.tokens += 1
        return f"tok{self.tokens}"

    async def async_archive_command(self, token: str, command: str = "play", **params: Any):
        self.calls.append(("archive_command", {"token": token, "command": command, **params}))
        return {"success": 1, "first_frame_ts": "2026-09-09 14:00:00"}

    async def async_ping(self, token: str) -> None:
        self.calls.append(("ping", {"token": token}))


class _Task:
    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class FakeHass:
    def __init__(self) -> None:
        self.tasks: list[Any] = []
        self.names: list[str] = []

    def async_create_task(self, coro: Any) -> Any:
        coro.close()
        self.tasks.append(coro)
        return None

    def async_create_background_task(self, coro: Any, name: str) -> Any:
        # ⚠ Настоящая шина: фоновая задача ПЛАНИРУЕТСЯ, а не выбрасывается.
        # Команда архива уходит фоном после ответа (`async_offer`), и спека,
        # закрывающая корутину, проверяла бы тишину вместо порядка.
        self.names.append(name)
        self.tasks.append(asyncio.ensure_future(coro))
        return _Task()


async def _quiet(hass: FakeHass, gateway: FakeGateway) -> None:
    """Дождаться фоновых задач и снять их: висящий пинг переживал бы спеку."""
    for _ in range(200):
        await asyncio.sleep(0)
    for owner in (hass, gateway.hass):
        for task in owner.tasks:
            if isinstance(task, asyncio.Task):
                task.cancel()
    for owner in (hass, gateway.hass):
        for task in owner.tasks:
            if isinstance(task, asyncio.Task):
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001, S110
                    pass


class FakeGateway:
    configured = True

    def __init__(self) -> None:
        self.client = FakeClient()
        self.settings = {"host": "192.168.1.50", "port": 8080, "rtspPort": 555, "clipSeconds": 60}
        self.hass = FakeHass()
        self.clips = ClipSessions(self)

    def event(self, event_id: str):
        return EVENT if event_id == "e1" else None


@pytest.fixture()
def gateway() -> FakeGateway:
    return FakeGateway()


def test_окно_клипа_строится_в_шкале_trassir(gateway: FakeGateway) -> None:
    answer = asyncio.run(gateway.clips.async_open("e1"))

    assert answer["id"] == f"{CLIP_PREFIX}tok1"
    # −10 секунд до метки: движение начинается раньше, чем его заметил детектор.
    assert answer["startUs"] == EVENT["timestampUs"] - 10_000_000
    assert answer["stopUs"] == EVENT["timestampUs"] + 60_000_000
    assert answer["cameraName"] == "Вход"


def test_дома_основной_архив_снаружи_суб(gateway: FakeGateway) -> None:
    """⚠ Замер стенда 2026-09-09 по прицеленному окну: основной архив
    1.43 Мбит/с и 13.4 к/с, суб — 0.16 Мбит/с и 10.7 к/с. Суб ВЕЗДЕ выглядел
    ровно тем, чем был: мелкой картинкой с выпадающими кадрами при живой
    камере в полном качестве рядом."""
    asyncio.run(gateway.clips.async_open("e1"))
    call = next(params for name, params in gateway.client.calls if name == "get_video")
    assert call["stream"] == "archive_main"
    assert call["container"] == "rtsp"

    gateway.client.calls.clear()
    asyncio.run(gateway.clips.async_open("e1", remote=True))
    call = next(params for name, params in gateway.client.calls if name == "get_video")
    assert call["stream"] == "archive_sub", "снаружи канал мобильный"


def test_перемотка_держит_качество_двери(gateway: FakeGateway) -> None:
    """⚠ Качество выбирает дверь при открытии, а перемотка дверь не меняет.
    Захардкоженный суб ронял домашний просмотр на субпоток после первого же
    жеста по таймлайну."""
    opened = asyncio.run(gateway.clips.async_open("e1"))
    gateway.client.calls.clear()

    asyncio.run(gateway.clips.async_seek(opened["id"], EVENT["timestampUs"]))

    call = next(params for name, params in gateway.client.calls if name == "get_video")
    assert call["stream"] == "archive_main"


def _offered(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch, clip_id: str
) -> FakeHass:
    """Переговоры без команды: ответ ушёл, архив молчит."""

    async def fake_negotiate(hass, url, identifier, source, sdp, what="", remote=False):
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> FakeHass:
        hass = FakeHass()
        await gateway.clips.async_offer(hass, clip_id, "offer-sdp")
        return hass

    return asyncio.run(scenario())


def test_переговоры_без_команды_греют_тракт(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ответ уходит БЕЗ команды архива: RTSP открыт, токен на пинге, а `play`
    ждёт готовность телефона. Команда раньше готовности — это пропуск начала,
    повтором по готовому потоку — вставшие данные (факт стенда)."""
    clip_id = _opened_clip_id(gateway)
    gateway.client.calls.clear()

    hass = _offered(gateway, monkeypatch, clip_id)

    assert not [n for n, _ in gateway.client.calls if n == "archive_command"]
    # ⚠ Пинг взводится ПРИ ОТКРЫТИИ, а не здесь: токен живёт десять секунд без
    # запросов, а между «открыть» и предложением телефона лежит сбор
    # ICE-кандидатов, снаружи — ещё и дорога через менеджер. Пока пинг ждал
    # переговоров, токен успевал умереть, и просмотр уходил в долгое молчание.
    assert any("ping" in name for name in gateway.hass.names), "токен под охраной с выдачи"
    assert any("fallback" in name for name in hass.names), "сторож слепого старта взведён"
    assert any("fallback" in name for name in hass.names), "сторож взведён"
    asyncio.run(_quiet(hass, gateway))


def test_команда_одна_и_по_готовности(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Готовность — это и есть старт: одна команда на соединение."""
    clip_id = _opened_clip_id(gateway)
    gateway.client.calls.clear()
    hass = _offered(gateway, monkeypatch, clip_id)

    async def scenario() -> dict[str, Any]:
        clip = gateway.clips._clips[clip_id]  # noqa: SLF001
        fallback = clip.fallback
        answer = await gateway.clips.async_ready(clip_id)
        # Опоздавшая и повторная готовности — безвредны.
        again = await gateway.clips.async_ready(clip_id)
        assert fallback is not None and fallback.cancelled, "сторож снят стартом"
        await _quiet(hass, gateway)
        return {**answer, "again": again["ready"]}

    answer = asyncio.run(scenario())

    assert answer["ready"] is True
    assert answer["again"] is False
    # Где курсор встал на самом деле — наружу: у архива бывают дыры.
    assert answer["firstFrameTs"] == "2026-09-09 14:00:00"
    commands = [p for n, p in gateway.client.calls if n == "archive_command"]
    assert len(commands) == 1
    assert commands[0]["start"] == EVENT["timestampUs"] - 10_000_000, (
        "метка события уходит как есть"
    )


def test_закрытие_снимает_поток_и_токен(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    dropped: list[str] = []
    closed: list[str] = []

    async def fake_negotiate(hass, url, identifier, source, sdp, what="", remote=False):
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr(webrtc, "close_own", lambda hass, sid: closed.append(sid) or True)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)
    monkeypatch.setattr(
        ClipSessions,
        "_async_drop_stream",
        lambda self, name: _record(dropped, name),
    )

    async def scenario() -> None:
        hass = FakeHass()
        opened = await gateway.clips.async_open("e1")
        await gateway.clips.async_offer(hass, opened["id"], "offer")
        await gateway.clips.async_close(hass, opened["id"], "s1")
        await _quiet(hass, gateway)

    asyncio.run(scenario())

    assert closed == ["s1"], "сессия своего go2rtc закрывается"
    assert dropped == ["trassir_tok1"], "временный поток снимается"
    assert gateway.clips.clip_of_session("s1") is None


async def _record(sink: list[str], name: str) -> None:
    sink.append(name)


def test_клип_закрывается_даже_если_приложение_не_назвало_его(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ключ уборки — сессия: поля `id` в закрытии может не быть."""

    async def fake_negotiate(hass, url, identifier, source, sdp, what="", remote=False):
        return {"sessionId": "s7", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> None:
        hass = FakeHass()
        opened = await gateway.clips.async_open("e1")
        await gateway.clips.async_offer(hass, opened["id"], "offer")
        await _quiet(hass, gateway)

    asyncio.run(scenario())

    assert gateway.clips.clip_of_session("s7") == f"{CLIP_PREFIX}tok1"


def test_чужой_id_не_становится_клипом(gateway: FakeGateway) -> None:
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_offer(FakeHass(), "trassir:ghost", "offer"))

    assert "заново" in err.value.message


def _opened_clip_id(gateway: FakeGateway) -> str:
    opened = asyncio.run(gateway.clips.async_open("e1"))
    return opened["id"]


def test_сторож_стартует_вслепую_без_готовности(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Старое приложение готовности не шлёт: ему достаётся прежнее поведение
    (команда после переговоров), а не чёрный экран."""
    monkeypatch.setattr("mega_home.trassir_clip.TRASSIR_READY_TIMEOUT", 0.01)
    # ⚠ Паузу перед командой архива тоже укорачиваем, а не отменяем: она несущая
    # (без неё регистратор отдаёт ноль байтов), и спека обязана ходить через
    # неё, а не мимо.
    monkeypatch.setattr("mega_home.trassir_clip.TRASSIR_ARCHIVE_SETTLE", 0.02)

    async def fake_negotiate(hass, url, identifier, source, sdp, what="", remote=False):
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)
    clip_id = _opened_clip_id(gateway)

    async def scenario() -> None:
        hass = FakeHass()
        gateway.client.calls.clear()
        await gateway.clips.async_offer(hass, clip_id, "offer-sdp")
        for _ in range(200):
            if any(n == "archive_command" for n, _ in gateway.client.calls):
                break
            await asyncio.sleep(0.005)
        clip = gateway.clips._clips[clip_id]  # noqa: SLF001
        assert clip.started, "сторож стартовал сам"
        # ⚠ Опоздавшая готовность второй командой НЕ становится: одна команда
        # на соединение, повтор роняет данные (факт стенда).
        assert (await gateway.clips.async_ready(clip_id))["ready"] is False
        await _quiet(hass, gateway)

    asyncio.run(scenario())

    commands = [p for n, p in gateway.client.calls if n == "archive_command"]
    assert len(commands) == 1
    assert commands[0]["start"] == EVENT["timestampUs"] - 10_000_000


def test_seek_переоткрывает_а_не_перекомандует(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Жест по таймлайну — новый токен, новый поток, новые переговоры.

    Повторный `play` по одному соединению стенд запретил (данные встают), и
    «та же команда с новым стартом» — не вариант. Старое соединение разбираем
    целиком, телефон сводит новый просмотр обычным путём.
    """
    dropped: list[str] = []
    monkeypatch.setattr(
        ClipSessions,
        "_async_drop_stream",
        lambda self, name: _record(dropped, name),
    )
    clip_id = _opened_clip_id(gateway)
    gateway.client.calls.clear()
    middle = EVENT["timestampUs"] + 20_000_000

    answer = asyncio.run(gateway.clips.async_seek(clip_id, middle))

    # Новый клип — новым id: телефон сводит заново, как при открытии.
    assert answer["id"] == f"{CLIP_PREFIX}tok2"
    assert answer["positionUs"] == middle
    assert answer["startUs"] == EVENT["timestampUs"] - 10_000_000, "окно не съезжает"
    assert answer["stopUs"] == EVENT["timestampUs"] + 60_000_000
    # Ни одной команды по старому соединению — только новый токен.
    assert [n for n, _ in gateway.client.calls if n == "archive_command"] == []
    tokens = [p for n, p in gateway.client.calls if n == "get_video"]
    assert len(tokens) == 1
    # Старый поток снят, старый клип забыт.
    assert dropped == ["trassir_tok1"]
    assert gateway.clips._clips.get(clip_id) is None  # noqa: SLF001


def test_seek_без_позиции_отказывает(gateway: FakeGateway) -> None:
    """«Вернуть на начало» без метки — это жест «к событию», и приложение шлёт
    его меткой само. Пустой позыв — 400, а не угадывание."""
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_seek(_opened_clip_id(gateway), None))

    assert err.value.status == 400


def test_seek_клампит_а_не_отказывает(gateway: FakeGateway) -> None:
    """Палец на таймлайне не обязан попадать в окно микросекунда в
    микросекунду, а ронять жест из-за края — хамство."""
    clip_id = _opened_clip_id(gateway)

    low = asyncio.run(gateway.clips.async_seek(clip_id, 1))
    high = asyncio.run(gateway.clips.async_seek(_opened_clip_id(gateway), 10**18))

    # ⚠ Окно НЕ съезжает: шкала таймлайна стоит на нём, а перемотка двигает
    # только позицию. Пока окно подменялось позицией, жилец перематывал на
    # середину и снова оказывался «в начале записи».
    assert low["startUs"] == EVENT["timestampUs"] - 10_000_000
    assert low["stopUs"] == EVENT["timestampUs"] + 60_000_000
    assert low["positionUs"] == EVENT["timestampUs"] - 10_000_000
    assert high["positionUs"] == EVENT["timestampUs"] + 60_000_000


def test_seek_мусором_объясняется(gateway: FakeGateway) -> None:
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_seek(_opened_clip_id(gateway), "мимо"))  # type: ignore[arg-type]

    assert err.value.status == 400


def test_seek_закрытой_записи_объясняется(gateway: FakeGateway) -> None:
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_seek("trassir:ghost", 1))

    assert "заново" in err.value.message


def test_seek_доходит_через_ops(gateway: FakeGateway) -> None:
    """Обёртка для HTTP-дверей (местной и переноса наружу)."""
    coordinator = type("Coordinator", (), {"trassir": gateway})()
    clip_id = _opened_clip_id(gateway)
    middle = EVENT["timestampUs"] + 20_000_000

    answer = asyncio.run(ops.trassir_seek(coordinator, clip_id, middle))

    assert answer["positionUs"] == middle
    assert answer["startUs"] == EVENT["timestampUs"] - 10_000_000, "окно не съезжает"


def test_ready_доходит_через_ops(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Обёртка готовности для HTTP-дверей."""
    coordinator = type("Coordinator", (), {"trassir": gateway})()
    clip_id = _opened_clip_id(gateway)
    hass = _offered(gateway, monkeypatch, clip_id)

    async def scenario() -> dict[str, Any]:
        answer = await ops.trassir_ready(coordinator, clip_id)
        await _quiet(hass, gateway)
        return answer

    answer = asyncio.run(scenario())

    assert answer["ready"] is True
    assert answer["positionUs"] == EVENT["timestampUs"] - 10_000_000


def test_закрытие_до_готовности_не_командует(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Шторку закрыли раньше готовности: ни команды, ни висящих задач."""

    async def fake_negotiate(hass, url, identifier, source, sdp, what="", remote=False):
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> None:
        hass = FakeHass()
        opened = await gateway.clips.async_open("e1")
        await gateway.clips.async_offer(hass, opened["id"], "offer")
        clip = gateway.clips._clips[opened["id"]]  # noqa: SLF001
        ping, fallback = clip.ping, clip.fallback
        await gateway.clips.async_close(hass, opened["id"], "s1")
        assert ping.cancelled and fallback.cancelled, "задачи сняты"
        await _quiet(hass, gateway)

    asyncio.run(scenario())

    assert not [n for n, _ in gateway.client.calls if n == "archive_command"], (
        "закрытому просмотру команда не уходит"
    )


def test_живая_камера_переключает_поток_адресом(gateway: FakeGateway) -> None:
    """⚠ Кнопка качества у ЖИВОЙ камеры не заводит ни токена, ни сеанса: у
    канала постоянный адрес, и дополнительный поток — тот же адрес с `_s/`.
    Замер стенда 2026-09-09: `_m` — 2.46 Мбит/с, `_s` — 0.36 при тех же 22 к/с."""
    main_name, main_url = gateway.clips.live_stream("cam1")
    sub_name, sub_url = gateway.clips.live_stream("cam1", "sub")

    assert main_url.endswith("/cam1_m/")
    assert sub_url.endswith("/cam1_s/")
    # ⚠ Имена потоков РАЗНЫЕ: одно имя на два источника значит, что go2rtc
    # оставит первый producer, и переключение качества ничего не поменяет.
    assert main_name != sub_name


def test_смена_качества_записи_переоткрывает_клип(gateway: FakeGateway) -> None:
    """Поток и токен привязаны к качеству, поэтому смена качества у записи —
    то же переоткрытие, что перемотка, только позиция остаётся прежней."""
    opened = asyncio.run(gateway.clips.async_open("e1"))
    position = EVENT["timestampUs"]
    gateway.client.calls.clear()

    answer = asyncio.run(gateway.clips.async_seek(opened["id"], position, "sub"))

    call = next(params for name, params in gateway.client.calls if name == "get_video")
    assert call["stream"] == "archive_sub"
    assert answer["positionUs"] == position
    assert answer["startUs"] == EVENT["timestampUs"] - 10_000_000, "окно не съезжает"


def test_канал_без_постоянного_адреса_идёт_по_токену(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """⚠ Постоянный адрес есть НЕ У КАЖДОГО канала: на стенде из 12 один отвечает
    404 на `<guid>_m/` всегда, а по `get_video` отдаётся нормально. Пока путь был
    один, такая камера не открывалась вовсе — «wrong response on DESCRIBE»."""
    sources: list[str] = []

    async def fake_negotiate(hass, url, identifier, source, sdp, what="", remote=False):
        sources.append(source)
        if source.endswith("_m/"):
            raise RuntimeError("webrtc: streams: wrong response on DESCRIBE")
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> dict[str, Any]:
        hass = FakeHass()
        answer = await gateway.clips.async_live_offer(hass, "cam1", "offer", "main")
        # Второе открытие того же канала идёт СРАЗУ по токену: платить двумя
        # переговорами за каждое открытие незачем.
        await gateway.clips.async_live_offer(hass, "cam1", "offer", "main")
        return answer

    answer = asyncio.run(scenario())

    assert answer["sessionId"] == "s1"
    assert sources[0].endswith("cam1_m/"), "сначала быстрый путь"
    assert "tok" in sources[1], "запасной — по токену"
    assert len(sources) == 3, "второе открытие постоянный адрес уже не пробует"
    assert sources[2] == sources[1].replace("tok1", "tok2")
    # ⚠ Токен живого просмотра тоже надо пинговать: он живёт 10 секунд.
    assert any("ping" in name for name in gateway.hass.names)


def test_живой_просмотр_по_токену_ничем_не_командует(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Запасной путь — это тот же сеанс, что у записи, но БЕЗ команды архива."""

    async def fake_negotiate(hass, url, identifier, source, sdp, what="", remote=False):
        if source.endswith("_m/"):
            raise RuntimeError("404")
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> None:
        hass = FakeHass()
        await gateway.clips.async_live_offer(hass, "cam1", "offer", "main")
        await _quiet(hass, gateway)

    asyncio.run(scenario())

    assert [n for n, _ in gateway.client.calls if n == "archive_command"] == []


# --- политика уехала в приложение (docs/plan-thin-integration.md) ---


def test_качество_архива_решает_приложение(gateway: FakeGateway) -> None:
    """⚠ `quality` от приложения ПЕРЕБИВАЕТ умолчание по двери.

    Правило «дома основной, снаружи суб» — политика, а не физика: приложение
    знает свою дверь лучше нас (у него два транспорта), а политика в Python
    стоит релиза HACS на каждом объекте.
    """
    asyncio.run(gateway.clips.async_open("e1", remote=True, quality="main"))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_main", "снаружи, но приложение просит основной"

    gateway.client.calls.clear()
    asyncio.run(gateway.clips.async_open("e1", remote=False, quality="sub"))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_sub", "дома, но приложение просит суб"


def test_старый_бандл_получает_умолчание_по_двери(gateway: FakeGateway) -> None:
    """⚠ Умолчание — ТОЛЬКО для бандлов, которые качества не шлют: иначе
    удалённый жилец получил бы основной архив на мобильном канале. Снять вместе
    со свёрткой событий, когда релизный бандл поднимут (правило выпуска)."""
    asyncio.run(gateway.clips.async_open("e1", remote=True))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_sub"

    gateway.client.calls.clear()
    asyncio.run(gateway.clips.async_open("e1", remote=False))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_main"
