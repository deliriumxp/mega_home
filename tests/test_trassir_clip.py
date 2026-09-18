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
import re

from typing import Any

import pytest

from mega_home import ops

# ⚠ Механика команды архива живёт в `trassir_archive.py`, реестр просмотров — в
# `trassir_clip.py`: файл перерос порог дробления, и его разрезали по владению.
from mega_home.trassir_archive import _stamp
from mega_home.trassir_clip import CLIP_PREFIX, ClipSessions

# ⚠ Метка и окно, с которыми запись открывает ПРИЛОЖЕНИЕ: по каналу и метке,
# окно считает оно. Открытие по СОБЫТИЮ у дома снято 2026-09-14 вместе с
# маршрутом — событие лишь одна из причин посмотреть запись, и отдельной двери
# у него больше нет (`docs/plan-video-rework.md`, этап 1).
AT = 1_788_960_000_000_000
WINDOW = (AT - 10_000_000, AT + 60_000_000)

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

    async def async_archive_status(self, kind: str = "timeline") -> list[dict[str, Any]]:
        """Шкала записанных участков за сутки — так отвечает регистратор.

        ⚠ Секунды ОТ НАЧАЛА СУТОК `day_start`, а не метки: перепутать их значит
        нарисовать жильцу разметку записи в другом веке.
        """
        self.calls.append(("archive_status", {"type": kind}))
        return [
            {
                "token": f"tok{self.tokens}",
                "day_start": "2026-09-09",
                "timeline": [
                    # Окно клипа — 13:19:50 + минута (метка события минус лид).
                    {"begin": "47990", "end": "47997"},  # 13:19:50–13:19:57
                    {"begin": "48020", "end": "48030"},  # 13:20:20–13:20:30
                    {"begin": "60000", "end": "60010"},  # вечером — вне окна
                ],
            }
        ]

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

    # Хозяин (`host.Host`) шлюза: та же шина, под своим именем.
    spawn = async_create_background_task

    def session(self, verify_ssl: bool = True) -> Any:
        return None


async def _quiet(hass: FakeHass, gateway: FakeGateway) -> None:
    """Дождаться фоновых задач и снять их: висящий пинг переживал бы спеку."""
    for _ in range(200):
        await asyncio.sleep(0)
    for owner in (hass, gateway.env):
        for task in owner.tasks:
            if isinstance(task, asyncio.Task):
                task.cancel()
    for owner in (hass, gateway.env):
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
        self.env = FakeHass()
        self.clips = ClipSessions(self)

    def event(self, event_id: str):
        return EVENT if event_id == "e1" else None


@pytest.fixture()
def gateway() -> FakeGateway:
    return FakeGateway()


def test_дома_основной_архив_снаружи_суб(gateway: FakeGateway) -> None:
    """⚠ Замер стенда 2026-09-09 по прицеленному окну: основной архив
    1.43 Мбит/с и 13.4 к/с, суб — 0.16 Мбит/с и 10.7 к/с. Суб ВЕЗДЕ выглядел
    ровно тем, чем был: мелкой картинкой с выпадающими кадрами при живой
    камере в полном качестве рядом."""
    asyncio.run(gateway.clips.async_open_at("cam1", AT, "Вход", window_start_us=WINDOW[0], window_stop_us=WINDOW[1]))
    call = next(params for name, params in gateway.client.calls if name == "get_video")
    assert call["stream"] == "archive_main"
    assert call["container"] == "rtsp"

    gateway.client.calls.clear()
    asyncio.run(gateway.clips.async_open_at("cam1", AT, remote=True))
    call = next(params for name, params in gateway.client.calls if name == "get_video")
    assert call["stream"] == "archive_sub", "снаружи канал мобильный"


def _offered(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch, clip_id: str
) -> FakeHass:
    """Переговоры без команды: ответ ушёл, архив молчит."""

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
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
    assert any("ping" in name for name in gateway.env.names), "токен под охраной с выдачи"
    assert any("fallback" in name for name in gateway.env.names), "сторож слепого старта взведён"
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
    # ⚠ Наружу уходит только ответ РЕГИСТРАТОРА: где он встал и откуда просили.
    # Участки записи, окно и «писали ли вообще» считает приложение — в доме этих
    # толкований больше нет (docs/plan-thin-integration.md, «Широкая дверь»).
    assert "segments" not in answer
    assert "outOfWindow" not in answer
    commands = [p for n, p in gateway.client.calls if n == "archive_command"]
    # ⚠⚠ Команд ДВЕ, и вторая не лишняя: `play` с запрошенной метки, затем
    # ПОВТОР с той, которую назвал сам регистратор (`first_frame_ts`).
    # Запрошенная точка почти всегда попадает в ДЫРУ — архив пишется по
    # движению, — и на такой метке регистратор отвечает `success: 1`, честно
    # называет ближайший кадр и НЕ ОТДАЁТ ДАННЫЕ. Замер объекта 2026-09-14: от
    # полуночи 298 КБ и замерший курсор против 2118 КБ и идущего курсора после
    # повтора с названной метки.
    assert len(commands) == 2
    assert commands[0]["start"] == _stamp(AT), (
        "метка события уходит меткой регистратора"
    )
    assert commands[1]["start"] == "20260909T140000", "повтор с названной метки"


def test_закрытие_снимает_поток_и_токен(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    dropped: list[str] = []
    closed: list[str] = []

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
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
        opened = await gateway.clips.async_open_at("cam1", AT, "Вход", window_start_us=WINDOW[0], window_stop_us=WINDOW[1])
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

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
        return {"sessionId": "s7", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> None:
        hass = FakeHass()
        opened = await gateway.clips.async_open_at("cam1", AT, "Вход", window_start_us=WINDOW[0], window_stop_us=WINDOW[1])
        await gateway.clips.async_offer(hass, opened["id"], "offer")
        await _quiet(hass, gateway)

    asyncio.run(scenario())

    assert gateway.clips.clip_of_session("s7") == f"{CLIP_PREFIX}tok1"


def test_чужой_id_не_становится_клипом(gateway: FakeGateway) -> None:
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_offer(FakeHass(), "trassir:ghost", "offer"))

    assert "заново" in err.value.message


def _opened_clip_id(gateway: FakeGateway) -> str:
    opened = asyncio.run(gateway.clips.async_open_at("cam1", AT, "Вход", window_start_us=WINDOW[0], window_stop_us=WINDOW[1]))
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
    monkeypatch.setattr("mega_home.trassir_archive.TRASSIR_ARCHIVE_SETTLE", 0.02)

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
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
    # ⚠ Две: `play` с запрошенной метки и повтор с той, что назвал регистратор
    # (запрошенная почти всегда попадает в дыру — см. `_play_where_told`).
    assert len(commands) == 2
    assert commands[0]["start"] == _stamp(AT)


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
    assert answer["positionUs"] == AT


def test_закрытие_до_готовности_не_командует(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Шторку закрыли раньше готовности: ни команды, ни висящих задач."""

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> None:
        hass = FakeHass()
        opened = await gateway.clips.async_open_at("cam1", AT, "Вход", window_start_us=WINDOW[0], window_stop_us=WINDOW[1])
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


def test_канал_без_постоянного_адреса_идёт_по_токену(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """⚠ Постоянный адрес есть НЕ У КАЖДОГО канала: на стенде из 12 один отвечает
    404 на `<guid>_m/` всегда, а по `get_video` отдаётся нормально. Пока путь был
    один, такая камера не открывалась вовсе — «wrong response on DESCRIBE»."""
    sources: list[str] = []

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
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
    assert any("ping" in name for name in gateway.env.names)


def test_живой_просмотр_по_токену_ничем_не_командует(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Запасной путь — это тот же сеанс, что у записи, но БЕЗ команды архива."""

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
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
    asyncio.run(gateway.clips.async_open_at("cam1", AT, remote=True, quality="main"))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_main", "снаружи, но приложение просит основной"

    gateway.client.calls.clear()
    asyncio.run(gateway.clips.async_open_at("cam1", AT, remote=False, quality="sub"))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_sub", "дома, но приложение просит суб"


def test_старый_бандл_получает_умолчание_по_двери(gateway: FakeGateway) -> None:
    """⚠ Умолчание — ТОЛЬКО для бандлов, которые качества не шлют: иначе
    удалённый жилец получил бы основной архив на мобильном канале. Снять вместе
    со свёрткой событий, когда релизный бандл поднимут (правило выпуска)."""
    asyncio.run(gateway.clips.async_open_at("cam1", AT, remote=True))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_sub"

    gateway.client.calls.clear()
    asyncio.run(gateway.clips.async_open_at("cam1", AT, remote=False))
    call = next(p for n, p in gateway.client.calls if n == "get_video")
    assert call["stream"] == "archive_main"


# --- классический архив по дням (2026-09-12) --------------------------------
#
# ⚠ Событие — лишь ОДНА из причин посмотреть запись. «Что было вчера в 21:40»
# события не имеет вовсе, а запись у регистратора есть; поэтому архив канала
# открывается ПО МЕТКЕ, и окно у него — СУТКИ, а не минуты вокруг события.


def test_окно_шкалы_приходит_от_приложения(gateway: FakeGateway) -> None:
    """⚠ Окно считает ПРИЛОЖЕНИЕ: шкалу рисует оно. Дом хранит присланное и
    уезжает обратно как есть — считать сутки в доме больше нечему."""
    at = EVENT["timestampUs"]
    window = (at - 3_600_000_000, at + 3_600_000_000)
    answer = asyncio.run(gateway.clips.async_open_at("cam1", at, None, False, None, *window))

    assert answer["startUs"] == window[0]
    assert answer["stopUs"] == window[1]
    assert answer["positionUs"] == at, "играем ровно с метки"


def test_без_окна_дом_ничего_не_считает(gateway: FakeGateway) -> None:
    """Окна нет — и нет: выдумывать сутки в доме запрещено (замок
    `test_thin_gateway.py`), и шкалу посчитает тот, кто её рисует."""
    answer = asyncio.run(gateway.clips.async_open_at("cam1", EVENT["timestampUs"]))

    assert answer["startUs"] is None and answer["stopUs"] is None


def test_без_метки_дом_не_шлёт_обречённую_команду(gateway: FakeGateway, monkeypatch) -> None:
    """⚠ Метки нет — команду старта НЕ ШЛЁМ и говорим об этом словами.

    ⚠ Замер стенда 2026-09-13 (`TRASSIR-4.8.2.0`): `play` без `start` регистратор
    отвергает — `{"error_code":"start is empty","help":"You should specify
    'start', 'stop' and 'speed' for playing archive"}`, а `stop`, приехавший из
    пустоты строкой "None", даёт `timestamp format is not valid`. То есть
    прежнее «метки нет — регистратор встанет на ближайшую запись сам» было
    догадкой, и стоила она чёрного кадра до таймаута проигрывателя: команда
    уходила, отказ оставался в журнале дома, а жилец видел то же, что при
    потере связи. Часов чужой шкалы дом по-прежнему не заводит — окно присылает
    приложение (оно узнаёт день у календаря открытого потока).
    """
    opened = asyncio.run(gateway.clips.async_open_at("cam1"))
    assert opened["positionUs"] is None

    hass = _offered(gateway, monkeypatch, opened["id"])

    async def scenario() -> dict[str, Any]:
        answer = await gateway.clips.async_ready(opened["id"])
        await _quiet(hass, gateway)
        return answer

    answer = asyncio.run(scenario())
    assert not [p for n, p in gateway.client.calls if n == "archive_command"], (
        "команда без окна регистратором отвергается — слать её незачем"
    )
    assert answer["error"], "жилец обязан прочитать причину, а не смотреть в чёрный кадр"


def test_окно_можно_уточнить_на_готовности(gateway: FakeGateway, monkeypatch) -> None:
    """⚠ Где играть, приложение узнаёт ТОЛЬКО у открытого потока.

    Календарь регистратор отдаёт лишь потоку с потребителем (замер стенда
    2026-09-13: без него 0 дней и день `1970-01-01`), а поток открывается на шаг
    раньше готовности. Поэтому окно приезжает сюда — и уходит в команду как
    есть, без единого пересчёта в доме.
    """
    opened = asyncio.run(gateway.clips.async_open_at("cam1"))
    hass = _offered(gateway, monkeypatch, opened["id"])
    day = 1_789_171_200_000_000

    async def scenario() -> dict[str, Any]:
        await gateway.clips.async_ready(opened["id"], day, day, day + 86_400_000_000)
        await _quiet(hass, gateway)
        return next(p for n, p in gateway.client.calls if n == "archive_command")

    command = asyncio.run(scenario())
    # ⚠ Метка регистратора, а не микросекунды: числом `play` встаёт куда просили
    # и НЕ отдаёт данные (замер объекта 2026-09-14 — 167 КБ против 1798 КБ).
    assert command["start"] == _stamp(day)
    assert command["stop"] == _stamp(day + 86_400_000_000)

def test_сторож_не_съедает_попытку_старта_без_окна(gateway: FakeGateway, monkeypatch) -> None:
    """⚠ Команда архива на соединение ОДНА, и претендентов на неё двое.

    Запись, открытая без метки, окна ещё не имеет: приложение в этот момент идёт
    за днём к календарю регистратора (раньше его не спросить — он отдаётся только
    у потока с потребителем). Сторож слепого старта просыпается первым и окна не
    видит. Если он при этом пометит клип начатым, пришедшая следом готовность с
    окном не сделает НИЧЕГО, и просмотр останется мёртвым навсегда.
    """
    opened = asyncio.run(gateway.clips.async_open_at("cam1"))
    hass = _offered(gateway, monkeypatch, opened["id"])
    day = 1_789_171_200_000_000

    async def scenario() -> dict[str, Any]:
        # Сторож сработал раньше приложения — окна ещё нет.
        await gateway.clips._async_play(opened["id"], gateway.clips._clips[opened["id"]])
        assert not [n for n, _ in gateway.client.calls if n == "archive_command"]
        # …а теперь приложение принесло день, и старт обязан состояться.
        answer = await gateway.clips.async_ready(opened["id"], day, day, day + 86_400_000_000)
        await _quiet(hass, gateway)
        return answer

    answer = asyncio.run(scenario())
    command = next(p for n, p in gateway.client.calls if n == "archive_command")
    assert command["start"] == _stamp(day)
    assert not answer["error"], "состоявшийся старт не жалуется на прошлый отказ"


# --- гонка двух `play` по одному соединению (правка по ревью) ---------------


class _Door:
    """Универсальная дверь, какой её видит `ops.gateway_call`."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def descriptor(self, access: Any) -> Any:
        # Описание доступа есть: «двери нет» — это отдельный, уже запертый путь.
        return {"kind": "http"}

    async def call(
        self,
        access: Any,
        method: str,
        path: str,
        params: Any,
        body: Any,
        session: dict[str, str],
    ) -> tuple[int, str, bytes]:
        self.calls.append(
            {"path": path, "params": params, "session": dict(session)}
        )
        return 200, "application/json", b'{"success": 1}'


def test_команда_дверью_считается_началом_соединения(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """⚠⚠ ГОНКА ДВУХ `play` по одному соединению, и стоит она данных.

    Перемотка идёт универсальной дверью, а бандл шлёт по ней ДВЕ команды: `seek`
    и обязательный за ним `play` (после `seek` поток СТОИТ — замер объекта
    2026-09-13). Если жилец мотнул раньше, чем приехала готовность, дом отдавал
    следом СВОЙ `play` от точки открытия: сторожем слепого старта
    (`TRASSIR_READY_TIMEOUT`) или пришедшей позже готовностью. Это второй `play`
    по уже играющему соединению — данные встают (замер стенда 2026-09-09, в
    документации SDK этого нет вовсе), а в лучшем случае курсор жильца
    возвращается в начало записи.

    ⚠ Дом при этом ничего не толкует: он видит ПУТЬ `archive_command` и делает
    из него один вывод — «по этому соединению уже командуют».
    """
    monkeypatch.setattr("mega_home.trassir_clip.TRASSIR_READY_TIMEOUT", 0.01)
    # ⚠ Паузу укорачиваем, а не отменяем: без неё лишний `play` не успел бы
    # дойти до клиента за время спеки, и она проходила бы и ДО правки.
    monkeypatch.setattr("mega_home.trassir_archive.TRASSIR_ARCHIVE_SETTLE", 0.0)

    async def fake_negotiate(
        hass, url, identifier, source, sdp, what="", remote=False, skip_list=False, trickle=False
    ):
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    door = _Door()
    coordinator = type(
        "Coordinator", (), {"trassir": gateway, "accesses": door}
    )()
    clip_id = _opened_clip_id(gateway)

    async def scenario() -> dict[str, Any]:
        hass = FakeHass()
        # Переговоры прошли, сторож слепого старта взведён — и жилец мотает.
        await gateway.clips.async_offer(hass, clip_id, "offer-sdp")
        gateway.client.calls.clear()
        for command in ("seek", "play"):
            await ops.gateway_call(
                coordinator,
                {
                    "access": "video",
                    "method": "GET",
                    "path": "/archive_command",
                    "params": {"command": command},
                    "clip": clip_id,
                },
            )
        clip = gateway.clips._clips[clip_id]  # noqa: SLF001
        assert clip.started, "по соединению уже командуют — оно НАЧАТО"
        assert clip.fallback is None, "сторож слепого старта снят"
        # Дать сторожу проснуться: до правки он стартовал бы здесь.
        await asyncio.sleep(0.08)
        late = await gateway.clips.async_ready(clip_id)
        await _quiet(hass, gateway)
        return late

    late = asyncio.run(scenario())

    assert door.calls[0]["session"]["token"] == "tok1", "токен клипа подставляет дом"
    assert late["ready"] is False, "опоздавшая готовность второй командой не становится"
    assert not [n for n, _ in gateway.client.calls if n == "archive_command"], (
        "своего `play` дом не отдаёт: второй `play` по играющему соединению роняет данные"
    )


def test_чужой_путь_дверью_началом_не_считается(gateway: FakeGateway) -> None:
    """⚠ Началом соединения считается ТОЛЬКО команда архива.

    Дверью ходит и всё остальное — календарь, шкала, подписка на события, — и
    пометить клип начатым по ним значит отобрать у него единственный `play`:
    просмотр остался бы мёртвым навсегда (ровно та беда, от которой сторож и
    завёлся).
    """
    clip_id = _opened_clip_id(gateway)
    clip = gateway.clips._clips[clip_id]  # noqa: SLF001

    gateway.clips.note_gateway_call(clip_id, "/archive_status")
    assert not clip.started
    gateway.clips.note_gateway_call(clip_id, "/archive_events")
    assert not clip.started

    # …а команда архива — считается, и с параметрами в пути тоже.
    gateway.clips.note_gateway_call(clip_id, "/archive_command?command=play")
    assert clip.started
    # Неизвестный клип не роняет дверь: жилец мог закрыть просмотр раньше.
    gateway.clips.note_gateway_call("trassir:ghost", "/archive_command")


def test_пинг_переживает_сбой_и_гаснет_только_отменой(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """⚠⚠ Один таймаут не имеет права убивать просмотр.

    Токен живёт десять секунд без запросов (`docs/docs-trassir/sdk-video.md`,
    блок «Важно»), а пинг идёт раз в пять. Цикл выходил по ЛЮБОЙ ошибке — и
    одна неудачная попытка означала смерть потока через десять секунд: токен
    оставался без запросов, регистратор его прибирал, жилец видел замерший
    кадр. Причём молча: в журнале оставалась строка `debug`.

    ⚠ Гаснуть цикл обязан только по отмене — иначе уборка клипа не останавливает
    ничего, а пинг продолжает ходить к регистратору за закрытым просмотром.
    """
    monkeypatch.setattr("mega_home.trassir_clip.TRASSIR_PING_INTERVAL", 0.001)

    from mega_home.trassir_archive import Clip
    from mega_home.trassir_client import TrassirError

    beats: list[str] = []
    breaks: list[Any] = [
        TrassirError("Trassir не отвечает на продление токена"),
        RuntimeError("что-то совсем другое"),
    ]

    async def flaky_ping(token: str) -> None:
        if breaks:
            raise breaks.pop(0)
        beats.append(token)

    gateway.client.async_ping = flaky_ping
    clip = Clip(token="tok1", guid="cam1", stream="trassir_tok1")

    async def scenario() -> asyncio.Task:
        task = asyncio.ensure_future(gateway.clips._async_ping(clip))  # noqa: SLF001
        for _ in range(500):
            if len(beats) >= 3:
                break
            await asyncio.sleep(0.001)
        task.cancel()
        # ⚠ Отмена работает штатно: `CancelledError` не проглочен, иначе задача
        # переживала бы закрытие просмотра.
        with pytest.raises(asyncio.CancelledError):
            await task
        return task

    task = asyncio.run(scenario())

    assert len(beats) >= 3, "цикл обязан продолжаться после сбоев, а не выходить"
    assert task.cancelled()


def test_метка_регистратора_симметрична_чтению_приложения() -> None:
    """⚠ Перевод «микросекунды → метка» обязан быть обратным тому, как
    приложение ЧИТАЕТ метки регистратора (`trassirTimeUs` разбирает их как
    UTC). Иначе пояс расходится молча: команда уйдёт с виду правильной, а
    регистратор встанет в другом часе — и это выглядит как «перемотка мимо».
    """
    from datetime import datetime, timezone

    from mega_home.trassir_archive import _stamp

    # 2026-09-14 09:43:51 UTC
    us = int(datetime(2026, 9, 14, 9, 43, 51, tzinfo=timezone.utc).timestamp()) * 1_000_000

    assert _stamp(us) == "20260914T094351"
    # Двузначность обязательна: регистратор ждёт ровно 8+1+6 знаков.
    midnight = int(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp()) * 1_000_000
    assert _stamp(midnight) == "20260102T030405"
    # Пусто остаётся пустым: параметр без значения дверь опускает, а строка
    # "None" даёт «timestamp format is not valid» (замер стенда 2026-09-13).
    assert _stamp(None) is None


def test_повтор_play_идёт_с_метки_НАЗВАННОЙ_регистратором() -> None:
    """⚠⚠ Замок на вторую беду того же рода, что и формат метки.

    Запрошенная точка почти всегда попадает в ДЫРУ: архив пишется по движению,
    и суток из двух сотен фрагментов по восемь секунд хватает, чтобы
    промахнуться мимо записи почти всегда. На такой метке регистратор отвечает
    `success: 1`, честно называет ближайший кадр в `first_frame_ts` — И НЕ
    ОТДАЁТ ДАННЫЕ.

    Замер объекта 2026-09-14 (вчерашний день, `play` от полуночи):
      · от полуночи       →  298 КБ, курсор ЗАМЕР на 00:22:42;
      · повтор с 00:22:42 → 2118 КБ, курсор идёт 00:22:42 → 00:22:50.

    ⚠ Повтор РОВНО ОДИН и только при расхождении: второй круг значил бы, что мы
    спорим с регистратором о его же ответе.
    """
    from mega_home.trassir_archive import _stamp_of_text

    # Перестановка символов, а не разбор даты: дом не толкует ответы.
    assert _stamp_of_text("2026-09-13 00:22:42") == "20260913T002242"
    assert _stamp_of_text("") is None
    assert _stamp_of_text(None) is None
    assert _stamp_of_text("совсем не метка") is None
    # ⚠ Полуразобранное тоже не метка: лучше не слать команду, чем слать кривую.
    assert _stamp_of_text("2026-09-13") is None
