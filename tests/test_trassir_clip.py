"""Просмотр записи события: тем же WebRTC, что и живая камера.

Здесь заперты три вещи, каждая из которых ломает просмотр молча:

* **порядок**: токен → потребитель открыл поток → `archive_command`. Команда,
  отданная раньше, получает от регистратора `stream is expired` — текст читается
  как таймаут и им не является;
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
        self.token = "tok1"

    async def async_get_video(self, guid: str, stream: str, container: str) -> str:
        self.calls.append(("get_video", {"guid": guid, "stream": stream, "container": container}))
        return self.token

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


def test_снаружи_берём_субархив(gateway: FakeGateway) -> None:
    asyncio.run(gateway.clips.async_open("e1"))

    call = next(params for name, params in gateway.client.calls if name == "get_video")
    # 0.45 против 3 Мбит/с — замер на стенде; клип события смотрят с телефона.
    assert call["stream"] == "archive_sub"
    assert call["container"] == "rtsp"


def test_команда_архива_уходит_после_переговоров_не_держа_ответ(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    opened = asyncio.run(gateway.clips.async_open("e1"))

    async def fake_negotiate(hass, url, identifier, source, sdp, what=""):
        order.append(f"negotiate:{identifier}:{source}")
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> dict[str, Any]:
        hass = FakeHass()
        answer = await gateway.clips.async_offer(hass, opened["id"], "offer-sdp")
        # ⚠ Команда — фоном: ответ телефону уже ушёл, а архив встаёт следом.
        # Ждём её здесь, иначе спека проверяет тишину вместо порядка.
        for _ in range(200):
            if any(name == "archive_command" for name, _ in gateway.client.calls):
                break
            await asyncio.sleep(0.005)
        order.extend(
            name for name, _ in gateway.client.calls if name == "archive_command"
        )
        await _quiet(hass, gateway)
        return answer

    answer = asyncio.run(scenario())

    assert answer["sessionId"] == "s1"
    # ⚠ Курсор в ответе offer БОЛЬШЕ НЕ ЕДЕТ: команда ушла фоном, и к моменту
    # ответа её ещё нет. Где курсор встал — отдаёт `seek` (см. ниже): у архива
    # бывают дыры, и молчать об этом нельзя.
    assert "firstFrameTs" not in answer
    assert order[0].startswith("negotiate:trassir_tok1:rtsp://192.168.1.50:555/tok1")
    # ⚠ Команда — второй, и только второй: до открытия потока регистратор
    # отвечает «stream is expired».
    assert order[1] == "archive_command"
    command = next(p for n, p in gateway.client.calls if n == "archive_command")
    assert command["start"] == opened["startUs"], "метка события уходит как есть"


def test_закрытие_снимает_поток_и_токен(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    dropped: list[str] = []
    closed: list[str] = []

    async def fake_negotiate(hass, url, identifier, source, sdp, what=""):
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

    async def fake_negotiate(hass, url, identifier, source, sdp, what=""):
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


def test_seek_без_позиции_возвращает_на_начало_окна(gateway: FakeGateway) -> None:
    """Первый кадр показан — архив возвращается на начало окна.

    Часы архива идут в реальном времени с команды `play`, а телефон показывает
    первый кадр на секунды позже (ICE/DTLS, раскрутка чтения с диска, ключевой
    кадр). Без возврата «запись события» стабильно начиналась на 3–4 секунды
    позже метки.
    """
    clip_id = _opened_clip_id(gateway)
    gateway.client.calls.clear()

    answer = asyncio.run(gateway.clips.async_seek(clip_id))

    assert answer["positionUs"] == EVENT["timestampUs"] - 10_000_000
    assert answer["firstFrameTs"] == "2026-09-09 14:00:00"
    commands = [p for n, p in gateway.client.calls if n == "archive_command"]
    assert len(commands) == 1
    assert commands[0]["command"] == "play"
    assert commands[0]["start"] == EVENT["timestampUs"] - 10_000_000
    assert commands[0]["stop"] == EVENT["timestampUs"] + 60_000_000


def test_seek_ставит_на_метку_таймлайна(gateway: FakeGateway) -> None:
    """Жест по таймлайну — та же команда с другой позицией: отдельной
    перемотки у сеанса Trassir нет."""
    clip_id = _opened_clip_id(gateway)
    gateway.client.calls.clear()
    middle = EVENT["timestampUs"] + 20_000_000

    answer = asyncio.run(gateway.clips.async_seek(clip_id, middle))

    assert answer["positionUs"] == middle
    commands = [p for n, p in gateway.client.calls if n == "archive_command"]
    assert len(commands) == 1
    assert commands[0]["start"] == middle


def test_seek_клампит_а_не_отказывает(gateway: FakeGateway) -> None:
    """Палец на таймлайне не обязан попадать в окно микросекунда в
    микросекунду, а ронять жест из-за края — хамство."""
    clip_id = _opened_clip_id(gateway)

    low = asyncio.run(gateway.clips.async_seek(clip_id, 1))
    high = asyncio.run(gateway.clips.async_seek(clip_id, 10**18))

    assert low["positionUs"] == EVENT["timestampUs"] - 10_000_000
    assert high["positionUs"] == EVENT["timestampUs"] + 60_000_000


def test_seek_мусором_объясняется(gateway: FakeGateway) -> None:
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_seek(_opened_clip_id(gateway), "мимо"))  # type: ignore[arg-type]

    assert err.value.status == 400


def test_seek_закрытой_записи_объясняется(gateway: FakeGateway) -> None:
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_seek("trassir:ghost"))

    assert "заново" in err.value.message


def test_seek_доходит_через_ops(gateway: FakeGateway) -> None:
    """Обёртка для HTTP-дверей (местной и переноса наружу)."""
    coordinator = type("Coordinator", (), {"trassir": gateway})()
    clip_id = _opened_clip_id(gateway)

    answer = asyncio.run(ops.trassir_seek(coordinator, clip_id))

    assert answer["positionUs"] == EVENT["timestampUs"] - 10_000_000


def test_закрытие_раньше_команды_не_течёт_пингом(
    gateway: FakeGateway, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Шторку закрыли раньше, чем дошла фоновая команда: пинг мёртвому токену
    не нужен — это утечка задачи навсегда."""

    async def fake_negotiate(hass, url, identifier, source, sdp, what=""):
        return {"sessionId": "s1", "answer": "sdp", "candidates": []}

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "negotiate_source", fake_negotiate)
    monkeypatch.setattr("mega_home.go2rtc_embed.is_running", lambda: True)

    async def scenario() -> None:
        hass = FakeHass()
        opened = await gateway.clips.async_open("e1")
        await gateway.clips.async_offer(hass, opened["id"], "offer")
        await gateway.clips.async_close(hass, opened["id"], "s1")
        await _quiet(hass, gateway)

    asyncio.run(scenario())

    assert not any("ping" in name for name in gateway.hass.names), (
        "пинг закрытому клипу не заводится"
    )
