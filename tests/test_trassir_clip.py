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

    def async_create_task(self, coro: Any) -> Any:
        coro.close()
        self.tasks.append(coro)
        return None

    def async_create_background_task(self, coro: Any, name: str) -> Any:
        coro.close()
        self.tasks.append(name)
        return _Task()


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


def test_команда_архива_уходит_ПОСЛЕ_ответа_на_предложение(
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
        answer = await gateway.clips.async_offer(FakeHass(), opened["id"], "offer-sdp")
        order.extend(name for name, _ in gateway.client.calls if name == "archive_command")
        return answer

    answer = asyncio.run(scenario())

    assert answer["sessionId"] == "s1"
    # Где курсор встал на самом деле — наружу: у архива бывают дыры.
    assert answer["firstFrameTs"] == "2026-09-09 14:00:00"
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
        opened = await gateway.clips.async_open("e1")
        await gateway.clips.async_offer(FakeHass(), opened["id"], "offer")
        await gateway.clips.async_close(FakeHass(), opened["id"], "s1")

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
        opened = await gateway.clips.async_open("e1")
        await gateway.clips.async_offer(FakeHass(), opened["id"], "offer")

    asyncio.run(scenario())

    assert gateway.clips.clip_of_session("s7") == f"{CLIP_PREFIX}tok1"


def test_чужой_id_не_становится_клипом(gateway: FakeGateway) -> None:
    with pytest.raises(ops.OpError) as err:
        asyncio.run(gateway.clips.async_offer(FakeHass(), "trassir:ghost", "offer"))

    assert "заново" in err.value.message
