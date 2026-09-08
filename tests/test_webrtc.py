"""Удалённый просмотр камеры: обмен предложением и ответом по каналу менеджера.

Проверяется здесь то, ради чего файл `webrtc.py` вообще существует: разрозненные
сообщения Home Assistant (ответ отдельно, кандидаты отдельно) сводятся в ОДИН
пакет, потому что канал до менеджера — запрос-ответ, а не подписка. И то, что
при любом отказе сессия камеры закрывается: иначе go2rtc держал бы поток с
камеры после каждой неудачной попытки жильца.

⚠ Модули камеры Home Assistant подменены здесь, а не в `conftest.py`: их видит
только этот тест, и подмена должна уметь врать по-разному (камера без WebRTC,
камера с ошибкой, молчащая камера).
"""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import pytest

from mega_home import ops


class _StreamType:
    HLS = "hls"
    WEB_RTC = "web_rtc"


@dataclass(frozen=True)
class _Message:
    pass


@dataclass(frozen=True)
class _Answer(_Message):
    answer: str


@dataclass(frozen=True)
class _Candidate(_Message):
    candidate: Any


@dataclass(frozen=True)
class _Error(_Message):
    code: str
    message: str


class _Ice:
    """Кандидат так, как его отдаёт Home Assistant: с готовым `to_dict()`."""

    def __init__(self, value: str) -> None:
        self._value = value

    def to_dict(self) -> dict[str, Any]:
        return {"candidate": self._value, "sdpMLineIndex": 0}


class _Capabilities:
    def __init__(self, types: set[str]) -> None:
        self.frontend_stream_types = types


class _Camera:
    """Камера, которая отвечает сценарием: список сообщений и задержка перед ним."""

    def __init__(
        self,
        messages: list[_Message] | None = None,
        *,
        webrtc: bool = True,
        delay: float = 0.0,
        raises: Exception | None = None,
    ) -> None:
        self.camera_capabilities = _Capabilities(
            {_StreamType.WEB_RTC} if webrtc else {_StreamType.HLS}
        )
        self._messages = messages or []
        self._delay = delay
        self._raises = raises
        self.offers: list[tuple[str, str]] = []
        self.closed: list[str] = []

    async def async_handle_async_webrtc_offer(self, offer_sdp, session_id, send_message):
        if self._raises:
            raise self._raises
        self.offers.append((offer_sdp, session_id))

        async def replay() -> None:
            await asyncio.sleep(self._delay)
            for message in self._messages:
                send_message(message)

        asyncio.ensure_future(replay())

    def close_webrtc_session(self, session_id: str) -> None:
        self.closed.append(session_id)


class _Coordinator:
    def __init__(self, tiles: list[dict[str, Any]]) -> None:
        self.data = {"tiles": tiles}


CAMERA_TILE = {"id": "cam1", "domain": "camera", "entityId": "camera.hall"}


@pytest.fixture(autouse=True)
def _ha_camera_modules():
    """Подменить модули камеры Home Assistant на время теста."""
    package = types.ModuleType("homeassistant.components.camera")
    const = types.ModuleType("homeassistant.components.camera.const")
    const.StreamType = _StreamType
    webrtc_module = types.ModuleType("homeassistant.components.camera.webrtc")
    webrtc_module.WebRTCMessage = _Message
    webrtc_module.WebRTCAnswer = _Answer
    webrtc_module.WebRTCCandidate = _Candidate
    webrtc_module.WebRTCError = _Error
    helper = types.ModuleType("homeassistant.components.camera.helper")

    cameras: dict[str, _Camera] = {}

    def get_camera_from_entity_id(hass, entity_id):
        from homeassistant.exceptions import HomeAssistantError

        if entity_id not in cameras:
            raise HomeAssistantError("Camera not found")
        return cameras[entity_id]

    helper.get_camera_from_entity_id = get_camera_from_entity_id
    added = {
        "homeassistant.components.camera": package,
        "homeassistant.components.camera.const": const,
        "homeassistant.components.camera.webrtc": webrtc_module,
        "homeassistant.components.camera.helper": helper,
    }
    sys.modules.update(added)
    try:
        yield cameras
    finally:
        for name in added:
            sys.modules.pop(name, None)


@pytest.fixture(autouse=True)
def _instant_gathering(monkeypatch):
    """Окно сбора кандидатов в тесте не ждём — проверяется сбор, а не часы."""
    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "CANDIDATE_WINDOW", 0.02)
    monkeypatch.setattr(webrtc, "ANSWER_TIMEOUT", 0.2)


def run(coro):
    return asyncio.run(coro)


def test_ответ_и_кандидаты_едут_одним_пакетом(_ha_camera_modules):
    camera = _Camera(
        [_Answer("v=0 answer"), _Candidate(_Ice("candidate:1 udp")), _Candidate(_Ice("candidate:2 srflx"))]
    )
    _ha_camera_modules["camera.hall"] = camera

    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc",
            {"id": "cam1", "offer": "v=0 offer"},
        )
    )

    assert result["answer"] == "v=0 answer"
    # ⚠ Кандидаты обязаны быть В ОТВЕТЕ: go2rtc отдаёт ответ сразу, а адрес, по
    # которому его видно снаружи, присылает следом. Ответ без них — соединение,
    # которому некуда встать.
    assert result["candidates"] == [
        {"candidate": "candidate:1 udp", "sdpMLineIndex": 0},
        {"candidate": "candidate:2 srflx", "sdpMLineIndex": 0},
    ]
    assert camera.offers[0][0] == "v=0 offer"
    # Сессия жива: её закроет жилец, закрыв просмотр.
    assert camera.closed == []
    assert result["sessionId"] == camera.offers[0][1]


def test_камера_без_webrtc_отказывает_понятно(_ha_camera_modules):
    _ha_camera_modules["camera.hall"] = _Camera(webrtc=False)

    with pytest.raises(ops.OpError) as err:
        run(
            ops.run(
                object(),
                _Coordinator([CAMERA_TILE]),
                "webrtc",
                {"id": "cam1", "offer": "v=0"},
            )
        )
    assert err.value.status == HTTPStatus.NOT_IMPLEMENTED


def test_молчание_камеры_закрывает_сессию(_ha_camera_modules):
    camera = _Camera([])
    _ha_camera_modules["camera.hall"] = camera

    with pytest.raises(ops.OpError) as err:
        run(
            ops.run(
                object(),
                _Coordinator([CAMERA_TILE]),
                "webrtc",
                {"id": "cam1", "offer": "v=0"},
            )
        )
    assert err.value.status == HTTPStatus.GATEWAY_TIMEOUT
    # ⚠ Иначе go2rtc держал бы поток с камеры после каждой неудачной попытки.
    assert camera.closed == [camera.offers[0][1]]


def test_ошибка_камеры_едет_текстом_и_закрывает_сессию(_ha_camera_modules):
    camera = _Camera([_Error("go2rtc_webrtc_offer_failed", "Stream source is not supported")])
    _ha_camera_modules["camera.hall"] = camera

    with pytest.raises(ops.OpError) as err:
        run(
            ops.run(
                object(),
                _Coordinator([CAMERA_TILE]),
                "webrtc",
                {"id": "cam1", "offer": "v=0"},
            )
        )
    assert "Stream source is not supported" in err.value.message
    assert camera.closed == [camera.offers[0][1]]


def test_закрытие_просмотра_отпускает_камеру(_ha_camera_modules):
    camera = _Camera([])
    _ha_camera_modules["camera.hall"] = camera

    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc-close",
            {"id": "cam1", "sessionId": "s7"},
        )
    )
    assert result == {"closed": True}
    assert camera.closed == ["s7"]


def test_сущность_берётся_из_состава_а_не_из_запроса(_ha_camera_modules):
    """⚠ Единственная защита от «покажи чужую камеру».

    В запросе жильца лежит id ПЛИТКИ его дома; `entity_id` в него подставить
    негде, и подставленный — не читается.
    """
    camera = _Camera([_Answer("v=0 answer")])
    _ha_camera_modules["camera.hall"] = camera
    _ha_camera_modules["camera.neighbour"] = _Camera([_Answer("чужая")])

    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc",
            {"id": "cam1", "entityId": "camera.neighbour", "offer": "v=0"},
        )
    )
    assert result["answer"] == "v=0 answer"


def test_не_камера_и_ненайденная_плитка_отказывают(_ha_camera_modules):
    coordinator = _Coordinator(
        [CAMERA_TILE, {"id": "l1", "domain": "light", "entityId": "light.hall"}]
    )
    with pytest.raises(ops.OpError):
        run(ops.run(object(), coordinator, "webrtc", {"id": "l1", "offer": "v=0"}))
    with pytest.raises(ops.OpError) as err:
        run(ops.run(object(), coordinator, "webrtc", {"id": "нет", "offer": "v=0"}))
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_предложение_обязательно(_ha_camera_modules):
    _ha_camera_modules["camera.hall"] = _Camera([])
    with pytest.raises(ops.OpError):
        run(ops.run(object(), _Coordinator([CAMERA_TILE]), "webrtc", {"id": "cam1"}))
