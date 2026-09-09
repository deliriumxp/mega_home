"""Лента событий обеими дверями: локальной и переносом снаружи.

⚠ Смысл тот же, что у `test_relay_api.py`: жилец СНАРУЖИ должен получать ровно
то же, что дома, теми же путями. У ленты это впервые упирается в `?guid=` —
пока перенос выбрасывал query, кнопка «События» на конкретной камере снаружи
молча показывала бы события всего объекта. Не отказ, не ошибка — просто другой
ответ, и заметить это можно было только в квартире.
"""

from __future__ import annotations

import asyncio
import base64
import json
from http import HTTPStatus
from pathlib import Path

import pytest

from mega_home import ops

EVENTS = [
    {"id": "e1", "type": "Motion Start", "guid": "cam1", "cameraName": "Вход", "timestampUs": 300},
    {"id": "e2", "type": "Motion Stop", "guid": "cam2", "cameraName": "Склад", "timestampUs": 200},
]
JPEG = b"\xff\xd8\xff\xe0" + b"0" * 32


class _Hass:
    async def async_add_executor_job(self, func, *args):
        return func(*args)


class FakeGateway:
    """Шлюз к регистратору: отвечает заготовленным и запоминает вопросы."""

    configured = True

    def __init__(self) -> None:
        self.asked: list[dict] = []
        self.thumbs: list[str] = []

    def events(self, guid=None, limit=50, before=None):
        self.asked.append({"guid": guid, "limit": limit, "before": before})
        rows = [e for e in EVENTS if guid is None or e["guid"] == guid]
        if before is not None:
            rows = [e for e in rows if e["timestampUs"] < before]
        return rows

    def event(self, event_id):
        return next((e for e in EVENTS if e["id"] == event_id), None)

    async def async_cameras(self, tiles=None):
        self.asked_tiles = tiles
        return [
            {
                "guid": "cam1",
                "name": "Вход",
                "codec": "h264",
                "hasArchive": True,
                "tile": (tiles or {}).get("cam1"),
            }
        ]

    async def async_thumb(self, event_id):
        self.thumbs.append(event_id)
        return JPEG


class _Coordinator:
    def __init__(self, gateway=None) -> None:
        self.data = {"version": "v1", "tiles": []}
        self.trassir = gateway


def call(coordinator, method: str, path: str):
    return asyncio.run(
        ops.run(_Hass(), coordinator, "http", {"method": method, "path": path})
    )


def json_of(answer):
    return json.loads(base64.b64decode(answer["body"]).decode("utf-8"))


@pytest.fixture()
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture()
def coordinator(gateway: FakeGateway) -> _Coordinator:
    return _Coordinator(gateway)


def test_лента_едет_переносом(coordinator):
    rows = json_of(call(coordinator, "GET", "api/trassir/events"))["events"]

    assert [row["id"] for row in rows] == ["e1", "e2"]


def test_фильтр_камеры_переживает_перенос(coordinator, gateway):
    rows = json_of(call(coordinator, "GET", "api/trassir/events?guid=cam2&limit=10"))["events"]

    assert [row["id"] for row in rows] == ["e2"]
    # ⚠ Именно это и терялось: параметры должны доехать до шлюза, а не осесть
    # в разборе пути.
    assert gateway.asked[-1] == {"guid": "cam2", "limit": 10, "before": None}


def test_страница_постарше_тоже_переживает(coordinator, gateway):
    json_of(call(coordinator, "GET", "api/trassir/events?before=300"))

    assert gateway.asked[-1]["before"] == 300


def test_камеры_едут_переносом(coordinator):
    cameras = json_of(call(coordinator, "GET", "api/trassir/cameras"))["cameras"]

    assert cameras[0]["guid"] == "cam1"
    assert cameras[0]["codec"] == "h264", "кодек нужен приложению: h265 WebRTC не отдаст"


def test_превью_едет_байтами_и_кэшируется_навсегда(coordinator, gateway):
    answer = call(coordinator, "GET", "api/trassir/events/e1/thumb")

    assert base64.b64decode(answer["body"]) == JPEG
    assert answer["contentType"] == "image/jpeg"
    # Кадр за прошедшую секунду больше не изменится никогда.
    assert "immutable" in answer.get("cacheControl", "")
    assert gateway.thumbs == ["e1"]


def test_объект_без_видеонаблюдения_отвечает_понятно():
    coordinator = _Coordinator(None)

    with pytest.raises(ops.OpError) as err:
        call(coordinator, "GET", "api/trassir/events")

    assert err.value.status == HTTPStatus.NOT_FOUND
    assert "видеонаблюдение" in err.value.message


def test_запись_идёт_той_же_операцией_что_и_камера(monkeypatch):
    """⚠ Замок на главное решение: своей операции у записи НЕТ.

    Приложение отдаёт id клипа туда же, куда id плитки камеры, — в `webrtc`.
    Заведись у архива своя операция, и снаружи он поехал бы вторым сеансом со
    своими сроками и своей уборкой (§5а плана).
    """

    class _Clips:
        def __init__(self) -> None:
            self.offered: list[str] = []

        async def async_offer(self, hass, clip_id, sdp):
            self.offered.append(clip_id)
            return {"sessionId": "s1", "answer": "sdp", "candidates": []}

        def clip_of_session(self, session_id):
            return None

    class _Gateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.clips = _Clips()

    gateway = _Gateway()
    coordinator = _Coordinator(gateway)

    answer = asyncio.run(
        ops.run(_Hass(), coordinator, "webrtc", {"id": "trassir:tok1", "offer": "sdp"})
    )

    assert answer["sessionId"] == "s1"
    assert gateway.clips.offered == ["trassir:tok1"]


def test_плитка_опознаётся_по_адресу_потока(monkeypatch):
    """⚠ Связь «плитка ↔ канал» — по guid В АДРЕСЕ, а не по имени.

    Имена правят с обеих сторон: у камеры в Home Assistant и у канала в
    Trassir. Совпадение по ним однажды подсунуло бы жильцу записи ЧУЖОЙ
    камеры — а это хуже, чем отсутствие ленты вовсе.
    """
    from mega_home import ops as ops_module

    class _Camera:
        def __init__(self, source: str) -> None:
            self._source = source

        async def stream_source(self) -> str:
            return self._source

    sources = {
        "camera.vhod": "rtsp://192.168.1.50:555/IAtwTYwK_m/",
        "camera.dvor": "rtsp://192.168.1.77:554/stream1",  # чужая камера
    }
    monkeypatch.setattr(
        "mega_home.webrtc._camera", lambda hass, entity_id: _Camera(sources[entity_id])
    )

    coordinator = _Coordinator(FakeGateway())
    coordinator.data = {
        "tiles": [
            {"id": "t1", "domain": "camera", "entityId": "camera.vhod"},
            {"id": "t2", "domain": "camera", "entityId": "camera.dvor"},
            {"id": "t3", "domain": "light", "entityId": "light.hall"},
        ]
    }

    found = asyncio.run(ops_module._tiles_by_guid(_Hass(), coordinator))

    assert found == {"IAtwTYwK": "t1"}, "чужой адрес плиткой Trassir не становится"
