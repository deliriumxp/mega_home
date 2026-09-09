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

    async def async_cameras(self):
        return [{"guid": "cam1", "name": "Вход", "codec": "h264", "hasArchive": True}]

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
