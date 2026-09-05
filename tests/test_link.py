"""Answering the manager: a resident who is away asks through the link.

Checked here is the promise that makes remote access usable at all — the house
ALWAYS answers. A refusal travels as a normal frame with `ok: false`; silence
would make "device not found" look like "the house is offline", three seconds
later, to a resident standing in that house.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus

from mega_home import ops
from mega_home.link import ManagerLink


class _Socket:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


class _DeadSocket(_Socket):
    async def send_json(self, payload: dict) -> None:
        raise ConnectionResetError("сокет закрылся")


def link(answer):
    instance = ManagerLink.__new__(ManagerLink)
    instance._hass = object()
    instance._coordinator = object()
    ops_run = ops.run

    async def patched(hass, coordinator, op, payload):
        return answer(op, payload)

    ops.run = patched
    instance._restore = lambda: setattr(ops, "run", ops_run)
    return instance


def answer(instance, frame, socket=None):
    socket = socket or _Socket()
    try:
        asyncio.run(instance._handle(frame, socket))
    finally:
        instance._restore()
    return socket.sent


def test_ответ_возвращается_с_тем_же_идентификатором():
    instance = link(lambda op, payload: {"connected": True})
    sent = answer(instance, {"t": "req", "id": "r7", "op": "states"})
    assert sent == [{"t": "res", "id": "r7", "ok": True, "payload": {"connected": True}}]


def test_отказ_едет_ответом_а_не_молчанием():
    def refuse(op, payload):
        raise ops.OpError("Устройство не найдено", HTTPStatus.NOT_FOUND)

    instance = link(refuse)
    sent = answer(instance, {"t": "req", "id": "r8", "op": "command"})
    assert sent == [
        {"t": "res", "id": "r8", "ok": False, "error": "Устройство не найдено", "status": 404}
    ]


def test_неожиданная_ошибка_тоже_возвращается_ответом():
    def boom(op, payload):
        raise RuntimeError("что-то сломалось")

    instance = link(boom)
    sent = answer(instance, {"t": "req", "id": "r9", "op": "states"})
    assert sent[0]["ok"] is False and sent[0]["status"] == 500
    # ⚠ Внутренности наружу не уезжают: жильцу нечего делать с текстом
    # исключения, а менеджеру — тем более.
    assert "что-то сломалось" not in sent[0]["error"]


def test_кадр_без_идентификатора_игнорируется():
    instance = link(lambda op, payload: {})
    assert answer(instance, {"t": "req", "op": "states"}) == []


def test_обрыв_сокета_на_ответе_не_роняет_канал():
    instance = link(lambda op, payload: {})
    # Исключения быть не должно: канал переподключится сам.
    assert answer(instance, {"t": "req", "id": "r1", "op": "states"}, _DeadSocket()) == []
