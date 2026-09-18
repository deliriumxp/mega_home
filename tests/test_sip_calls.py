"""Вызовы домофонии через ARI: панель держим звонящей, телефон соединяем мостом.

⚠ Asterisk здесь поддельный: проверяется НАШЕ решение по событиям — что панели
не отвечаем до телефона, кого соединяем и кого гасим отбоем. Что ARI понимает
эти запросы, проверяет стенд.
"""

from __future__ import annotations

import asyncio
from typing import Any

from mega_home.core.sip_calls import DoorCalls


class _Calls(DoorCalls):
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        super().__init__(None, 8188, "x", lambda kind, data: self.events.append((kind, data)))
        self.sent: list[tuple[str, str, dict[str, str] | None]] = []

    async def _request(self, method, path, params=None):  # noqa: ANN001, ANN201
        self.sent.append((method, path, params))
        if method == "POST" and path == "/bridges":
            return {"id": "br1"}
        return {}


def _start(channel: str, role: str, number: str = "") -> dict[str, Any]:
    return {
        "type": "StasisStart",
        "args": [role],
        "channel": {"id": channel, "caller": {"number": number}},
    }


def _end(channel: str) -> dict[str, Any]:
    return {"type": "StasisEnd", "channel": {"id": channel}}


def _run(calls: _Calls, *events: dict[str, Any]) -> None:
    async def go() -> None:
        for event in events:
            await calls.handle(event)

    asyncio.run(go())


def test_панель_звонит_и_не_отвечена_до_телефона() -> None:
    calls = _Calls()
    _run(calls, _start("p1", "panel", "192.168.88.90"))
    assert calls.sent == [("POST", "/channels/p1/ring", None)]
    assert calls.events == [("call", {"caller": "192.168.88.90"})]


def test_ответ_телефона_соединяет_с_панелью() -> None:
    calls = _Calls()
    _run(calls, _start("p1", "panel"), _start("t1", "answer"))
    assert ("POST", "/channels/p1/answer", None) in calls.sent
    assert ("POST", "/channels/t1/answer", None) in calls.sent
    assert ("POST", "/bridges/br1/addChannel", {"channel": "p1,t1"}) in calls.sent
    assert calls.events[-1][0] == "answered"


def test_ответ_без_вызова_кладёт_трубку() -> None:
    calls = _Calls()
    _run(calls, _start("t1", "answer"))
    assert calls.sent == [("DELETE", "/channels/t1", {"reason": "normal"})]


def test_второй_телефон_не_перехватывает_разговор() -> None:
    calls = _Calls()
    _run(calls, _start("p1", "panel"), _start("t1", "answer"), _start("t2", "answer"))
    assert calls.sent[-1] == ("DELETE", "/channels/t2", {"reason": "normal"})


def test_гость_ушёл_до_ответа_это_отмена() -> None:
    calls = _Calls()
    _run(calls, _start("p1", "panel"), _end("p1"))
    assert calls.events[-1][0] == "cancel"
    assert calls.state()["calls"] == []


def test_жилец_положил_трубку_гасит_панель() -> None:
    calls = _Calls()
    _run(calls, _start("p1", "panel"), _start("t1", "answer"), _end("t1"))
    assert ("DELETE", "/bridges/br1", None) in calls.sent
    assert ("DELETE", "/channels/p1", {"reason": "normal"}) in calls.sent
    assert calls.events[-1][0] == "ended"
    # Отбой, что мы сами разослали, возвращается событием — второго «конца» нет.
    before = len(calls.events)
    _run(calls, _end("p1"))
    assert len(calls.events) == before


def test_чужой_номер_в_stasis_отбивается() -> None:
    calls = _Calls()
    _run(calls, _start("x1", "что-то"))
    assert calls.sent == [("DELETE", "/channels/x1", {"reason": "unallocated"})]
