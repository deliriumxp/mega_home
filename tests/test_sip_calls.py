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


def _start(channel: str, role: str, number: str = "", peer: str = "") -> dict[str, Any]:
    # Второй аргумент диалплана — СЕТЕВОЙ адрес отправителя («ip:порт»).
    return {
        "type": "StasisStart",
        "args": [role, f"{peer}:5060"] if peer else [role],
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
    _run(calls, _start("p1", "panel", "192.168.88.90", peer="192.168.88.90"))
    assert calls.sent == [("POST", "/channels/p1/ring", None)]
    assert calls.events == [
        ("call", {"caller": "192.168.88.90", "call": "p1", "peer": "192.168.88.90"})
    ]


def test_в_событии_уезжает_адрес_а_не_только_номер() -> None:
    """⚠ Номер выбирает сам отправитель, а по опознанной панели жилец ОТКРЫВАЕТ ДВЕРЬ.

    Поэтому снаружи (менеджер, приложение) панель ищут по `peer`, и он обязан
    быть в каждом событии вызова — и в отмене тоже.
    """
    calls = _Calls()
    _run(calls, _start("p1", "panel", "999222", peer="192.168.88.91"), _end("p1"))
    assert calls.events[0] == (
        "call",
        {"caller": "999222", "call": "p1", "peer": "192.168.88.91"},
    )
    assert calls.events[-1] == (
        "cancel",
        {"caller": "999222", "call": "p1", "peer": "192.168.88.91"},
    )


def test_второй_вызов_той_же_панели_сбрасывается() -> None:
    """⚠ Адрес панели по UDP подделывается: у панели один вызов разом."""
    calls = _Calls()
    _run(calls, _start("p1", "panel", "192.168.88.90"), _start("p2", "panel", "192.168.88.90"))
    assert ("DELETE", "/channels/p2", {"reason": "busy"}) in calls.sent
    assert [e for e, _ in calls.events] == ["call"]


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


def test_остановка_моста_отменяет_вызовы_вслух() -> None:
    """⚠ «Вызов» уже ушёл в push: без «отмены» телефоны звонили бы по вызову,
    которого нет (мост остановлен, Asterisk перезапущен)."""
    calls = _Calls()
    _run(calls, _start("p1", "panel", peer="192.168.88.90"), _start("p2", "panel", peer="192.168.88.91"), _start("t1", "answer"))
    asyncio.run(calls.stop())
    assert sorted(kind for kind, _ in calls.events[-2:]) == ["cancel", "ended"]
    assert calls.state()["calls"] == []


def test_отклонение_гасит_только_наше_плечо() -> None:
    """«Отклонить» у жильца — это отказ 486 по НАШЕМУ приглашению.

    ⚠ Вызов у панели целиком не снимаем (`/api/call/hangup` панели тут не при
    чём): групповой вызов — отдельные приглашения каждому адресату, и мониторы
    в квартире обязаны звонить дальше.
    """
    calls = _Calls()
    _run(calls, _start("p1", "panel", peer="192.168.88.90"))
    rejected = asyncio.run(calls.reject("p1"))
    assert rejected is True
    assert ("DELETE", "/channels/p1", {"reason": "busy"}) in calls.sent
    # Событие «отмена» даст `StasisEnd` этого же канала, а не сам отказ:
    # второй источник того же события разошёлся бы с первым.
    assert [kind for kind, _ in calls.events] == ["call"]
    _run(calls, _end("p1"))
    assert calls.events[-1][0] == "cancel"


def test_отклонение_ушедшего_вызова_не_ошибка() -> None:
    calls = _Calls()
    assert asyncio.run(calls.reject("нет такого")) is False
    assert calls.sent == []


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
