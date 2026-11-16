"""Штора на импульсных реле (`core/pulse_blind.py`) — перенос спек `pulse_cover`.

⚠ Время настоящее (`asyncio.sleep`), тайминги сжаты до долей секунды: предмет —
порядок импульсов и оценка положения, а виртуальные часы подтвердили бы только
выдуманное.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from mega_home.core.pulse_blind import STOP_METHODS, PulseBlind, spec_of


def make(**patch: Any) -> tuple[PulseBlind, list[tuple[float, str, str]], list[int]]:
    calls: list[tuple[float, str, str]] = []
    settled: list[int] = []

    async def call(domain: str, service: str, data: dict[str, Any]) -> None:
        calls.append((time.monotonic(), service, data["entity_id"]))

    spec = spec_of({
        "id": "b1", "name": "Штора", "open": "switch.up", "close": "switch.down",
        "travelUp": 0.6, "travelDown": 0.6, "pulseMove": 50, "pulseStop": 50, "failsafe": 0.2,
        "stopMethod": "pulse_both", **patch,
    })
    assert spec is not None
    blind: PulseBlind

    def on_settled() -> None:
        settled.append(blind.position)

    blind = PulseBlind(spec, call, lambda: None, on_settled)
    return blind, calls, settled


async def done(blind: PulseBlind) -> None:
    while blind.moving:
        await asyncio.sleep(0.02)


def ons(calls: list[tuple[float, str, str]]) -> list[str]:
    return [c[2] for c in calls if c[1] == "turn_on"]


@pytest.mark.parametrize("direction, final, relay", [("open", 100, "switch.up"), ("close", 0, "switch.down")])
@pytest.mark.parametrize("method", STOP_METHODS)
def test_полный_ход_без_стоп_импульса_калибрует(direction: str, final: int, relay: str, method: str) -> None:
    async def scenario() -> Any:
        blind, calls, settled = make(stopMethod=method)
        await getattr(blind, direction)()
        await done(blind)
        return blind, calls, settled

    blind, calls, settled = asyncio.run(scenario())
    assert blind.position == final and blind.calibrated
    assert [(service, entity) for _, service, entity in calls] == [("turn_on", relay), ("turn_off", relay)]
    assert settled == [final], "итог хода публикуется на общий адрес"


@pytest.mark.parametrize("target, relay", [(0, "switch.down"), (100, "switch.up")])
def test_крайняя_позиция_ползунка_тоже_без_стопа(target: int, relay: str) -> None:
    async def scenario() -> Any:
        blind, calls, _ = make()
        blind.position = 50
        await blind.set_position(target)
        await done(blind)
        return blind, calls

    blind, calls = asyncio.run(scenario())
    assert blind.position == target and blind.calibrated
    assert ons(calls) == [relay]


@pytest.mark.parametrize("method", STOP_METHODS)
def test_стоп_без_движения_не_запускает_штору(method: str) -> None:
    async def scenario() -> Any:
        blind, calls, _ = make(stopMethod=method)
        await blind.stop()
        await blind.close()
        await done(blind)
        await blind.stop()
        return calls

    assert ons(asyncio.run(scenario())) == ["switch.down"]


@pytest.mark.parametrize("method, stops", [
    ("pulse_both", ["switch.up", "switch.down"]),
    ("pulse_last", ["switch.up"]),
    ("pulse_opposite", ["switch.down"]),
    ("pulse_up", ["switch.up"]),
    ("pulse_down", ["switch.down"]),
    ("none", []),
])
def test_частичный_ход_останавливается_и_не_калибрует(method: str, stops: list[str]) -> None:
    async def scenario() -> Any:
        blind, calls, _ = make(stopMethod=method)
        blind.position = 0
        await blind.set_position(50)
        await done(blind)
        return blind, calls

    blind, calls = asyncio.run(scenario())
    assert blind.position == 50 and not blind.calibrated
    assert ons(calls) == ["switch.up", *stops]
    if stops:
        assert 0.3 <= calls[2][0] - calls[1][0] < 0.8, "таймер не ждёт следующего обновления позиции"


def test_стоп_посреди_хода_оценивает_положение() -> None:
    async def scenario() -> Any:
        blind, calls, _ = make(travelUp=2.0)
        blind.position = 0
        await blind.open()
        await asyncio.sleep(0.3)
        await blind.stop()
        return blind, calls

    blind, calls = asyncio.run(scenario())
    assert 0 < blind.position < 100
    assert not blind.opening and not blind.closing
    assert ons(calls) == ["switch.up", "switch.up", "switch.down"]


def test_разворот_стоп_раньше_нового_старта() -> None:
    async def scenario() -> list[tuple[float, str, str]]:
        blind, calls, _ = make(travelUp=2.0, travelDown=2.0, stopMethod="pulse_up")
        blind.position = 50
        await blind.open()
        await asyncio.sleep(0.1)
        await blind.close()
        await done(blind)
        return calls

    calls = asyncio.run(scenario())
    assert ons(calls)[:3] == ["switch.up", "switch.up", "switch.down"]
    start = next(i for i, c in enumerate(calls) if c[1] == "turn_on" and c[2] == "switch.down")
    assert calls[start - 1][1:] == ("turn_off", "switch.up"), "стоп-импульс отпущен до старта"


def test_способ_остановки_противоположный() -> None:
    async def scenario() -> list[tuple[float, str, str]]:
        blind, calls, _ = make(stopMethod="pulse_opposite", travelUp=0.3)
        await blind.open()
        await asyncio.sleep(0.15)
        await blind.stop()
        return calls

    assert ons(asyncio.run(scenario())) == ["switch.up", "switch.down"]


def test_чужая_позиция_только_показ_и_не_эхо() -> None:
    blind, calls, settled = make()
    blind.external_position(42.4)
    assert blind.position == 42 and not blind.calibrated
    assert calls == [] and settled == [], "доклад Control4 обратно в шину не уходит"
    assert blind.calibrate(10) and settled == [10]


def test_мусорная_штора_не_роняет_конфиг() -> None:
    assert spec_of({"id": "x"}) is None
    assert spec_of("мусор") is None
    assert spec_of({"id": "x", "open": "a", "close": "b", "stopMethod": "??"}).stop_method == "pulse_both"
