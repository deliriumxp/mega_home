"""Штора на импульсных реле (`core/pulse_blind.py`) — перенос спек `pulse_cover`.

⚠ Время настоящее (`asyncio.sleep`), тайминги сжаты до долей секунды: предмет —
порядок импульсов и оценка положения, а виртуальные часы подтвердили бы только
выдуманное.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from mega_home.core.pulse_blind import PulseBlind, spec_of


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


def test_полный_ход_импульс_и_стоп_калибрует() -> None:
    async def scenario() -> Any:
        blind, calls, settled = make()
        await blind.open()
        await done(blind)
        return blind, calls, settled

    blind, calls, settled = asyncio.run(scenario())
    assert blind.position == 100 and blind.calibrated
    assert ons(calls) == ["switch.up", "switch.up", "switch.down"]
    assert settled == [100], "итог хода публикуется на общий адрес"


def test_частичный_ход_не_калибрует() -> None:
    async def scenario() -> PulseBlind:
        blind, _, _ = make()
        blind.position = 0
        await blind.set_position(50)
        await done(blind)
        return blind

    blind = asyncio.run(scenario())
    assert blind.position == 50 and not blind.calibrated


def test_стоп_посреди_хода_оценивает_положение() -> None:
    async def scenario() -> PulseBlind:
        blind, _, _ = make(travelUp=2.0)
        blind.position = 0
        await blind.open()
        await asyncio.sleep(0.3)
        await blind.stop()
        return blind

    blind = asyncio.run(scenario())
    assert 0 < blind.position < 100
    assert not blind.opening and not blind.closing


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
        await done(blind)
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
