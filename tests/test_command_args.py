"""Аргументы команды по ОПИСАНИЮ из конфига (`fields`), а не по коду дома.

⚠ Смысл схемы (`docs/plan-ha-domains.md` менеджера, этап 0): цвет света,
диапазон климата, громкость и любой следующий аргумент следующего домена
доезжают в дом ДАННЫМИ. Проверяет их дом — службу зовёт он.
"""

from __future__ import annotations

import asyncio

import pytest
from homeassistant.core import State

from fake_host import FakeSource
from mega_home.core import ops
from mega_home.core.ops_base import arguments


class _Coordinator:
    version = "sha256:abc"
    bundle = None

    def __init__(self, commands: dict) -> None:
        self.data = {
            "tiles": [
                {"id": "t1", "domain": "climate", "entityId": "climate.hall", "commands": commands}
            ]
        }


def _run(commands: dict, command: str, value, attributes: dict | None = None):
    source = FakeSource({"climate.hall": State("heat", attributes or {})})
    coordinator = _Coordinator(commands)
    coordinator.source = source
    asyncio.run(ops.run(coordinator, "command", {"id": "t1", "command": command, "value": value}))
    return source.calls


def test_несколько_полей_уходят_одной_службой():
    commands = {
        "set_range": {
            "service": "set_temperature",
            "fields": {
                "target_temp_low": {"type": "number", "min": 5, "max": 35},
                "target_temp_high": {"type": "number", "min": 5, "max": 35},
            },
        }
    }
    calls = _run(commands, "set_range", {"target_temp_low": 19, "target_temp_high": 24})
    assert calls == [
        (
            "climate",
            "set_temperature",
            {"entity_id": "climate.hall", "target_temp_low": 19.0, "target_temp_high": 24.0},
        )
    ]


def test_границы_берутся_из_атрибутов_прибора():
    # entity-climate.md: `min_temp`/`max_temp` — пределы самого термостата.
    commands = {
        "set_temperature": {
            "service": "set_temperature",
            "fields": {"temperature": {"type": "number", "min": 5, "max": 40, "minAttr": "min_temp", "maxAttr": "max_temp"}},
        }
    }
    attributes = {"min_temp": 16, "max_temp": 30}
    assert _run(commands, "set_temperature", 30, attributes)[0][2]["temperature"] == 30.0
    with pytest.raises(ops.OpError) as err:
        _run(commands, "set_temperature", 35, attributes)
    assert "от 16 до 30" in err.value.message
    # Атрибута нет — действует число из правила.
    assert _run(commands, "set_temperature", 35)[0][2]["temperature"] == 35.0


def test_варианты_строки_из_атрибута_прибора():
    commands = {"set_fan_mode": {"service": "set_fan_mode", "fields": {"fan_mode": {"type": "string", "optionsAttr": "fan_modes"}}}}
    attributes = {"fan_modes": ["auto", "low", "high"]}
    assert _run(commands, "set_fan_mode", "low", attributes)[0][2] == {"entity_id": "climate.hall", "fan_mode": "low"}
    with pytest.raises(ops.OpError):
        _run(commands, "set_fan_mode", "turbo", attributes)
    # Прибор списка не дал — не пропускаем ничего, а не всё.
    with pytest.raises(ops.OpError):
        _run(commands, "set_fan_mode", "low")


def test_список_проверяется_по_каждому_месту():
    # core-light-services.yaml: `hs_color` — [оттенок 0–360, насыщенность 0–100].
    spec = {"fields": {"hs_color": {"type": "list", "items": [{"type": "number", "min": 0, "max": 360}, {"type": "number", "min": 0, "max": 100}]}}}
    assert arguments(spec, [200, 50], {}) == {"hs_color": [200.0, 50.0]}
    for bad in ([200], [200, 150], "200,50", [200, "много"]):
        with pytest.raises(ValueError):
            arguments(spec, bad, {})


def test_булево_только_булево():
    spec = {"fields": {"is_volume_muted": {"type": "boolean"}}}
    assert arguments(spec, True, {}) == {"is_volume_muted": True}
    for bad in (1, "true", None):
        with pytest.raises(ValueError):
            arguments(spec, bad, {})
    # И наоборот: «да» не проходит числом (float(True) == 1.0).
    with pytest.raises(ValueError):
        arguments({"fields": {"brightness_pct": {"type": "number", "min": 0, "max": 100}}}, True, {})


def test_чужое_поле_и_пропущенное_обязательное_отвергаются():
    spec = {"fields": {"a": {"type": "string"}, "b": {"type": "string", "optional": True}}}
    assert arguments(spec, {"a": "x"}, {}) == {"a": "x"}
    with pytest.raises(ValueError):
        arguments(spec, {"a": "x", "entity_id": "lock.front"}, {})
    with pytest.raises(ValueError):
        arguments(spec, {"b": "y"}, {})


def test_постоянные_данные_команды_уходят_как_есть():
    spec = {"data": {"is_volume_muted": True}}
    assert arguments(spec, None, {}) == {"is_volume_muted": True}


def test_старое_описание_одним_аргументом_читается():
    # Кэш конфига от менеджера до схемы `fields`: arg/min/max.
    assert arguments({"arg": "position", "min": 0, "max": 100}, 40, {}) == {"position": 40.0}
    assert arguments({"arg": "hvac_mode"}, "heat", {}) == {"hvac_mode": "heat"}
    assert arguments({}, 40, {}) == {}
    with pytest.raises(ValueError):
        arguments({"arg": "position", "min": 0, "max": 100}, 400, {})


def test_ответ_службы_только_по_описанию_команды():
    # weather.get_forecasts: прогноза нет в атрибутах, он есть только в ответе.
    commands = {
        "forecast": {
            "service": "get_forecasts",
            "domain": "weather",
            "response": True,
            "fields": {"type": {"type": "string", "options": ["daily", "hourly"]}},
        },
        "on": {"service": "turn_on"},
    }
    source = FakeSource({"climate.hall": State("heat", {})})
    coordinator = _Coordinator(commands)
    coordinator.source = source
    asked = asyncio.run(ops.run(coordinator, "command", {"id": "t1", "command": "forecast", "value": "daily"}))
    plain = asyncio.run(ops.run(coordinator, "command", {"id": "t1", "command": "on"}))
    assert asked["response"] == {"asked": "weather.get_forecasts"}
    assert "response" not in plain
    assert source.calls[0] == ("weather", "get_forecasts", {"entity_id": "climate.hall", "type": "daily"})


def test_число_без_границ_не_проходит():
    with pytest.raises(ValueError):
        arguments({"fields": {"x": {"type": "number"}}}, 1, {})
