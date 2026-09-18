"""Обновление по кнопке менеджера (`ha_update.py`): HACS ставит, HA перезапускается."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from homeassistant.core import State
from homeassistant.exceptions import HomeAssistantError

from fake_host import FakeSource
from mega_home import ha_update
from mega_home.core import ops
from mega_home.core.ops_base import OpError

RELEASE = "https://github.com/deliriumxp/mega_home/releases/v0.2.73"


class _Entity(State):
    def __init__(self, entity_id: str, attributes: dict[str, Any]) -> None:
        super().__init__("on", attributes)
        self.entity_id = entity_id


class _States:
    def __init__(self, entities: list[_Entity]) -> None:
        self.entities = entities

    def async_all(self, domain: str) -> list[_Entity]:
        return [e for e in self.entities if e.entity_id.startswith(domain + ".")]

    def get(self, entity_id: str) -> _Entity | None:
        return next((e for e in self.entities if e.entity_id == entity_id), None)


class _Hass:
    def __init__(self, entities: list[_Entity], fail: Exception | None = None) -> None:
        self.states = _States(entities)
        self.calls: list[tuple[str, str]] = []
        self.tasks: list[str] = []
        self.fail = fail

    class _Services:
        def __init__(self, hass: _Hass) -> None:
            self.hass = hass

        async def async_call(self, domain, service, data, blocking=False):  # noqa: ANN001, ANN201
            self.hass.calls.append((domain, service))
            if (domain, service) == ("update", "install") and self.hass.fail:
                raise self.hass.fail

    @property
    def services(self) -> _Hass._Services:
        return _Hass._Services(self)

    def async_create_background_task(self, coro, name):  # noqa: ANN001, ANN201
        self.tasks.append(name)
        coro.close()


def _ours(installed: str, latest: str) -> _Entity:
    return _Entity(
        "update.mega_home_update",
        {"installed_version": installed, "latest_version": latest, "release_url": RELEASE},
    )


def _other() -> _Entity:
    return _Entity(
        "update.hacs_update",
        {"installed_version": "2.0", "latest_version": "2.1",
         "release_url": "https://github.com/hacs/integration/releases/2.1"},
    )


def test_ставит_свежий_релиз_и_перезапускает_после_ответа() -> None:
    hass = _Hass([_other(), _ours("0.2.72", "0.2.73")])
    answer = asyncio.run(ha_update.async_self_update(hass))

    assert ("update", "install") in hass.calls
    # ⚠ Перезапуск — отложенной задачей, а не прямо здесь: ответ менеджеру
    # обязан уйти раньше, чем HA закроет канал.
    assert ("homeassistant", "restart") not in hass.calls
    assert hass.tasks == ["mega_home self-update restart"]
    assert answer["installing"] is True and answer["latest"] == "0.2.73"


def test_нечего_ставить_но_перезапуск_всё_равно() -> None:
    """Файлы уже на диске, в памяти старый код — лечится ровно перезапуском."""
    hass = _Hass([_ours("0.2.73", "0.2.73")])
    answer = asyncio.run(ha_update.async_self_update(hass))
    assert ("update", "install") not in hass.calls
    assert hass.tasks and answer["restarting"] is True


def test_без_hacs_отказ_понятный_и_без_перезапуска() -> None:
    hass = _Hass([_other()])
    with pytest.raises(OpError, match="HACS"):
        asyncio.run(ha_update.async_self_update(hass))
    assert hass.tasks == []


def test_отказ_установки_отменяет_перезапуск() -> None:
    """Перезапустить дом со старым кодом — минута без дома ради ничего."""
    hass = _Hass([_ours("0.2.72", "0.2.73")], fail=HomeAssistantError("нет сети"))
    with pytest.raises(OpError, match="не поставил"):
        asyncio.run(ha_update.async_self_update(hass))
    assert hass.tasks == []


def test_ядро_зовёт_то_что_дал_адаптер() -> None:
    class _Coordinator:
        data = {"tiles": []}
        source = FakeSource()

        async def self_update(self) -> dict:
            return {"restarting": True}

    assert asyncio.run(ops.run(_Coordinator(), "self-update", None)) == {"restarting": True}
    _Coordinator.self_update = None  # type: ignore[assignment]
    with pytest.raises(OpError, match="не умеет"):
        asyncio.run(ops.run(_Coordinator(), "self-update", None))
