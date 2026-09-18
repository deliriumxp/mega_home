"""Источник состояний над Home Assistant (`ha_source.py`): отказы и подписка."""

from __future__ import annotations

import asyncio

import pytest
import voluptuous as vol
from homeassistant.exceptions import ServiceNotFound
from homeassistant.helpers import event as event_helper

from mega_home.ha_source import HaSource
from mega_home.core.source import CommandRejected, CommandUnknown


class _Services:
    def __init__(self, raises: Exception | None = None) -> None:
        self.calls: list[tuple] = []
        self.raises = raises

    async def async_call(self, domain, service, data, blocking=False):  # noqa: ANN001, ANN201
        self.calls.append((domain, service, data, blocking))
        if self.raises:
            raise self.raises


class _Hass:
    def __init__(self, raises: Exception | None = None) -> None:
        self.services = _Services(raises)


def test_команда_ждёт_выполнения() -> None:
    """Ответ жильцу несёт состояние ПОСЛЕ команды — служба зовётся блокирующе."""
    hass = _Hass()
    asyncio.run(HaSource(hass).call("light", "turn_on", {"entity_id": "light.a"}))
    assert hass.services.calls == [("light", "turn_on", {"entity_id": "light.a"}, True)]


@pytest.mark.parametrize(
    ("raised", "expected"),
    [(ServiceNotFound(), CommandUnknown), (vol.Invalid("bad"), CommandRejected)],
)
def test_отказы_ha_становятся_отказами_источника(raised, expected) -> None:  # noqa: ANN001
    with pytest.raises(expected):
        asyncio.run(HaSource(_Hass(raised)).call("climate", "set_temperature", {}))


def test_подписка_отдаёт_сущность_и_новое_состояние() -> None:
    event_helper.async_track_state_change_event.calls.clear()
    seen: list[tuple] = []
    HaSource(_Hass()).subscribe(["light.a"], lambda entity_id, state: seen.append((entity_id, state)))
    entity_ids, action = event_helper.async_track_state_change_event.calls[-1]
    assert entity_ids == ["light.a"]

    class _Event:
        data = {"entity_id": "light.a", "new_state": "S"}

    action(_Event())
    assert seen == [("light.a", "S")]
