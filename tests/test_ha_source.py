"""Источник состояний над Home Assistant (`ha_source.py`): отказы и подписка."""

from __future__ import annotations

import asyncio

import pytest
import voluptuous as vol
from homeassistant.exceptions import HomeAssistantError, ServiceNotFound
from homeassistant.helpers import event as event_helper

from mega_home.ha_source import HaSource
from mega_home.core.source import CommandRejected, CommandUnknown


class _Services:
    def __init__(self, raises: Exception | None = None) -> None:
        self.calls: list[tuple] = []
        self.raises = raises

    async def async_call(self, domain, service, data, blocking=False, return_response=False):  # noqa: ANN001, ANN201
        self.calls.append((domain, service, data, blocking, return_response))
        if self.raises:
            raise self.raises
        return {"forecast": []} if return_response else None


class _Hass:
    def __init__(self, raises: Exception | None = None) -> None:
        self.services = _Services(raises)


def test_команда_ждёт_выполнения() -> None:
    """Ответ жильцу несёт состояние ПОСЛЕ команды — служба зовётся блокирующе."""
    hass = _Hass()
    assert asyncio.run(HaSource(hass).call("light", "turn_on", {"entity_id": "light.a"})) is None
    assert hass.services.calls == [("light", "turn_on", {"entity_id": "light.a"}, True, False)]


def test_ответ_службы_только_по_просьбе() -> None:
    # dev-api-websocket.md: `return_response` — только для служб с ответом.
    hass = _Hass()
    answer = asyncio.run(HaSource(hass).call("weather", "get_forecasts", {"type": "daily"}, True))
    assert answer == {"forecast": []}
    assert hass.services.calls[0][-1] is True


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (ServiceNotFound(), CommandUnknown),
        (vol.Invalid("bad"), CommandRejected),
        # Отказ самого прибора (режим не из списка) — не пятисотка.
        (HomeAssistantError("bad mode"), CommandRejected),
    ],
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


def test_камера_без_потока_не_спрашивается_на_каждый_опрос(monkeypatch) -> None:
    """⚠ Ответ «потока нет» не запоминался: опрос состояний раз в 3 с спрашивал
    HA о потоке такой камеры каждые 3 с. Теперь — раз в `SOURCE_RETRY`."""
    import sys
    import types

    from mega_home import ha_source

    asked: list[str] = []

    async def async_get_stream_source(_hass, entity_id):  # noqa: ANN001, ANN202
        asked.append(entity_id)
        return None

    async def async_get_image(_hass, entity_id, width=None):  # noqa: ANN001, ANN202
        return types.SimpleNamespace(content=b"jpeg", content_type="image/jpeg")

    monkeypatch.setitem(
        sys.modules,
        "homeassistant.components.camera",
        types.SimpleNamespace(async_get_stream_source=async_get_stream_source, async_get_image=async_get_image),
    )
    clock = [1000.0]
    monkeypatch.setattr(ha_source, "monotonic", lambda: clock[0])

    async def scenario() -> None:
        tasks: list[asyncio.Task] = []
        hass = types.SimpleNamespace(async_create_task=lambda coro: tasks.append(asyncio.ensure_future(coro)))
        cameras = ha_source.HaCameras(hass)
        for _ in range(3):
            cameras.warm("camera.gate")
            await asyncio.gather(*tasks)
        assert asked == ["camera.gate"]
        clock[0] += ha_source.SOURCE_RETRY + 1
        cameras.warm("camera.gate")
        await asyncio.gather(*tasks)
        assert asked == ["camera.gate", "camera.gate"]

    asyncio.run(scenario())
