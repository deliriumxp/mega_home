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
    def __init__(self, raises: Exception | None = None, log: list | None = None) -> None:
        self.calls: list[tuple] = []
        self.contexts: list = []
        self.raises = raises
        self.log = log if log is not None else []

    async def async_call(self, domain, service, data, blocking=False, return_response=False, context=None):  # noqa: ANN001, ANN201
        self.calls.append((domain, service, data, blocking, return_response))
        self.contexts.append(context)
        self.log.append(("call", context))
        if self.raises:
            raise self.raises
        return {"forecast": []} if return_response else None


class _Bus:
    def __init__(self, log: list) -> None:
        self.fired: list[tuple] = []
        self.log = log

    def async_fire(self, event_type, event_data=None, context=None):  # noqa: ANN001, ANN201
        self.fired.append((event_type, event_data, context))
        self.log.append(("fire", context))


class _Hass:
    def __init__(self, raises: Exception | None = None) -> None:
        # Общий журнал вызовов шины и служб: проверяется их ПОРЯДОК.
        self.log: list[tuple] = []
        self.services = _Services(raises, self.log)
        self.bus = _Bus(self.log)


def test_подпись_команды_событием_до_вызова_тем_же_контекстом() -> None:
    """Журнал HA подписывает смену ПЕРВЫМ событием контекста (`src-core.py`,
    `origin_event`): позже вызова им стал бы сам вызов службы."""
    hass = _Hass()
    by = {"name": "Иван", "via": "удалённое приложение"}
    asyncio.run(HaSource(hass).call("light", "turn_on", {"entity_id": "light.a"}, by=by))
    assert [step for step, _ in hass.log] == ["fire", "call"]
    ((event_type, data, context),) = hass.bus.fired
    assert event_type == "mega_home_command"
    assert context is hass.services.contexts[0]
    # Только ИСТОЧНИК: что переключилось, HA пишет сам. И ни в коем случае
    # не `entity_id` — журнал прибора показал бы событие отдельной строкой.
    assert data == by


def test_без_подписи_событие_не_пишется() -> None:
    hass = _Hass()
    asyncio.run(HaSource(hass).call("light", "turn_on", {"entity_id": "light.a"}))
    assert hass.bus.fired == []
    assert hass.services.contexts[0] is not None


# ⚠ Каждое описанное событие — строка «Активности»: слайдер яркости давал
# строку «Mega Home — …» на каждый шаг. Серия одного человека — одно событие.
def test_серия_команд_одного_человека_одним_контекстом_и_одним_событием() -> None:
    hass = _Hass()
    source = HaSource(hass)
    ivan = {"name": "Иван", "via": "удалённое приложение"}
    ira = {"name": "Ира", "via": "локальное приложение"}
    first = source._context(ivan, now=100.0)
    assert source._context(ivan, now=105.0) is first  # слайдер тянут дальше
    assert source._context(ivan, now=114.0) is first  # пауза меньше SERIES_GAP от ПОСЛЕДНЕЙ
    other = source._context(ira, now=114.5)
    assert other is not first  # другой человек — своя серия
    later = source._context(ivan, now=130.0)
    assert later is not first  # пауза дольше SERIES_GAP — новая серия
    assert [data for _, data, _ in hass.bus.fired] == [ivan, ira, ivan]


def test_команды_без_подписи_серий_не_образуют() -> None:
    source = HaSource(_Hass())
    assert source._context(None, now=1.0) is not source._context(None, now=1.5)


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
