"""The four operations of the resident app, independent of transport.

They are reached two ways — locally over HTTP and remotely over the manager
link — and the point of `ops.py` is that both get the SAME answers and the same
refusal wording. These tests exercise the module directly, which is also the
only place where that promise can be checked once instead of twice.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus

import pytest
from homeassistant.core import State
from homeassistant.exceptions import ServiceNotFound

from mega_home import ops


class _States:
    def __init__(self, states: dict[str, State]) -> None:
        self._states = states

    def get(self, entity_id: str) -> State | None:
        return self._states.get(entity_id)


class _Services:
    def __init__(self, raises: Exception | None = None) -> None:
        self.calls: list[tuple[str, str, dict]] = []
        self._raises = raises

    async def async_call(self, domain, service, data, blocking=False):
        if self._raises:
            raise self._raises
        self.calls.append((domain, service, data))


class _Hass:
    def __init__(self, states=None, raises=None) -> None:
        self.states = _States(states or {})
        self.services = _Services(raises)


class _Bundle:
    version = "1.4.0"


class _Coordinator:
    version = "sha256:abc"
    bundle = _Bundle()

    def __init__(self, data=None) -> None:
        self.data = data if data is not None else _CONFIG


_CONFIG = {
    "version": "sha256:abc",
    "home": {"name": "Дом"},
    "floors": [],
    "rooms": [{"id": "r1", "name": "Кухня", "floorId": "f1"}],
    "tiles": [
        {
            "id": "t1",
            "roomId": "r1",
            "name": "Свет",
            "domain": "light",
            "entityId": "light.kitchen",
            "dimmable": True,
        },
        {
            "id": "t2",
            "roomId": "r1",
            "name": "Розетка",
            "domain": "switch",
            "entityId": None,
            "dimmable": False,
        },
    ],
    "scenarios": [{"id": "s1", "roomId": "r1", "name": "Вечер", "icon": "evening", "entityId": "script.evening"}],
}


def run(hass, coordinator, op, payload=None):
    return asyncio.run(ops.run(hass, coordinator, op, payload))


def test_дом_без_конфига_отвечает_понятным_отказом():
    with pytest.raises(ops.OpError) as err:
        run(_Hass(), None, "config")
    assert err.value.status == HTTPStatus.SERVICE_UNAVAILABLE
    with pytest.raises(ops.OpError):
        run(_Hass(), _Coordinator(data={}), "states")


def test_неизвестная_операция_это_отказ_а_не_падение():
    with pytest.raises(ops.OpError) as err:
        run(_Hass(), _Coordinator(), "выключи-всё")
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_состояния_несут_версии_конфига_и_бандла():
    hass = _Hass({"light.kitchen": State("on", {"brightness": 255})})
    answer = run(hass, _Coordinator(), "states")
    assert answer["configVersion"] == "sha256:abc"
    assert answer["appVersion"] == "1.4.0"
    # Плитка без сущности в Home Assistant остаётся в списке, но недоступна:
    # приложение показывает ВЕСЬ состав объекта, а не только отправленное.
    light, socket = answer["entities"]
    assert light["state"]["power"] is True and light["state"]["brightness"] == 100
    assert "brightness" in light["capabilities"]
    assert socket["available"] is False


def test_команда_превращается_в_вызов_службы():
    hass = _Hass({"light.kitchen": State("off")})
    assert run(hass, _Coordinator(), "command", {"id": "t1", "command": "set_brightness", "value": 40}) == {
        "accepted": True
    }
    assert hass.services.calls == [
        ("light", "turn_on", {"entity_id": "light.kitchen", "brightness_pct": 40.0})
    ]


def test_чужая_команда_и_чужое_устройство_отвергаются():
    hass = _Hass()
    # Команды вне таблицы нет — через нас нельзя позвать произвольную службу.
    with pytest.raises(ops.OpError):
        run(hass, _Coordinator(), "command", {"id": "t1", "command": "delete_everything"})
    with pytest.raises(ops.OpError):
        run(hass, _Coordinator(), "command", {"id": "нет-такого", "command": "turn_on"})
    # Элемент есть в составе, но в Home Assistant не отправлен — управлять нечем.
    with pytest.raises(ops.OpError) as err:
        run(hass, _Coordinator(), "command", {"id": "t2", "command": "turn_on"})
    assert "не отправлен" in err.value.message
    assert hass.services.calls == []


def test_значение_вне_диапазона_не_уходит_в_дом():
    hass = _Hass()
    with pytest.raises(ops.OpError) as err:
        run(hass, _Coordinator(), "command", {"id": "t1", "command": "set_brightness", "value": 900})
    assert "от 0 до 100" in err.value.message
    assert hass.services.calls == []


def test_отсутствующая_служба_это_понятный_отказ_а_не_пятисотка():
    hass = _Hass(raises=ServiceNotFound())
    with pytest.raises(ops.OpError) as err:
        run(hass, _Coordinator(), "command", {"id": "t1", "command": "turn_on"})
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_сценарий_запускает_скрипт():
    hass = _Hass()
    assert run(hass, _Coordinator(), "scenario", {"id": "s1"}) == {"accepted": True}
    assert hass.services.calls == [("script", "turn_on", {"entity_id": "script.evening"})]
    with pytest.raises(ops.OpError):
        run(hass, _Coordinator(), "scenario", {"id": "нет-такого"})
