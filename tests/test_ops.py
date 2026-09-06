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
            # ⚠ Чем командовать плиткой, дом узнаёт ИЗ КОНФИГА (фаза 2 плана):
            # новый управляемый домен больше не стоит релиза HACS.
            "commands": {
                "turn_on": {"domain": "light", "service": "turn_on"},
                "turn_off": {"domain": "light", "service": "turn_off"},
                "set_brightness": {
                    "domain": "light",
                    "service": "turn_on",
                    "arg": "brightness_pct",
                    "min": 0,
                    "max": 100,
                },
            },
        },
        {
            "id": "t2",
            "roomId": "r1",
            "name": "Розетка",
            "domain": "switch",
            "entityId": None,
            "dimmable": False,
            "commands": {"turn_on": {"domain": "switch", "service": "turn_on"}},
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
    # ⚠ Дом больше НЕ толкует состояние: наружу уходит сырое значение и атрибуты
    # Home Assistant, а `power`, яркость и способности считает приложение
    # (docs/plan-thin-integration.md, фаза 1). Вернуть сюда вычисленные поля =
    # снова платить релизом HACS за каждое поле экрана.
    assert light["state"] == {"value": "on"}
    assert light["attributes"] == {"brightness": 255}
    assert "capabilities" not in light
    assert socket["available"] is False


def test_команда_превращается_в_вызов_службы():
    hass = _Hass({"light.kitchen": State("off")})
    answer = run(hass, _Coordinator(), "command", {"id": "t1", "command": "set_brightness", "value": 40})
    assert answer["accepted"] is True
    assert hass.services.calls == [
        ("light", "turn_on", {"entity_id": "light.kitchen", "brightness_pct": 40.0})
    ]
    # ⚠ Ответ несёт НОВОЕ состояние плитки: иначе приложение либо ждёт снимка,
    # либо рисует угаданное — и то и другое неправильно внутри дома.
    assert answer["entity"]["id"] == "t1"
    assert answer["entity"]["state"]["value"] == "off"


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


# Камера. ⚠ Форма состояния — КОНТРАКТ с менеджером (smart-home-view.util.ts):
# приложение одно и то же, а проекций две, в разных репозиториях. Разъедутся —
# и камера покажет кадр на одном транспорте и пустоту на другом.


def _камера(attributes: dict) -> dict:
    return ops.entity_view(
        {"id": "cam1", "domain": "camera", "entityId": "camera.gate", "name": "Калитка"},
        State("idle", attributes),
    )


def test_кадр_и_поток_строятся_по_entity_id_и_подписанному_токену():
    view = _камера({"access_token": "tok en", "frontend_stream_type": "hls"})

    assert view["state"]["picture"] == "/api/camera_proxy/camera.gate?token=tok%20en"
    assert view["state"]["stream"] == "/api/camera_proxy_stream/camera.gate?token=tok%20en"
    assert view["state"]["streamType"] == "hls"
    assert view["available"] is True


def test_без_токена_адресов_не_обещаем():
    # Токен ротируется; адрес без него отдаст 401, а битая картинка на плитке
    # читается как сломанная камера.
    view = _камера({})

    assert view["state"]["picture"] == ""
    assert view["state"]["stream"] == ""


def test_у_камеры_нет_вкл_выкл():
    # Состояние камеры в HA — idle/recording/streaming. Выдуманный `power`
    # сделал бы плитку выключателем, которым нечего выключать.
    assert "power" not in _камера({"access_token": "t"})["state"]


def test_элемент_без_сущности_адресов_не_получает():
    view = ops.entity_view(
        {"id": "cam1", "domain": "camera", "entityId": None, "name": "Калитка"}, None
    )

    assert view["state"]["picture"] == ""
    assert view["available"] is False

# Медиаплеер. ⚠ Проекция ЕГО СОСТОЯНИЯ отсюда убрана вместе со всеми
# остальными: пять состояний плеера, подписи, громкость и кнопки из
# `supported_features` толкует приложение, в одном месте на продукт
# (`ha-entity.spec.ts`). Здесь остаётся проверка, что дом ничего не выдумывает.


def _плеер(state: str, attributes: dict) -> dict:
    return ops.entity_view(
        {
            "id": "tv1",
            "domain": "media_player",
            "entityId": "media_player.tv",
            "name": "Телевизор",
        },
        State(state, attributes),
    )


def test_состояние_плеера_уходит_сырым():
    view = _плеер("paused", {"media_title": "Сюита №3", "supported_features": 1})

    assert view["state"] == {"value": "paused"}
    assert view["attributes"]["media_title"] == "Сюита №3"
    assert "capabilities" not in view


# Атрибуты Home Assistant. ⚠ Решение 2026-09-06: отдаём ЦЕЛИКОМ, а не выборкой —
# приложение живёт только внутри HA, и сокращать уже посчитанное им значит
# платить правкой в двух репозиториях за каждое поле детального экрана. Спека
# держит именно это свойство и единственное исключение из него.

def test_атрибуты_уходят_целиком():
    view = ops.entity_view(
        {"id": "l1", "domain": "light", "entityId": "light.hall", "name": "Холл"},
        State(
            "on",
            {
                "friendly_name": "Холл",
                "supported_color_modes": ["color_temp", "hs"],
                "effect_list": ["Радуга", "Свеча"],
                "какой_то_свой_атрибут": 7,
            },
        ),
    )

    assert view["attributes"] == {
        "friendly_name": "Холл",
        "supported_color_modes": ["color_temp", "hs"],
        "effect_list": ["Радуга", "Свеча"],
        "какой_то_свой_атрибут": 7,
    }

def test_токен_доступа_наружу_не_уходит():
    # Не «фильтр полезного», а секрет: из него уже собраны адреса кадра и
    # потока, и отдать его отдельным полем значит отдать право собрать любой
    # другой адрес того же Home Assistant.
    view = _камера({"access_token": "секрет", "friendly_name": "Калитка"})

    assert "access_token" not in view["attributes"]
    assert view["attributes"]["friendly_name"] == "Калитка"
    assert "секрет" in view["state"]["picture"] or "%D1%81" in view["state"]["picture"]

def test_без_состояния_атрибуты_пустые():
    view = ops.entity_view(
        {"id": "l1", "domain": "light", "entityId": "light.hall", "name": "Холл"}, None
    )

    assert view["attributes"] == {}


# Карта команд (docs/plan-thin-integration.md, фаза 2). ⚠ Смысл всей затеи:
# новый управляемый домен приезжает в дом ДАННЫМИ, обычной синхронизацией
# конфига, а не релизом HACS с перезапуском Home Assistant на каждом объекте.


def test_служба_берётся_из_конфига_плитки():
    hass = _Hass()
    config = {
        **_CONFIG,
        "tiles": [
            {
                "id": "t9",
                "roomId": "r1",
                "name": "Вытяжка",
                # Домена `fan` эта интеграция не знает и знать не должна.
                "domain": "fan",
                "entityId": "fan.hood",
                "dimmable": False,
                "commands": {
                    "turn_on": {"domain": "fan", "service": "turn_on"},
                    "set_speed": {
                        "domain": "fan",
                        "service": "set_percentage",
                        "arg": "percentage",
                        "min": 0,
                        "max": 100,
                    },
                },
            }
        ],
    }

    run(hass, _Coordinator(data=config), "command", {"id": "t9", "command": "turn_on"})
    run(
        hass,
        _Coordinator(data=config),
        "command",
        {"id": "t9", "command": "set_speed", "value": 30},
    )

    assert hass.services.calls == [
        ("fan", "turn_on", {"entity_id": "fan.hood"}),
        ("fan", "set_percentage", {"entity_id": "fan.hood", "percentage": 30.0}),
    ]


def test_границы_из_конфига_проверяет_дом():
    # ⚠ Границы приходят данными, но проверяет их ЭТА сторона: службу зовём мы,
    # а браузеру жильца верить нельзя.
    hass = _Hass()
    with pytest.raises(ops.OpError) as err:
        run(
            hass,
            _Coordinator(),
            "command",
            {"id": "t1", "command": "set_brightness", "value": 900},
        )
    assert "от 0 до 100" in err.value.message
    assert hass.services.calls == []


def test_дом_со_старым_конфигом_управляется_по_прежней_таблице():
    # ⚠ Кэш конфига старше кода ровно до первой синхронизации. Без фолбэка
    # объект после обновления интеграции остался бы без управления до неё.
    hass = _Hass()
    старый = {
        **_CONFIG,
        "tiles": [
            {
                "id": "t1",
                "roomId": "r1",
                "name": "Свет",
                "domain": "light",
                "entityId": "light.kitchen",
                "dimmable": True,
            }
        ],
    }

    run(
        hass,
        _Coordinator(data=старый),
        "command",
        {"id": "t1", "command": "set_brightness", "value": 40},
    )

    assert hass.services.calls == [
        ("light", "turn_on", {"entity_id": "light.kitchen", "brightness_pct": 40.0})
    ]
