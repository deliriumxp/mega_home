"""Операции жильца, независимые от транспорта.

Обслуживают запрос и локальной дверью, и переносом через менеджер — точка
входа в обоих случаях одна (`ops.run`), и это единственное место, где обещание
«тот же ответ, где бы жилец ни стоял» проверяется один раз, а не дважды.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus

import pytest
from homeassistant.core import State

from fake_host import FakeHost, FakeSource
from mega_home.core import ops
from mega_home.core.source import CommandUnknown


class _Bundle:
    version = "1.4.0"


class _Coordinator:
    env = FakeHost()
    version = "sha256:abc"
    bundle = _Bundle()
    accesses = None

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
            # ⚠ Чем командовать плиткой, дом узнаёт ИЗ КОНФИГА: новый
            # управляемый домен больше не стоит релиза HACS.
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


def run(source, coordinator, op, payload=None):
    if coordinator is not None:
        coordinator.source = source
    return asyncio.run(ops.run(coordinator, op, payload))


def test_дом_без_конфига_отвечает_понятным_отказом():
    with pytest.raises(ops.OpError) as err:
        run(FakeSource(), None, "config")
    assert err.value.status == HTTPStatus.SERVICE_UNAVAILABLE
    with pytest.raises(ops.OpError):
        run(FakeSource(), _Coordinator(data={}), "states")


def test_неизвестная_операция_это_отказ_а_не_падение():
    with pytest.raises(ops.OpError) as err:
        run(FakeSource(), _Coordinator(), "выключи-всё")
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_состояния_несут_версии_конфига_и_бандла():
    source = FakeSource({"light.kitchen": State("on", {"brightness": 255})})
    answer = run(source, _Coordinator(), "states")
    assert answer["configVersion"] == "sha256:abc"
    assert answer["appVersion"] == "1.4.0"
    # Плитка без сущности в Home Assistant остаётся в списке, но недоступна:
    # приложение показывает ВЕСЬ состав объекта, а не только отправленное.
    light, socket = answer["entities"]
    # ⚠ Дом больше НЕ толкует состояние: наружу уходит сырое значение и атрибуты
    # Home Assistant, а `power`, яркость и способности считает приложение.
    assert light["state"] == {"value": "on"}
    assert light["attributes"] == {"brightness": 255}
    assert "capabilities" not in light
    assert socket["available"] is False


def test_команда_превращается_в_вызов_службы():
    source = FakeSource({"light.kitchen": State("off")})
    answer = run(source, _Coordinator(), "command", {"id": "t1", "command": "set_brightness", "value": 40})
    assert answer["accepted"] is True
    assert source.calls == [
        ("light", "turn_on", {"entity_id": "light.kitchen", "brightness_pct": 40.0})
    ]
    # ⚠ Ответ несёт НОВОЕ состояние плитки: иначе приложение либо ждёт снимка,
    # либо рисует угаданное — и то и другое неправильно внутри дома.
    assert answer["entity"]["id"] == "t1"
    assert answer["entity"]["state"]["value"] == "off"


def test_чужая_команда_и_чужое_устройство_отвергаются():
    source = FakeSource()
    with pytest.raises(ops.OpError):
        run(source, _Coordinator(), "command", {"id": "t1", "command": "delete_everything"})
    with pytest.raises(ops.OpError):
        run(source, _Coordinator(), "command", {"id": "нет-такого", "command": "turn_on"})
    with pytest.raises(ops.OpError) as err:
        run(source, _Coordinator(), "command", {"id": "t2", "command": "turn_on"})
    assert "не отправлен" in err.value.message
    assert source.calls == []


def test_значение_вне_диапазона_не_уходит_в_дом():
    source = FakeSource()
    with pytest.raises(ops.OpError) as err:
        run(source, _Coordinator(), "command", {"id": "t1", "command": "set_brightness", "value": 900})
    assert "от 0 до 100" in err.value.message
    assert source.calls == []


def test_отсутствующая_служба_это_понятный_отказ_а_не_пятисотка():
    source = FakeSource(raises=CommandUnknown())
    with pytest.raises(ops.OpError) as err:
        run(source, _Coordinator(), "command", {"id": "t1", "command": "turn_on"})
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_сценарий_запускает_скрипт():
    source = FakeSource()
    assert run(source, _Coordinator(), "scenario", {"id": "s1"}) == {"accepted": True}
    assert source.calls == [("script", "turn_on", {"entity_id": "script.evening"})]
    with pytest.raises(ops.OpError):
        run(source, _Coordinator(), "scenario", {"id": "нет-такого"})


# Камера. ⚠ Форма состояния — КОНТРАКТ с менеджером (smart-home-view.util.ts):
# приложение одно и то же, а проекций две, в разных репозиториях.


def _camera(attributes: dict) -> dict:
    return ops.entity_view(
        {"id": "cam1", "domain": "camera", "entityId": "camera.gate", "name": "Калитка"},
        State("idle", attributes),
    )


def test_кадр_и_поток_строятся_по_entity_id_и_подписанному_токену():
    view = _camera({"access_token": "tok en", "frontend_stream_type": "hls"})

    assert view["state"]["picture"] == "/api/camera_proxy/camera.gate?token=tok%20en"
    assert view["state"]["stream"] == "/api/camera_proxy_stream/camera.gate?token=tok%20en"
    assert view["state"]["streamType"] == "hls"
    assert view["available"] is True


def test_без_токена_адресов_не_обещаем():
    view = _camera({})

    assert view["state"]["picture"] == ""
    assert view["state"]["stream"] == ""


def test_у_камеры_нет_вкл_выкл():
    assert "power" not in _camera({"access_token": "t"})["state"]


def test_элемент_без_сущности_адресов_не_получает():
    view = ops.entity_view(
        {"id": "cam1", "domain": "camera", "entityId": None, "name": "Калитка"}, None
    )

    assert view["state"]["picture"] == ""
    assert view["available"] is False


# Медиаплеер. ⚠ Проекция ЕГО СОСТОЯНИЯ отсюда убрана: толкует приложение.


def _player(state: str, attributes: dict) -> dict:
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
    view = _player("paused", {"media_title": "Сюита №3", "supported_features": 1})

    assert view["state"] == {"value": "paused"}
    assert view["attributes"]["media_title"] == "Сюита №3"
    assert "capabilities" not in view


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
    view = _camera({"access_token": "секрет", "friendly_name": "Калитка"})

    assert "access_token" not in view["attributes"]
    assert view["attributes"]["friendly_name"] == "Калитка"
    assert "секрет" in view["state"]["picture"] or "%D1%81" in view["state"]["picture"]


def test_дом_не_подмешивает_состав_в_ответ_о_приборе():
    view = ops.entity_view(
        {
            "id": "l1",
            "roomId": "r1",
            "name": "Холл",
            "domain": "light",
            "entityId": "light.hall",
            "dimmable": True,
        },
        State("on", {"friendly_name": "Холл"}),
    )

    assert set(view) == {
        "id",
        "domain",
        "state",
        "attributes",
        "available",
        "updatedAt",
    }


def test_без_состояния_атрибуты_пустые():
    view = ops.entity_view(
        {"id": "l1", "domain": "light", "entityId": "light.hall", "name": "Холл"}, None
    )

    assert view["attributes"] == {}


# Карта команд. ⚠ Смысл всей затеи: новый управляемый домен приезжает в дом
# ДАННЫМИ, обычной синхронизацией конфига, а не релизом HACS.


def test_служба_берётся_из_конфига_плитки():
    source = FakeSource()
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

    run(source, _Coordinator(data=config), "command", {"id": "t9", "command": "turn_on"})
    run(
        source,
        _Coordinator(data=config),
        "command",
        {"id": "t9", "command": "set_speed", "value": 30},
    )

    assert source.calls == [
        ("fan", "turn_on", {"entity_id": "fan.hood"}),
        ("fan", "set_percentage", {"entity_id": "fan.hood", "percentage": 30.0}),
    ]


def test_границы_из_конфига_проверяет_дом():
    source = FakeSource()
    with pytest.raises(ops.OpError) as err:
        run(
            source,
            _Coordinator(),
            "command",
            {"id": "t1", "command": "set_brightness", "value": 900},
        )
    assert "от 0 до 100" in err.value.message
    assert source.calls == []


def test_плитка_без_карты_команд_не_исполняется_втихую():
    source = FakeSource()
    no_commands = {
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

    with pytest.raises(ops.OpError) as err:
        run(
            source,
            _Coordinator(data=no_commands),
            "command",
            {"id": "t1", "command": "set_brightness", "value": 40},
        )

    assert err.value.status == HTTPStatus.NOT_FOUND
    assert source.calls == []


def test_дом_говорит_о_себе_в_общем_канале_а_не_своим_маршрутом() -> None:
    """⚠ Паспорт дома: версия и СПИСОК ПОДНЯТЫХ путей.

    ⚠ И это ОБЩИЙ КАНАЛ, а не новый маршрут: правило требует сперва обойтись
    тем, за чем приложение и так приходит первым запросом.
    """
    from mega_home.core.const import INTEGRATION_VERSION
    from mega_home.http import VIEWS

    coordinator = _Coordinator()
    coordinator.routes = sorted(view.url for view in VIEWS)
    answer = ops.config(coordinator)
    passport = answer["integration"]

    assert passport["version"] == INTEGRATION_VERSION
    assert answer["rooms"] == _CONFIG["rooms"]
    assert len(passport["routes"]) == len(VIEWS)
    assert "/mega-home/api/connect" in passport["routes"]
    assert passport["routes"] == sorted(passport["routes"]), "список нестабилен между ответами"
    assert "go2rtc" in passport
    assert "running" in passport["go2rtc"] and "why" in passport["go2rtc"]
    # ⚠ И ЧТО дом умеет достать снаружи: устройства объекта, а не версия.
    # У голого координатора реестра нет вовсе — пустой список.
    assert passport["devices"] == []


def test_реестр_устройств_в_паспорте() -> None:
    from mega_home.core.devices import DeviceRegistry

    coordinator = _Coordinator()
    coordinator.routes = []
    registry = DeviceRegistry()
    registry.apply([{"id": "hub", "host": "192.168.1.5"}])
    coordinator.accesses = registry

    passport = ops.config(coordinator)["integration"]
    assert passport["devices"] == [{"id": "hub"}]
