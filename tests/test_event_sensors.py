"""Датчики по событиям устройств (`core/event_sensors.py`, раздел `sensors[]`).

Состояние — по словам описания (тот же разбор, что у записи по движению камеры), доступность — жизнь
источника в концентраторе; источник встал — «нет», а не застывшее «есть».
"""

from __future__ import annotations

import asyncio
from typing import Any

from mega_home.core import event_sensors as es
from mega_home.core.device_events import EventHub

PERSON = {
    "id": "person_vorota",
    "entityId": "binary_sensor.mega_person_vorota",
    "name": "Человек — Ворота",
    "deviceClass": "occupancy",
    "access": "onvif-1",
    "source": "motion",
    "on": ['"person":"start"'],
    "off": ['"person":"stop"'],
}
CAMERA_MOTION = {
    "id": "motion_dvor",
    "name": "Движение — Двор",
    "deviceClass": "motion",
    "access": "onvif-2",
    "source": "events",
    "on": ['Name="IsMotion" Value="true"'],
    "off": ['Name="IsMotion" Value="false"'],
    "split": "NotificationMessage",
    "require": ["/Motion<"],
    "skip": ['PropertyOperation="Initialized"'],
}


def _wired(*specs: dict[str, Any]) -> tuple[EventHub, es.EventSensors, list[str]]:
    hub = EventHub()
    sensors = es.EventSensors(hub.alive)
    hub.observe(sensors.on_event)
    hub.watch_alive(sensors.on_alive)
    changed: list[str] = []
    sensors.listen(changed.append)
    sensors.apply({"sensors": list(specs)})
    return hub, sensors, changed


def test_человек_включает_и_выключает_по_словам_своего_источника() -> None:
    hub, sensors, changed = _wired(PERSON)
    hub.running("onvif-1", "motion", True)
    assert sensors.available("person_vorota") and not sensors.is_on("person_vorota")
    # Движение того же источника и человек чужой камеры датчик не трогают.
    hub.publish("onvif-1", "motion", "motion", {"state": "start", "share": 0.1}, local=True)
    hub.publish("onvif-9", "motion", "person", {"person": "start", "score": 0.9}, local=True)
    assert not sensors.is_on("person_vorota")
    hub.publish("onvif-1", "motion", "person", {"person": "start", "score": 0.97}, local=True)
    assert sensors.is_on("person_vorota")
    hub.publish("onvif-1", "motion", "person", {"person": "stop"}, local=True)
    assert not sensors.is_on("person_vorota")
    assert changed == ["person_vorota"] * 3


def test_событие_без_local_тоже_доходит_до_датчика() -> None:
    """Подписка камеры может быть не `local` (тело не для локального контура), а датчику нужно и оно."""
    hub, sensors, _ = _wired(CAMERA_MOTION)
    hub.running("onvif-2", "events", True)
    xml = (
        '<NotificationMessage><Topic>tns1:RuleEngine/CellMotionDetector/Motion</Topic>'
        '<SimpleItem Name="IsMotion" Value="true"/></NotificationMessage>'
    )
    hub.publish("onvif-2", "events", "response", {"text": xml})
    assert sensors.is_on("motion_dvor")


def test_разбор_сообщений_как_у_записи_вход_в_подписку_и_чужие_темы_не_считаются() -> None:
    hub, sensors, _ = _wired(CAMERA_MOTION)
    hub.running("onvif-2", "events", True)
    initialized = (
        '<NotificationMessage><Topic>…/CellMotionDetector/Motion</Topic><Message PropertyOperation="Initialized">'
        '<SimpleItem Name="IsMotion" Value="true"/></Message></NotificationMessage>'
    )
    dark = '<NotificationMessage><Topic>…/ImageTooDark</Topic><SimpleItem Name="IsMotion" Value="true"/></NotificationMessage>'
    hub.publish("onvif-2", "events", "response", {"text": initialized + dark}, local=True)
    assert not sensors.is_on("motion_dvor")
    # «Началось и кончилось» одним ответом — после события датчик выключен (последняя смена).
    both = (
        '<NotificationMessage><Topic>…/CellMotionDetector/Motion</Topic><SimpleItem Name="IsMotion" Value="true"/>'
        '</NotificationMessage><NotificationMessage><Topic>…/CellMotionDetector/Motion</Topic>'
        '<SimpleItem Name="IsMotion" Value="false"/></NotificationMessage>'
    )
    hub.publish("onvif-2", "events", "response", {"text": both}, local=True)
    assert not sensors.is_on("motion_dvor")


def test_источник_встал_датчик_недоступен_и_сброшен() -> None:
    """Конец окна источник уже не пришлёт: «есть» до остановки иначе висело бы вечно."""
    hub, sensors, changed = _wired(PERSON)
    assert not sensors.available("person_vorota")
    hub.running("onvif-1", "motion", True)
    hub.publish("onvif-1", "motion", "person", {"person": "start"}, local=True)
    changed.clear()
    hub.running("onvif-1", "motion", False)
    assert not sensors.available("person_vorota") and not sensors.is_on("person_vorota")
    assert changed == ["person_vorota"]
    # Повтор того же — без лишних оповещений.
    hub.running("onvif-1", "motion", False)
    assert changed == ["person_vorota"]


def test_выключение_по_тишине_у_источника_без_конца() -> None:
    async def scenario() -> list[bool]:
        hub, sensors, _ = _wired({**PERSON, "off": [], "offAfterSec": 0.05})
        hub.running("onvif-1", "motion", True)
        hub.publish("onvif-1", "motion", "person", {"person": "start"}, local=True)
        seen = [sensors.is_on("person_vorota")]
        await asyncio.sleep(0.03)
        # Новое «началось» отсчитывает заново.
        hub.publish("onvif-1", "motion", "person", {"person": "start"}, local=True)
        await asyncio.sleep(0.03)
        seen.append(sensors.is_on("person_vorota"))
        await asyncio.sleep(0.05)
        seen.append(sensors.is_on("person_vorota"))
        return seen

    assert asyncio.run(scenario()) == [True, True, False]


def test_смена_описания_сбрасывает_состояние_а_прежнее_не_трогает() -> None:
    hub, sensors, _ = _wired(PERSON, CAMERA_MOTION)
    specs: list[int] = []
    sensors.on_specs(lambda: specs.append(len(sensors.specs)))
    hub.running("onvif-1", "motion", True)
    hub.running("onvif-2", "events", True)
    hub.publish("onvif-1", "motion", "person", {"person": "start"}, local=True)
    sensors.apply({"sensors": [PERSON, CAMERA_MOTION]})
    assert specs == [] and sensors.is_on("person_vorota")
    sensors.apply({"sensors": [{**PERSON, "name": "Человек — Калитка"}]})
    assert specs == [1] and not sensors.is_on("person_vorota")
    assert sensors.specs["person_vorota"].name == "Человек — Калитка"


def test_описание_без_слов_или_доступа_не_датчик() -> None:
    assert es.spec_of({"id": "x", "access": "a"}) is None
    assert es.spec_of({"id": "x", "on": ["1"]}) is None
    assert es.spec_of({"id": "x", "access": "a", "on": ["1"], "offAfterSec": 0}).off_after is None
    sensors = es.EventSensors(lambda *_: True)
    sensors.apply({"sensors": "не список"})
    assert sensors.specs == {}


TEMPERATURE = {
    "id": "temp_street",
    "kind": "value",
    "name": "Температура — Улица",
    "deviceClass": "temperature",
    "unit": "°C",
    "stateClass": "measurement",
    "access": "wb",
    "source": "mqtt",
    "event": "message",
    "where": {"topic": "/devices/meteo/controls/temp"},
    "path": "text.value",
}


def test_показание_по_пути_в_json_тексте_и_только_своего_топика() -> None:
    hub, sensors, changed = _wired(TEMPERATURE)
    hub.running("wb", "mqtt", True)
    changed.clear()
    assert sensors.value("temp_street") is None
    hub.publish("wb", "mqtt", "message", {"topic": "/devices/meteo/controls/hum", "text": '{"value": 80}'})
    assert sensors.value("temp_street") is None
    hub.publish("wb", "mqtt", "message", {"topic": "/devices/meteo/controls/temp", "text": '{"value": "-3.5"}'})
    assert sensors.value("temp_street") == -3.5
    # Событие без значения или с нечислом состояние не меняет.
    hub.publish("wb", "mqtt", "message", {"topic": "/devices/meteo/controls/temp", "text": '{"value": "ошибка"}'})
    assert sensors.value("temp_street") == -3.5
    assert changed == ["temp_street"]


def test_показание_выражением_по_xml_с_масштабом_и_текстом() -> None:
    hub, sensors, _ = _wired(
        {"id": "lux", "kind": "value", "access": "cam", "source": "events", "pattern": r'Name="Level" Value="(\d+)"', "scale": 0.1},
        {"id": "mode", "kind": "value", "valueType": "text", "access": "cam", "source": "events", "pattern": r'Mode="(\w+)"'},
    )
    hub.publish("cam", "events", "response", {"text": '<x Name="Level" Value="425"/><y Mode="night"/>'})
    assert sensors.value("lux") == 42.5 and sensors.value("mode") == "night"


def test_показание_устаревает_по_тишине_и_сбрасывается_с_источником() -> None:
    async def scenario() -> list:
        hub, sensors, _ = _wired({**TEMPERATURE, "where": {}, "offAfterSec": 0.03})
        hub.running("wb", "mqtt", True)
        hub.publish("wb", "mqtt", "message", {"text": '{"value": 5}'})
        seen = [sensors.value("temp_street")]
        await asyncio.sleep(0.05)
        seen.append(sensors.value("temp_street"))
        hub.publish("wb", "mqtt", "message", {"text": '{"value": 6}'})
        hub.running("wb", "mqtt", False)
        seen.append(sensors.value("temp_street"))
        return seen

    assert asyncio.run(scenario()) == [5.0, None, None]


def test_показание_без_пути_и_выражения_или_с_битым_выражением_не_датчик() -> None:
    assert es.spec_of({"id": "x", "kind": "value", "access": "a"}) is None
    assert es.spec_of({"id": "x", "kind": "value", "access": "a", "pattern": "("}) is None


def test_уверенность_человека_в_процентах_и_ноль_с_уходом() -> None:
    """Показание «Человек — уверенность»: `score` события «началось» ×100, «кончилось» — `offValue` 0, а не
    последнее значение (ушедший человек с уверенностью 97 % врал бы в реакциях по порогу)."""
    hub, sensors, _ = _wired({
        "id": "mega_person_score_cam", "kind": "value", "access": "onvif-1", "source": "motion", "event": "person",
        "path": "score", "scale": 100, "off": ['"person":"stop"'], "offValue": 0, "unit": "%",
    })
    hub.running("onvif-1", "motion", True)
    hub.publish("onvif-1", "motion", "motion", {"state": "start", "share": 0.1}, local=True)
    assert sensors.value("mega_person_score_cam") is None
    hub.publish("onvif-1", "motion", "person", {"person": "start", "score": 0.973}, local=True)
    assert round(sensors.value("mega_person_score_cam"), 1) == 97.3
    hub.publish("onvif-1", "motion", "person", {"person": "stop"}, local=True)
    assert sensors.value("mega_person_score_cam") == 0
    # Без `offValue` «кончилось» — «неизвестно».
    plain = es.spec_of({"id": "x", "kind": "value", "access": "a", "path": "v", "off": ["stop"]})
    assert plain is not None and plain.off_value is None
