"""Реестры Home Assistant для приложения: области, этажи, метки, устройства, записи сущностей.

Зачем (`docs/superpowers/specs/2026-10-10-vacuum-design.md` менеджера, §8): зоны уборки пылесоса —
это области HA, сопоставленные с сегментами (`options.vacuum.area_mapping` в записи сущности), а
названия и этажи областей лежат в реестрах. Состояния этого не несут. Читает адаптер HA
(`ha_registry.py`), ядро получает готовый словарь.
"""

from __future__ import annotations

import asyncio
import json
from enum import Enum
from types import SimpleNamespace as NS

from fake_host import FakeHost, FakeSource
from mega_home.core import ops
from mega_home.core.relay_api import handle
from mega_home.ha_registry import snapshot


class _Disabler(Enum):
    USER = "user"


class _Category(Enum):
    DIAGNOSTIC = "diagnostic"


def _entry(entity_id: str, **over):
    base = dict(
        entity_id=entity_id,
        device_id=None,
        area_id=None,
        labels=set(),
        device_class=None,
        original_device_class=None,
        entity_category=None,
        disabled_by=None,
        hidden_by=None,
        unit_of_measurement=None,
        name=None,
        original_name=None,
        options={},
    )
    base.update(over)
    return NS(**base)


FLOORS = [NS(floor_id="f1", name="Первый этаж", icon="mdi:home-floor-1", level=1)]
AREAS = [
    NS(id="kitchen", name="Кухня", icon="mdi:silverware", floor_id="f1"),
    NS(id="yard", name="Двор", icon=None, floor_id=None),
]
LABELS = [NS(label_id="mm", name="Mega Manager", color="blue", icon=None)]
DEVICES = {"d1": NS(id="d1", name="Робот", name_by_user=None, manufacturer="Roborock", model="S7", area_id="kitchen")}
ENTITIES = {
    "vacuum.robot": _entry(
        "vacuum.robot",
        device_id="d1",
        original_name="Робот",
        options={"vacuum": {"area_mapping": {"kitchen": ["16"], "yard": ["17", "18"]}}},
    ),
    "sensor.robot_battery": _entry(
        "sensor.robot_battery",
        device_id="d1",
        device_class="battery",
        original_device_class="battery",
        unit_of_measurement="%",
        entity_category=_Category.DIAGNOSTIC,
        labels={"mm"},
    ),
    "sensor.robot_off": _entry("sensor.robot_off", device_id="d1", disabled_by=_Disabler.USER),
    "sensor.other": _entry("sensor.other", options={"sensor": {"display_precision": 1}}),
}


def _read(entity_ids):
    return snapshot(
        entity_ids,
        entity=lambda eid: ENTITIES.get(eid),
        entities_of_device=lambda did: [e for e in ENTITIES.values() if e.device_id == did],
        device=lambda did: DEVICES.get(did),
        areas=AREAS,
        floors=FLOORS,
        labels=LABELS,
    )


def test_этажи_области_и_метки_целиком() -> None:
    data = _read(["vacuum.robot"])
    assert data["floors"] == [{"id": "f1", "name": "Первый этаж", "icon": "mdi:home-floor-1", "level": 1}]
    assert data["areas"] == [
        {"id": "kitchen", "name": "Кухня", "icon": "mdi:silverware", "floorId": "f1"},
        {"id": "yard", "name": "Двор", "icon": None, "floorId": None},
    ]
    assert data["labels"] == [{"id": "mm", "name": "Mega Manager", "color": "blue", "icon": None}]


def test_запись_пылесоса_несёт_сопоставление_областей() -> None:
    data = _read(["vacuum.robot"])
    robot = next(e for e in data["entities"] if e["entityId"] == "vacuum.robot")
    assert robot["options"]["vacuum"]["area_mapping"] == {"kitchen": ["16"], "yard": ["17", "18"]}
    assert robot["deviceId"] == "d1"


def test_соседи_по_устройству_идут_с_метаданными_включая_отключённых() -> None:
    data = _read(["vacuum.robot"])
    ids = sorted(e["entityId"] for e in data["entities"])
    assert ids == ["sensor.robot_battery", "sensor.robot_off", "vacuum.robot"]
    battery = next(e for e in data["entities"] if e["entityId"] == "sensor.robot_battery")
    assert battery["deviceClass"] == "battery"
    assert battery["unit"] == "%"
    assert battery["entityCategory"] == "diagnostic"
    assert battery["labels"] == ["mm"]
    off = next(e for e in data["entities"] if e["entityId"] == "sensor.robot_off")
    assert off["disabledBy"] == "user"
    assert data["devices"] == [
        {"id": "d1", "name": "Робот", "manufacturer": "Roborock", "model": "S7", "areaId": "kitchen"}
    ]


def test_сущность_без_устройства_и_без_записи_не_падает() -> None:
    data = _read(["sensor.other", "light.yaml_only"])
    assert [e["entityId"] for e in data["entities"]] == ["sensor.other"]
    assert data["entities"][0]["options"] == {"sensor": {"display_precision": 1}}
    assert data["devices"] == []


def test_ответ_сериализуется_в_json() -> None:
    json.dumps(_read(["vacuum.robot", "sensor.other"]))


class _Coordinator:
    env = FakeHost()
    version = "sha256:abc"
    bundle = None
    accesses = None

    def __init__(self, source) -> None:
        self.source = source
        self.data = {
            "version": "sha256:abc",
            "tiles": [
                {"id": "t1", "roomId": "r1", "name": "Робот", "domain": "vacuum", "entityId": "vacuum.robot"},
                {"id": "t2", "roomId": "r1", "name": "Розетка", "domain": "switch", "entityId": None},
            ],
            "rooms": [],
            "floors": [],
            "scenarios": [],
        }


class _RegistrySource(FakeSource):
    def __init__(self) -> None:
        super().__init__()
        self.asked: list[list[str]] = []

    def registry(self, entity_ids):
        self.asked.append(list(entity_ids))
        return _read(entity_ids)


def test_ответ_спрашивает_источник_про_сущности_плиток() -> None:
    source = _RegistrySource()
    answer = ops.registry(_Coordinator(source))
    assert source.asked == [["vacuum.robot"]]
    assert answer["areas"][0]["id"] == "kitchen"


def test_источник_без_реестров_отвечает_пустым_а_не_падает() -> None:
    answer = ops.registry(_Coordinator(FakeSource()))
    assert answer == {"areas": [], "floors": [], "labels": [], "devices": [], "entities": []}


def test_путь_api_registry_работает_и_для_двери_и_для_переноса() -> None:
    import base64

    reply = asyncio.run(handle(_Coordinator(_RegistrySource()), {"method": "GET", "path": "api/registry"}))
    body = json.loads(base64.b64decode(reply["body"]).decode("utf-8"))
    assert body["floors"][0]["id"] == "f1"
