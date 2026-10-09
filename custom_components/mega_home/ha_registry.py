"""Реестры Home Assistant для приложения: области, этажи, метки, устройства, записи сущностей.

Зачем (`docs/superpowers/specs/2026-10-10-vacuum-design.md` менеджера, §8). Состояния и атрибуты
(`ha_source.py`) не несут того, что HA хранит в реестрах: области и этажи (зоны уборки пылесоса —
это области, сопоставленные с его сегментами в `options.vacuum.area_mapping` записи сущности),
метки, точность показаний (`options.sensor.display_precision`), соседей по устройству. Приложению
нужны сырые данные HA; толкует их оно само (`docs/plan-thin-integration.md`: проекции в доме нет).

⚠ Модуль — адаптер HA, ядро о реестрах не знает: оно просит у источника `registry(entity_ids)` и
получает готовый словарь (`core/ops.py`, `registry`). Чистая часть (`snapshot`) не зовёт HA сама —
реестры ей передают, поэтому она проверяется без Home Assistant (`tests/test_registry.py`).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
    floor_registry as fr,
    label_registry as lr,
)


def _plain(value: Any) -> Any:
    """Опции записи (`ReadOnlyDict`) — в обычные словари и списки, пригодные для JSON."""
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_plain(item) for item in value]
    return value


def _enum(value: Any) -> Any:
    """`disabled_by`/`hidden_by`/`entity_category` — перечисления HA; наружу строкой."""
    return getattr(value, "value", value)


def _entity(entry: Any) -> dict[str, Any]:
    return {
        "entityId": entry.entity_id,
        "deviceId": entry.device_id,
        "areaId": entry.area_id,
        "labels": sorted(entry.labels),
        "deviceClass": entry.device_class,
        "originalDeviceClass": entry.original_device_class,
        "entityCategory": _enum(entry.entity_category),
        "disabledBy": _enum(entry.disabled_by),
        "hiddenBy": _enum(entry.hidden_by),
        "unit": entry.unit_of_measurement,
        "name": entry.name,
        "originalName": entry.original_name,
        "options": _plain(entry.options),
    }


def snapshot(
    entity_ids: Iterable[str],
    *,
    entity: Callable[[str], Any],
    entities_of_device: Callable[[str], Iterable[Any]],
    device: Callable[[str], Any],
    areas: Iterable[Any],
    floors: Iterable[Any],
    labels: Iterable[Any],
) -> dict[str, Any]:
    """Данные реестров по сущностям плиток: сами записи, их соседи по устройству, устройства.

    Области, этажи и метки — целиком (их немного, а зоны пылесоса берут любую). Соседи
    (сущности того же устройства, включая отключённые) идут только метаданными, без состояний:
    состояния едут по плиткам (`ops.states`). Сущность без записи в реестре (YAML без
    `unique_id`) пропускается — о ней нечего сказать.
    """
    entries: dict[str, Any] = {}
    device_ids: list[str] = []
    for entity_id in entity_ids:
        entry = entity(entity_id)
        if entry is None:
            continue
        entries[entry.entity_id] = entry
        if entry.device_id and entry.device_id not in device_ids:
            device_ids.append(entry.device_id)
    devices: list[dict[str, Any]] = []
    for device_id in device_ids:
        for neighbour in entities_of_device(device_id):
            entries.setdefault(neighbour.entity_id, neighbour)
        found = device(device_id)
        if found is not None:
            devices.append(
                {
                    "id": found.id,
                    "name": found.name_by_user or found.name,
                    "manufacturer": found.manufacturer,
                    "model": found.model,
                    "areaId": found.area_id,
                }
            )
    return {
        "areas": [
            {"id": area.id, "name": area.name, "icon": area.icon, "floorId": area.floor_id} for area in areas
        ],
        "floors": [
            {"id": floor.floor_id, "name": floor.name, "icon": floor.icon, "level": floor.level}
            for floor in floors
        ],
        "labels": [
            {"id": label.label_id, "name": label.name, "color": label.color, "icon": label.icon}
            for label in labels
        ],
        "devices": devices,
        "entities": [_entity(entry) for entry in entries.values()],
    }


def read(hass: HomeAssistant, entity_ids: Iterable[str]) -> dict[str, Any]:
    """`snapshot` над реестрами этого Home Assistant (в памяти, без ввода-вывода)."""
    entities = er.async_get(hass)
    devices = dr.async_get(hass)
    return snapshot(
        entity_ids,
        entity=entities.async_get,
        entities_of_device=lambda device_id: er.async_entries_for_device(
            entities, device_id, include_disabled_entities=True
        ),
        device=devices.async_get,
        areas=ar.async_get(hass).async_list_areas(),
        floors=fr.async_get(hass).async_list_floors(),
        labels=lr.async_get(hass).async_list_labels(),
    )
