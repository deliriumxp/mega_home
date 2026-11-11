"""Сущности HA датчиков по событиям (`core/event_sensors.py`): общая часть платформ `binary_sensor` и `sensor`.

⚠ `entity_id` задаёт менеджер (как у штор, `cover.py`): на нём уже висят привязка элемента состава и реакции.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity import Entity
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .core.const import DOMAIN
from .core.event_sensors import EventSensors, SensorSpec

PREFIX = f"{DOMAIN}_event_"


def setup_event_entities(
    hass: HomeAssistant,
    entry: Any,
    async_add_entities: AddConfigEntryEntitiesCallback,
    domain: str,
    kind: str,
    make: Callable[[EventSensors, SensorSpec], Entity],
) -> None:
    """Заводит и снимает сущности вида `kind` по каждому новому составу датчиков."""
    sensors: EventSensors | None = getattr(entry.runtime_data, "event_sensors", None)
    if sensors is None:
        return
    entities: dict[str, Any] = {}
    lock = asyncio.Lock()

    def wanted() -> dict[str, SensorSpec]:
        return {s.id: s for s in sensors.specs.values() if s.kind == kind}

    async def sync() -> None:
        # ⚠ Под замком и С ОЖИДАНИЕМ снятия, как у штор (`cover.py`): сущность с тем же `unique_id`, добавленную,
        # пока прежняя ещё снимается, HA отбросил бы как дубль.
        async with lock:
            want = wanted()
            registry = er.async_get(hass)
            for sensor_id in [s for s in entities if s not in want or entities[s].spec != want[s]]:
                entity = entities.pop(sensor_id)
                entity_id = entity.entity_id
                await entity.async_remove(force_remove=True)
                if sensor_id not in want and entity_id and registry.async_get(entity_id) is not None:
                    # Датчик убрали из состава — и из реестра, иначе его `entity_id` остался бы занят (повторный — `_2`).
                    registry.async_remove(entity_id)
            fresh = [make(sensors, spec) for sensor_id, spec in want.items() if sensor_id not in entities]
            for entity in fresh:
                entities[entity.spec.id] = entity
            if fresh:
                async_add_entities(fresh)

    # Сироты: датчик убрали из состава, пока дом был выключен, — запись реестра держала бы его `entity_id`.
    registry = er.async_get(hass)
    keep = wanted()
    for row in er.async_entries_for_config_entry(registry, entry.entry_id):
        unique = row.unique_id or ""
        if row.domain == domain and unique.startswith(PREFIX) and unique[len(PREFIX):] not in keep:
            registry.async_remove(row.entity_id)
    entry.async_on_unload(sensors.on_specs(lambda: hass.async_create_task(sync())))
    hass.async_create_task(sync())


class EventEntity(Entity):
    """Состояние и доступность — у `EventSensors`, здесь только HA."""

    _attr_should_poll = False

    def __init__(self, sensors: EventSensors, spec: SensorSpec, domain: str) -> None:
        self.sensors = sensors
        self.spec = spec
        self._attr_unique_id = f"{PREFIX}{spec.id}"
        self._attr_name = spec.name
        if spec.entity_id.startswith(f"{domain}."):
            # Реестр возьмёт этот id, если он свободен.
            self.entity_id = spec.entity_id

    @property
    def available(self) -> bool:
        return self.sensors.available(self.spec.id)

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(self.sensors.listen(self._changed))

    @callback
    def _changed(self, sensor_id: str) -> None:
        if sensor_id == self.spec.id and self.hass is not None:
            self.async_write_ha_state()


def enum_or_none(enum: Any, value: str) -> Any:
    """Слово HA из описания; незнакомое HA отверг бы вместе с сущностью — тогда без него."""
    return enum(value) if value in {c.value for c in enum} else None
