"""Платформа `sensor` — показания приборов с состоянием по опросу (`polled.py`) и показания по событиям
устройств (`core/event_sensors.py`, вид `value`; общая часть — `event_entities.py`).

Список показаний прибора — раздел `sensors` его описания: поле прибора, имя, единица, класс HA.
Класс и единица — слова HA (`SensorDeviceClass`), их выбирает генератор менеджера.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorStateClass
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .core.event_sensors import EventSensors, SensorSpec
from .core.polled_device import DeviceSpec
from .event_entities import EventEntity, enum_or_none, setup_event_entities
from .polled import UNIQUE_PREFIX, PolledDevices, PolledEntity, polled_of, remove_orphans


def readings_of(spec: DeviceSpec) -> list[dict[str, Any]]:
    items = spec.extra.get("sensors")
    return [i for i in items if isinstance(i, dict) and i.get("key") and i.get("field")] if isinstance(items, list) else []


async def async_setup_entry(hass: HomeAssistant, entry: Any, async_add_entities: AddConfigEntryEntitiesCallback) -> None:
    polled = polled_of(hass, entry)
    entities: dict[str, MegaPolledSensor] = {}

    def wanted() -> dict[str, tuple[DeviceSpec, dict[str, Any]]]:
        return {f"{s.id}_{r['key']}": (s, r) for s in polled.wanted().values() for r in readings_of(s)}

    @callback
    def sync() -> None:
        want = wanted()
        for key in [k for k in entities if k not in want or (entities[k].spec, entities[k].reading) != want[k]]:
            hass.async_create_task(entities.pop(key).async_remove(force_remove=True))
        fresh = [MegaPolledSensor(polled, spec, reading) for key, (spec, reading) in want.items() if key not in entities]
        for entity in fresh:
            entities[f"{entity.spec.id}_{entity.reading['key']}"] = entity
        if fresh:
            async_add_entities(fresh)

    remove_orphans(hass, entry, "sensor", {f"{UNIQUE_PREFIX}{key}" for key in wanted()})
    entry.async_on_unload(polled.on_devices(sync))
    sync()
    setup_event_entities(hass, entry, async_add_entities, "sensor", "value", MegaEventValue)


class MegaPolledSensor(PolledEntity, SensorEntity):
    def __init__(self, polled: PolledDevices, spec: DeviceSpec, reading: dict[str, Any]) -> None:
        super().__init__(polled, spec, f"{spec.id}_{reading['key']}", str(reading.get("name") or reading["key"]))
        self.reading = reading
        self._attr_native_unit_of_measurement = reading.get("unit") or None
        try:
            self._attr_device_class = SensorDeviceClass(reading["deviceClass"]) if reading.get("deviceClass") else None
        except ValueError:
            self._attr_device_class = None
        try:
            self._attr_state_class = SensorStateClass(reading["stateClass"]) if reading.get("stateClass") else None
        except ValueError:
            self._attr_state_class = None
        labels = reading.get("labels")
        self._labels = labels if isinstance(labels, dict) else None

    @property
    def native_value(self) -> Any:
        value = self.values.get(str(self.reading["field"]))
        if self._labels is not None and value is not None:
            # Код состояния прибора словами описания («Установка работает»); незнакомый код — как есть.
            return self._labels.get(str(value), value)
        return value


class MegaEventValue(EventEntity, SensorEntity):
    """Показание по событиям устройства (вид `value`): число или текст из события, слова HA — из описания."""

    def __init__(self, sensors: EventSensors, spec: SensorSpec) -> None:
        super().__init__(sensors, spec, "sensor")
        self._attr_native_unit_of_measurement = spec.unit or None
        self._attr_device_class = enum_or_none(SensorDeviceClass, spec.device_class)
        self._attr_state_class = enum_or_none(SensorStateClass, spec.state_class)

    @property
    def native_value(self) -> Any:
        return self.sensors.value(self.spec.id)
