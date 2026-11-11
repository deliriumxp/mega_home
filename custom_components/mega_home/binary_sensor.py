"""Платформа `binary_sensor`: авария приборов с состоянием по опросу (`polled.py`) и двоичные датчики по событиям
устройств (`core/event_sensors.py`, вид `binary`; общая часть — `event_entities.py`).

Авария — раздел `problem` описания прибора: поле-маска аварий, имя и подписи битов. Включено, пока маска
не ноль; какие аварии стоят — атрибутом `problems` словами описания (бит → подпись). Номер бита и текст —
знание вендора, их пишет генератор менеджера.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .core.event_sensors import EventSensors, SensorSpec
from .core.polled_device import DeviceSpec
from .event_entities import EventEntity, enum_or_none, setup_event_entities
from .polled import UNIQUE_PREFIX, PolledDevices, PolledEntity, polled_of, remove_orphans

KEY = "problem"


def problem_of(spec: DeviceSpec) -> dict[str, Any] | None:
    block = spec.extra.get(KEY)
    return block if isinstance(block, dict) and block.get("field") else None


async def async_setup_entry(hass: HomeAssistant, entry: Any, async_add_entities: AddConfigEntryEntitiesCallback) -> None:
    polled = polled_of(hass, entry)
    entities: dict[str, MegaPolledProblem] = {}

    @callback
    def sync() -> None:
        wanted = {s.id: s for s in polled.wanted().values() if problem_of(s) is not None}
        for device_id in [d for d in entities if d not in wanted or entities[d].spec != wanted[d]]:
            hass.async_create_task(entities.pop(device_id).async_remove(force_remove=True))
        fresh = [MegaPolledProblem(polled, spec) for device_id, spec in wanted.items() if device_id not in entities]
        for entity in fresh:
            entities[entity.spec.id] = entity
        if fresh:
            async_add_entities(fresh)

    remove_orphans(hass, entry, "binary_sensor", {f"{UNIQUE_PREFIX}{s.id}_{KEY}" for s in polled.wanted().values()})
    entry.async_on_unload(polled.on_devices(sync))
    sync()
    setup_event_entities(hass, entry, async_add_entities, "binary_sensor", "binary", MegaEventBinary)


class MegaPolledProblem(PolledEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.PROBLEM

    def __init__(self, polled: PolledDevices, spec: DeviceSpec) -> None:
        block = problem_of(spec) or {}
        super().__init__(polled, spec, f"{spec.id}_{KEY}", str(block.get("name") or f"{spec.name}: авария"))
        self._block = block

    def _mask(self) -> int | None:
        value = self.values.get(str(self._block["field"]))
        if isinstance(value, bool):
            return int(value)
        return int(value) if isinstance(value, (int, float)) else None

    @property
    def is_on(self) -> bool | None:
        mask = self._mask()
        return None if mask is None else mask != 0

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        mask = self._mask()
        labels = self._block.get("labels")
        if not mask or not isinstance(labels, dict):
            return {"problems": []}
        return {"problems": [str(text) for bit, text in labels.items() if str(bit).isdigit() and text and mask >> int(bit) & 1]}


class MegaEventBinary(EventEntity, BinarySensorEntity):
    """Двоичный датчик по событиям: «человек», «движение», «дверь открыта» и т. п."""

    def __init__(self, sensors: EventSensors, spec: SensorSpec) -> None:
        super().__init__(sensors, spec, "binary_sensor")
        self._attr_device_class = enum_or_none(BinarySensorDeviceClass, spec.device_class)

    @property
    def is_on(self) -> bool:
        return self.sensors.is_on(self.spec.id)
