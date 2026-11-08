"""Приборы с состоянием по опросу (`core/polled_device.py`) — общий хозяин для платформ HA.

Приборы приходят КОНФИГОМ объекта (`climates[]`, менеджер), как шторы (`cover.py`).
Один прибор — ОДИН опрос, сколько бы сущностей из него ни выросло: климат, датчики
и авария (`climate.py`, `sensor.py`, `binary_sensor.py`) подписываются на него здесь.
Второй опрос того же контроллера удвоил бы нагрузку на слабое устройство.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity import Entity

from .core import connect
from .core.const import DOMAIN
from .core.polled_device import DeviceSpec, PolledDevice, spec_of

UNIQUE_PREFIX = f"{DOMAIN}_polled_"


def request_of(descriptor: Any) -> dict[str, Any] | None:
    """Адрес, учётка и TLS доступа — та же форма, что у вызова `connect` бандла."""
    if descriptor is None:
        return None
    request: dict[str, Any] = {
        "host": descriptor.host, "port": descriptor.port, "tls": descriptor.tls, "timeout": descriptor.timeout,
        **descriptor.tls_options,
    }
    auth = descriptor.auth
    if auth.type != "none":
        request["auth"] = {"type": auth.type, "user": auth.user, "pass": auth.password, **({"clock": auth.clock} if auth.clock else {})}
    return request


class PolledDevices:
    """Приборы объекта: заводятся и снимаются по каждому новому конфигу."""

    def __init__(self, hass: HomeAssistant, coordinator: Any) -> None:
        self._hass = hass
        self._coordinator = coordinator
        self.devices: dict[str, PolledDevice] = {}
        self._listeners: dict[str, list[Callable[[], None]]] = {}
        self._platforms: list[Callable[[], None]] = []

    def wanted(self) -> dict[str, DeviceSpec]:
        blocks = (self._coordinator.data or {}).get("climates") or []
        return {s.id: s for s in (spec_of(b) for b in blocks) if s}

    def on_devices(self, platform_sync: Callable[[], None]) -> Callable[[], None]:
        """Платформа узнаёт о смене состава приборов, чтобы завести или снять свои сущности."""
        self._platforms.append(platform_sync)
        return lambda: self._platforms.remove(platform_sync)

    def listen(self, device_id: str, listener: Callable[[], None]) -> Callable[[], None]:
        self._listeners.setdefault(device_id, []).append(listener)

        def remove() -> None:
            listeners = self._listeners.get(device_id, [])
            if listener in listeners:
                listeners.remove(listener)

        return remove

    async def sync(self) -> None:
        wanted = self.wanted()
        for device_id in [d for d in self.devices if d not in wanted or self.devices[d].spec != wanted[d]]:
            await self.devices.pop(device_id).shutdown()
        for device_id, spec in wanted.items():
            if device_id in self.devices:
                continue
            device = PolledDevice(
                spec,
                lambda access=spec.access: request_of(self._coordinator.accesses.descriptor(access)) if self._coordinator.accesses else None,
                connect.perform,
                lambda device_id=device_id: self._changed(device_id),
            )
            self.devices[device_id] = device
            device.start()
        for platform_sync in list(self._platforms):
            platform_sync()

    async def shutdown(self) -> None:
        for device in list(self.devices.values()):
            await device.shutdown()
        self.devices = {}

    @callback
    def _changed(self, device_id: str) -> None:
        for listener in list(self._listeners.get(device_id, [])):
            listener()


def polled_of(hass: HomeAssistant, entry: Any) -> PolledDevices:
    """Один хозяин на запись: первая платформа заводит, остальные берут готового."""
    coordinator = entry.runtime_data
    existing = getattr(coordinator, "polled", None)
    if existing is None:
        existing = PolledDevices(hass, coordinator)
        coordinator.polled = existing
        entry.async_on_unload(coordinator.async_add_listener(lambda: hass.async_create_task(existing.sync())))
        entry.async_on_unload(lambda: hass.async_create_task(existing.shutdown()))
    return existing


def remove_orphans(hass: HomeAssistant, entry: Any, domain: str, keep: set[str]) -> None:
    """Сироты: прибор убрали из состава, пока дом был выключен, — запись реестра держала бы entity_id."""
    registry = er.async_get(hass)
    for row in er.async_entries_for_config_entry(registry, entry.entry_id):
        unique = row.unique_id or ""
        if row.domain == domain and unique.startswith(UNIQUE_PREFIX) and unique not in keep:
            registry.async_remove(row.entity_id)


class PolledEntity(Entity):
    """Сущность прибора: состояние — значения его опроса, доступность — его доступность."""

    _attr_should_poll = False

    def __init__(self, polled: PolledDevices, spec: DeviceSpec, unique: str, name: str) -> None:
        self.polled = polled
        self.spec = spec
        self._attr_unique_id = f"{UNIQUE_PREFIX}{unique}"
        self._attr_name = name

    @property
    def device(self) -> PolledDevice | None:
        return self.polled.devices.get(self.spec.id)

    @property
    def values(self) -> dict[str, Any]:
        device = self.device
        return device.values if device is not None else {}

    @property
    def available(self) -> bool:
        device = self.device
        return device is not None and device.available

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(self.polled.listen(self.spec.id, self.async_write_ha_state))

    async def write(self, name: str, value: Any) -> None:
        device = self.device
        if device is None:
            raise ValueError(f"прибора {self.spec.id} больше нет в составе")
        await device.write(name, value)
