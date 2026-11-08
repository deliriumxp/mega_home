"""Платформа `climate` — HA-адаптер приборов с состоянием по опросу (`polled.py`).

Какое поле прибора — уставка, какое — скорость, говорит раздел `climate` описания
(генератор менеджера под вендора); здесь только перевод на модель климата HA
(`docs/docs-ha` менеджера, `core-climate-const.py`): режимы — `off` и один режим
«включено» из описания, скорость — `fan_modes` строками, как их назвало описание.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.climate import ClimateEntity, ClimateEntityFeature, HVACAction, HVACMode
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .core.polled_device import DeviceSpec
from .polled import UNIQUE_PREFIX, PolledDevices, PolledEntity, polled_of, remove_orphans

# Режим «включено», если описание его не назвало: вентиляция без охлаждения — `fan_only`.
DEFAULT_ON_MODE = HVACMode.FAN_ONLY


def _climate(spec: DeviceSpec) -> dict[str, Any] | None:
    block = spec.extra.get("climate")
    return block if isinstance(block, dict) else None


async def async_setup_entry(hass: HomeAssistant, entry: Any, async_add_entities: AddConfigEntryEntitiesCallback) -> None:
    polled = polled_of(hass, entry)
    entities: dict[str, MegaPolledClimate] = {}

    @callback
    def sync() -> None:
        wanted = {s.id: s for s in polled.wanted().values() if _climate(s) is not None}
        for device_id in [d for d in entities if d not in wanted or entities[d].spec != wanted[d]]:
            hass.async_create_task(entities.pop(device_id).async_remove(force_remove=True))
        fresh = [MegaPolledClimate(polled, spec) for device_id, spec in wanted.items() if device_id not in entities]
        for entity in fresh:
            entities[entity.spec.id] = entity
        if fresh:
            async_add_entities(fresh)

    remove_orphans(hass, entry, "climate", {f"{UNIQUE_PREFIX}{s.id}" for s in polled.wanted().values()})
    entry.async_on_unload(polled.on_devices(sync))
    await polled.sync()


class MegaPolledClimate(PolledEntity, ClimateEntity):
    """Климат прибора: включение, уставка, скорость, что делает установка сейчас."""

    # ⚠ Имена свойств — HA 2026.9 (объекты): `current_temperature`, `target_temperature`, `temperature_unit`.
    # В ветке `dev` (2026.12) они `native_*`, а старые переносит `migrate_deprecated_entity_members`
    # (`climate/entity.py`) — работают на обеих; перейти на `native_*`, когда парк уйдёт с 2026.9.
    _attr_temperature_unit = UnitOfTemperature.CELSIUS

    def __init__(self, polled: PolledDevices, spec: DeviceSpec) -> None:
        super().__init__(polled, spec, spec.id, spec.name)
        block = _climate(spec) or {}
        self._block = block
        entity_id = str(spec.extra.get("entityId") or "")
        if entity_id.startswith("climate."):
            # Реестр возьмёт этот id, если он свободен (как у штор).
            self.entity_id = entity_id
        try:
            self._on_mode = HVACMode(str(block.get("onMode") or DEFAULT_ON_MODE))
        except ValueError:
            self._on_mode = DEFAULT_ON_MODE
        self._attr_hvac_modes = [HVACMode.OFF, self._on_mode]
        self._attr_fan_modes = [str(m) for m in block.get("fanModes") or []] or None
        for attr, key in (("_attr_min_temp", "minTemp"), ("_attr_max_temp", "maxTemp"), ("_attr_target_temperature_step", "step")):
            if isinstance(block.get(key), (int, float)):
                setattr(self, attr, float(block[key]))
        features = ClimateEntityFeature.TURN_ON | ClimateEntityFeature.TURN_OFF
        if self._name("target") in spec.writes:
            features |= ClimateEntityFeature.TARGET_TEMPERATURE
        if self._name("fan") in spec.writes and self._attr_fan_modes:
            features |= ClimateEntityFeature.FAN_MODE
        self._attr_supported_features = features

    def _name(self, role: str) -> str:
        """Поле прибора под ролью климата: описание может назвать его иначе, по умолчанию — как роль."""
        return str(self._block.get(role) or role)

    def _field(self, role: str) -> Any:
        return self.values.get(self._name(role))

    async def _write(self, role: str, value: Any) -> None:
        await self.write(self._name(role), value)

    @property
    def hvac_mode(self) -> HVACMode | None:
        power = self._field("power")
        if power is None:
            return None
        return self._on_mode if _truthy(power) else HVACMode.OFF

    @property
    def hvac_action(self) -> HVACAction | None:
        actions = self._block.get("actions")
        raw = self._field("action")
        if not isinstance(actions, dict) or raw is None:
            return None
        try:
            return HVACAction(str(actions.get(str(raw)) or ""))
        except ValueError:
            return None

    @property
    def current_temperature(self) -> float | None:
        return _float(self._field("current"))

    @property
    def target_temperature(self) -> float | None:
        return _float(self._field("target"))

    @property
    def fan_mode(self) -> str | None:
        fan = self._field("fan")
        return None if fan is None else str(fan)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        # Сырое состояние установки (этап пуска, разморозка) — то, что HVACAction не выражает.
        status = self._block.get("statusText")
        raw = self._field("action")
        if isinstance(status, dict) and raw is not None and str(raw) in status:
            return {"status": status[str(raw)]}
        return {}

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        await self._write("power", 0 if hvac_mode == HVACMode.OFF else 1)

    async def async_turn_on(self) -> None:
        await self._write("power", 1)

    async def async_turn_off(self) -> None:
        await self._write("power", 0)

    async def async_set_temperature(self, **kwargs: Any) -> None:
        if kwargs.get(ATTR_TEMPERATURE) is not None:
            await self._write("target", float(kwargs[ATTR_TEMPERATURE]))

    async def async_set_fan_mode(self, fan_mode: str) -> None:
        await self._write("fan", int(fan_mode) if fan_mode.isdigit() else fan_mode)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "off", "false")
    return bool(value)


def _float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
