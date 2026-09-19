"""Платформа `cover` — HA-адаптер штор на импульсных реле (`core/pulse_blind.py`).

Шторы приходят КОНФИГОМ объекта (`covers[]`, менеджер), а не записями мастера
настройки: добавить или перенастроить штору — правка в менеджере. Сущности
появляются и снимаются на лету, по каждому новому конфигу.

⚠ `entity_id` задаёт менеджер: у штор, переехавших из `pulse_cover`, это их
прежний id — плитки жильца и привязки состава не отваливаются.

⚠ `assumed_state = True` — несущее решение, а не деталь: положение устройство
не подтверждает (штору двигают руками), и кнопки «открыть/закрыть» в HA не
должны гаснуть по оценке «уже закрыта». Фронтенд HA (`data/cover.ts`,
`canOpen`/`canClose`) смотрит `assumed_state` РАНЬШЕ `current_position`.
"""

from __future__ import annotations

import asyncio
from typing import Any

import voluptuous as vol
from homeassistant.components.cover import ATTR_POSITION, CoverEntity, CoverEntityFeature
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers import entity_platform, entity_registry as er
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .core.const import DOMAIN, LOGGER
from .core.pulse_blind import BlindSpec, PulseBlind, spec_of

SERVICE_CALIBRATE = "calibrate_position"


async def async_setup_entry(hass: HomeAssistant, entry: Any, async_add_entities: AddConfigEntryEntitiesCallback) -> None:
    coordinator = entry.runtime_data
    covers: dict[str, MegaPulseCover] = {}
    lock = asyncio.Lock()
    prefix = f"{DOMAIN}_cover_"

    def wanted_specs() -> dict[str, BlindSpec]:
        return {s.id: s for s in (spec_of(b) for b in (coordinator.data or {}).get("covers") or []) if s}

    async def sync() -> None:
        # ⚠ Под замком и С ОЖИДАНИЕМ удаления: сущность с тем же `unique_id`,
        # добавленная, пока прежняя ещё снимается (штора едет — удаление ждёт
        # стоп-импульса), HA отбросила бы как дубль, и штора пропала бы до
        # перезагрузки записи (повторное ревью 2026-09-19).
        async with lock:
            wanted = wanted_specs()
            registry = er.async_get(hass)
            for cover_id in [c for c in covers if c not in wanted or covers[c].spec != wanted[c]]:
                cover = covers.pop(cover_id)
                entity_id = cover.entity_id
                await cover.async_remove(force_remove=True)
                if cover_id not in wanted and registry.async_get(entity_id) is not None:
                    # Штору убрали из состава — и из реестра, иначе её id остался бы занят.
                    registry.async_remove(entity_id)
            fresh = [MegaPulseCover(coordinator, spec) for cover_id, spec in wanted.items() if cover_id not in covers]
            for cover in fresh:
                covers[cover.spec.id] = cover
            if fresh:
                async_add_entities(fresh)

    # Сироты: штору убрали из состава, пока дом был выключен, — в памяти её нет,
    # а запись реестра держала бы entity_id (повторная штора получила бы `_2`).
    registry = er.async_get(hass)
    wanted = wanted_specs()
    for entry_row in er.async_entries_for_config_entry(registry, entry.entry_id):
        unique = entry_row.unique_id or ""
        if entry_row.domain == "cover" and unique.startswith(prefix) and unique[len(prefix):] not in wanted:
            registry.async_remove(entry_row.entity_id)

    await sync()
    entry.async_on_unload(coordinator.async_add_listener(lambda: hass.async_create_task(sync())))
    entity_platform.async_get_current_platform().async_register_entity_service(
        SERVICE_CALIBRATE,
        {vol.Required(ATTR_POSITION): vol.All(vol.Coerce(int), vol.Range(min=0, max=100))},
        "async_calibrate_position",
    )


class MegaPulseCover(CoverEntity, RestoreEntity):
    """Сущность шторы; логика — в `PulseBlind`, здесь только HA."""

    _attr_assumed_state = True
    _attr_should_poll = False
    _attr_supported_features = (
        CoverEntityFeature.OPEN | CoverEntityFeature.CLOSE | CoverEntityFeature.STOP | CoverEntityFeature.SET_POSITION
    )

    def __init__(self, coordinator: Any, spec: BlindSpec) -> None:
        self.spec = spec
        self._attr_unique_id = f"{DOMAIN}_cover_{spec.id}"
        self._attr_name = spec.name
        if spec.entity_id.startswith("cover."):
            # Реестр возьмёт этот id, если он свободен (прежняя штора снята).
            self.entity_id = spec.entity_id
        self.blind = PulseBlind(spec, coordinator.source.call, self._changed, self._settled)
        self._unsub_knx: Any = None

    # --- состояние ----------------------------------------------------------

    @property
    def current_cover_position(self) -> int | None:
        return self.blind.position

    @property
    def is_opening(self) -> bool:
        return self.blind.opening

    @property
    def is_closing(self) -> bool:
        return self.blind.closing

    @property
    def is_closed(self) -> bool | None:
        return None if self.blind.position is None else self.blind.position == 0

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {"position_calibrated": self.blind.calibrated, "last_direction": self.blind.last_direction}

    @callback
    def _changed(self) -> None:
        if self.hass is not None:
            self.async_write_ha_state()

    @callback
    def _settled(self) -> None:
        if self.hass is not None and self.blind.position is not None:
            self.hass.async_create_task(self._publish_position())

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        last = await self.async_get_last_state()
        if last is not None and last.attributes.get("current_position") is not None:
            # «Калибрована» после перезапуска не верим: ход мог оборваться ровно тогда.
            self.blind.position = int(last.attributes["current_position"])
        address = self.spec.position_address
        if address:
            try:
                await self.hass.services.async_call(
                    "knx", "event_register", {"address": [address], "type": "percent"}, blocking=True
                )
            except Exception:  # noqa: BLE001 — KNX может быть не настроен
                LOGGER.warning("%s: не подписаться на адрес позиции %s (KNX не настроен?)", self.entity_id, address)
            else:
                self._unsub_knx = self.hass.bus.async_listen("knx_event", self._knx_event)

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_knx is not None:
            self._unsub_knx()
            self._unsub_knx = None
        await self.blind.shutdown()
        await super().async_will_remove_from_hass()

    @callback
    def _knx_event(self, event: Event) -> None:
        # Позиция, сообщённая Control4 (`knx_pulse_blind`) на общем адресе, — только показ.
        if event.data.get("destination") == self.spec.position_address and event.data.get("value") is not None:
            self.blind.external_position(float(event.data["value"]))

    async def _publish_position(self) -> None:
        if not self.spec.position_address:
            return
        try:
            await self.hass.services.async_call(
                "knx", "send",
                {"address": [self.spec.position_address], "payload": self.blind.position, "type": "percent"},
                blocking=True,
            )
        except Exception:  # noqa: BLE001 — сбой шины не ломает ход
            LOGGER.warning("%s: позиция не отправлена в KNX %s", self.entity_id, self.spec.position_address)

    # --- команды ------------------------------------------------------------

    async def async_open_cover(self, **kwargs: Any) -> None:
        await self.blind.open()

    async def async_close_cover(self, **kwargs: Any) -> None:
        await self.blind.close()

    async def async_set_cover_position(self, **kwargs: Any) -> None:
        await self.blind.set_position(int(kwargs[ATTR_POSITION]))

    async def async_stop_cover(self, **kwargs: Any) -> None:
        await self.blind.stop()

    async def async_calibrate_position(self, position: int) -> None:
        if not self.blind.calibrate(position):
            LOGGER.warning("%s: калибровка на ходу не принимается", self.entity_id)
