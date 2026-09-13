"""Diagnostics: what an installer needs to answer "did my edits reach the flat?"."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from .const import CONF_TOKEN
from .coordinator import MegaHomeConfigEntry

TO_REDACT = [CONF_TOKEN]


def _go2rtc_state() -> dict[str, Any]:
    """Состояние своего go2rtc; модуля нет — так и скажем."""
    try:
        from .go2rtc_embed import state

        return state()
    except Exception as err:  # noqa: BLE001 — диагностика не имеет права падать
        return {"running": False, "why": f"состояние недоступно: {err}"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: MegaHomeConfigEntry
) -> dict[str, Any]:
    """Return the sync state, not the whole home."""
    coordinator = entry.runtime_data
    config = coordinator.data or {}
    return {
        "entry": async_redact_data(dict(entry.data), TO_REDACT),
        "sync": {
            "version": config.get("version"),
            "last_update_success": coordinator.last_update_success,
            "last_success_at": (
                coordinator.last_success_at.isoformat()
                if coordinator.last_success_at
                else None
            ),
            "last_error": coordinator.last_error,
            # Канал поднят — правки приезжают push'ем; лежит — работает опрос.
            "link_connected": bool(
                coordinator.link and coordinator.link.connected
            ),
            # Пусто — раздаётся копия, упакованная в интеграцию (объект ещё не
            # скачал бандл).
            "app_version": coordinator.bundle.version if coordinator.bundle else None,
            # Почему интерфейс не обновился и когда его проверяли: без этих двух
            # полей «в доме старый интерфейс» неотличимо от нормы.
            "app_error": coordinator.app_error,
            "app_checked_at": (
                coordinator.app_checked_at.isoformat()
                if coordinator.app_checked_at
                else None
            ),
            "update_interval_seconds": (
                coordinator.update_interval.total_seconds()
                if coordinator.update_interval
                else None
            ),
        },
        # Сторож объекта (`agent.py`): какие правила у дома есть, что он по ним
        # видел и сколько отчётов не доехало до менеджера. Без этого «контроллер
        # перезагрузился сам» неотличимо от «его перезагрузил кто-то».
        "agent": coordinator.agent.summary if coordinator.agent else None,
        # ⚠ Запись и удалённая камера идут ТОЛЬКО через свой go2rtc, и когда он
        # не поднялся, у жильца молчат сразу три экрана: живой поток, архив и
        # календарь (календарь читается у ОТКРЫТОГО потока). Причина при этом
        # уезжала в журнал Home Assistant уровнем debug — то есть инсталлятору
        # оставалось слово «не поднят» без продолжения (живой отчёт 2026-09-13).
        "go2rtc": _go2rtc_state(),
        "home": {
            "name": config.get("home", {}).get("name"),
            "floors": len(config.get("floors", [])),
            "rooms": len(config.get("rooms", [])),
            "tiles": len(config.get("tiles", [])),
            # A tile with no entity is in the project but was never pushed to
            # Home Assistant — the usual reason a tile shows "no data".
            "tiles_without_entity": sum(
                1 for tile in config.get("tiles", []) if not tile.get("entityId")
            ),
            "scenarios": len(config.get("scenarios", [])),
            # Backgrounds the resident uploaded. Nothing synchronises them, so
            # this is the only way to tell "this flat has no photos" from "the
            # photos are here and the app is not showing them".
            "room_photos": await hass.async_add_executor_job(coordinator.photos.count),
        },
    }
