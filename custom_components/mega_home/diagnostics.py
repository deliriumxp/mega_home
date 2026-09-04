"""Diagnostics: what an installer needs to answer "did my edits reach the flat?"."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from .const import CONF_TOKEN
from .coordinator import MegaHomeConfigEntry

TO_REDACT = [CONF_TOKEN]


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
            "update_interval_seconds": (
                coordinator.update_interval.total_seconds()
                if coordinator.update_interval
                else None
            ),
        },
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
        },
    }
