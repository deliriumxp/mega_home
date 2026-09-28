"""Подпись Mega Home в журнале Home Assistant («Активность»).

Смена прибора по команде из приложения подписывается источником:
«Пилон → Выключено · С помощью: Mega Home — Иван, удалённое приложение». Связь —
общий контекст события `mega_home_command` и вызова службы (`ha_source.py`).

⚠ Вся подпись — в ИМЕНИ записи, текста нет. «С помощью: …» у строки прибора
журнал собирает только из имени (`context_name`); текст записи (`message`) виден
лишь в строке самого события, и там он повторял бы имя.

⚠ Страницы документации у платформы журнала НЕТ — контракт только в коде ядра
(`docs/ha-api-registry.json`, записи `src-components-logbook-*`): ядро само
находит `logbook.py` интеграции и зовёт `async_describe_events`
(`src-components-logbook-__init__.py`). Так же сделан HomeKit
(`src-components-homekit-logbook.py`).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from homeassistant.components.logbook import LOGBOOK_ENTRY_NAME
from homeassistant.core import HomeAssistant, callback

from .core.const import DOMAIN
from .ha_source import EVENT_COMMAND

NAME = "Mega Home"


def describe(data: Any) -> str:
    """«Mega Home — Иван, удалённое приложение». Что переключилось — пишет HA."""
    body = data if isinstance(data, dict) else {}
    who = ", ".join(
        part for part in (body.get("name"), body.get("via")) if isinstance(part, str) and part
    )
    return f"{NAME} — {who}" if who else NAME


@callback
def async_describe_events(
    hass: HomeAssistant,
    async_describe_event: Callable[[str, str, Callable[[Any], dict[str, Any]]], None],
) -> None:
    """Описать для журнала событие команды Mega Home."""

    # ⚠ Журнал передаёт сюда не `Event`, а свою лёгкую запись
    # (`LazyEventPartialState`, `src-components-logbook-__init__.py`) — у неё
    # есть `data`, и больше нам ничего не нужно.
    @callback
    def _describe(event: Any) -> dict[str, Any]:
        return {LOGBOOK_ENTRY_NAME: describe(event.data)}

    async_describe_event(DOMAIN, EVENT_COMMAND, _describe)
