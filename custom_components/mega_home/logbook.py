"""Подпись Mega Home в журнале Home Assistant («Активность»).

Строка события: «Mega Home: Ноутбук, Локально — Включение — Люстра»; строка
прибора, которую она вызвала: «Люстра → Включено · С помощью: Mega Home:
Ноутбук, Локально». Связь — общий контекст события `mega_home_command` и вызова
службы (`ha_source.py`).

⚠ Кто — в ИМЕНИ записи, что сделано — в ТЕКСТЕ. «С помощью: …» у строки прибора
журнал собирает только из имени (`context_name`), а текст виден лишь в строке
самого события: там действие и прибор нужны, у прибора — повторяли бы его строку.

⚠ Страницы документации у платформы журнала НЕТ — контракт только в коде ядра
(`docs/ha-api-registry.json`, записи `src-components-logbook-*`): ядро само
находит `logbook.py` интеграции и зовёт `async_describe_events`
(`src-components-logbook-__init__.py`). Так же сделан HomeKit
(`src-components-homekit-logbook.py`).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from homeassistant.components.logbook import LOGBOOK_ENTRY_MESSAGE, LOGBOOK_ENTRY_NAME
from homeassistant.core import HomeAssistant, callback

from .core.const import DOMAIN
from .ha_source import EVENT_COMMAND

NAME = "Mega Home"


def _parts(body: dict[str, Any], *keys: str) -> list[str]:
    return [part for part in (body.get(key) for key in keys) if isinstance(part, str) and part]


def describe(data: Any) -> dict[str, str]:
    """Запись журнала: кто — «Mega Home: Ноутбук, Локально», что — «— Включение — Люстра»."""
    body = data if isinstance(data, dict) else {}
    who = ", ".join(_parts(body, "name", "via"))
    what = " — ".join(_parts(body, "action", "target"))
    entry = {LOGBOOK_ENTRY_NAME: f"{NAME}: {who}" if who else NAME}
    if what:
        entry[LOGBOOK_ENTRY_MESSAGE] = f"— {what}"
    return entry


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
        return describe(event.data)

    async_describe_event(DOMAIN, EVENT_COMMAND, _describe)
