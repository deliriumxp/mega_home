"""Обновить интеграцию по команде менеджера: HACS ставит релиз, HA перезапускается.

Зачем. Код интеграции — единственное, что не доезжает до дома само: HACS кладёт
файлы, а применяет их только перезапуск Home Assistant (`docs/mega-home-updates.md`
в менеджере, раздел 3). Раньше это значило выезд или звонок жильцу; теперь
инсталлятор жмёт кнопку в карточке объекта.

⚠ Код приходит ИЗ HACS, то есть из релиза на GitHub, — ровно тем путём, что и
при ручном обновлении. Менеджер присылает только команду: интеграция не
загрузчик произвольного кода, и компрометация менеджера не должна означать
исполнение чужого Python на всех объектах.

⚠ НЕ ПРОВЕРЕНО на живом объекте (2026-09-19): как HACS называет сущность
обновления и что лежит в её атрибутах. Ищем по двум признакам сразу — адресу
релиза и картинке бренда, — чтобы промах одного не оставил дом без кнопки.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import Any

from homeassistant.core import HomeAssistant, State
from homeassistant.exceptions import HomeAssistantError

from .core.const import DOMAIN, INTEGRATION_VERSION, LOGGER
from .core.ops_base import OpError

# Сколько ждём, пока HACS скачает и разложит релиз. Архив — сотни килобайт; запас
# для медленного канала объекта, а не норма.
INSTALL_TIMEOUT = 150.0
# Пауза перед перезапуском: ответ менеджеру обязан уйти ДО того, как HA начнёт
# останавливаться и закроет канал, иначе инсталлятор увидит «дом отключился»
# вместо «обновление поставлено».
RESTART_DELAY = 2.0


def find_update_entity(hass: HomeAssistant) -> State | None:
    """Сущность обновления HACS, которая отвечает за ЭТУ интеграцию."""
    for state in hass.states.async_all("update"):
        attrs = state.attributes
        release = str(attrs.get("release_url") or "")
        picture = str(attrs.get("entity_picture") or "")
        if f"/{DOMAIN}/releases" in release or f"/{DOMAIN}/" in picture:
            return state
    return None


async def async_self_update(hass: HomeAssistant, wanted: str | None = None) -> dict[str, Any]:
    """Поставить релиз `wanted` (или свежий по мнению HACS) и перезапустить HA.

    ⚠ `wanted` — версия, которую менеджер сам увидел на GitHub. Без неё дом
    верил `latest_version` HACS, а тот перечитывает релизы раз в несколько
    часов, и `update_entity` этого не ускоряет: живой факт 2026-09-19 — релиз
    0.4.1 вышел, менеджер его знал, а кнопка ставила «нечего ставить» и просто
    перезапускала дом на 0.4.0. `update.install` с явной `version` ставит тег,
    не дожидаясь, пока HACS о нём вспомнит.

    ⚠ Перезапуск — ВСЕГДА, даже если ставить нечего: кнопку жмут и ради дома,
    где HACS уже положил файлы, но HA их ещё не загрузил (на диске новая версия,
    в памяти старая), — и лечится это ровно перезапуском.
    """
    entity = find_update_entity(hass)
    if entity is None:
        raise OpError(
            "Mega Home не найден среди обновлений HACS — интеграция поставлена не через HACS?",
            HTTPStatus.CONFLICT,
        )
    entity_id = entity.entity_id
    try:
        # HACS знает о новом релизе не сразу (проверяет раз в несколько часов);
        # просим сверить сейчас, иначе кнопка ставила бы вчерашнюю версию.
        await hass.services.async_call(
            "homeassistant", "update_entity", {"entity_id": entity_id}, blocking=True
        )
    except HomeAssistantError as err:
        LOGGER.debug("Сверка обновления %s не прошла: %s", entity_id, err)

    fresh = hass.states.get(entity_id) or entity
    installed = fresh.attributes.get("installed_version")
    latest = fresh.attributes.get("latest_version")
    target = (wanted or "").strip() or latest
    installing = bool(target) and target != installed
    if installing:
        LOGGER.warning("Обновление по команде менеджера: %s → %s", installed, target)
        data: dict[str, Any] = {"entity_id": entity_id}
        if wanted:
            data["version"] = target
        try:
            async with asyncio.timeout(INSTALL_TIMEOUT):
                await hass.services.async_call("update", "install", data, blocking=True)
        except TimeoutError as err:
            raise OpError(
                "HACS не успел поставить обновление — перезапуск отменён",
                HTTPStatus.GATEWAY_TIMEOUT,
            ) from err
        except HomeAssistantError as err:
            raise OpError(
                f"HACS не поставил обновление: {err}", HTTPStatus.BAD_GATEWAY
            ) from err

    hass.async_create_background_task(_restart_later(hass), "mega_home self-update restart")
    return {
        "loaded": INTEGRATION_VERSION,
        "installed": installed,
        "latest": latest,
        "target": target,
        "installing": installing,
        "restarting": True,
    }


async def _restart_later(hass: HomeAssistant) -> None:
    await asyncio.sleep(RESTART_DELAY)
    LOGGER.warning("Перезапуск Home Assistant по команде менеджера")
    await hass.services.async_call("homeassistant", "restart", {})
