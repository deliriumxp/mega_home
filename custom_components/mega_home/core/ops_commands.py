"""Команды плиток и сценарии: служба источника из описания конфига и её отказ — словами.

Разбор операций канала и вид прибора — `ops.py`; здесь — исполнение команды: что звать (`command_spec`),
подпись в журнале источника и перевод отказа службы в ответ жильцу (`call`).
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from .const import LOGGER
from .ops_base import Actor, OpError, arguments, changes_state, find, signed
from .source import CommandRejected, CommandUnknown, StateSource

# Название запуска сценария в журнале HA: «… — Сценарий — Вечер».
SCENARIO_ACTION = "Сценарий"


async def command(
    coordinator: Any, payload: dict[str, Any], by: Actor | None = None
) -> dict[str, Any]:
    """One command for one tile, mapped onto a Home Assistant service call.

    `by` — кто скомандовал (`ops_base.actor`): его подписывает журнал источника.
    """
    tile = find(coordinator.data.get("tiles", []), payload.get("id"))
    if tile is None:
        raise OpError("Устройство не найдено", HTTPStatus.NOT_FOUND)
    if not tile.get("entityId"):
        # Same wording as the manager: the element is in the project but was
        # never pushed to Home Assistant, so there is nothing to command.
        raise OpError(
            "Элемент ещё не отправлен в Home Assistant — управлять им пока нечем"
        )

    name = payload.get("command")
    spec = command_spec(tile, name)
    if spec is None:
        raise OpError("Команда не поддерживается устройством", HTTPStatus.NOT_FOUND)

    state = coordinator.source.get(tile["entityId"])
    try:
        data = arguments(spec, payload.get("value"), state.attributes if state else {})
    except ValueError as err:
        raise OpError(str(err)) from err

    # Мультирум: наша группа — не группировка HA (вне одной интеграции её нет), состав ведёт
    # музыкальная служба дома и она же берёт такую команду себе (`join`/`unjoin`).
    playback = getattr(coordinator, "music_playback", None)
    if name in ("join", "unjoin", "play", "pause", "play_pause", "stop", "turn_off") and playback is not None:
        # Подпись считаем ЗДЕСЬ: у команд группы нет своего `execute`/`call`, и дом сам решает,
        # меняют ли они состояние. Без подписи транспорт сгруппированного плеера не оставлял
        # строки в журнале, хотя одиночный плеер её оставлял (ревью 2026-11-24).
        group_by = (
            signed(by, spec.get("label") or name, tile.get("name"))
            if by is not None and changes_state(spec, state)
            else None
        )
        handled = await playback.group_command(tile, name, data, group_by)
        if handled is not None:
            return handled

    # `response` в описании — службе нужен её ОТВЕТ (`weather.get_forecasts`,
    # `calendar.get_events`, `todo.get_items`: этих данных нет в атрибутах).
    wants = spec.get("response") is True
    async def execute() -> dict[str, Any]:
        return await call(
            coordinator.source,
            spec["domain"],
            spec["service"],
            {"entity_id": tile["entityId"], **data},
            wants,
            signed(by, spec.get("label") or name, tile.get("name"))
            if by is not None and changes_state(spec, state)
            else None,
        )

    replace_source = getattr(playback, "replace_player_source", None)
    if name in ("play_stream", "set_source") and replace_source is not None:
        answer = await replace_source(tile, name, data, execute)
    else:
        answer = await execute()
    # ⚠ Только «выполнено». Здесь отдавалось состояние сразу после вызова с
    # припиской «машина состояний уже обновлена» — это наша догадка, а не
    # контракт: HA обещает выполнение службы, а изменения велит слушать
    # `state_changed` (менеджер: `docs/docs-ha/dev-api-websocket.md`, «Calling a
    # service action»). У KNX состояние меняется по телеграмме подтверждения,
    # позже ответа. Новое состояние приносит поток дома (`events.py`).
    return {"accepted": True, **({"response": answer["response"]} if wants else {})}

async def scenario(
    coordinator: Any, payload: dict[str, Any], by: Actor | None = None
) -> dict[str, Any]:
    """Run one scenario (a Home Assistant script)."""
    item = find(coordinator.data.get("scenarios", []), payload.get("id"))
    if item is None:
        raise OpError("Сценарий не найден", HTTPStatus.NOT_FOUND)
    if not item.get("entityId"):
        raise OpError("Сценарий не создан в Home Assistant", HTTPStatus.NOT_FOUND)
    await call(
        coordinator.source,
        "script",
        "turn_on",
        {"entity_id": item["entityId"]},
        by=signed(by, SCENARIO_ACTION, item.get("name")) if by is not None else None,
    )
    return {"accepted": True}

async def call(
    source: StateSource,
    domain: str,
    service: str,
    data: dict[str, Any],
    response: bool = False,
    by: Actor | None = None,
) -> dict[str, Any]:
    """Command the source of states and turn its refusals into plain answers.

    A service can be missing outright — `climate.set_temperature` does not exist
    on an installation with no climate integration loaded — and that raises.
    Without this the resident would get a bare 500 for a house that is simply
    not set up yet.
    """
    try:
        # Источник ждёт выполнения службы: отказ приходит ответом на команду.
        result = await source.call(domain, service, data, response, by)
    except CommandUnknown as err:
        LOGGER.warning("Service %s.%s is not available", domain, service)
        raise OpError(
            "Home Assistant не умеет выполнять эту команду на этом объекте",
            HTTPStatus.NOT_FOUND,
        ) from err
    except CommandRejected as err:
        LOGGER.warning("Service %s.%s rejected the payload: %s", domain, service, err)
        raise OpError("Home Assistant отклонил команду", cause=str(err) or None) from err
    return {"accepted": True, "response": result}

def command_spec(tile: dict[str, Any], name: Any) -> dict[str, Any] | None:
    """Чем исполнять команду: службой ИЗ КОНФИГА — другого источника больше нет.

    ⚠ Ради этого затевался тонкий шлюз (docs/plan-thin-integration.md, фаза 2).
    Пока карта команд жила в Python, новый управляемый домен — вентилятор, замок,
    пылесос — стоил релиза HACS и перезапуска Home Assistant НА КАЖДОМ объекте.
    Теперь менеджер кладёт службу в конфиг плитки, и она доезжает обычной
    синхронизацией. Не заводи таблицу доменов здесь снова: она немедленно начнёт
    расходиться с менеджерской (`tileCommands` в `smart-home-view.util.ts`), а
    чинится такое расхождение только выездом.

    ⚠ Фолбэк `COMMAND_SERVICES`/`LEGACY_ARGS` убран в 0.1.14 (фаза 3). Он жил
    ровно один выпуск — на дом, чей кэш конфига старше кода. Теперь такого дома
    не бывает: обновление интеграции идёт через HACS, то есть по интернету, а
    тот же интернет приносит и конфиг с картой команд.
    """
    if not isinstance(name, str):
        return None
    described = (tile.get("commands") or {}).get(name)
    if not isinstance(described, dict) or not described.get("service"):
        return None
    # Аргументы разбирает `ops_base.arguments` по описанию целиком.
    return {**described, "domain": described.get("domain") or tile["domain"]}
