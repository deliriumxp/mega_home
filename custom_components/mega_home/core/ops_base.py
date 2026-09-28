"""Общее для всех операций: отказ, поиск плитки и разбор чисел.

⚠ Отдельным модулем, чтобы классы устройств (`ops_camera`, `ops.py`) не тянули
друг друга ради одного `OpError`. Всё остальное делится ПО КЛАССАМ, а не по
вендорам — правило заказчика, `CLAUDE.md`.
"""

from __future__ import annotations

import json
from datetime import date, time
from http import HTTPStatus
from typing import Any


class OpError(Exception):
    """A refusal the resident should read, with the status that fits it."""

    def __init__(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        super().__init__(message)
        self.message = message
        self.status = int(status)

# Кто скомандовал домом — для журнала источника состояний (в HA — «Mega Home:
# Ноутбук, Локально — Включение — Люстра»; действие и прибор добавляет команда).
#
# ⚠ Имя — то, что задано в ПРИЛОЖЕНИИ (`by` в теле, решение заказчика
# 2026-09-28); имя учётки от менеджера — лишь запасное. А ИСТОЧНИК (`via`)
# называет ДВЕРЬ, и телу его не доверяем: локальную дверь дом знает сам, а
# снаружи источник ставит менеджер («Удалённо», «Временный доступ №5») — его
# знает только сессия.
VIA_LOCAL = "Локально"
VIA_REMOTE = "Удалённо"
ACTOR_MAX = 64
Actor = dict[str, str]


def actor(given: Any, payload: Any, via: str) -> Actor:
    """Подпись команды из того, что дала дверь, и того, что назвало приложение."""
    trusted = given if isinstance(given, dict) else {}
    body = payload if isinstance(payload, dict) else {}
    return {
        "name": _label(body.get("by")) or _label(trusted.get("name")),
        "via": _label(trusted.get("via")) or via,
    }


def signed(by: Actor, action: Any, target: Any) -> Actor:
    """Подпись с самим действием: «Включение» и «Люстра» — в строку журнала."""
    return {**by, "action": _label(action), "target": _label(target)}


def _label(value: Any) -> str:
    return " ".join(value.split())[:ACTOR_MAX] if isinstance(value, str) else ""


def changes_state(spec: dict[str, Any], state: Any) -> bool:
    """Может ли команда сменить СОСТОЯНИЕ прибора — подписывать ли её в журнале.

    Подпись нужна только там, где у прибора будет своя строка журнала, а она
    бывает лишь при смене состояния: яркость включённого света журнал не
    показывает, и подпись рядом с ней — голая лишняя строка. Что команда сделает
    с состоянием, знает её описание из менеджера (`keepsState`, `stateAfter`), а
    не дом: своих правил по доменам здесь нет. Не размечено — подписываем.
    """
    if spec.get("keepsState") is True:
        return False
    after = spec.get("stateAfter")
    current = getattr(state, "state", None)
    return not (isinstance(after, str) and current == after)

def json_default(value: Any) -> Any:
    """Чего нет в JSON — так же, как у энкодера Home Assistant (`helpers/json.py`).

    ⚠ ОДИН энкодер на все дороги ответа. Атрибуты HA бывают датами
    (`media_position_updated_at` у плеера), и каждая дорога кодировала их
    по-своему: локальная дверь — энкодером HA (ISO с `T`), поток и watch —
    `default=str` (с пробелом: Safari такую дату не разбирает), перенос и ответ
    канала — голым `json.dumps`, который на дате ПАДАЛ: снаружи `api/states`
    отвечал 500, а ответ канала не уходил вовсе.
    """
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if hasattr(value, "as_dict"):
        return value.as_dict()
    return str(value)

def dumps(payload: Any) -> str:
    return json.dumps(payload, default=json_default)

def find(items: list[dict[str, Any]], item_id: Any) -> dict[str, Any] | None:
    if not isinstance(item_id, str):
        return None
    return next((item for item in items if item.get("id") == item_id), None)

def number(value: Any, low: Any, high: Any) -> float:
    # ⚠ `bool` отдельно: `float(True)` — это 1.0, и «да» прошло бы как яркость.
    try:
        parsed, low, high = float(value), float(low), float(high)
    except (TypeError, ValueError) as err:
        raise ValueError(f"Значение должно быть от {low} до {high}") from err
    if isinstance(value, bool) or not low <= parsed <= high:
        raise ValueError(f"Значение должно быть от {low:g} до {high:g}")
    return parsed

def arguments(spec: dict[str, Any], value: Any, attributes: dict[str, Any]) -> dict[str, Any]:
    """Данные службы из значения жильца, проверенные по ОПИСАНИЮ команды.

    ⚠ Описание приходит из конфига (`fields` у команды, `smart-home-commands.util.ts`
    менеджера), а проверяет его ЭТА сторона: службу зовём мы, браузеру верить
    нельзя. Схема — данные: новый аргумент нового домена (цвет, диапазон
    климата, громкость) доезжает синхронизацией, а не релизом дома; белого
    списка служб здесь нет и не будет (`docs/plan-ha-domains.md`, этап 0).

    Одно поле принимает голое значение, несколько — объект `{поле: значение}`;
    поле без `optional` обязательно, чужое поле — отказ. `data` — постоянные.
    ⚠ Старый вид `arg/min/max` (один аргумент) читается, пока жив кэш конфига
    от менеджера до этой схемы.
    """
    fields = spec.get("fields")
    if fields is None and spec.get("arg"):
        rule = {"type": "number", "min": spec["min"], "max": spec["max"]} if spec.get("max") is not None else {}
        fields = {spec["arg"]: rule}
    fields = fields or {}
    if len(fields) == 1 and not isinstance(value, dict):
        value = {next(iter(fields)): value}
    given = value if isinstance(value, dict) else {}
    if set(given) - set(fields):
        raise ValueError("Команда не принимает " + ", ".join(sorted(set(given) - set(fields))))
    out = dict(spec.get("data") or {})
    for name, rule in fields.items():
        if name in given:
            out[name] = _checked(given[name], rule, attributes)
        elif not rule.get("optional"):
            raise ValueError(f"Не указано значение {name}")
    return out

def _checked(value: Any, rule: dict[str, Any], attributes: dict[str, Any]) -> Any:
    """Одно значение по правилу поля. Границы и варианты — числом в правиле или
    ИМЕНЕМ атрибута прибора (`minAttr`, `optionsAttr`): пределы термостата и
    список входов плеера знает сам прибор (`docs/docs-ha/entity-climate.md`)."""
    kind = rule.get("type", "string")
    if kind == "number":
        bound = lambda key: attributes.get(rule.get(key + "Attr"), rule.get(key))  # noqa: E731
        return number(value, bound("min"), bound("max"))
    if kind == "list":
        items = rule.get("items") or []
        if not isinstance(value, list) or len(value) != len(items):
            raise ValueError(f"Ожидается список из {len(items)} значений")
        return [_checked(item, each, attributes) for item, each in zip(value, items)]
    if not isinstance(value, bool if kind == "boolean" else str):
        raise ValueError("Значение другого типа")
    allowed = attributes.get(rule["optionsAttr"]) or [] if rule.get("optionsAttr") else rule.get("options")
    if allowed is not None and value not in allowed:
        raise ValueError(f"Недопустимое значение {value}")
    return value
