"""Общее для всех операций: отказ, поиск плитки и разбор чисел.

⚠ Отдельным модулем, чтобы классы устройств (`ops_camera`, `ops.py`) не тянули
друг друга ради одного `OpError`. Всё остальное делится ПО КЛАССАМ, а не по
вендорам — правило заказчика, `CLAUDE.md`.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any


class OpError(Exception):
    """A refusal the resident should read, with the status that fits it."""

    def __init__(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        super().__init__(message)
        self.message = message
        self.status = int(status)

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
