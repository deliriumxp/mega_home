"""Язык условий правил дома — ОДИН на уведомления (`alerts.py`) и сторожа (`agent.py`).

Зачем (ревью заказчика 2026-10-30, «универсально, чтобы не выпускать релизы из-за мелочей»). У
каждого движка правил был свой словарь: уведомление знало `on/off/eq/gt/lt` над ОДНИМ состоянием
сущности, сторож — `ok/status/json/equals/contains` над ответом пробы. Любое новое условие — «между»,
«не равно», атрибут вместо состояния, поле ответа больше/меньше — стоило релиза дома.

Условие — данные: `{path, op, value}`.
- `path` — путь к полю через точку по дереву данных правила (состояние и атрибуты сущности, ответы
  заданий сторожа); номер — элемент списка; строка с JSON по пути раскрывается сама (тело ответа).
- `op` — сравнение, ниже; `value` — с чем.

⚠ Смысла полей модуль не знает: какие пути и значения осмысленны, решает тот, кто правило собрал
(менеджер, приложение). Новое СРАВНЕНИЕ — правка здесь; новое ПОЛЕ — никогда.
"""

from __future__ import annotations

import json
import math
from typing import Any

# Сравнения. Число — если обе стороны числа (строка «21.5» — число), иначе строки.
OPS = ("eq", "ne", "gt", "ge", "lt", "le", "between", "contains", "in", "exists", "on", "off")
_NUMERIC = ("gt", "ge", "lt", "le")
MAX_PATH = 200
_MISSING = object()


def number(value: Any) -> float | None:
    """Конечное число или `None`. ⚠ `nan`/`inf` — не число: NaN-порог не срабатывает никогда, а
    хранилище HA пишет его как `null`, и после перезапуска правило молча пропадало бы."""
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def value_at(tree: Any, path: str) -> Any:
    """Поле дерева по пути через точку; нет поля — `_MISSING` (отличается от `None` значения)."""
    node = tree
    for part in [p for p in str(path).split(".") if p != ""]:
        if isinstance(node, str):
            try:
                node = json.loads(node)
            except ValueError:
                return _MISSING
        if isinstance(node, dict):
            if part not in node:
                return _MISSING
            node = node[part]
        elif isinstance(node, list) and part.lstrip("-").isdigit() and -len(node) <= int(part) < len(node):
            node = node[int(part)]
        else:
            return _MISSING
    return node


def valid(cond: Any) -> dict[str, Any] | None:
    """Условие в нашей форме (значение приведено) или `None` — испорченное правило не исполняется."""
    if not isinstance(cond, dict) or cond.get("op") not in OPS:
        return None
    path = cond.get("path", "")
    if not isinstance(path, str) or len(path) > MAX_PATH:
        return None
    op, value = cond["op"], cond.get("value")
    if op in _NUMERIC:
        value = number(value)
        if value is None:
            return None
    elif op == "between":
        low, high = (number(v) for v in value) if isinstance(value, list) and len(value) == 2 else (None, None)
        if low is None or high is None:
            return None
        value = [min(low, high), max(low, high)]
    elif op in ("eq", "ne"):
        if isinstance(value, bool) or not isinstance(value, (str, int, float)) or value == "":
            return None
    elif op == "contains":
        if not isinstance(value, str) or not value:
            return None
    elif op == "in":
        if not isinstance(value, list) or not value or not all(isinstance(v, (str, int, float)) for v in value):
            return None
    else:
        value = None
    return {"path": path, "op": op, **({"value": value} if value is not None else {})}


def _same(actual: Any, wanted: Any) -> bool:
    a, w = number(actual), number(wanted)
    return a == w if a is not None and w is not None else str(actual) == str(wanted)


def holds(tree: Any, cond: dict[str, Any]) -> bool:
    """Выполнено ли условие (уже проверенное `valid`) на дереве данных."""
    op, wanted = cond["op"], cond.get("value")
    actual = value_at(tree, cond.get("path", ""))
    if op == "exists":
        return actual is not _MISSING and actual is not None
    if actual is _MISSING:
        return False
    if op == "on":
        return actual == "on" or actual is True
    if op == "off":
        return actual == "off" or actual is False
    if op == "eq":
        return _same(actual, wanted)
    if op == "ne":
        return not _same(actual, wanted)
    if op == "in":
        return any(_same(actual, item) for item in wanted)
    if op == "contains":
        return str(wanted) in (actual if isinstance(actual, str) else json.dumps(actual, ensure_ascii=False))
    value = number(actual)
    if value is None:
        return False
    if op == "between":
        return wanted[0] <= value <= wanted[1]
    return {"gt": value > wanted, "ge": value >= wanted, "lt": value < wanted, "le": value <= wanted}[op]
