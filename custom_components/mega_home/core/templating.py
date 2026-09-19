"""Подстановка `{имя}` в строках описания слушателя (`listeners_out.py`).

⚠ Только подстановка — никаких вычислений входа (`docs/plan-thin-gateway.md`,
«Слушатели»). Вход, требующий хэша или разового случайного значения, держит
МЕНЕДЖЕР через `connect … stream: true`, а не дом: посчитать вход и часы
вызова может только тот, кто знает пароль, а это больше не дом.
"""

from __future__ import annotations

import re
from typing import Any

_PLACEHOLDER = re.compile(r"\{([^{}]+)\}")

def render(template: str, values: dict[str, Any]) -> str:
    """Подставить значения; незнакомое имя оставляет место как есть."""

    def one(match: re.Match[str]) -> str:
        name = match.group(1)
        return str(values[name]) if name in values else match.group(0)

    return _PLACEHOLDER.sub(one, template)

def pick(data: Any, path: str) -> str:
    """Поле ответа по пути через точку (`data.token`, `result.id`, `list.0.id`)."""
    current = data
    for step in path.split(".") if path else []:
        if isinstance(current, dict):
            current = current.get(step)
        elif isinstance(current, list) and step.isdigit() and int(step) < len(current):
            current = current[int(step)]
        else:
            return ""
    return str(current) if isinstance(current, (str, int, float)) and not isinstance(current, bool) else ""
