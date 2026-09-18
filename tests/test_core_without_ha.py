"""ЗАМОК: ядро (`core/`) не знает о Home Assistant и об адаптере над собой.

HA — один из адаптеров дома, а не его основа (`docs/plan-core-without-ha.md` в
менеджере): модули ядра получают среду хозяином (`host.py`) и сущности
источником (`source.py`), а не `hass`, и поднимутся под самостоятельным демоном
без переписывания.

Ядро — это КАТАЛОГ, а не список: всё, что лежит в `core/`, обязано пройти.
Запрещены два хода наружу:
* `homeassistant` (и `voluptuous`, который приезжает с ним);
* импорт уровнем выше (`from ..`) — через адаптер HA пришёл бы транзитивно.
Импорты внутри функций считаются тоже: локальный импорт прячет зависимость, а
не снимает её.

⚠ Модулю ядра понадобился HA — вынеси связку в адаптер (`ha_host.py`,
`ha_source.py`, `coordinator.py`), а не модуль из `core/`.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

CORE = Path(__file__).resolve().parent.parent / "custom_components" / "mega_home" / "core"
FORBIDDEN = {"homeassistant", "voluptuous"}
MODULES = sorted(CORE.glob("*.py"))


def _offences(path: Path) -> list[str]:
    found: list[str] = []
    for node in ast.walk(ast.parse(path.read_text("utf-8"))):
        if isinstance(node, ast.Import):
            found += [a.name for a in node.names if a.name.split(".")[0] in FORBIDDEN]
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and (node.module or "").split(".")[0] in FORBIDDEN:
                found.append(node.module or "")
            elif node.level > 1:
                found.append("." * node.level + (node.module or ""))
    return found


def test_ядро_не_пустое() -> None:
    """Каталог переехал — замок обязан упасть, а не молча проверить ничего."""
    assert len(MODULES) > 20


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_модуль_ядра_не_выходит_наружу(path: Path) -> None:
    assert _offences(path) == [], f"{path.name} тянет наружу ядра"
