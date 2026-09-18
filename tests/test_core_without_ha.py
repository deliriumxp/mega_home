"""ЗАМОК: модули ядра не знают о Home Assistant.

HA — один из адаптеров дома, а не его основа (`docs/plan-core-without-ha.md` в
менеджере): модули ниже получают среду хозяином (`host.py`), а не `hass`, и
поднимутся под самостоятельным демоном без переписывания.

Проверяется ЗАМЫКАНИЕ, а не только прямой импорт: модуль списка, импортирующий
наш модуль вне списка, тянет HA через него (так `probe.py` брал `OpError` из
`ops.py`). Импорты внутри функций считаются тоже — локальный импорт прячет
зависимость, а не снимает её.

⚠ Список ТОЛЬКО РАСТЁТ. Модуль выпал — значит в него вернулся `homeassistant`:
вынеси связку в `__init__.py`/`coordinator.py`/`ha_host.py`, а не вычёркивай.
"""

from __future__ import annotations

import ast
from pathlib import Path

MODULES = Path(__file__).resolve().parent.parent / "custom_components" / "mega_home"

CORE = {
    "agent",
    "api",
    "assets",
    "bundle",
    "const",
    "crops",
    "gateway",
    "go2rtc_embed",
    "host",
    "imaging",
    "ops_base",
    "photos",
    "probe",
    "scan",
    "sip_bridge",
    "sip_calls",
    "sip_config",
    "stream",
    "trassir_archive",
    "trassir_client",
}


def _imports(name: str) -> tuple[set[str], set[str]]:
    """Внешние корни и свои модули, которые импортирует файл."""
    tree = ast.parse((MODULES / f"{name}.py").read_text("utf-8"))
    external: set[str] = set()
    local: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            external.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                external.add((node.module or "").split(".")[0])
            elif node.module:
                local.add(node.module.split(".")[0])
            else:
                local.update(alias.name for alias in node.names)
    return external, local


def test_ядро_не_импортирует_homeassistant() -> None:
    bad = sorted(name for name in CORE if "homeassistant" in _imports(name)[0])
    assert bad == [], f"в модулях ядра появился homeassistant: {bad}"


def test_ядро_замкнуто() -> None:
    leaks = {
        name: sorted(_imports(name)[1] - CORE)
        for name in sorted(CORE)
        if _imports(name)[1] - CORE
    }
    assert leaks == {}, f"модули ядра тянут модули вне ядра: {leaks}"
