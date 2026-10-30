"""ЗАМОК: ядро дома не решает по ДОМЕНУ плитки, что ей можно.

Дом — тонкий шлюз (`docs/plan-thin-gateway.md` менеджера): что плитка умеет, решают приложение и
менеджер, а дом хранит и исполняет описанное. Список доменов в доме — знание, которое меняется с
каждой фичей приложения, и каждое изменение стоит релиза дома. Дважды так и вышло (заказчик
2026-10-30): кадр жильца дом пускал только домену `camera` — у вызывной панели «Камера не найдена»;
правила уведомлений — только `sensor`/`binary_sensor`.

Запрет: сравнение домена плитки (`tile["domain"]`, `tile.get("domain")`, переменная `domain`) в
`core/`. Ключ записи — «плитка есть в составе» (`photo_key_known`, `crop_key_known`), свойство —
полем описания плитки от менеджера.

Исключения — адаптер ИСТОЧНИКА, а не решение о плитке: камера Home Assistant (`camera.*`) отдаёт
кадр и поток штатными путями HA, и это знание о HA, а не о приложении. Новое исключение — сюда, с
причиной; лучше — поле описания плитки.
"""

from __future__ import annotations

import ast
from pathlib import Path

CORE = Path(__file__).resolve().parent.parent / "custom_components" / "mega_home" / "core"

# Файл → сколько сравнений домена в нём законно, и почему.
ALLOWED = {
    # Плитка-камера → сущность HA `camera.*` (защита «чужую камеру не открыть») и прогрев её снимка.
    "ops_camera.py": 2,
    # Адреса кадра и потока камеры HA (`camera_proxy`) в состоянии плитки — путь самого HA.
    "ops.py": 1,
}


def _is_domain(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "domain"
    if isinstance(node, ast.Subscript):
        key = node.slice
        return isinstance(key, ast.Constant) and key.value == "domain"
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "get":
        return bool(node.args) and isinstance(node.args[0], ast.Constant) and node.args[0].value == "domain"
    return False


def _domain_checks(path: Path) -> int:
    count = 0
    for node in ast.walk(ast.parse(path.read_text("utf-8"))):
        if isinstance(node, ast.Compare) and any(_is_domain(part) for part in [node.left, *node.comparators]):
            count += 1
    return count


def test_ядро_не_решает_по_домену_плитки() -> None:
    found = {path.name: n for path in sorted(CORE.glob("*.py")) if (n := _domain_checks(path))}
    extra = {name: n for name, n in found.items() if n > ALLOWED.get(name, 0)}
    assert not extra, (
        f"сравнение домена плитки в ядре дома: {extra}. Что плитке можно, решает приложение — "
        "ключ записи «плитка есть в составе», свойство — полем описания от менеджера (шапка замка)"
    )


def test_замок_видит_сравнение_домена(tmp_path: Path) -> None:
    sample = tmp_path / "sample.py"
    sample.write_text('ok = tile.get("domain") == "camera" or tile["domain"] in {"a"} or domain != "b"\n', "utf-8")
    assert _domain_checks(sample) == 3
