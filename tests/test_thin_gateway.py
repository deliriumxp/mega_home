"""ЗАМОК: в шлюзе нет ТОЛКОВАНИЯ ответов регистратора.

⚠ Этот тест не проверяет работу — он не даёт поселить в доме арифметику дат и
шкал. Причина та же, ради чего интеграцию делали тонкой: её код — единственное,
что не обновляется само, и каждая строка толкования стоит HACS-обновления и
перезапуска Home Assistant на КАЖДОМ объекте (`docs/plan-thin-integration.md`).

Как это выглядит на практике (2026-09-12, за один вечер): в шлюз приехали
`day_bounds` (окно = сутки метки), `trassir_now_us` (наши часы в чужой шкале) и
правило «эпоха — не день». Регистратор на это отвечал ЧЕСТНО (`1970-01-01` — его
«не знаю»), а «врал» уже наш код: он взял ответ за день и нарисовал жильцу
календарь 1970 года. Из-за этого вышли 0.2.46 и 0.2.47 — то есть две правки,
которых в доме не должно было быть вовсе.

**Правило.** Дом владеет СЕССИЕЙ (токен, поток, пинг, уборка, «одна команда
`play` на соединение») и ТРАНСПОРТОМ. Всё, что разбирает ответ регистратора —
даты, шкалы, сутки, поля — живёт в бандле, который обновляется сам.

**Что можно:** монотонное время для пауз и сторожей (`time.monotonic`) — это
физика сессии, а не шкала архива.

⚠ `DEBT` — долги, приехавшие ДО замка. Числа точные и МОГУТ ТОЛЬКО УМЕНЬШАТЬСЯ:
замок требует поправить таблицу при любом изменении, и там же сказано, почему.
Снимаются они широкой дверью (описанные вызовы регистратора вместо словаря
операций) — тогда в доме не остаётся ни одной даты.
"""

from __future__ import annotations

from pathlib import Path

MODULES = Path(__file__).resolve().parent.parent / "custom_components" / "mega_home" / "core"

# Файлы, которые говорили с регистратором. Толкование ищем только здесь: в
# остальном доме даты — своё дело Home Assistant.
# ⚠ ПУСТО, и пусто должно остаться. Модули, которые здесь стояли
# (`trassir*.py`, `gateway.py`), снесены тонким шлюзом целиком
# (`docs/plan-thin-gateway.md`, «Что сносится из дома»): видео объекта теперь
# идёт транспортом `connect` в бандл, дом ответы регистратора не разбирает
# вовсе. Возвращать сюда имя файла — значит возвращать в дом вендорский модуль.
SCANNED: tuple[str, ...] = ()

# Что считать толкованием: разбор и счёт дат/шкалы регистратора.
FORBIDDEN = (
    "calendar.",
    "timegm",
    "strptime",
    "datetime.now",
    "86_400",
)

# Долги: файл → шаблон → сколько раз. Точное число — чтобы любое изменение
# требовало заглянуть сюда и сказать, зачем оно.
#
# ⚠ Что их держит (2026-09-12): сборки, которые окно шкалы ещё не шлют, и дома
# без универсальной двери (к ним бандл возвращается сам). Оба пути уходят
# следующим выпуском ЗА тем, где бандл перешёл на дверь: правило выпуска
# запрещает снимать замену и заменяемое разом. Тогда эти числа станут нулями.
# ⚠ ПУСТО, и пусто должно остаться. Разбор дат и шкал жил здесь для двух путей:
# сборок, которые окно шкалы не шлют, и домов без универсальной двери. И то и
# другое ушло выпуском 0.2.47 — вместе с `_segments`, `_outside`, `day_bounds`,
# `trassir_now_us` и `_day_start_of`. Появится число — значит в дом возвращают
# толкование, и это почти наверняка нужно не здесь, а в бандле.
DEBT: dict[str, dict[str, int]] = {}


def _code(path: Path) -> str:
    """Только КОД: без комментариев и строк-объяснений.

    ⚠ Иначе замок считал бы упоминания в объяснениях: «день переводим тем же
    `timegm`» — это текст про толкование, а не толкование. Правка комментария
    роняла бы замок и заставляла трогать числа просто так.
    """
    lines: list[str] = []
    in_doc = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.count('"""') % 2:
            in_doc = not in_doc
            continue
        if in_doc or line.strip().startswith("#"):
            continue
        lines.append(line)
    return "\n".join(lines)


def test_в_шлюзе_нет_толкования_шкалы() -> None:
    for name in SCANNED:
        source = _code(MODULES / name)
        allowed = DEBT.get(name, {})
        for pattern in FORBIDDEN:
            hits = source.count(pattern)
            assert hits == allowed.get(pattern, 0), (
                f"{name}: «{pattern}» встречается {hits} раз, а в замке {allowed.get(pattern, 0)}. "
                "Разбор ответов регистратора — дело БАНДЛА, а не дома: правка в доме стоит "
                "HACS-обновления и перезапуска Home Assistant на каждом объекте "
                "(docs/plan-thin-integration.md, «Широкая дверь»). Убрал толкование — "
                "уменьши число в DEBT; добавил — почти наверняка оно не нужно здесь вовсе."
            )


# =============================================================================
# ЗАМКИ ТОНКОГО ШЛЮЗА (docs/plan-thin-gateway.md, раздел «Замки»)
#
# ⚠ Дом — транспорт, процессы и хранилище: без вендора, без перечня
# разрешённого, без прикладной логики (решение заказчика 2026-09-19). Пять
# замков ниже фиксируют состав, к которому идёт переделка; на момент коммита
# ожидаемо КРАСНЫЕ — `core/` ещё несёт снесённое (`docs/plan-thin-gateway.md`,
# «Что сносится из дома и куда переезжает»).
# =============================================================================

import ast
import re

CORE = Path(__file__).resolve().parent.parent / "custom_components" / "mega_home" / "core"
CORE_MODULES = sorted(CORE.glob("*.py"))

# ⚠ Список только растёт: новое вендорское или предметное слово — сюда, а не
# терпим в core/. `session` и `camera` намеренно НЕ здесь: HTTP-сессия
# (`aiohttp.ClientSession`) — транспорт, `camera` — домен сущностей источника HA и
# кропы плиток (части B, F). Вход-по-описанию ловят `login`, `challenge`.
# ⚠ Имён хэшей (`sha1`, `sha256`, `md5`) здесь тоже НЕТ: они законны в хранилище
# (имена файлов фонов и кропов — `sha1(id)`, сверка бандла — `sha256`). Запрет
# `sha1` в 0.4.0 заставил агента переименовать файлы на диске через `blake2b`,
# и все фоны комнат и кропы на объектах «пропали» (живой объект 2026-09-20).
# Вычисления ВХОДА ловит замок 4 (`templating` без `hashlib`), а не слова.
FORBIDDEN_WORDS = (
    "trassir", "akuvox", "hikvision", "dahua", "onvif", "door", "archive",
    "nonce", "deny", "manageronly", "login", "challenge",
)


def test_ядро_без_вендорских_слов() -> None:
    """Замок 1 плана. Исключения — `sip_*.py` (`panel`/`call`) и `digest.py`
    (стандарт HTTP RFC 7616, не вендор: `md5`/`sha256`/`nonce` там законны)."""
    offences: list[str] = []
    for path in CORE_MODULES:
        if path.name.startswith("sip_") or path.name == "digest.py":
            continue
        text = path.read_text("utf-8").lower()
        for word in FORBIDDEN_WORDS:
            if word in text:
                offences.append(f"{path.name}: {word}")
    assert offences == []


def _string_eq_rhs(node: ast.Compare, name_left: str) -> str | None:
    """Значение `<name_left> == "..."` из сравнения AST."""
    if (
        isinstance(node.left, ast.Name)
        and node.left.id == name_left
        and len(node.ops) == 1
        and isinstance(node.ops[0], ast.Eq)
        and len(node.comparators) == 1
        and isinstance(node.comparators[0], ast.Constant)
        and isinstance(node.comparators[0].value, str)
    ):
        return node.comparators[0].value
    return None


def _startswith_arg(node: ast.Call) -> str | None:
    """Строка из вызова `op.startswith("...")`/`kind.startswith("...")` — так
    диспетчеризуется `stream.*` (`link.py`), а не сравнением `==`."""
    if (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "startswith"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in ("op", "kind")
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return node.args[0].value
    return None


def _ops_from(path: Path) -> set[str]:
    """`op == "..."` и `op.startswith("...")` из кода — не список из головы."""
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text("utf-8"))):
        if isinstance(node, ast.Compare):
            value = _string_eq_rhs(node, "op")
            if value is not None:
                found.add(value)
        elif isinstance(node, ast.Call):
            value = _startswith_arg(node)
            if value is not None:
                found.add(value + "*")
    return found


def _routes_from(path: Path) -> set[str]:
    """Маршруты `path == "api/..."` и `path.startswith("api/...")` из кода."""
    text = path.read_text("utf-8")
    found: set[str] = set()
    for match in re.finditer(r'path\s*==\s*"(api/[^"]*)"', text):
        found.add(match.group(1))
    for match in re.finditer(r'path\.startswith\("(api/[^"]*)"\)', text):
        found.add(match.group(1) + "*")
    return found


def test_операции_канала_и_маршруты_api_заперты() -> None:
    """Замок 2 плана: список операций и маршрутов равен составу частей A–G.

    Читает код (`core/ops.py`, `link.py`, `core/relay_api.py`), а не список из
    головы. Новая операция или маршрут — только с доказательством, что это
    транспорт, процесс или хранилище (не перечень «что уже умеем»).
    """
    link_py = CORE.parent / "link.py"
    ops = _ops_from(CORE / "ops.py") | (_ops_from(link_py) if link_py.exists() else set())
    routes = _routes_from(CORE / "relay_api.py")

    locked_ops = {
        "config", "states", "command", "scenario", "connect", "http", "probe", "scan",
        "self-update", "watch", "stream.*",
        # ⚠ `intercom` — ПРОЦЕСС дома, а не перечень умений: вызовами своего
        # Asterisk дом управляет по ARI с петли, паролем, который сам и
        # сгенерировал. Ни `connect`, ни описанный вызов туда не дотянутся — и
        # не должны: ARI это полный контроль над вызовами. Действие ровно одно
        # («отклонить»), и второе сюда не попадёт без такого же доказательства.
        "intercom",
    }
    locked_routes = {
        "api/config", "api/states", "api/command", "api/scenario", "api/connect",
        "api/asset/*", "api/photo*", "api/crop*",
        # кадр сущности camera.* источника — часть B, не вендор
        "api/camera-frame/*",
        # хранилище событий устройств — часть F
        "api/device-events",
        # отбой идущего вызова домофонии — процесс дома (см. `locked_ops`)
        "api/intercom",
    }

    assert ops == locked_ops
    assert routes == locked_routes


def test_реестр_служб_транспорта_пуст_до_запуска_процессов() -> None:
    """Замок 3 плана: нет списка loopback-портов в коде, реестр наполняют процессы."""
    from mega_home.core import services as services_mod  # noqa: PLC0415

    assert not hasattr(services_mod, "LOCAL_SERVICES")
    assert dict(services_mod.REGISTRY) == {}


def test_шаблоны_без_вычисления_входа() -> None:
    """Замок 4 плана: `templating` не считает хэши и время, только подставляет."""
    forbidden = {"hashlib", "base64", "time"}
    offences: list[str] = []
    for node in ast.walk(ast.parse((CORE / "templating.py").read_text("utf-8"))):
        if isinstance(node, ast.Import):
            offences += [a.name for a in node.names if a.name.split(".")[0] in forbidden]
        elif isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] in forbidden:
            offences.append(node.module or "")
    assert offences == []


def test_размер_ядра_не_растёт_прикладной_логикой() -> None:
    """Замок 5 плана: КОД `core/*.py` ≤ 4 800 строк суммарно.

    ⚠ Считается только код (`_code`: без докстрингов, комментариев и пустых
    строк). Замок на полный объём заставил однажды вырезать из нетронутых
    модулей причины решений ради лимита — комментарий у строки и есть место,
    где живёт «почему» (`CLAUDE.md` менеджера), резать его нельзя. Планка —
    состав A–H после сноса (4 404 строки на 2026-09-19) плюс запас на транспорт.
    """
    total = sum(len([l for l in _code(path).splitlines() if l.strip()]) for path in CORE_MODULES)
    assert total <= 4800
