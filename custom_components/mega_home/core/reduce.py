"""Обработка данных в доме: записи ответа → отрезки времени (`reduce` у `connect`).

Решение заказчика 2026-10-19 (`docs/superpowers/specs/2026-10-19-home-reduce-design.md`
менеджера): дом выполняет математику над данными источника и отдаёт наружу готовый
результат — килобайты вместо мегабайт истории. Ось «потребление ответа» двери
(`docs/home-gateway.md`): рядом с «данные коду» и «ресурс браузеру» — «обработанные
данные коду». Не операция и не маршрут — поле того же описания `connect`.

⚠ Дом не знает, ЧТО сворачивает: путь к записям, к метке времени и к значениям
приходит описанием, смысл ряда толкует вызывающий. Предметное слово здесь — то
самое толкование, которое чинится только выпуском (`docs/plan-thin-integration.md`
менеджера), замок — `tests/test_reduce.py`.

⚠ Набор свёрток ЗАКРЫТ и заложен полным сразу: новая — код дома и релиз, а новая
комбинация имеющихся — нет (решение заказчика того же дня).

Смысл (решения описания, менять их значит менять контракт):
- запись действует с её метки до метки следующей записи группы, последняя — до `to`;
  раньше первой записи значения нет;
- отсутствующий путь и `null` — одно и то же «значения нет»: оно не совпадает ни с
  `in`, ни с `notIn` и не входит в числа;
- отрезок — полуинтервал `[начало, конец)`, последний кончается на `to`; доли и
  среднее — по ПОКРЫТОМУ времени отрезка;
- `start`/`end` — значение в МОМЕНТ начала/конца, включая запись ровно на нём.
"""

from __future__ import annotations

import json
import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .ops_base import OpError
from .templating import dig as _resolve

FOLDS = ("start", "end", "mean", "min", "max", "share", "count", "changes", "delta", "sum")
TIME_FORMATS = ("iso", "s", "ms")
# Сутки по минуте — 1 440 отрезков: мельче суточному графику не нужно, а предел
# держит ответ в килобайтах при любом описании.
MAX_BUCKETS = 1500
MAX_SERIES = 32
# Группа — ряд одного источника (у истории HA — сущность); больше в одном
# ответе — уже не «график», а выгрузка, ей место в сырой форме `connect`.
MAX_GROUPS = 64
# Дробные результаты (среднее, доля) — до 6 знаков: длинный хвост float не
# несёт смысла, а ответ растёт вдвое.
DIGITS = 6


@dataclass(frozen=True)
class Series:
    name: str
    path: tuple[str, ...]
    fold: str
    match: tuple[Any, ...] | None
    negate: bool


@dataclass(frozen=True)
class Plan:
    rows: tuple[str, ...]
    time_path: tuple[str, ...]
    time_format: str
    group: tuple[str, ...] | None
    start: int
    end: int
    step: int
    count: int
    series: tuple[Series, ...]


def parse(spec: Any) -> Plan:
    """Разобрать описание ДО вызова адресата: ошибка описания — отказ сразу, а не после
    того, как источник отдал мегабайты."""
    if not isinstance(spec, dict):
        raise OpError("reduce — объект")
    time = spec.get("time") if isinstance(spec.get("time"), dict) else {}
    time_format = str(time.get("format") or "iso")
    if time_format not in TIME_FORMATS:
        raise OpError(f"reduce.time.format — {', '.join(TIME_FORMATS)}")
    if not isinstance(time.get("path"), str) or not time["path"]:
        raise OpError("reduce.time.path не указан")
    start, end, step = (_int(spec.get(key), f"reduce.{key}") for key in ("from", "to", "step"))
    if step <= 0:
        raise OpError("reduce.step — больше нуля")
    if end <= start:
        raise OpError("reduce.to — позже from")
    count = -(-(end - start) // step)
    if count > MAX_BUCKETS:
        raise OpError(f"reduce: отрезков больше {MAX_BUCKETS}")
    raw_series = spec.get("series")
    if not isinstance(raw_series, dict) or not raw_series:
        raise OpError("reduce.series — непустой объект")
    if len(raw_series) > MAX_SERIES:
        raise OpError(f"reduce: рядов больше {MAX_SERIES}")
    group = spec.get("groupBy")
    if group is not None and not isinstance(group, str):
        raise OpError("reduce.groupBy — путь строкой")
    return Plan(
        rows=_path(spec.get("rows", ""), "reduce.rows"),
        time_path=_path(time["path"], "reduce.time.path"),
        time_format=time_format,
        group=_path(group, "reduce.groupBy") if group else None,
        start=start,
        end=end,
        step=step,
        count=count,
        series=tuple(_series(name, item) for name, item in raw_series.items()),
    )


def apply(plan: Plan, raw: bytes) -> dict[str, Any] | None:
    """Свернуть тело ответа; None — тело не JSON (вызывающий получит его как есть).

    ⚠ Синхронная и тяжёлая: зовётся в исполнителе (`connect._http`), 8 МБ JSON в
    цикле событий — пауза всего Home Assistant.
    """
    try:
        document = json.loads(raw)
    except ValueError:
        return None
    rows = _resolve(document, plan.rows)
    if not isinstance(rows, list):
        raise OpError("reduce.rows: массива записей по этому пути нет")
    records: list[Any] = []
    for item in rows:
        records.extend(item if isinstance(item, list) else [item])
    groups: dict[str, list[tuple[float, Any]]] = {}
    skipped = 0
    for record in records:
        at = _time(_resolve(record, plan.time_path), plan.time_format)
        if at is None:
            skipped += 1
            continue
        key = _resolve(record, plan.group) if plan.group else None
        groups.setdefault("" if key is None else str(key), []).append((at, record))
        if len(groups) > MAX_GROUPS:
            raise OpError(f"reduce: групп больше {MAX_GROUPS}")
    out: dict[str, dict[str, list[Any]]] = {}
    for key, items in groups.items():
        items.sort(key=lambda item: item[0])
        times = [at for at, _ in items]
        # Один путь — один проход по записям, сколько бы рядов его ни сворачивали
        # (значение и доли разных вариантов того же поля — частый случай).
        by_path = {s.path: [_resolve(record, s.path) for _, record in items] for s in plan.series}
        out[key] = {s.name: _fold(plan, s, times, by_path[s.path]) for s in plan.series}
    return {
        "from": plan.start,
        "to": plan.end,
        "step": plan.step,
        "n": plan.count,
        "rows": len(records),
        "skipped": skipped,
        "groups": out,
    }


def _fold(plan: Plan, s: Series, times: list[float], values: list[Any]) -> list[Any]:
    out: list[Any] = []
    # Последнее непустое значение СТРОГО до записи i — только для `changes`.
    before: list[Any] = []
    last: Any = None
    if s.fold == "changes":
        for value in values:
            before.append(last)
            if value is not None:
                last = value
    for k in range(plan.count):
        b0 = plan.start + k * plan.step
        b1 = min(b0 + plan.step, plan.end)
        # Записи, действующие в отрезке: последняя на его начало и все внутри.
        lo = bisect_right(times, b0)
        hi = bisect_left(times, b1)
        # Записи, СТОЯЩИЕ в отрезке `[b0, b1)`, — для счёта и суммы.
        first = bisect_left(times, b0)
        if s.fold == "start":
            out.append(values[lo - 1] if lo else None)
        elif s.fold == "end":
            at_end = bisect_right(times, b1)
            out.append(values[at_end - 1] if at_end else None)
        elif s.fold == "delta":
            at_end = bisect_right(times, b1)
            a = _number(values[lo - 1]) if lo else None
            b = _number(values[at_end - 1]) if at_end else None
            out.append(_round(b - a) if a is not None and b is not None else None)
        elif s.fold == "count":
            out.append(sum(1 for i in range(first, hi) if values[i] is not None))
        elif s.fold == "sum":
            numbers = [_number(values[i]) for i in range(first, hi)]
            out.append(_round(sum(n for n in numbers if n is not None)))
        elif s.fold == "changes":
            changes = 0
            prev = before[first] if first < len(before) else last
            for i in range(first, hi):
                if values[i] is None:
                    continue
                if prev is not None and values[i] != prev:
                    changes += 1
                prev = values[i]
            out.append(changes)
        else:
            out.append(_over_time(s, _pieces(times, values, lo, hi, b0, b1), b1 - b0))
    return out


def _pieces(
    times: list[float], values: list[Any], lo: int, hi: int, b0: int, b1: int
) -> list[tuple[float, Any]]:
    """(длительность, значение) кусков отрезка; до первой записи кусков нет."""
    starts: list[tuple[float, Any]] = [(b0, values[lo - 1])] if lo else []
    starts += [(times[i], values[i]) for i in range(lo, hi)]
    return [
        (((starts[j + 1][0] if j + 1 < len(starts) else b1) - at), value)
        for j, (at, value) in enumerate(starts)
    ]


def _over_time(s: Series, pieces: list[tuple[float, Any]], covered: int) -> Any:
    if s.fold == "share":
        if not pieces or covered <= 0:
            return None
        hit = sum(span for span, value in pieces if value is not None and _matches(value, s.match) != s.negate)
        return _round(hit / covered)
    numbers = [(span, n) for span, value in pieces if span > 0 and (n := _number(value)) is not None]
    if not numbers:
        return None
    if s.fold == "min":
        return min(n for _, n in numbers)
    if s.fold == "max":
        return max(n for _, n in numbers)
    return _round(sum(span * n for span, n in numbers) / sum(span for span, _ in numbers))


def _matches(value: Any, match: tuple[Any, ...] | None) -> bool:
    """Строгое равенство JSON-значений: `true` не равно `1`, как в Python было бы."""
    return any(
        value == item and (isinstance(value, bool) == isinstance(item, bool))
        for item in match or ()
    )


def _series(name: Any, item: Any) -> Series:
    if not isinstance(item, dict):
        raise OpError(f"reduce.series.{name} — объект")
    fold = str(item.get("fold") or "")
    if fold not in FOLDS:
        raise OpError(f"reduce.series.{name}.fold — {', '.join(FOLDS)}")
    if not isinstance(item.get("path"), str):
        raise OpError(f"reduce.series.{name}.path не указан")
    match: tuple[Any, ...] | None = None
    negate = False
    if fold == "share":
        has_in, has_out = isinstance(item.get("in"), list), isinstance(item.get("notIn"), list)
        if has_in == has_out:
            raise OpError(f"reduce.series.{name}: у share ровно одно из in и notIn — списком")
        match = tuple(item["in"] if has_in else item["notIn"])
        negate = has_out
    return Series(str(name), _path(item["path"], f"reduce.series.{name}.path"), fold, match, negate)


def _path(value: Any, what: str) -> tuple[str, ...]:
    if not isinstance(value, str):
        raise OpError(f"{what} — путь строкой")
    return tuple(value.split(".")) if value else ()


def _time(value: Any, time_format: str) -> float | None:
    """Метка → мс эпохи; неразборчивая — None. ISO без смещения — тоже None: чьи
    это часы, по строке не узнать."""
    if time_format == "iso":
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed.timestamp() * 1000 if parsed.tzinfo is not None else None
    number = _number(value)
    if number is None:
        return None
    return number * 1000 if time_format == "s" else number


def _number(value: Any) -> float | None:
    """Число записи. ⚠ Строка с числом — тоже число: состояние сенсора у HA — строка
    (`"21.5"`), и среднее по нему — ровно та математика, ради которой это всё."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            return None
        return number if math.isfinite(number) else None
    return None


def _int(value: Any, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise OpError(f"{what} — число (мс эпохи)")
    return int(value)


def _round(value: float) -> float:
    return round(value, DIGITS)
