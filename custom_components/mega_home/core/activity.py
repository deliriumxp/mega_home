"""Сила движения камеры по 10 с — раскраска ленты архива, как у Frigate (решение заказчика 2026-10-24).

Детектор дома (`motion.py`) отмечает долю изменений кадра, пока идёт окно движения; здесь — максимум
по корзине 10 с. Запись (`recorder.py`) раз в минуту забирает закрытые корзины своей камеры и дописывает
их в `<день>/activity.csv` хранилища; служба (`recorder_api.py`, `/activity`) отдаёт ряд за отрезок.

⚠ Ключ — имя доступа камеры в описании (тот же, что у её событий), а не источник: у камеры, чьё движение приходит от неё самой,
детекция дома идёт ради людей (`motionEvents: false`), и силу движения считает она же. Камера без
детекции дома ряда не имеет — лента рисует окна движения, как прежде.
⚠ Только корзины с движением: сутки тишины не стоят ни строки.
⚠ Память ограничена (`MAX_BUCKETS`): запись может не забирать ряд (хранилище отвалилось, камера не
пишется), а детектор работает всё равно.
"""

from __future__ import annotations

from pathlib import Path

STEP_S = 10
FILE = "activity.csv"
# Час корзин на камеру — дольше запись не забирать не может (сторож раз в минуту).
MAX_BUCKETS = 360

_live: dict[str, dict[int, float]] = {}


def note(key: str, at: float, share: float) -> None:
    """Доля изменений кадра в момент `at` (секунды UTC) — в корзину, если больше прежней."""
    buckets = _live.setdefault(key, {})
    bucket = int(at // STEP_S) * STEP_S
    if share > buckets.get(bucket, 0.0):
        buckets[bucket] = share
    while len(buckets) > MAX_BUCKETS:
        del buckets[min(buckets)]


def take(key: str, now: float) -> list[tuple[int, float]]:
    """Закрытые корзины камеры (конец не позже `now`) — по порядку, из памяти вон."""
    buckets = _live.get(key) or {}
    done = sorted(b for b in buckets if b + STEP_S <= now)
    return [(b, buckets.pop(b)) for b in done]


def pending(key: str, start: float, stop: float) -> list[tuple[int, float]]:
    """Корзины в памяти (идущая и ещё не записанные) в отрезке — ответ службы их добавляет."""
    return sorted((b, v) for b, v in (_live.get(key) or {}).items() if start - STEP_S < b < stop)


def append(day_dir: Path, rows: list[tuple[int, float]]) -> None:
    """Дописать корзины в ряд суток (в исполнителе: файловая система)."""
    day_dir.mkdir(parents=True, exist_ok=True)
    with (day_dir / FILE).open("a", encoding="ascii") as handle:
        handle.writelines(f"{b},{v:.4f}\n" for b, v in rows)


def read(day_dir: Path, start: float, stop: float) -> list[tuple[int, float]]:
    """Корзины суток в отрезке; файла нет или строка битая — пропуск (в исполнителе)."""
    out: list[tuple[int, float]] = []
    try:
        lines = (day_dir / FILE).read_text(encoding="ascii", errors="ignore").splitlines()
    except OSError:
        return out
    for line in lines:
        head, _, tail = line.partition(",")
        try:
            bucket, value = int(head), float(tail)
        except ValueError:
            continue
        if start - STEP_S < bucket < stop:
            out.append((bucket, value))
    return out
