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

class ActivityBuckets:
    """Корзины одной камеры. Конвейер камеры (`video_pipeline.py`) держит свой экземпляр; функции модуля
    ниже — тот же ряд по ключу для детекции слушателем (`motion.py`, раздел `recorder`)."""

    def __init__(self) -> None:
        self._buckets: dict[int, float] = {}

    def note(self, at: float, share: float) -> None:
        """Доля изменений кадра в момент `at` (секунды UTC) — в корзину, если больше прежней."""
        bucket = int(at // STEP_S) * STEP_S
        if share > self._buckets.get(bucket, 0.0):
            self._buckets[bucket] = share
        while len(self._buckets) > MAX_BUCKETS:
            del self._buckets[min(self._buckets)]

    def take(self, now: float) -> list[tuple[int, float]]:
        """Закрытые корзины (конец не позже `now`) — по порядку, из памяти вон."""
        done = sorted(b for b in self._buckets if b + STEP_S <= now)
        return [(b, self._buckets.pop(b)) for b in done]

    def pending(self, start: float, stop: float) -> list[tuple[int, float]]:
        """Корзины в памяти (идущая и ещё не записанные) в отрезке — ответ службы их добавляет."""
        return sorted((b, v) for b, v in self._buckets.items() if start - STEP_S < b < stop)


_live: dict[str, ActivityBuckets] = {}


def note(key: str, at: float, share: float) -> None:
    _live.setdefault(key, ActivityBuckets()).note(at, share)


def take(key: str, now: float) -> list[tuple[int, float]]:
    return _live[key].take(now) if key in _live else []


def pending(key: str, start: float, stop: float) -> list[tuple[int, float]]:
    return _live[key].pending(start, stop) if key in _live else []


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
