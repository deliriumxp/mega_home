"""Повреждённая запись: кусок, который не читается (ни кадра видео), в архив не кладётся — на его месте отметка.

Зачем. Битый кусок в архиве — «запись была», а смотреть нечего: сессия просмотра на нём встаёт. Дом его не чинит и не
перепаковывает (заказчик 2026-10-30: «дом начинает заниматься не своим делом»): проверил первым пакетом, выбросил,
отметил участок. Жилец видит его на ленте красным («Запись повреждена»), инсталлятор — счётчиком в состоянии записи.
Отметки лежат в каталоге дня (`damaged.jsonl`, строка `[начало, конец]` в секундах UTC) и уходят уборкой вместе с днём.
"""

from __future__ import annotations

import json
from pathlib import Path

FILE = "damaged.jsonl"


def append(day: Path, start: float, stop: float) -> None:
    day.mkdir(parents=True, exist_ok=True)
    with (day / FILE).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps([round(start, 3), round(stop, 3)]) + "\n")


def read(days: list[Path], start: float, stop: float) -> list[list[int]]:
    """Повреждённые участки в окне, мс; строка, которую не разобрать, пропускается."""
    out: list[list[int]] = []
    for day in days:
        path = day / FILE
        for line in path.read_text(encoding="utf-8").splitlines() if path.is_file() else []:
            try:
                begin, end = (float(v) for v in json.loads(line))
            except (ValueError, TypeError):
                continue
            if end > start and begin < stop:
                out.append([int(begin * 1000), int(end * 1000)])
    return sorted(out)
