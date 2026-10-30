"""События камеры — метки НА ЗАПИСИ: окно движения, его ярлыки (человек) и кадр, у хранилища рядом с кусками.

Решение заказчика 2026-10-29: «события должны быть завязаны на видео — нет смысла показывать их, если
архива нет». До этого у события, его кадра и записи было три срока жизни: журнал устройств (500 на
камеру), вложения (`event_files.py`, 300 файлов на ВЕСЬ дом) и запись (`retainDays`). На четырёх камерах
вложения вытеснялись за часы, а журнал продолжал говорить «кадр есть» — чёрные заглушки в ленте; события
старше записи открывали «запись не велась» (объект .90.250).

Как у Frigate: событие — строка рядом с записью и её снимок, уходят уборкой вместе с днём (`clean` сносит
каталог дня). Метка ложится в хранилище, только когда туда лёг кусок записи, пересекающий её окно
(`recorded`): нет записи — нет события. Пока куска нет, метка живёт в памяти.

Файлы дня: `<камера>/<день>/events.jsonl` (строка на смену метки, последняя по `id` главнее) и
`<камера>/<день>/events/<id>.jpg`. День — по НАЧАЛУ метки; `id` — начало в мс: одно окно на камеру разом.
Смысла ярлыка дом не знает: имя приходит от того, кто его поставил (конвейер камеры — `person`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .recorder_index import day_of

FILE = "events.jsonl"
FRAMES = "events"
# Сколько закрытых меток без записи держать в памяти: запись по движению решает судьбу куска за `post_s`, но
# хранилище может не отвечать — память не копит метки, которые уже не лягут.
MAX_PENDING = 64


@dataclass
class Mark:
    start: float
    stop: float | None = None
    labels: list[str] = field(default_factory=list)
    picture: bytes | None = None
    # Лёг ли кусок записи в окно; записано ли в файл то, что знаем сейчас.
    recorded: bool = False
    dirty: bool = True

    @property
    def id(self) -> str:
        return str(int(self.start * 1000))

    def row(self) -> dict[str, Any]:
        return {"id": self.id, "start": int(self.start * 1000), "stop": None if self.stop is None else int(self.stop * 1000), "labels": self.labels}


class EventMarks:
    """Метки одной камеры: открыть с окном движения, закрыть, ярлык; на диск — с первым куском записи."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.marks: list[Mark] = []

    def open(self, at: float, picture: bytes | None) -> None:
        if self.marks and self.marks[-1].stop is None:
            return
        self.marks.append(Mark(at, picture=picture))
        del self.marks[:-MAX_PENDING]

    def close(self, at: float) -> None:
        mark = self._open()
        if mark is not None:
            mark.stop, mark.dirty = max(at, mark.start), True

    def label(self, name: str, picture: bytes | None) -> None:
        """Ярлык окну, которое идёт; его кадр — снимок ярлыка (участок с человеком), если он есть."""
        mark = self._open()
        if mark is None or name in mark.labels:
            return
        mark.labels.append(name)
        if picture:
            mark.picture = picture
        mark.dirty = True

    def recorded(self, start: float, stop: float, pre_s: float, post_s: float) -> None:
        """Кусок записи `[start, stop]` лёг в хранилище: метки, чьё окно с запасом он задевает, — записанные."""
        for mark in self.marks:
            end = mark.stop if mark.stop is not None else stop
            if mark.start - pre_s <= stop and start <= end + post_s:
                mark.recorded = True

    def take(self) -> list[tuple[Path, dict[str, Any], bytes | None]]:
        """Что записать (каталог дня, строка, кадр); закрытые и записанные — из памяти вон.

        ⚠ Отбор — в цикле событий, где метки меняются; запись файлов (`write`) — в исполнителе: перенос куска
        на сетевую папку идёт там же, и общий список менялся бы из двух потоков."""
        out: list[tuple[Path, dict[str, Any], bytes | None]] = []
        for mark in self.marks:
            if mark.recorded and mark.dirty:
                out.append((self.root / day_of(mark.start), mark.row(), mark.picture))
                mark.picture, mark.dirty = None, False
        self.marks = [m for m in self.marks if not (m.recorded and m.stop is not None and not m.dirty)]
        return out

    def _open(self) -> Mark | None:
        return self.marks[-1] if self.marks and self.marks[-1].stop is None else None


def write(items: list[tuple[Path, dict[str, Any], bytes | None]]) -> None:
    """Дописать строки меток и их кадры (исполнитель). Строка — с признаком кадра на диске."""
    for day, row, picture in items:
        frame = day / FRAMES / f"{row['id']}.jpg"
        if picture is not None:
            frame.parent.mkdir(parents=True, exist_ok=True)
            frame.write_bytes(picture)
        day.mkdir(parents=True, exist_ok=True)
        with (day / FILE).open("a", encoding="utf-8") as out:
            out.write(json.dumps({**row, "frame": frame.is_file()}) + "\n")


def read(root: Path, start: float, stop: float, labels: set[str] | None = None) -> list[dict[str, Any]]:
    """Записанные метки с началом в `[start, stop)` (секунды), новые первыми; `labels` — только с одним из ярлыков."""
    days = sorted({day_of(start), day_of(stop)} | {day_of(at) for at in range(int(start), int(stop), 86400)})
    rows: dict[str, dict[str, Any]] = {}
    for day in days:
        try:
            lines = (root / day / FILE).read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if isinstance(row, dict) and start * 1000 <= int(row.get("start") or 0) < stop * 1000 and (not labels or labels & set(row.get("labels") or [])):
                rows[str(row.get("id"))] = row
    return sorted(rows.values(), key=lambda row: -int(row["start"]))


def picture(root: Path, mark_id: str) -> Path | None:
    """Кадр метки по её `id` (начало в мс); нет — `None`."""
    if not mark_id.isdigit():
        return None
    path = root / day_of(int(mark_id) / 1000) / FRAMES / f"{mark_id}.jpg"
    return path if path.is_file() else None
