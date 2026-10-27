"""Запись одной камеры без процесса: готовые куски из кэша — в хранилище или в корзину, окна движения.

Куски пишет либо свой ffmpeg записи (`recorder._Camera`, раздел `recorder`), либо выход читателя
конвейера камеры (`video_reader.py`, раздел `video`) — разбор у них один.

⚠ «По движению» — по образцу Frigate и предзаписи регистраторов: кусок ждёт в кэше, пока не станет ясно,
попал ли он в окно движения (запас до и после); не попал — удаляется, и на диск или в сеть без движения не
пишется ничего. Кэш — в памяти (`/dev/shm`), если её хватает (`recorder_cache.py`).
⚠ Хранилище проверяется перед КАЖДЫМ куском: отвалившийся диск оставляет пустой каталог на флеше
контроллера.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import activity
from .const import LOGGER
from .recorder_cache import cap_bytes, memory_available, shed
from .recorder_index import MAX_SEGMENT_S, SegmentIndex, day_of, epoch_of, length_ms, segment_name
from .recorder_preview import PreviewIndex
from .recorder_spec import CameraSpec

if TYPE_CHECKING:
    from .recorder import Recorder
    from .recorder_preview import PreviewSink

SEGMENT_S = 10
RAM_DIR = Path("/dev/shm")
# Камера, не сказавшая «движение кончилось», не держит окно вечно.
MAX_MOTION_S = 300.0
# ⚠ Перенос куска в хранилище ждёт не дольше этого: сетевая папка с зависшим соединением держит поток
# исполнителя минутами, а без срока стоял бы весь разбор камеры — и кэш в памяти копился бы часами.
SAVE_TIMEOUT_S = 120.0
# Предупреждение о сбросе кэша — не чаще этого, секунд.
SHED_WARN_S = 60.0
# Список готовых кусков ведёт сам ffmpeg (`-segment_list`, CSV «файл,начало,конец»): длительность
# куска и «кусок закрыт» без ffprobe на каждый кусок и без догадки «последний ещё пишется».
SEGMENT_LIST = "segments.csv"


class RecordSink:
    """Одна камера: готовые куски кэша — в хранилище по окнам движения."""

    def __init__(self, recorder: Recorder, spec: CameraSpec, cache: Path) -> None:
        self.rec, self.spec, self.cache = recorder, spec, cache / spec.id
        self.index = SegmentIndex(recorder.storage / spec.id)
        self.why = ""
        self.last_saved = 0.0
        self.saved = 0
        self.dropped = 0
        # Готовые куски по списку ffmpeg: имя → длительность; прочитанная часть списка — смещением.
        self._listed: dict[str, float] = {}
        self._list_offset = 0
        # Кэш в памяти (`recorder_cache.py`): поток камеры по её кускам (байт/с), сброшенные куски, перенос в работе.
        self.in_ram = self.cache.is_relative_to(RAM_DIR)
        self.rate = 0.0
        self.shed = 0
        self._shed_warned = 0.0
        self._saving: asyncio.Future[None] | None = None
        # Кусок, который сейчас переносится: сброс кэша в памяти его не трогает.
        self.busy: str | None = None
        self._last_ready = 0.0
        # Окна движения: [начало, конец или None, последнее «есть движение»] — часы дома.
        self.windows: list[list[float | None]] = []
        # Превью-видео камеры (`recorder_preview.py`): индекс — у хранилища, куски кладёт хозяин превью.
        self.previews = PreviewIndex(recorder.storage / spec.id)
        self.preview: PreviewSink | None = None

    def restart_list(self) -> None:
        """Новый ffmpeg начинает список заново."""
        self._listed, self._list_offset = {}, 0
        self._last_ready = time.monotonic()

    async def guard_once(self) -> None:
        """Предел кэша в памяти: сверх него самые старые куски сбрасываются, не дожидаясь переносчика.

        ⚠ Отдельный проход, а не часть `_collect`: тот ждёт хранилище, а память переполняется ровно тогда,
        когда оно не отвечает (разбор 2026-10-25: 15–17 часов кусков в `/dev/shm`, HA в свопе).
        """
        cameras = sum(1 for sink in self.rec.sinks() if sink.in_ram) or 1
        horizon = self.spec.pre_s + 2 * SEGMENT_S
        cap = cap_bytes(self.rate, horizon, cameras, await self.rec.env.run(memory_available))
        removed = await self.rec.env.run(shed, self.cache, cap, self.busy)
        if removed:
            self.shed += removed
            if time.monotonic() - self._shed_warned > SHED_WARN_S:
                self._shed_warned = time.monotonic()
                LOGGER.warning(
                    "Запись %s: кэш в памяти больше %d МБ — сброшено %d старых кусков (хранилище не успевает)",
                    self.spec.id, cap // 1024**2, removed,
                )

    def _read_list(self) -> None:
        """Дочитать список ffmpeg с прошлого места: строка `файл,начало,конец` на готовый кусок."""
        path = self.cache / SEGMENT_LIST
        try:
            size = path.stat().st_size
        except OSError:
            return
        if size < self._list_offset:
            self._list_offset = 0
        if size == self._list_offset:
            return
        with path.open("rb") as handle:
            handle.seek(self._list_offset)
            chunk = handle.read(size - self._list_offset)
        # Неполная последняя строка дочитается следующим разом.
        cut = chunk.rfind(b"\n") + 1
        self._list_offset += cut
        for line in chunk[:cut].decode(errors="ignore").splitlines():
            parts = line.rsplit(",", 2)
            try:
                self._listed[Path(parts[0]).name] = max(float(parts[2]) - float(parts[1]), 0.0)
            except (IndexError, ValueError):
                continue

    async def collect(self, final: bool = False) -> float:
        """Разобрать готовое; ответ — монотонное время последнего готового куска (прогресс читателя)."""
        await self._collect(final)
        return self._last_ready

    async def _collect(self, final: bool = False) -> None:
        """Готовые куски (по списку ffmpeg) — в хранилище или в корзину. `final` — и тот, что
        ffmpeg не успел закрыть в список (остановлен): длительность у него спрашивает ffprobe."""
        if self._saving is not None and self._saving.done() and self.why.startswith("хранилище не отвечает"):
            self.why = ""  # опоздавший перенос вернулся — хранилище снова отвечает
        await self.rec.env.run(self._read_list)
        files = sorted(await self.rec.env.run(lambda: list(self.cache.glob("*.mkv"))))
        ready = files if final else [path for path in files if path.name in self._listed]
        if ready:
            self._last_ready = time.monotonic()
        now = time.time()
        names = {path.name for path in files}
        for stale in [name for name in self._listed if name not in names]:
            del self._listed[stale]
        for path in ready:
            start = epoch_of(path.stem)
            if start is None:
                await self.rec.env.run(path.unlink, True)
                continue
            length = self._listed.get(path.name, 0.0) or await self.rec.probe(path)
            if length > MAX_SEGMENT_S:
                # ⚠ Метки камеры скакнули (обрыв RTP, `bad cseq`): список ffmpeg считает длину по ним, и кусок
                # в 10 часов лёг бы в индекс (замер 2026-10-25) — окна поиска записи не знают таких.
                measured = await self.rec.probe(path)
                if not 0 < measured <= MAX_SEGMENT_S:
                    LOGGER.warning("Запись %s: кусок %s длиной %.0f с отброшен", self.spec.id, path.name, length)
                    self._listed.pop(path.name, None)
                    self.dropped += 1
                    await self.rec.env.run(path.unlink, True)
                    continue
                length = measured
            if length <= 0:
                self._listed.pop(path.name, None)
                await self.rec.env.run(path.unlink, True)
                continue
            if self.in_ram:
                self._measure(path, length)
            if not self.spec.motion_mode or self.wanted(start, start + length, now):
                if not await self._save_bounded(path, start, length):
                    return  # хранилище не отвечает: остальные ждут в кэше, пределом их держит `guard_once`
            elif start + length + self.spec.pre_s < now - SEGMENT_S or final:
                self.dropped += 1
                await self.rec.env.run(path.unlink, True)
            else:
                continue
            self._listed.pop(path.name, None)

    def _measure(self, path: Path, length: float) -> None:
        """Поток камеры по её куску, байт/с: от него зависит, сколько памяти нужно кэшу."""
        try:
            rate = path.stat().st_size / length
        except OSError:
            return
        self.rate = rate if not self.rate else 0.8 * self.rate + 0.2 * rate

    async def _save_bounded(self, path: Path, start: float, length: float) -> bool:
        """Перенос куска не дольше `SAVE_TIMEOUT_S`; `False` — хранилище не ответило, кусок остаётся в кэше.

        ⚠ Поток исполнителя, повисший на сети, не снять: пока он не вернулся, второго переноса не
        заводим — иначе каждый проход съедал бы по потоку из пула HA (как у пробы хранилища, `_check`).
        """
        if self._saving is not None and not self._saving.done():
            return False
        self.busy = path.name
        self._saving = asyncio.ensure_future(self.rec.env.run(self._save, path, start, length))
        # Результат опоздавшего переноса никто не ждёт: прочесть, чтобы цикл не жаловался.
        self._saving.add_done_callback(self._saved)
        try:
            await asyncio.wait_for(asyncio.shield(self._saving), SAVE_TIMEOUT_S)
        except asyncio.TimeoutError:
            self.why = "хранилище не отвечает: куски ждут в кэше"
            LOGGER.warning("Запись %s: перенос куска %s длится дольше %g с", self.spec.id, path.name, SAVE_TIMEOUT_S)
            return False
        except Exception as err:  # noqa: BLE001
            # ⚠ Отказ переноса (нет места, EIO, мигание NAS) — причина в состоянии, кусок ждёт в кэше: исключение,
            # ушедшее в цикл процесса, снимало ffmpeg записи вместе с идущим куском (ревью 2026-10-27).
            why = f"кусок {path.name} не сохранился: {err}"
            if why != self.why:
                LOGGER.warning("Запись %s: %s", self.spec.id, why)
            self.why = why
            return False
        if self.why.startswith(("хранилище не отвечает", "кусок ")):
            self.why = ""
        return True

    def _saved(self, done: asyncio.Future[None]) -> None:
        self.busy = None
        if not done.cancelled():
            done.exception()

    def _save(self, path: Path, start: float, length: float) -> None:
        # ⚠ Перед КАЖДЫМ куском: отвалившийся диск оставляет пустой каталог на флеше контроллера.
        problem = self.rec.problem()
        if problem:
            raise OSError(problem)
        size = path.stat().st_size
        target = self.index.root / day_of(start) / segment_name(start, length_ms(length))
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(target))
        # В индекс — после переноса: сорвавшийся перенос не оставляет в нём куска, которого нет.
        self.index.add(start, length, size)
        self.saved += 1
        self.last_saved = start + length

    def wanted(self, start: float, stop: float, now: float) -> bool:
        """Попал ли кусок в окно движения с запасом до и после."""
        for begin, end, _seen in self.windows:
            if begin - self.spec.pre_s <= stop and start <= (end if end is not None else now) + self.spec.post_s:
                return True
        return False

    def motion(self, on: bool, at: float) -> None:
        """Смена движения своей детекции — вызовом конвейера камеры, без подстрок в событии."""
        self._step(on, at)
        self._expire(at)

    def on_event(self, frame: dict[str, Any]) -> None:
        """Ответ источника событий: смены «началось/кончилось» по порядку, каждая — своим шагом.

        ⚠ Не «последнее слово решает»: короткое движение между двумя опросами приходит одним
        ответом «началось, кончилось», и по последнему слову окна не было бы вовсе.
        """
        if frame.get("access") != self.spec.access or frame.get("source") != self.spec.events:
            return
        data = frame.get("data") if isinstance(frame.get("data"), dict) else {}
        # Ответ устройства — его текст; событие дома (своя детекция, `motion.py`) — данные одной
        # строкой JSON без пробелов: подстроки `on`/`off` менеджер пишет под эту форму.
        text = str(data.get("text") or "") or json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        at = float(frame.get("at") or time.time())
        for message in text.split(self.spec.split) if self.spec.split else [text]:
            if self.spec.require and not any(word in message for word in self.spec.require):
                continue
            if any(word in message for word in self.spec.skip):
                continue
            marks = sorted(
                [(m.start(), True) for word in self.spec.on for m in re.finditer(re.escape(word), message)]
                + [(m.start(), False) for word in self.spec.off for m in re.finditer(re.escape(word), message)]
            )
            for _pos, on in marks:
                self._step(on, at)
        self._expire(at)

    def _step(self, on: bool, at: float) -> None:
        open_ = self.windows[-1] if self.windows and self.windows[-1][1] is None else None
        if on:
            if open_ is None:
                self.windows.append([at, None, at])
            else:
                open_[2] = at
        elif open_ is not None:
            open_[1] = at
        else:
            # «Кончилось» без открытого окна — движение уложилось между опросами: окно-точка,
            # запас до и после сохранит его куски.
            self.windows.append([at, at, at])

    def _expire(self, now: float) -> None:
        for window in self.windows:
            if window[1] is None and now - float(window[2] or now) > MAX_MOTION_S:
                window[1] = window[2]
        keep = now - self.spec.post_s - 3600
        self.windows = [w for w in self.windows if w[1] is None or float(w[1]) > keep]

    def store_activity(self, rows: list[tuple[int, float]]) -> None:
        """Корзины силы движения — в `<день>/activity.csv`; уходят вместе с каталогом дня (`clean`)."""
        if self.rec.problem():
            return
        for day in sorted({day_of(bucket) for bucket, _value in rows}):
            activity.append(self.index.root / day, [row for row in rows if day_of(row[0]) == day])

    def state(self) -> dict[str, Any]:
        return {
            "mode": "motion" if self.spec.motion_mode else "continuous",
            "why": self.why,
            "saved": self.saved,
            "dropped": self.dropped,
            "lastSaved": round(self.last_saved, 3) or None,
            "motion": bool(self.windows and self.windows[-1][1] is None),
            "preview": self.preview.why if self.preview is not None else "",
            "shed": self.shed,
        }
