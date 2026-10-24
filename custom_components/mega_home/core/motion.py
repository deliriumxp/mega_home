"""Своя детекция движения камеры — источник событий `motion` слушателя (`listeners.py`).

Зачем. У большинства камер объекта детектора нет или он не отдаётся наружу (решение заказчика
2026-10-23: «свою детекцию — в дом»). Что смотреть и насколько чувствительно — данные описания
источника от менеджера; дом считает долю изменившихся пикселей и говорит «началось/кончилось».

Как — по образцу Frigate (`frigate/motion/improved_motion.py`, `ImprovedMotionDetector`, его
умолчания), но дешевле на слабом контроллере:
  * декодирует ffmpeg ДОПОЛНИТЕЛЬНЫЙ поток у СВОЕГО go2rtc (RTSP на петле — подключение к камере
    одно на всех): `-skip_loop_filter all` (деблокинг движению не нужен, замер −15…25 % процессора),
    один поток декодера, приоритет ниже HA (`nice`). Замер 2026-10-23, Tapo 640×360: 1,2 % ядра x86;
    основной поток 1080p — 3,5 %, поэтому описание даёт дополнительный;
  * кадр сразу маленький и серый: `fps=5` (детект Frigate), 176×96 усреднением, размытие — тоже у
    ffmpeg; Python получает 17 КБ на кадр;
  * numpy вместо OpenCV (его нет в образе HA, numpy есть): растяжка контраста по скользящим
    процентилям 4/96, разница с фоном, порог, расширение 3×3; контуры заменены сжатием 3×3 и счётом
    по окнам 16×16 — пятно меньше ~3×3 пикселей шумом не считается (у Frigate — площадь контура 10).
    Замер — 0,2 мс на кадр x86.
⚠ Фон и контраст — как у Frigate: пока доля изменений не упала ниже 5 %, детектор «калибруется»
и движения не объявляет; доля выше `lightning` (смена ИК-режима, вспышка) — снова калибровка.
⚠ История контраста стартует с первого кадра, а не с 0/255, как у Frigate: иначе первые 10 секунд
растяжка «плывёт», и тишина видится движением (замер на кадрах Tapo: 34 ложных кадра из 100).
⚠ Отступление от Frigate — «живость»: движение объявляется только в окне, которое менялось ОТ
КАДРА К КАДРУ в 2 из 3 последних кадров. Часы, которые камера впечатывает в кадр (есть почти у
всех), меняются раз в секунду — у Frigate их закрывают маской руками, иначе секунды — вечное
движение. Замер 2026-10-23, Tapo, минута пустой комнаты: без живости 77 кадров «движения» из
300, все в окне секунд; с ней — 0. Живой объект при 5 кадрах в секунду меняет кадр почти всегда.
"""

from __future__ import annotations

import asyncio
import importlib
import shutil
import time
from collections import deque
from typing import Any
from urllib.parse import quote

import aiohttp

from . import host_load
from .const import LOGGER
from .go2rtc_embed import API_PORT, RTSP_PORT

WIDTH, HEIGHT, CELL = 176, 96, 8
FRAME = WIDTH * HEIGHT
# Кадров нет дольше — поток встал: перезапуск источника (`listeners._forever`). Первый кадр ждём
# дольше: go2rtc сперва поднимает подключение к камере, ffmpeg разбирает поток.
IDLE_S = 30.0
FIRST_FRAME_S = 60.0
# Окно живое, если оно менялось от кадра к кадру в 2 из 3 последних кадров (см. шапку).
LIVELY_OF, LIVELY_NEED = 3, 2


class Detector:
    """Доля изменившейся площади кадра; 0 — движения нет (или детектор калибруется)."""

    def __init__(
        self, np: Any, threshold: float = 30, area: int = 6, lightning: float = 0.8, alpha: float = 0.01, mask: Any = None
    ) -> None:
        self.np, self.threshold, self.area, self.lightning, self.alpha = np, threshold, area, lightning, alpha
        # Маска — многоугольники в долях кадра, как у Frigate (блик, мигающий фонарь, ветки): там
        # кадр обнуляется, и ни фон, ни живость изменений не видят.
        self.masked = masked_area(np, mask)
        self.background = np.zeros((HEIGHT, WIDTH), np.float32)
        self.contrast = np.zeros((50, 2), np.float32)
        self.contrast_at = -1
        self.calibrating = True
        self.moving_frames = 0
        self.previous: Any = None
        self.lively: deque[Any] = deque(maxlen=LIVELY_OF)

    def _windows(self, mask: Any) -> Any:
        """Окна 16×16 с шагом 8, где точек маски не меньше `area`."""
        rows = mask.view(self.np.uint8).reshape(HEIGHT // CELL, CELL, WIDTH).sum(axis=1, dtype=self.np.uint16)
        cells = rows.reshape(HEIGHT // CELL, WIDTH // CELL, CELL).sum(axis=2)
        return (cells[:-1, :-1] + cells[1:, :-1] + cells[:-1, 1:] + cells[1:, 1:]) >= self.area

    def detect(self, raw: bytes) -> float | None:
        """Доля изменений; None — кадр-повтор: поток камеры реже `fps`, и ffmpeg дублирует кадры.
        Повтор нового не говорит (живой кадр отличается хоть шумом матрицы) и не должен считаться
        вторым кадром движения."""
        np = self.np
        pixels = np.frombuffer(raw, np.uint8).reshape(HEIGHT, WIDTH)
        if self.previous is not None and np.array_equal(pixels, self.previous[1]):
            return None
        counts = np.cumsum(np.bincount(pixels[::2, ::2].ravel(), minlength=256))
        low, high = np.searchsorted(counts, counts[-1] * 0.04), np.searchsorted(counts, counts[-1] * 0.96)
        frame = pixels.astype(np.float32)
        if low < high:
            if self.contrast_at < 0:
                self.contrast[:] = (low, high)
            self.contrast_at = (self.contrast_at + 1) % len(self.contrast)
            self.contrast[self.contrast_at] = (low, high)
            floor, ceiling = self.contrast.mean(axis=0)
            if ceiling > floor:
                np.clip(frame, floor, ceiling, out=frame)
                frame -= floor
                frame *= 255.0 / (ceiling - floor)
        if self.masked is not None:
            frame[self.masked] = 0
        changed =np.abs(frame - self.background) > self.threshold
        dilated = _grow(changed, np.bitwise_or)
        found = self._windows(_grow(dilated, np.bitwise_and))
        spots = int(np.count_nonzero(found))
        share = np.count_nonzero(dilated) / dilated.size
        if self.previous is not None:
            self.lively.append(self._windows(np.abs(frame - self.previous[0]) > self.threshold))
        self.previous = (frame, pixels)
        alive = len(self.lively) == LIVELY_OF and np.count_nonzero(found & (sum(self.lively) >= LIVELY_NEED))
        if share < 0.05 and spots <= 4:
            self.calibrating = False
        if share > self.lightning:
            self.calibrating = True
        rate = 0.2 if self.calibrating else self.alpha
        # Фон впитывает кадр с движением, только если оно держится (Frigate: 10 кадров) — так
        # оставленная машина за полминуту становится фоном, а идущий человек — нет.
        self.moving_frames = self.moving_frames + 1 if spots else 0
        if not spots or self.moving_frames >= 10:
            self.background += (frame - self.background) * rate
        return 0.0 if self.calibrating or not alive else float(share)


def masked_area(np: Any, polygons: Any) -> Any:
    """Многоугольники `[[x, y], …]` в долях кадра → маска пикселей (чёт-нечет лучом); нет — None."""
    rows, cols = np.mgrid[0:HEIGHT, 0:WIDTH]
    py, px = (rows + 0.5) / HEIGHT, (cols + 0.5) / WIDTH
    out = np.zeros((HEIGHT, WIDTH), bool)
    for polygon in polygons if isinstance(polygons, list) else []:
        try:
            points = [(float(x), float(y)) for x, y in polygon]
        except (TypeError, ValueError):
            continue
        if len(points) < 3:
            continue
        inside = np.zeros((HEIGHT, WIDTH), bool)
        for (x1, y1), (x2, y2) in zip(points, points[1:] + points[:1]):
            if y1 == y2:
                continue
            inside ^= ((y1 > py) != (y2 > py)) & (px < (x2 - x1) * (py - y1) / (y2 - y1) + x1)
        out |= inside
    return out if out.any() else None


def _grow(mask: Any, join: Any) -> Any:
    """Расширение (`or`) или сжатие (`and`) маски окном 3×3 — без OpenCV."""
    across = mask.copy()
    join(across[:, 1:], mask[:, :-1], out=across[:, 1:])
    join(across[:, :-1], mask[:, 1:], out=across[:, :-1])
    out = across.copy()
    join(out[1:], across[:-1], out=out[1:])
    join(out[:-1], across[1:], out=out[:-1])
    return out


def _number(spec: dict[str, Any], key: str, default: float, low: float, high: float) -> float:
    try:
        return min(max(float(spec.get(key, default)), low), high)
    except (TypeError, ValueError):
        return default


async def motion(ctx: Any, descriptor: Any, source: str, spec: dict[str, Any]) -> None:
    """Источник `motion`: поток `stream` у своего go2rtc → события `motion` `{state, share}`."""
    stream = str(spec.get("stream") or "")
    if not stream:
        raise ValueError("у источника motion не задан поток")
    loop = asyncio.get_running_loop()
    np = await loop.run_in_executor(None, importlib.import_module, "numpy")
    if spec.get("source"):
        # Тот же источник под тем же именем, что у бандла (`<плитка>_sub`): повторная регистрация
        # идущий поток не рвёт, а без неё go2rtc не знал бы потока, пока его никто не смотрит.
        url = f"http://127.0.0.1:{API_PORT}/api/streams?name={quote(stream, safe='')}&src={quote(str(spec['source']), safe='')}"
        async with aiohttp.ClientSession() as session, session.put(url) as answer:
            if answer.status >= 400:
                raise RuntimeError(f"go2rtc не принял поток ({answer.status})")
    detector = Detector(
        np,
        threshold=_number(spec, "threshold", 30, 5, 120),
        area=int(_number(spec, "area", 6, 1, 256)),
        lightning=_number(spec, "lightning", 0.8, 0.1, 1.0),
        alpha=_number(spec, "alpha", 0.01, 0.001, 0.5),
        mask=spec.get("mask"),
    )
    fps = _number(spec, "fps", 5, 1, 10)
    need = int(_number(spec, "frames", 2, 1, 50))
    off_s = _number(spec, "offDelay", 30, 1, 600)
    ffmpeg, nice = await loop.run_in_executor(None, lambda: (shutil.which("ffmpeg"), shutil.which("nice")))
    if not ffmpeg:
        raise RuntimeError("в системе нет ffmpeg")
    command = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-rtsp_transport", "tcp",
        "-threads", "1", "-skip_loop_filter", "all", "-flags2", "+fast",
        "-i", f"rtsp://127.0.0.1:{RTSP_PORT}/{stream}", "-an", "-sn", "-dn",
        "-vf", f"fps={fps:g},scale={WIDTH}:{HEIGHT}:flags=area,format=gray,gblur=sigma=1:steps=1",
        "-f", "rawvideo", "-pix_fmt", "gray", "pipe:1",
    ]
    proc = await asyncio.create_subprocess_exec(
        *([nice, "-n", "10"] if nice else []), *command,
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    tail: deque[str] = deque(maxlen=6)
    drain = asyncio.ensure_future(_drain(proc, tail))
    # Нагрузка (`host_load`): ffmpeg детекции и счёт кадра — одна строка камеры. `nice` исполняет
    # ffmpeg на своём pid.
    load_key = f"motion:{descriptor.id}"
    host_load.track(load_key, proc.pid)
    moving, peak, last_motion = 0, 0.0, 0.0
    active = False
    try:
        assert proc.stdout is not None
        while True:
            wait = IDLE_S if detector.previous is not None else FIRST_FRAME_S
            try:
                raw = await asyncio.wait_for(proc.stdout.readexactly(FRAME), wait)
            except asyncio.IncompleteReadError as err:
                raise RuntimeError(f"ffmpeg закончил поток: {' | '.join(tail) or 'без слов'}") from err
            except asyncio.TimeoutError as err:
                raise ConnectionError(f"нет кадров дольше {wait:g} с") from err
            # Счёт идёт в процессе дома, а не в ffmpeg. Время ПОТОКА, а не процесса (`process_time`):
            # тот считал бы и работу других потоков HA, шедшую в эти доли миллисекунды.
            began = time.thread_time()
            share = detector.detect(raw)
            host_load.spent(load_key, time.thread_time() - began)
            if share is None:
                continue
            now = loop.time()
            moving = moving + 1 if share else 0
            if share:
                last_motion, peak = now, max(peak, share)
            if not active and moving >= need:
                active = True
                ctx.emit(descriptor, source, spec, "motion", {"state": "start", "share": round(share, 4)})
            elif active and now - last_motion >= off_s:
                active = False
                ctx.emit(descriptor, source, spec, "motion", {"state": "stop", "share": round(peak, 4)})
                peak = 0.0
    finally:
        if active:
            # Источник встал посреди движения: окно записи и метка ленты не висят открытыми.
            ctx.emit(descriptor, source, spec, "motion", {"state": "stop", "share": round(peak, 4)})
        drain.cancel()
        host_load.untrack(load_key, proc.pid)
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


async def _drain(proc: asyncio.subprocess.Process, tail: deque[str]) -> None:
    assert proc.stderr is not None
    async for line in proc.stderr:
        text = line.decode(errors="ignore").strip()
        if text:
            tail.append(text)
