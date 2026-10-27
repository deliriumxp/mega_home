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
import os
import shutil
import time
from collections import deque
from typing import Any, Callable, NamedTuple
from urllib.parse import quote

import aiohttp

from . import activity, host_load, person
from .const import LOGGER
from .go2rtc_embed import API_PORT, RTSP_PORT

WIDTH, HEIGHT, CELL = 176, 96, 8
# Умолчания раздела `person` описания (менеджер шлёт их данными — подстройка без релиза дома). Ширина
# кадра для выреза: человек на трети высоты — сотня с лишним пикселей, сети хватает; снимок события —
# ширина JPEG крупного участка (`person.shot`).
PERSON_WIDTH = 640
SHOT_WIDTH = 480
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
        self,
        np: Any,
        threshold: float = 30,
        area: int = 6,
        lightning: float = 0.8,
        alpha: float = 0.01,
        mask: Any = None,
    ) -> None:
        self.np, self.threshold, self.area, self.lightning, self.alpha = np, threshold, area, lightning, alpha
        # ⚠ Порога размера у движения нет, как у Frigate: его `contour_area` — шум в 10 точек (здесь — `area`
        # окна), а размер объекта (`min_area`) проверяется у рамки ОБЪЕКТА. Порог по изменившимся точкам
        # (0.5.38–0.6.5) отсекал людей целиком: у идущего человека меняется 1,6–5,3 % кадра при пороге
        # 10–15 % (записи объекта .90.250, 2026-10-26/27). Размер человека — `PersonTune.min_size`.
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
        # Рамка живого движения последнего кадра (пиксели кадра 176×96); None — движения нет. Из неё
        # вырезается кусок для сети «человек» (`person.py`).
        self.box: tuple[int, int, int, int] | None = None

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
        live = found & (sum(self.lively) >= LIVELY_NEED) if len(self.lively) == LIVELY_OF else None
        self.box = _live_box(np, live) if live is not None else None
        alive = self.box is not None
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
        if self.calibrating:
            self.box = None
        return 0.0 if self.calibrating or not alive else float(share)


def _live_box(np: Any, windows: Any) -> tuple[int, int, int, int] | None:
    """Рамка живых окон (пиксели кадра) или None. Окно (r, c) — 16×16 пикселей от (c, r)·CELL (склейка 2×2 ячеек)."""
    rows, cols = np.nonzero(windows)
    if not len(rows):
        return None
    return int(cols.min()) * CELL, int(rows.min()) * CELL, (int(cols.max()) + 2) * CELL, (int(rows.max()) + 2) * CELL


def box_share(box: tuple[int, int, int, int]) -> float:
    """Площадь рамки в процентах кадра — мера `person.minSize`, как `min_area` объекта у Frigate."""
    return (box[2] - box[0]) * (box[3] - box[1]) * 100.0 / (WIDTH * HEIGHT)


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
    fps = _number(spec, "fps", 5, 1, 10)
    ffmpeg, nice = await loop.run_in_executor(None, lambda: (shutil.which("ffmpeg"), shutil.which("nice")))
    if not ffmpeg:
        raise RuntimeError("в системе нет ffmpeg")
    watch, model, tune = await _person_setup(loop, np, spec)
    # Нагрузка (`host_load`): ffmpeg детекции, счёт кадра и сети «человек» — одна строка камеры.
    load_key = f"motion:{descriptor.id}"
    run = MotionLoop(
        np, spec,
        emit=lambda event, data, file: ctx.emit(descriptor, source, spec, event, data, file=file),
        on_share=lambda at, share: activity.note(descriptor.id, at, share),
        load_key=load_key,
        person=(watch, model, tune) if watch is not None else None,
    )
    ffmpeg_args = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-rtsp_transport", "tcp",
        "-threads", "1", "-skip_loop_filter", "all", "-flags2", "+fast",
        "-i", f"rtsp://127.0.0.1:{RTSP_PORT}/{stream}", "-an", "-sn", "-dn",
    ]
    small = f"fps={fps:g},scale={WIDTH}:{HEIGHT}:flags=area,format=gray,gblur=sigma=1:steps=1"
    pipe_out = -1
    # Крупный кадр — каждый `ratio`-й маленький: оба режет `fps` от одного начала (`_person_pairs`).
    ratio = run.ratio
    if watch is None:
        ffmpeg_args += ["-vf", small, "-f", "rawvideo", "-pix_fmt", "gray", "pipe:1"]
    else:
        # Кадр для выреза — вторым выходом того же декодера: подключение и декод одни. Цветной (сеть
        # учена на RGB), крупнее кадра движения (человек в 176×96 — десятки пикселей) и в своих
        # пропорциях; BMP — потому что размер кадра камеры заранее не известен, а заголовок его несёт.
        pipe_in, pipe_out = os.pipe()
        ffmpeg_args += [
            "-filter_complex",
            f"[0:v]split=2[m][p];[m]{small}[small];[p]fps={fps / ratio:g},scale={tune.width}:-2:flags=area[big]",
            "-map", "[small]", "-f", "rawvideo", "-pix_fmt", "gray", "pipe:1",
            # ⚠ Номер дескриптора в ffmpeg — тот же, что у нас: `pass_fds` его сохраняет.
            "-map", "[big]", "-f", "image2pipe", "-c:v", "bmp", "-pix_fmt", "bgr24", f"pipe:{pipe_out}",
        ]
    proc = await asyncio.create_subprocess_exec(
        *([nice, "-n", "10"] if nice else []), *ffmpeg_args,
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        **({"pass_fds": (pipe_out,)} if watch is not None else {}),
    )
    frames_task: asyncio.Future[None] | None = None
    if watch is not None:
        os.close(pipe_out)
        frames_task = asyncio.ensure_future(_person_frames(loop, np, pipe_in, run.big))
    tail: deque[str] = deque(maxlen=6)
    drain = asyncio.ensure_future(_drain(proc, tail))
    # `nice` исполняет ffmpeg на своём pid.
    host_load.track(load_key, proc.pid)
    try:
        assert proc.stdout is not None
        while True:
            wait = IDLE_S if run.detector.previous is not None else FIRST_FRAME_S
            try:
                raw = await asyncio.wait_for(proc.stdout.readexactly(FRAME), wait)
            except asyncio.IncompleteReadError as err:
                raise RuntimeError(f"ffmpeg закончил поток: {' | '.join(tail) or 'без слов'}") from err
            except asyncio.TimeoutError as err:
                raise ConnectionError(f"нет кадров дольше {wait:g} с") from err
            await run.frame(raw)
    finally:
        run.close()
        if frames_task is not None:
            frames_task.cancel()
        drain.cancel()
        host_load.untrack(load_key, proc.pid)
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


class MotionLoop:
    """Кадры детекции одной камеры → «началось/кончилось», сила движения, люди.

    Один разбор у двух хозяев: источник `motion` слушателя (раздел `recorder`, `motion()` выше) и читатель
    конвейера камеры (`video_pipeline.py`, раздел `video`). Конвейер получает смены движения ВЫЗОВОМ
    (`on_motion`) — запись по движению своей камеры больше не ловит их подстрокой в событии.
    """

    def __init__(
        self,
        np: Any,
        spec: dict[str, Any],
        *,
        emit: Callable[[str, dict[str, Any], tuple[str, bytes] | None], None],
        on_motion: Callable[[bool, float], None] | None = None,
        on_share: Callable[[float, float], None] | None = None,
        load_key: str = "",
        person: tuple[Any, Any, PersonTune] | None = None,
    ) -> None:
        self.np, self.emit, self.on_motion, self.on_share, self.load_key = np, emit, on_motion, on_share, load_key
        self.detector = Detector(
            np,
            threshold=_number(spec, "threshold", 30, 5, 120),
            area=int(_number(spec, "area", 6, 1, 256)),
            lightning=_number(spec, "lightning", 0.8, 0.1, 1.0),
            alpha=_number(spec, "alpha", 0.01, 0.001, 0.5),
            mask=spec.get("mask"),
        )
        fps = _number(spec, "fps", 5, 1, 10)
        self.need = int(_number(spec, "frames", 2, 1, 50))
        self.off_s = _number(spec, "offDelay", 30, 1, 600)
        # Движение публикуется, если описание не сказало иначе: у камеры, чьё движение приходит от неё
        # самой, детекция дома нужна только ради людей (`motionEvents: false`), и строк движения дважды нет.
        self.publish = spec.get("motionEvents") is not False
        self.watch, self.model, self.tune = person if person is not None else (None, None, PersonTune())
        self.ratio = max(round(fps * self.watch.every), 1) if self.watch is not None else 1
        self.big: deque[tuple[int, Any]] = deque(maxlen=4)
        self.boxes: deque[tuple[int, Any]] = deque(maxlen=4 * self.ratio)
        self.index = -1
        self.moving, self.peak, self.last_motion, self.active = 0, 0.0, 0.0, False

    async def frame(self, raw: bytes) -> None:
        loop = asyncio.get_running_loop()
        # Счёт идёт в процессе дома, а не в ffmpeg. Время ПОТОКА, а не процесса (`process_time`):
        # тот считал бы и работу других потоков HA, шедшую в эти доли миллисекунды.
        began = time.thread_time()
        share = self.detector.detect(raw)
        host_load.spent(self.load_key, time.thread_time() - began)
        self.index += 1
        if self.watch is not None:
            # Повтор кадра — тоже кадр потока: номер растёт, рамка — прежняя. Рамка меньше `minSize` сети не идёт
            # (далёкий человек, кошка): окно движения и запись она не трогает.
            box = self.detector.box if share != 0.0 else None
            self.boxes.append((self.index, box if box is not None and box_share(box) >= self.tune.min_size else None))
        if share is None:
            return
        now = loop.time()
        self.moving = self.moving + 1 if share else 0
        if share:
            self.last_motion, self.peak = now, max(self.peak, share)
        if not self.active and self.moving >= self.need:
            self.active = True
            self._changed(True, {"state": "start", "share": round(share, 4)})
        elif self.active and now - self.last_motion >= self.off_s:
            self._stop()
        if self.active and share and self.on_share is not None:
            # Сила движения для ленты архива (`activity.py`) — только внутри окна, как полоса движения.
            self.on_share(time.time(), share)
        watch = self.watch
        for frame, box in _person_pairs(self.big, self.boxes, self.ratio, self.index) if watch is not None else ():
            if not (self.active and box and watch.due(now)):
                continue
            value, spent = await loop.run_in_executor(None, _person_score, self.np, self.model, frame, box, self.tune.margin)
            # Счёт сети — в строке камеры, как счёт кадра; пишется в цикле событий (`host_load.spent`).
            host_load.spent(self.load_key, spent)
            if watch.seen(value, now):
                # Снимок события — крупный участок ТОГО кадра, где сеть увидела человека (заказчик
                # 2026-10-24): снимок камеры правилом вложения пришёл бы секундами позже.
                picture = await loop.run_in_executor(None, _person_shot, frame, box, self.tune)
                # ⚠ Своё имя события и свой ключ данных (`person`, а не `state`): запись по движению
                # раздела `recorder` и лента ловят движение подстрокой `"state":"start"`.
                self.emit("person", {"person": "start", "score": round(value, 3)}, ("image/jpeg", picture) if picture else None)

    def close(self) -> None:
        """Источник встал: окно записи и метка ленты не висят открытыми посреди движения."""
        if self.active:
            self._stop()
        elif self.watch is not None and self.watch.end():
            self.emit("person", {"person": "stop"}, None)

    def _stop(self) -> None:
        self.active = False
        self._changed(False, {"state": "stop", "share": round(self.peak, 4)})
        self.peak = 0.0
        if self.watch is not None and self.watch.end():
            self.emit("person", {"person": "stop"}, None)

    def _changed(self, on: bool, data: dict[str, Any]) -> None:
        if self.publish:
            self.emit("motion", data, None)
        if self.on_motion is not None:
            self.on_motion(on, time.time())


class PersonTune(NamedTuple):
    """Кадр выреза, снимок события и наименьшая рамка человека (`minSize`, % кадра) — из раздела `person` описания."""

    width: int = PERSON_WIDTH
    margin: float = person.MARGIN
    shot: int = SHOT_WIDTH
    min_size: float = 0.0


async def _person_setup(loop: asyncio.AbstractEventLoop, np: Any, spec: dict[str, Any]) -> tuple[Any, Any, PersonTune]:
    """Сеть «человек» по описанию (`person: {every, threshold, frames, width, margin, shot}`); нет раздела или
    весов — без сети: движение работает как прежде."""
    options = spec.get("person")
    if not isinstance(options, dict):
        return None, None, PersonTune()
    try:
        model = await loop.run_in_executor(None, person.load, np)
    except (OSError, KeyError, ValueError) as err:
        LOGGER.warning("Сеть «человек» не загрузилась, детекция людей выключена: %s", err)
        return None, None, PersonTune()
    watch = person.PersonWatch(
        every=_number(options, "every", 1.0, 0.2, 10.0),
        threshold=_number(options, "threshold", 0.5, 0.05, 0.99),
        frames=int(_number(options, "frames", 2, 1, 10)),
    )
    # Ширина — чётная: `scale=W:-2` ffmpeg.
    tune = PersonTune(
        width=int(_number(options, "width", PERSON_WIDTH, 320, 1920)) // 2 * 2,
        margin=_number(options, "margin", person.MARGIN, 0.0, 1.0),
        shot=int(_number(options, "shot", SHOT_WIDTH, 160, 1280)),
        min_size=_number(options, "minSize", 0.0, 0.0, 100.0),
    )
    return watch, model, tune


async def _person_frames(loop: asyncio.AbstractEventLoop, np: Any, fd: int, big: deque[tuple[int, Any]]) -> None:
    """Кадры для выреза из второй трубы ffmpeg с их номерами. ⚠ Читать ВСЕГДА: кадр крупнее буфера трубы,
    и непрочитанная труба остановила бы ffmpeg, а с ним и движение."""
    reader = asyncio.StreamReader(limit=2**24)
    transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), os.fdopen(fd, "rb", 0))
    try:
        number = 0
        while True:
            big.append((number, await person.read_bmp(np, reader)))
            number += 1
    except (asyncio.IncompleteReadError, ValueError):
        return
    finally:
        transport.close()


def _person_pairs(big: deque[tuple[int, Any]], boxes: deque[tuple[int, Any]], ratio: int, index: int) -> list[tuple[Any, Any]]:
    """Крупные кадры, чей маленький двойник уже разобран, — с рамкой движения ТОГО ЖЕ момента.

    ⚠ По номеру, а не по времени прихода: два выхода одного ffmpeg приходят пачками (пять маленьких и
    крупный разом, проба 2026-10-24), а идущий человек за секунду уходит на сотню пикселей — вырез из
    кадра другого момента мимо (0,13 вместо 0,9). Оба выхода режет `fps` от первого кадра: крупный № j —
    тот же момент, что маленький № j·ratio."""
    known = dict(boxes)
    out = []
    while big and big[0][0] * ratio <= index:
        number, frame = big.popleft()
        out.append((frame, known.get(number * ratio)))
    return out


def _scaled(frame: Any, box: tuple[int, int, int, int]) -> tuple[float, float, float, float]:
    """Рамка движения (кадр 176×96) → пиксели крупного кадра."""
    height, width = frame.shape[:2]
    sx, sy = width / WIDTH, height / HEIGHT
    return box[0] * sx, box[1] * sy, box[2] * sx, box[3] * sy


def _person_score(np: Any, model: Any, frame: Any, box: tuple[int, int, int, int], margin: float) -> tuple[float, float]:
    """Уверенность сети и время потока (в исполнителе)."""
    began = time.thread_time()
    value = person.score(np, model, frame, _scaled(frame, box), margin)
    return value, time.thread_time() - began


def _person_shot(frame: Any, box: tuple[int, int, int, int], tune: PersonTune) -> bytes | None:
    """JPEG снимка события (в исполнителе); не вышло — None, событие уходит без своего снимка."""
    try:
        return person.shot(frame, _scaled(frame, box), tune.margin, tune.shot)
    except (OSError, ValueError) as err:
        LOGGER.warning("Снимок «человек» не сжался: %s", err)
        return None


async def _drain(proc: asyncio.subprocess.Process, tail: deque[str]) -> None:
    assert proc.stderr is not None
    async for line in proc.stderr:
        text = line.decode(errors="ignore").strip()
        if text:
            tail.append(text)
