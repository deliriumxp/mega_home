"""Читатель потока конвейера камеры: ОДИН ffmpeg на поток go2rtc, который дом читает, со всеми выходами.

Зачем. До 0.6.0 запись, превью и детекция одной камеры были тремя процессами: дважды читали основной
поток и декодировали его ключевые кадры под превью, хотя детекция той же камеры уже декодировала
дополнительный. Здесь правило одно: что нужно от потока — то выходы его читателя
(`docs/superpowers/specs/2026-10-26-video-engine-rewrite-design.md` менеджера, «Конвейер камеры»).

⚠ Превью и последний кадр — с того декодера, который уже есть: поток детекции, если детекция есть,
иначе только ключевые кадры потока записи (`-skip_frame nokey` — опция декодера: копия на соседнем
выходе получает все пакеты, проба 2026-10-26 — 250 пакетов из 250 при 5 декодированных кадрах).
⚠ `-use_wallclock_as_timestamps` здесь НЕ ставится: вход общий с копией записи, и метки кусков стали бы
временем прихода пакетов. Начало куска превью — из имени (`strftime`), как и у кусков записи.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .go2rtc_embed import RTSP_PORT
from .motion import HEIGHT, PERSON_WIDTH, WIDTH
from .recorder_preview import PREVIEW_DIR, PreviewTune, preview_output_args
from .video_spec import CameraPlan

SEGMENT_S = 10
SEGMENT_LIST = "segments.csv"
# Последний кадр камеры (плитка, кадр события, редактор маски): раз в 2 с, шириной плитки во весь экран.
LATEST_FPS = 0.5
LATEST_WIDTH = 640
# Превью при полном декоде — кадр раз в 2 с, как у превью по ключевым кадрам камер объекта (2–3 с).
PREVIEW_FPS = 0.5


@dataclass(frozen=True)
class ReaderSpec:
    """Что один ffmpeg делает с одним потоком камеры."""

    key: str
    stream: str
    record_dir: Path | None
    detect_fps: float | None
    person_width: int | None
    # Крупный кадр — каждый `person_ratio`-й маленький: `motion._person_pairs` сводит их по номеру.
    person_ratio: int
    preview_dir: Path | None
    latest: bool
    keyframes_only: bool


@dataclass(frozen=True)
class ReaderFds:
    """Дескрипторы труб читателя, кроме stdout (сырой серый кадр детекции)."""

    person: int | None
    latest: int | None


def _bounded(value: object, default: float, low: float, high: float) -> float:
    try:
        return min(max(float(value), low), high)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def reader_plan(camera: CameraPlan, cache: Path, record_on: bool) -> list[ReaderSpec]:
    """Читатели камеры — не больше одного на поток. `record_on=False` — хранилище не годно: без записи и
    превью (детекция и последний кадр от хранилища не зависят)."""
    record = camera.record.stream if camera.record and record_on else None
    detect = camera.detect.stream if camera.detect else None
    decoder = detect or record
    out: list[ReaderSpec] = []
    for key in ("main", "sub"):
        if key not in (record, detect):
            continue
        params = camera.detect.params if camera.detect and key == detect else None
        person = params.get("person") if params else None
        fps = _bounded(params.get("fps", 5), 5.0, 1.0, 10.0) if params is not None else None
        every = _bounded(person.get("every", 1.0), 1.0, 0.2, 10.0) if isinstance(person, dict) else 1.0
        out.append(
            ReaderSpec(
                key=key,
                stream=camera.streams[key].name,
                record_dir=cache if key == record else None,
                detect_fps=fps,
                person_width=int(_bounded(person.get("width", PERSON_WIDTH), PERSON_WIDTH, 320, 1920)) // 2 * 2
                if isinstance(person, dict)
                else None,
                person_ratio=max(round((fps or 1.0) * every), 1),
                preview_dir=cache / PREVIEW_DIR if key == decoder and record else None,
                latest=key == decoder,
                keyframes_only=key == decoder and params is None,
            )
        )
    return out


def reader_command(binary: str, nice: str, spec: ReaderSpec, tune: PreviewTune, fds: ReaderFds) -> list[str]:
    decode = spec.detect_fps is not None or spec.preview_dir is not None or spec.latest
    full = spec.detect_fps is not None
    argv = [*([nice, "-n", "10"] if nice else []), binary, "-hide_banner", "-loglevel", "error", "-nostdin", "-rtsp_transport", "tcp"]
    if full:
        # Деблокинг движению не нужен (−15…25 % процессора), декодер в один поток (`motion.py`, замер 2026-10-23).
        argv += ["-threads", "1", "-skip_loop_filter", "all", "-flags2", "+fast"]
    elif decode:
        argv += ["-skip_frame", "nokey"]
    argv += ["-i", f"rtsp://127.0.0.1:{RTSP_PORT}/{spec.stream}"]
    if spec.record_dir is not None:
        argv += [
            "-map", "0:v", "-map", "0:a?", "-c", "copy",
            "-f", "segment", "-segment_time", str(SEGMENT_S), "-segment_format", "matroska",
            "-reset_timestamps", "1", "-strftime", "1",
            "-segment_list", str(spec.record_dir / SEGMENT_LIST), "-segment_list_type", "csv",
            str(spec.record_dir / "%Y%m%dT%H%M%S.mkv"),
        ]
    if not decode:
        return argv
    branches: list[tuple[str, str, list[str]]] = []
    if spec.detect_fps is not None:
        small = f"fps={spec.detect_fps:g},scale={WIDTH}:{HEIGHT}:flags=area,format=gray,gblur=sigma=1:steps=1"
        branches.append(("det", small, ["-f", "rawvideo", "-pix_fmt", "gray", "pipe:1"]))
        if spec.person_width is not None and fds.person is not None:
            big = f"fps={spec.detect_fps / spec.person_ratio:g},scale={spec.person_width}:-2:flags=area"
            branches.append(("big", big, ["-f", "image2pipe", "-c:v", "bmp", "-pix_fmt", "bgr24", f"pipe:{fds.person}"]))
    if spec.preview_dir is not None:
        rate = f"fps={PREVIEW_FPS:g}," if full else ""
        branches.append(("pv", f"{rate}scale=-2:{tune.height},format=yuv420p", preview_output_args(tune, spec.preview_dir.parent)))
    if spec.latest and fds.latest is not None:
        lf = f"fps={LATEST_FPS:g},scale={LATEST_WIDTH}:-2"
        branches.append(("lf", lf, ["-f", "image2pipe", "-c:v", "mjpeg", "-q:v", "5", f"pipe:{fds.latest}"]))
    labels = "".join(f"[{name}_in]" for name, _f, _o in branches)
    graph = [f"[0:v]split={len(branches)}{labels}"] + [f"[{name}_in]{chain}[{name}]" for name, chain, _o in branches]
    argv += ["-filter_complex", ";".join(graph)]
    for name, _chain, output in branches:
        argv += ["-map", f"[{name}]", *output]
    return argv
