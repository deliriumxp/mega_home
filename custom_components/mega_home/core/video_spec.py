"""Раздел `video` конфига менеджера: камера одним описанием — её потоки, запись и детекция.

Зачем. До 0.6.0 одна и та же камера была размазана по трём местам конфига: раздел `recorder` (запись),
слушатель `motion` у устройства (детекция) и правила вложений (кадр события) — и каждое заводило свой
процесс и своё имя потока в go2rtc. Конвейер камеры (`video_pipeline.py`) ведёт её одним хозяином, и
описание у него одно. Смысла дом не толкует: только проверяет, что имена годятся в путь RTSP и в имя
каталога, а запись и детекция смотрят на поток, который камера назвала
(`docs/superpowers/specs/2026-10-26-video-engine-rewrite-design.md` менеджера).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .recorder_preview import PreviewTune, tune_of
from .recorder_spec import CameraSpec

_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
# Имя потока go2rtc уходит в путь RTSP на петле (`rtsp://127.0.0.1:…/<имя>`).
_NAME = _ID
_KEYS = ("main", "sub")


@dataclass(frozen=True)
class StreamPlan:
    name: str
    source: str


@dataclass(frozen=True)
class CameraMotion:
    """Словарь «началось/кончилось» в ответе подписки камеры — как у раздела `recorder` (`CameraSpec`)."""

    access: str
    source: str
    on: tuple[str, ...]
    off: tuple[str, ...]
    split: str
    require: tuple[str, ...]
    skip: tuple[str, ...]


@dataclass(frozen=True)
class RecordPlan:
    stream: str
    mode: str
    retain_days: int
    pre_s: float
    post_s: float
    camera_motion: CameraMotion | None


@dataclass(frozen=True)
class DetectPlan:
    """Поток детекции и её параметры — то же, что описание источника `motion` слушателя."""

    stream: str
    params: dict[str, Any]


@dataclass(frozen=True)
class CameraPlan:
    id: str
    streams: dict[str, StreamPlan]
    record: RecordPlan | None
    detect: DetectPlan | None


@dataclass(frozen=True)
class VideoSection:
    storage: Path
    mount: dict[str, Any] | None
    local: bool
    limit: int
    preview: PreviewTune
    cameras: tuple[CameraPlan, ...]


def video_section(config: Any) -> VideoSection | None:
    """Раздел `video`; нет его или он не объект — `None` (дом идёт прежним разделом `recorder`)."""
    raw = config.get("video") if isinstance(config, dict) else None
    if not isinstance(raw, dict):
        return None
    mount = raw.get("mount") if isinstance(raw.get("mount"), dict) else None
    return VideoSection(
        storage=Path(str(raw.get("storage") or "")),
        mount=mount,
        local=raw.get("local") is True,
        limit=int(float(raw.get("limitGb") or 0) * 1024**3),
        preview=tune_of(raw.get("preview")),
        cameras=tuple(camera for item in raw.get("cameras") or [] if (camera := _camera(item)) is not None),
    )


def _camera(raw: Any) -> CameraPlan | None:
    """Камера; без безопасного id или без годного потока — пропускается (id — имя каталога записи)."""
    if not isinstance(raw, dict) or not _ID.match(str(raw.get("id", ""))):
        return None
    streams: dict[str, StreamPlan] = {}
    for key in _KEYS:
        item = (raw.get("streams") or {}).get(key) if isinstance(raw.get("streams"), dict) else None
        if isinstance(item, dict) and _NAME.match(str(item.get("name", ""))) and item.get("source"):
            streams[key] = StreamPlan(str(item["name"]), str(item["source"]))
    if "main" not in streams:
        return None
    return CameraPlan(id=str(raw["id"]), streams=streams, record=_record(raw.get("record"), streams), detect=_detect(raw.get("detect"), streams))


def _record(raw: Any, streams: dict[str, StreamPlan]) -> RecordPlan | None:
    if not isinstance(raw, dict) or raw.get("stream") not in streams:
        return None
    return RecordPlan(
        stream=str(raw["stream"]),
        mode="motion" if raw.get("mode") == "motion" else "continuous",
        retain_days=max(int(raw.get("retainDays") or 7), 1),
        pre_s=float(raw.get("preSec") or 10),
        post_s=float(raw.get("postSec") or 20),
        camera_motion=_words(raw.get("cameraMotion")),
    )


def _words(raw: Any) -> CameraMotion | None:
    if not isinstance(raw, dict):
        return None
    words = lambda key: tuple(str(s) for s in raw.get(key) or [] if s)  # noqa: E731
    return CameraMotion(
        access=str(raw.get("access") or ""),
        source=str(raw.get("source") or ""),
        on=words("on"),
        off=words("off"),
        split=str(raw.get("split") or ""),
        require=words("require"),
        skip=words("skip"),
    )


def _detect(raw: Any, streams: dict[str, StreamPlan]) -> DetectPlan | None:
    if not isinstance(raw, dict) or raw.get("stream") not in streams:
        return None
    return DetectPlan(stream=str(raw["stream"]), params={k: v for k, v in raw.items() if k != "stream"})


def record_spec(camera: CameraPlan) -> CameraSpec:
    """Запись камеры словами прежнего раздела: тот же `RecordSink`, что у раздела `recorder`.

    ⚠ Движение СВОЕЙ детекции записи приходит вызовом (`RecordSink.motion`), а не подстроками в событии:
    словарь здесь есть только у движения от самой камеры (подписка PullPoint).
    """
    record = camera.record
    assert record is not None
    stream = camera.streams[record.stream]
    words = record.camera_motion
    return CameraSpec(
        id=camera.id,
        stream=stream.name,
        source=stream.source,
        motion_mode=record.mode == "motion",
        retain_days=record.retain_days,
        pre_s=record.pre_s,
        post_s=record.post_s,
        access=words.access if words else "",
        events=words.source if words else "",
        on=words.on if words else (),
        off=words.off if words else (),
        split=words.split if words else "",
        require=words.require if words else (),
        skip=words.skip if words else (),
    )
