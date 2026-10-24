"""Раздел `recorder` конфига менеджера и годность хранилища — данные записи, без процессов.

Вынесено из `recorder.py` (порог размера файла): камеры раздела и «можно ли писать сюда» читают и
запись, и служба (`recorder_api.py`), а процесса им не нужно.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import storage

_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
# Путь хранилища уходит строкой в список ffconcat (`file '…'`) и в источник go2rtc (`#` — разделитель).
_PATH = re.compile(r"^/[A-Za-z0-9_./-]+$")


@dataclass(frozen=True)
class CameraSpec:
    """Камера раздела `recorder`: всё — данные менеджера."""

    id: str
    stream: str
    source: str
    motion_mode: bool
    retain_days: int
    pre_s: float
    post_s: float
    access: str
    events: str
    on: tuple[str, ...]
    off: tuple[str, ...]
    # Ответ режется на сообщения по `split`; смены считаются только в сообщениях, где есть одна
    # из подстрок `require` (тема движения) — `State` несут и темы «темно», «закрыли объектив»;
    # сообщение с любой из подстрок `skip` — не смена (первичное состояние подписки).
    split: str
    require: tuple[str, ...]
    skip: tuple[str, ...] = ()


def camera_specs(section: Any) -> list[CameraSpec]:
    """Камеры раздела; неполная или с чужими символами в id — пропускается (id — имя каталога)."""
    out: list[CameraSpec] = []
    for raw in (section or {}).get("cameras") or []:
        if not isinstance(raw, dict) or not _ID.match(str(raw.get("id", ""))):
            continue
        if not raw.get("stream") or not raw.get("source"):
            continue
        motion = raw.get("motion") if isinstance(raw.get("motion"), dict) else {}
        words = lambda key: tuple(str(s) for s in motion.get(key) or [] if s)  # noqa: E731
        out.append(
            CameraSpec(
                id=str(raw["id"]),
                stream=str(raw["stream"]),
                source=str(raw["source"]),
                motion_mode=raw.get("mode") == "motion",
                retain_days=max(int(raw.get("retainDays") or 7), 1),
                pre_s=float(raw.get("preSec") or 10),
                post_s=float(raw.get("postSec") or 20),
                access=str(motion.get("access") or ""),
                events=str(motion.get("source") or ""),
                on=words("on"),
                off=words("off"),
                split=str(motion.get("split") or ""),
                require=words("require"),
                skip=words("skip"),
            )
        )
    return out


def storage_problem(path: Path, reference: Path, local: bool = False, mounted_at: Path | None = None) -> str:
    """Почему писать сюда нельзя; пусто — можно.

    `reference` — каталог настроек дома: его диск — системный (флеш контроллера). Писать на него
    можно только явно (`local` — «локальный диск» с лимитом). `mounted_at` — точка, которую
    смонтировал дом: путь обязан быть внутри неё, иначе пропавший диск означал бы запись на флеш.
    """
    if not path.is_absolute() or not _PATH.match(str(path)):
        return f"путь хранилища «{path}» — абсолютный, из латиницы, цифр и знаков _ . - /"
    if mounted_at is not None and (mounted_at != path and mounted_at not in path.parents or not os.path.ismount(mounted_at)):
        return f"диск хранилища не смонтирован в {mounted_at}"
    if mounted_at is None and storage.MOUNT_POINT.parent in path.parents:
        return f"{path} — точка, куда дом сам подключает диск: выберите USB-диск или сетевую папку видом хранилища"
    if not path.is_dir():
        return f"нет каталога хранилища {path}: подключите диск или сетевую папку"
    disk = storage.disk_kind(path, reference)
    if disk == "container":
        return f"папка {path} внутри контейнера Home Assistant — пропадёт при его обновлении: выберите диск"
    if disk == "system" and not local:
        return (
            f"папка {path} на системном диске контроллера — запись не идёт, чтобы не заполнить его: "
            "выберите диск или сетевую папку, либо «Локальный диск» с лимитом"
        )
    if not os.access(path, os.W_OK):
        return f"в {path} нельзя писать"
    return ""
