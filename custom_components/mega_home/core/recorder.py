"""Запись камер дома: процесс на камеру, куски, уборка — закрытый список, пункты 2 и 3.

Зачем. Объекту без регистратора нужна запись, а писать её может только тот, кто работает
круглосуточно, — дом (менеджер: `docs/superpowers/specs/2026-10-23-own-video-design.md`).
Что писать, как долго хранить и что считать движением — раздел `recorder` конфига менеджера;
дом исполняет и смысла не знает.

⚠ Поток камеры берётся у СВОЕГО go2rtc (RTSP на петле, `go2rtc_embed.RTSP_PORT`), а не у
камеры: подключение к камере одно на всех — живое видео жильца, кадр плитки, запись. Поток
заводится под тем же именем и той же строкой, что у бандла; повторная регистрация того же
источника идущую запись не рвёт (замер 2026-10-23, go2rtc 1.9.14).

⚠ Куски — Matroska по 10 с без перекодирования: звук камер бывает A-law, а MP4 его не
несёт (замер стенда — «Could not find tag for codec pcm_alaw»). Время в имени — UTC,
длительность — в имени готового куска: индекс записи есть сам каталог, базы нет.

⚠ «По движению» — по образцу Frigate и предзаписи регистраторов: кусок ждёт в кэше, пока не
станет ясно, попал ли он в окно движения (запас до и после); не попал — удаляется, и на диск
или в сеть без движения не пишется ничего. Кэш — в памяти (`/dev/shm`), если её хватает.

⚠ Хранилище проверяется ДО записи: каталог обязан быть точкой монтирования (или нести метку
`.mega-home-storage`, поставленную человеком) — иначе отвалившийся диск означал бы запись на
флеш контроллера до его заполнения.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

from . import services
from .const import LOGGER
from .go2rtc_embed import API_PORT, RTSP_PORT
from .host import Host

SERVICE = "recorder"
SEGMENT_S = 10
SCAN_S = 2.0
RESTART_S = 10.0
CLEAN_EVERY_S = 60.0
# Меньше свободного — удаляются самые старые куски всех камер, не дожидаясь срока.
MIN_FREE_BYTES = 2 * 1024**3
# Камера, не сказавшая «движение кончилось», не держит окно вечно.
MAX_MOTION_S = 300.0
STORAGE_MARK = ".mega-home-storage"
RAM_DIR = Path("/dev/shm")
# Кэш «по движению» на камеру: запас до события плюс кусок в работе (~4 Мбит/с, с запасом).
RAM_PER_CAMERA = 64 * 1024**2
_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_NAME = re.compile(r"^(\d{6})-(\d+)\.mkv$")


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


def camera_specs(section: Any) -> list[CameraSpec]:
    """Камеры раздела; неполная или с чужими символами в id — пропускается (id — имя каталога)."""
    out: list[CameraSpec] = []
    for raw in (section or {}).get("cameras") or []:
        if not isinstance(raw, dict) or not _ID.match(str(raw.get("id", ""))):
            continue
        if not raw.get("stream") or not raw.get("source"):
            continue
        motion = raw.get("motion") if isinstance(raw.get("motion"), dict) else {}
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
                on=tuple(str(s) for s in motion.get("on") or [] if s),
                off=tuple(str(s) for s in motion.get("off") or [] if s),
            )
        )
    return out


def storage_problem(path: Path) -> str:
    """Почему писать сюда нельзя; пусто — можно."""
    if not path.is_absolute():
        return f"путь хранилища «{path}» должен быть абсолютным"
    if not path.is_dir():
        return f"нет каталога хранилища {path}: подключите диск или сетевую папку"
    if not os.path.ismount(path) and not (path / STORAGE_MARK).exists():
        return (
            f"по пути {path} не смонтирован диск — запись не идёт, чтобы не заполнить флеш "
            f"контроллера (каталог без диска: положите в него файл {STORAGE_MARK})"
        )
    if not os.access(path, os.W_OK):
        return f"в {path} нельзя писать"
    return ""


def epoch_of(stamp: str) -> float | None:
    """`YYYYMMDDTHHMMSS` (UTC, имя куска ffmpeg) → секунды."""
    try:
        return datetime(
            int(stamp[0:4]), int(stamp[4:6]), int(stamp[6:8]),
            int(stamp[9:11]), int(stamp[11:13]), int(stamp[13:15]), tzinfo=timezone.utc,
        ).timestamp()
    except (ValueError, IndexError):
        return None


def day_of(epoch: float) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(epoch))


def segments_of(root: Path, camera: str, start: float, stop: float) -> list[tuple[float, float, Path]]:
    """Готовые куски камеры, пересекающие `[start, stop]`: (начало, длительность, файл)."""
    out: list[tuple[float, float, Path]] = []
    day = start - 24 * 3600
    while day <= stop + 24 * 3600:
        folder = root / camera / day_of(day)
        day += 24 * 3600
        if not folder.is_dir():
            continue
        for item in folder.iterdir():
            match = _NAME.match(item.name)
            begin = epoch_of(f"{folder.name.replace('-', '')}T{match.group(1)}") if match else None
            if begin is None:
                continue
            length = int(match.group(2)) / 1000
            if begin + length >= start and begin <= stop:
                out.append((begin, length, item))
    return sorted(set(out))


class _Camera:
    """Одна камера: ffmpeg пишет куски в кэш, разбор раскладывает их в хранилище."""

    def __init__(self, recorder: Recorder, spec: CameraSpec, cache: Path) -> None:
        self.rec, self.spec, self.cache = recorder, spec, cache / spec.id
        self.why = ""
        self.last_saved = 0.0
        self.saved = 0
        self.dropped = 0
        self._tail: deque[str] = deque(maxlen=8)
        # Окна движения: [начало, конец или None, последнее «есть движение»] — часы дома.
        self.windows: list[list[float | None]] = []

    async def run(self) -> None:
        await self.rec.env.run(lambda: self.cache.mkdir(parents=True, exist_ok=True))
        while True:
            try:
                await self._register()
                await self._record()
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — камера не роняет запись остальных
                self.why = f"запись не идёт: {err}"
                LOGGER.warning("Запись %s: %s", self.spec.id, err)
            await self._collect(final=True)
            await asyncio.sleep(RESTART_S)

    async def _register(self) -> None:
        url = (
            f"http://127.0.0.1:{API_PORT}/api/streams?name={quote(self.spec.stream, safe='')}"
            f"&src={quote(self.spec.source, safe='')}"
        )
        async with self.rec.env.session().put(url) as answer:
            if answer.status >= 400:
                raise RuntimeError(f"go2rtc не принял поток ({answer.status})")

    async def _record(self) -> None:
        binary = await self.rec.env.run(shutil.which, "ffmpeg")
        if not binary:
            raise RuntimeError("в системе нет ffmpeg")
        proc = await asyncio.create_subprocess_exec(
            binary, "-hide_banner", "-loglevel", "error", "-nostdin",
            "-rtsp_transport", "tcp", "-i", f"rtsp://127.0.0.1:{RTSP_PORT}/{self.spec.stream}",
            "-map", "0:v", "-map", "0:a?", "-c", "copy",
            "-f", "segment", "-segment_time", str(SEGMENT_S), "-segment_format", "matroska",
            "-reset_timestamps", "1", "-strftime", "1", str(self.cache / "%Y%m%dT%H%M%S.mkv"),
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE, env={**os.environ, "TZ": "UTC"},
        )
        drain = asyncio.ensure_future(self._drain(proc))
        self.why = ""
        try:
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(proc.wait()), SCAN_S)
                    break
                except asyncio.TimeoutError:
                    await self._collect()
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), 5)
                except asyncio.TimeoutError:
                    proc.kill()
            drain.cancel()
        raise RuntimeError(f"ffmpeg завершился ({proc.returncode}): {' | '.join(self._tail) or 'без слов'}")

    async def _drain(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        async for line in proc.stderr:
            text = line.decode(errors="ignore").strip()
            if text:
                self._tail.append(text)

    async def _collect(self, final: bool = False) -> None:
        """Готовые куски — в хранилище или в корзину; последний пишется, его не трогаем."""
        files = sorted(await self.rec.env.run(lambda: list(self.cache.glob("*.mkv"))))
        ready = files if final else files[:-1]
        now = time.time()
        for path in ready:
            start = epoch_of(path.stem)
            if start is None:
                await self.rec.env.run(path.unlink, True)
                continue
            length = await self.rec.probe(path)
            if length <= 0:
                await self.rec.env.run(path.unlink, True)
                continue
            if not self.spec.motion_mode or self.wanted(start, start + length, now):
                await self.rec.env.run(self._save, path, start, length)
            elif start + length + self.spec.pre_s < now - SEGMENT_S or final:
                self.dropped += 1
                await self.rec.env.run(path.unlink, True)

    def _save(self, path: Path, start: float, length: float) -> None:
        folder = self.rec.storage / self.spec.id / day_of(start)
        folder.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%H%M%S", time.gmtime(start))
        shutil.move(str(path), str(folder / f"{stamp}-{int(length * 1000)}.mkv"))
        self.saved += 1
        self.last_saved = start + length

    def wanted(self, start: float, stop: float, now: float) -> bool:
        """Попал ли кусок в окно движения с запасом до и после."""
        for begin, end, _seen in self.windows:
            if begin - self.spec.pre_s <= stop and start <= (end if end is not None else now) + self.spec.post_s:
                return True
        return False

    def on_event(self, frame: dict[str, Any]) -> None:
        """Ответ источника событий: последнее слово в теле решает — началось или кончилось."""
        if frame.get("access") != self.spec.access or frame.get("source") != self.spec.events:
            return
        data = frame.get("data") if isinstance(frame.get("data"), dict) else {}
        text = str(data.get("text") or "")
        at = float(frame.get("at") or time.time())
        on = max((text.rfind(s) for s in self.spec.on), default=-1)
        off = max((text.rfind(s) for s in self.spec.off), default=-1)
        open_ = self.windows[-1] if self.windows and self.windows[-1][1] is None else None
        if on > off:
            if open_ is None:
                self.windows.append([at, None, at])
            else:
                open_[2] = at
        elif off > on and open_ is not None:
            open_[1] = at
        self._expire(at)

    def _expire(self, now: float) -> None:
        for window in self.windows:
            if window[1] is None and now - float(window[2] or now) > MAX_MOTION_S:
                window[1] = window[2]
        keep = now - self.spec.post_s - 3600
        self.windows = [w for w in self.windows if w[1] is None or float(w[1]) > keep]

    def state(self) -> dict[str, Any]:
        return {
            "mode": "motion" if self.spec.motion_mode else "continuous",
            "why": self.why,
            "saved": self.saved,
            "dropped": self.dropped,
            "lastSaved": round(self.last_saved, 3) or None,
            "motion": bool(self.windows and self.windows[-1][1] is None),
        }


class Recorder:
    """Раздел `recorder` конфига → камеры, уборка и служба на петле (`recorder_api.py`)."""

    def __init__(self, env: Host, subscribe: Callable[[Callable[[dict[str, Any]], None]], Callable[[], None]]) -> None:
        self.env = env
        self._subscribe = subscribe
        self._unsubscribe: Callable[[], None] | None = None
        self._key: str | None = None
        self._lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self.cameras: dict[str, _Camera] = {}
        self.storage = Path("/")
        self.cache = Path("/")
        self.why = ""
        self._api: Any = None

    def apply(self, config: dict[str, Any] | None) -> None:
        section = (config or {}).get("recorder") if isinstance((config or {}).get("recorder"), dict) else None
        key = json.dumps(section, sort_keys=True)
        if key == self._key:
            return
        self._key = key
        self.env.spawn(self._reconfigure(section), "mega_home: запись камер")

    async def async_stop(self) -> None:
        self._key = None
        await self._reconfigure(None)

    async def _reconfigure(self, section: dict[str, Any] | None) -> None:
        async with self._lock:
            await self._stop_all()
            specs = camera_specs(section)
            if not section or not specs:
                self.why = ""
                return
            self.storage = Path(str(section.get("storage") or ""))
            self.why = await self.env.run(storage_problem, self.storage)
            if self.why:
                LOGGER.warning("Запись камер: %s", self.why)
                return
            self.cache = await self.env.run(self._cache_dir, sum(1 for s in specs if s.motion_mode))
            from .recorder_api import start_api  # noqa: PLC0415 — веб-сервер только при записи

            self._api = await start_api(self)
            self.cameras = {spec.id: _Camera(self, spec, self.cache) for spec in specs}
            self._unsubscribe = self._subscribe(self._on_event)
            for camera in self.cameras.values():
                self._tasks.append(self.env.spawn(camera.run(), f"mega_home: запись {camera.spec.id}"))
            self._tasks.append(self.env.spawn(self._clean_loop(), "mega_home: уборка записи"))

    async def _stop_all(self) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for camera in self.cameras.values():
            await camera._collect(final=True)  # noqa: SLF001 — дописать начатое до остановки
        self.cameras = {}
        services.unregister(SERVICE)
        if self._api is not None:
            await self._api.cleanup()
            self._api = None

    def _cache_dir(self, motion_cameras: int) -> Path:
        """Кэш кусков: память, если её хватает камерам «по движению», иначе само хранилище.

        ⚠ Для постоянной записи — всегда хранилище: перенос внутри одной файловой системы —
        переименование, а из памяти — копия каждого куска.
        """
        if motion_cameras and RAM_DIR.is_dir():
            free = shutil.disk_usage(RAM_DIR).free
            if free >= motion_cameras * RAM_PER_CAMERA:
                path = RAM_DIR / "mega_home_rec"
                path.mkdir(exist_ok=True)
                return path
        path = self.storage / ".cache"
        path.mkdir(exist_ok=True)
        return path

    def _on_event(self, frame: dict[str, Any]) -> None:
        for camera in self.cameras.values():
            camera.on_event(frame)

    async def probe(self, path: Path) -> float:
        """Длительность куска по ffprobe; не прочитался — 0 (кусок битый)."""
        binary = await self.env.run(shutil.which, "ffprobe")
        if not binary:
            return float(SEGMENT_S)
        proc = await asyncio.create_subprocess_exec(
            binary, "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), 15)
            return float(out.decode().strip() or 0)
        except (asyncio.TimeoutError, ValueError):
            if proc.returncode is None:
                proc.kill()
            return 0.0

    async def _clean_loop(self) -> None:
        while True:
            try:
                await self.env.run(clean, self.storage, list(self.cameras.values()), time.time())
            except Exception as err:  # noqa: BLE001 — уборка не роняет запись
                LOGGER.warning("Запись камер: уборка не прошла: %s", err)
            await asyncio.sleep(CLEAN_EVERY_S)

    def state(self) -> dict[str, Any]:
        enabled = self._key not in (None, "null")
        return {
            "enabled": enabled,
            "storage": str(self.storage) if self.cameras else None,
            "cache": str(self.cache) if self.cameras else None,
            "why": self.why,
            "cameras": {cid: camera.state() for cid, camera in self.cameras.items()},
        }


def clean(storage: Path, cameras: list[_Camera], now: float) -> None:
    """По сроку камеры — каталоги дней целиком; по месту — самые старые куски всех камер."""
    for camera in cameras:
        cutoff = day_of(now - camera.spec.retain_days * 24 * 3600)
        root = storage / camera.spec.id
        for day in sorted(root.iterdir()) if root.is_dir() else []:
            if day.is_dir() and day.name < cutoff:
                shutil.rmtree(day, ignore_errors=True)
    # Шагами по 60 кусков (10 минут записи), не больше полусотни шагов за проход.
    for _step in range(50):
        if shutil.disk_usage(storage).free >= MIN_FREE_BYTES:
            break
        days = sorted(
            (day.name, day)
            for camera in cameras
            for day in ((storage / camera.spec.id).iterdir() if (storage / camera.spec.id).is_dir() else [])
            if day.is_dir()
        )
        if not days:
            break
        oldest = days[0][1]
        for item in sorted(oldest.iterdir())[:60]:
            item.unlink(missing_ok=True)
        if not any(oldest.iterdir()):
            oldest.rmdir()
    plays = storage / ".cache" / "play"
    for item in plays.iterdir() if plays.is_dir() else []:
        if now - item.stat().st_mtime > 6 * 3600:
            item.unlink(missing_ok=True)
