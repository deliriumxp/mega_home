"""Проба потока камеры ДО записи: несколько секунд тем же путём, что пишет запись, и факты по дорожкам (дом 0.6.13+).

Зачем. Камера объявляет звук, а пакетов нет (EYEPLUS «Прихожая»), или пакеты есть, но их метки застыли на пути
через go2rtc (EYEPLUS «Гостиная», объект .90.250 2026-10-30: напрямую с камеры звук ровный) — и архив месяц пишется
битым, а видно это, когда он понадобился. Заголовки потока (ffprobe по описанию, как у мастера камеры Frigate) этого
не покажут: камера объявляет то же, что и исправная. Поэтому проба СНИМАЕТ кусок путём записи — камера → свой
go2rtc → ffmpeg копией в Matroska — и считает пакеты и метки каждой дорожки.

Дом отдаёт ФАКТЫ, а толкует их менеджер (`camera-probe.util.ts`): новое «так ломается» — правка менеджера.

  * `POST /probe` `{"stream"?: имя потока go2rtc, "source"?: строка источника, "input"?: [аргументы входа], "seconds"?}`
    → `{"seconds", "tracks": [{"type", "codec", "width"?, "height"?, "fps"?, "rate"?, "packets", "first", "last"}],
    "error"?, "id"?}`; у звука с пакетами — `meanDb`, `peakDb`. `stream` — камера уже заведена (поток держит конвейер:
    второго подключения к камере нет); `source` — ещё нет: поток заводится на время пробы и снимается.
  * `GET /probe/{id}/frame.jpg`, `GET /probe/{id}/audio.m4a` — кадр и отрывок звука пробы (послушать в мастере камеры),
    `KEEP_S`; дальше — 404.
Только менеджеру (`recorder_api._installer_only`): проба держит ffmpeg и подключение к камере.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote
from uuid import uuid4

from aiohttp import web

from .go2rtc_embed import API_PORT, RTSP_PORT

if TYPE_CHECKING:
    from .recorder import Recorder

DEFAULT_S = 6.0
MAX_S = 15.0
# Проба держит ffmpeg и подключение к камере: одна за раз на дом.
_ONE = asyncio.Lock()
_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
MAX_INPUT = 32
_RECORDER: web.AppKey[Any] = web.AppKey("probe_recorder")
# Кадр и отрывок звука пробы — во временном каталоге (не на хранилище записи), живут `KEEP_S`.
PROBE_DIR = Path(tempfile.gettempdir()) / "mega_home_probe"
KEEP_S = 600.0
FRAME_WIDTH = 640
_PID = re.compile(r"^[0-9a-f]{32}$")
_TYPES = {"frame.jpg": "image/jpeg", "audio.m4a": "audio/mp4"}


def input_args(raw: Any) -> tuple[str, ...]:
    """Аргументы входа ffmpeg из описания (`video_spec.StreamPlan.input`, тело пробы): список строк, иначе пусто.

    ⚠ Сырые, а не словарь разрешённых: перечень разрешённого — сам по себе адаптер, и камера, которой нужен параметр
    вне него, значила бы релиз (`docs/home-gateway.md` менеджера). Описание приходит только от менеджера, ffmpeg
    запускается без оболочки. `-i` запрещён: вход у записи один — свой go2rtc."""
    if not isinstance(raw, list) or len(raw) > MAX_INPUT:
        return ()
    args = tuple(str(item) for item in raw if isinstance(item, (str, int, float)) and str(item))
    return () if "-i" in args else args


def tracks_of(answer: dict[str, Any]) -> list[dict[str, Any]]:
    """Ответ ffprobe (`stream=…:packet=…`, JSON) → факты по дорожкам."""
    out: dict[int, dict[str, Any]] = {}
    for stream in answer.get("streams") or []:
        kind = stream.get("codec_type")
        if kind not in ("video", "audio"):
            continue
        track: dict[str, Any] = {"type": kind, "codec": stream.get("codec_name"), "packets": 0, "first": None, "last": None}
        if kind == "video":
            track.update(width=stream.get("width"), height=stream.get("height"), fps=_rate(stream.get("avg_frame_rate")))
        else:
            track.update(rate=_int(stream.get("sample_rate")), channels=stream.get("channels"))
        out[int(stream.get("index", len(out)))] = track
    for packet in answer.get("packets") or []:
        track = out.get(int(packet.get("stream_index", -1)))
        pts = _float(packet.get("pts_time"))
        if track is None:
            continue
        track["packets"] += 1
        if pts is not None:
            track["first"] = pts if track["first"] is None else min(track["first"], pts)
            track["last"] = pts if track["last"] is None else max(track["last"], pts)
    return list(out.values())


def _rate(raw: Any) -> float | None:
    try:
        top, bottom = str(raw).split("/")
        return round(int(top) / int(bottom), 2) if int(bottom) else None
    except ValueError:
        return None


def _int(raw: Any) -> int | None:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _float(raw: Any) -> float | None:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


async def probe(recorder: Recorder, stream: str, source: str, args: tuple[str, ...], seconds: float) -> dict[str, Any]:
    """Снять `seconds` потока путём записи и разобрать; поток, заведённый под пробу, снимается при любом исходе."""
    ffmpeg, ffprobe = await recorder.env.run(shutil.which, "ffmpeg"), await recorder.env.run(shutil.which, "ffprobe")
    if not ffmpeg or not ffprobe:
        return {"seconds": 0, "tracks": [], "error": "в системе нет ffmpeg"}
    name = stream or f"probe_{uuid4().hex[:12]}"
    api = f"http://127.0.0.1:{API_PORT}/api/streams?name={quote(name, safe='')}"
    target = Path(tempfile.gettempdir()) / f"mega_home_{name}.mkv"
    async with _ONE:
        try:
            if not stream:
                async with recorder.env.session().put(f"{api}&src={quote(source, safe='')}") as answer:
                    if answer.status >= 400:
                        return {"seconds": 0, "tracks": [], "error": f"go2rtc не принял поток ({answer.status})"}
            started = time.monotonic()
            _out, errors = await _run(
                [ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-rtsp_transport", "tcp", *args,
                 "-i", f"rtsp://127.0.0.1:{RTSP_PORT}/{name}", "-t", f"{seconds:g}",
                 "-map", "0:v", "-map", "0:a?", "-c", "copy", "-f", "matroska", "-y", str(target)],
                seconds + 20,
            )
            took = round(time.monotonic() - started, 1)
            raw, _err = await _run(
                [ffprobe, "-v", "error", "-show_entries",
                 "stream=index,codec_type,codec_name,width,height,avg_frame_rate,sample_rate,channels:packet=stream_index,pts_time",
                 "-of", "json", str(target)],
                20,
            )
            tracks = tracks_of(json.loads(raw or "{}"))
            out: dict[str, Any] = {"seconds": took, "tracks": tracks}
            if not tracks:
                # Причина — первая строка ffmpeg; нет строк — поток молчал весь срок.
                out["error"] = " | ".join(errors.strip().splitlines()[:3]) or "поток не ответил вовремя"
            if tracks:
                out["id"] = await _keep(recorder, ffmpeg, target, tracks)
            return out
        finally:
            await recorder.env.run(lambda: target.unlink(missing_ok=True))
            if not stream:
                try:
                    async with recorder.env.session().delete(api):
                        pass
                except Exception:  # noqa: BLE001 — поток пробы снимет и перезапуск go2rtc
                    pass


async def _keep(recorder: Recorder, ffmpeg: str, sample: Path, tracks: list[dict[str, Any]]) -> str:
    """Что показать инсталлятору из пробы: кадр видео и отрывок звука (AAC в MP4 — играет любой браузер), уровень звука
    у дорожки (`meanDb`, `peakDb`: тишина видна и без наушников). Лежат `KEEP_S` во временном каталоге."""
    pid = uuid4().hex
    folder = PROBE_DIR / pid
    await recorder.env.run(_sweep_and_make, folder)
    await _run([ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-i", str(sample), "-map", "0:v:0", "-frames:v", "1",
                "-vf", f"scale={FRAME_WIDTH}:-2", "-y", str(folder / "frame.jpg")], 20)
    sound = next((t for t in tracks if t["type"] == "audio" and t["packets"]), None)
    if sound is not None:
        await _run([ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-i", str(sample), "-map", "0:a:0",
                    "-c:a", "aac", "-b:a", "48k", "-y", str(folder / "audio.m4a")], 20)
        _out, levels = await _run([ffmpeg, "-hide_banner", "-nostats", "-nostdin", "-i", str(sample), "-map", "0:a:0",
                                   "-af", "volumedetect", "-f", "null", "-"], 20)
        sound["meanDb"], sound["peakDb"] = _db(levels, "mean_volume"), _db(levels, "max_volume")
    return pid


def _sweep_and_make(folder: Path) -> None:
    """Убрать отрывки старше `KEEP_S` и завести каталог новой пробы."""
    now = time.time()
    for old in PROBE_DIR.glob("*") if PROBE_DIR.exists() else []:
        if now - old.stat().st_mtime > KEEP_S:
            shutil.rmtree(old, ignore_errors=True)
    folder.mkdir(parents=True, exist_ok=True)


def _db(text: str, key: str) -> float | None:
    found = re.search(rf"{key}:\s*(-?[\d.]+|-inf) dB", text)
    if not found:
        return None
    return None if found.group(1) == "-inf" else float(found.group(1))


async def _run(argv: list[str], timeout: float) -> tuple[str, str]:
    """Процесс со сроком; ответ — stdout и stderr текстом (`volumedetect` пишет в stderr). Срок вышел — пусто."""
    proc = await asyncio.create_subprocess_exec(*argv, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return "", ""
    return out.decode(errors="replace"), err.decode(errors="replace")


async def _probe(request: web.Request) -> web.Response:
    recorder: Recorder = request.app[_RECORDER]
    try:
        body = await request.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text='{"error": "тело — объект JSON"}', content_type="application/json")
    stream, source = str(body.get("stream") or ""), str(body.get("source") or "")
    if (stream and not _NAME.match(stream)) or (not stream and not source):
        raise web.HTTPBadRequest(text='{"error": "нужен поток go2rtc или строка источника"}', content_type="application/json")
    try:
        seconds = min(max(float(body.get("seconds") or DEFAULT_S), 2.0), MAX_S)
    except (TypeError, ValueError):
        seconds = DEFAULT_S
    return web.json_response(await probe(recorder, stream, source, input_args(body.get("input")), seconds))


async def _artifact(request: web.Request) -> web.Response:
    """Кадр (`frame.jpg`) или отрывок звука (`audio.m4a`) пробы; срок вышел или не снят — 404."""
    recorder: Recorder = request.app[_RECORDER]
    pid, name = request.match_info["pid"], request.match_info["name"]
    if not _PID.match(pid) or name not in _TYPES:
        raise web.HTTPNotFound()
    path = PROBE_DIR / pid / name
    data = await recorder.env.run(lambda: path.read_bytes() if path.is_file() else b"")
    if not data:
        raise web.HTTPNotFound(text='{"error": "проба устарела — проверьте снова"}', content_type="application/json")
    return web.Response(body=data, content_type=_TYPES[name], headers={"Cache-Control": "no-store"})


def add_routes(app: web.Application, recorder: Recorder) -> None:
    app[_RECORDER] = recorder
    app.router.add_post("/probe", _probe)
    app.router.add_get("/probe/{pid}/{name}", _artifact)
