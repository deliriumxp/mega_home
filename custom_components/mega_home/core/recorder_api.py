"""Служба записи на петле (`recorder` в реестре `services.py`): дни, участки, список воспроизведения;
диски, папки и подключение хранилища для его выбора (`storage.py`).

Бандл и менеджер ходят сюда `connect` по имени службы — внутри дома и снаружи одинаково, без
нового маршрута двери (закрытый список `docs/home-gateway.md`: процесс и его хранилище). Служба
поднята всегда, а не только при записи: хранилище выбирают до неё.

⚠ Воспроизведение — не здесь: служба только пишет список кусков ffconcat с `inpoint` и отдаёт
готовую строку источника go2rtc (`ffmpeg:<список>#input=playlist`). Поток открывает go2rtc по
переговорам бандла, как живую камеру; перемотка — новый список (стык кусков без шва).

⚠ Поток идёт копированием, а копия начинается с КЛЮЧЕВОГО кадра не позже `inpoint`: у камер с
GOP в 3–4 с видео встаёт на секунды раньше метки (замер на живом доме 2026-10-24: 1–3 с). Поэтому
`inpoint` ставится на сам ключевой кадр (`_keyframe_before`), а `startMs` называет его время: игла
бандла встаёт туда, где видео на самом деле началось.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from aiohttp import web

from . import activity, services, storage
from .go2rtc_embed import PLAYLIST_INPUT
from .recorder import SERVICE, day_of
from .recorder_play_api import add_routes
from .recorder_preview import whole_fragments

if TYPE_CHECKING:
    from .recorder import Recorder
    from .recorder_index import SegmentIndex
    from .recorder_sink import RecordSink

# Куски с зазором меньше этого — один участок шкалы (стык кусков ffmpeg — доли секунды).
JOIN_GAP_S = 1.5
# Сколько записи вперёд кладётся в один список: дальше — новый список перемоткой.
PLAYLIST_S = 3 * 3600
# Насколько далеко за участком искать продолжение записи.
NEXT_LOOKUP_S = 2 * 24 * 3600
# Насколько вперёд от метки в пустоте искать запись (`_playlist`): сутки. Не дальше — край прошлых
# суток бандла (23:59:59 перед сутками без записи) иначе уехал бы через них в следующие.
FORWARD_S = 24 * 3600
_RECORDER: web.AppKey[Any] = web.AppKey("recorder")
# ⚠ Две разных очереди: список воспроизведения не стоит за кадрами под пальцем.
_FRAMES = asyncio.Semaphore(2)
_PROBES = asyncio.Semaphore(2)
# Сколько до метки смотрим назад в поисках ключевого кадра: больше типичного GOP камеры.
KEYFRAME_LOOKBACK_S = 12.0
KEYFRAME_TIMEOUT_S = 5


async def start_api(recorder: Recorder) -> web.AppRunner:
    app = web.Application()
    app[_RECORDER] = recorder
    app.router.add_get("/days", _days)
    app.router.add_get("/segments", _segments)
    app.router.add_post("/playlist", _playlist)
    app.router.add_get("/frame", _frame)
    app.router.add_get("/previews", _previews)
    app.router.add_get("/preview", _preview)
    app.router.add_get("/overview", _overview)
    app.router.add_get("/state", _state)
    app.router.add_get("/activity", _activity)
    app.router.add_get("/disks", _disks)
    app.router.add_get("/folders", _folders)
    app.router.add_post("/folders", _make_folder)
    app.router.add_post("/mount", _preview_mount)
    app.router.add_delete("/mount", _preview_unmount)
    # Просмотр сессией и последний кадр камеры (дом 0.6.0+): `recorder_play_api.py`.
    add_routes(app, recorder)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    # Метка кэша — своя (`Service.own_cache`): закрытый кусок превью и кадр под меткой не меняются, идущий — да.
    services.register(SERVICE, int(runner.addresses[0][1]), bundle_guard=_installer_only, own_cache=True)
    return runner


def _installer_only(what: str, value: str) -> str | None:
    """Диски, папки и подключение хранилища — только менеджеру (инсталлятору): телефон с ключом
    устройства не монтирует сетевые папки ядром дома и не создаёт папки на его дисках. Путь сюда
    приходит уже приведённым (`connect.service_path`)."""
    path = value.split("?", 1)[0].rstrip("/") if what == "path" else ""
    if path in ("/disks", "/folders", "/mount"):
        return "хранилище записи настраивает инсталлятор через менеджер"
    if path == "/state":
        return "состояние записи смотрит инсталлятор через менеджер"
    # Поток сессии читает свой go2rtc на петле; телефону он идёт WebRTC, а не дверью.
    if path.startswith("/sessions/") and path.endswith(".ts"):
        return "поток просмотра читает go2rtc дома"
    return None


def _camera(request: web.Request) -> str:
    recorder: Recorder = request.app[_RECORDER]
    camera = request.query.get("camera", "")
    if recorder.sink(camera) is None:
        raise web.HTTPNotFound(text='{"error": "такой камеры в записи нет"}', content_type="application/json")
    return camera


def _sink(recorder: Recorder, camera: str) -> RecordSink:
    sink = recorder.sink(camera)
    assert sink is not None  # `_camera` уже ответил 404 на чужую камеру
    return sink


async def _index(request: web.Request) -> tuple[str, SegmentIndex]:
    """Индекс записи камеры (`recorder_index.py`): ответы службы — из памяти, без обхода диска.
    До первого прохода записи (служба поднята раньше камер) индекс читается здесь, в исполнителе."""
    recorder: Recorder = request.app[_RECORDER]
    camera = _camera(request)
    index = _sink(recorder, camera).index
    if not index.loaded:
        await recorder.env.run(index.ensure, bool(recorder.limit))
    return camera, index


def _number(request: web.Request, name: str, default: float | None = None) -> float:
    try:
        return float(request.query[name])
    except (KeyError, ValueError) as err:
        if default is not None:
            return default
        raise web.HTTPBadRequest(text=f'{{"error": "нет числа {name}"}}', content_type="application/json") from err


async def _days(request: web.Request) -> web.Response:
    """Дни с записью. `offset` (секунды к UTC) — сутки по стенным часам объекта: каталоги дней
    в хранилище — UTC, и сутки зрителя захватывают два соседних."""
    _camera_id, index = await _index(request)
    offset = _number(request, "offset", 0.0)
    days = {day_of(edge + offset) for _name, first, last in index.days() for edge in (first, last)}
    return web.json_response({"days": sorted(days)})


async def _segments(request: web.Request) -> web.Response:
    """Участки записи в окне `from`..`to` (секунды): `[[начало мс, конец мс], …]`."""
    _camera_id, index = await _index(request)
    start, stop = _number(request, "from"), _number(request, "to")
    found = index.between(start, stop)
    merged: list[list[int]] = []
    for begin, length, _path in found:
        if merged and begin * 1000 - merged[-1][1] <= JOIN_GAP_S * 1000:
            merged[-1][1] = max(merged[-1][1], int((begin + length) * 1000))
        else:
            merged.append([int(begin * 1000), int((begin + length) * 1000)])
    return web.json_response({"segments": merged})


async def _keyframe_before(recorder: Recorder, path: Any, offset: float) -> float:
    """Время (секунды от начала куска) ближайшего ключевого кадра не позже `offset`; не нашли
    или ffprobe нет — сам `offset`: список играется с ближайшего ключевого кадра и так.

    ⚠ Ошибка тут — не отказ списка: время старта станет приблизительным, как было раньше.
    ⚠ По ПАКЕТАМ (флаг `K`), а не по кадрам: `frame=` заставлял ffprobe декодировать до 12 с видео —
    2,5 с на кусок 720p, и столько ждала каждая перемотка (живой прогон сессии 2026-10-26).
    """
    binary = await recorder.env.run(shutil.which, "ffprobe")
    if not binary:
        return offset
    low = max(offset - KEYFRAME_LOOKBACK_S, 0.0)
    async with _PROBES:
        proc = await asyncio.create_subprocess_exec(
            binary, "-v", "error", "-select_streams", "v:0", "-read_intervals", f"{low:.3f}%{offset + 0.5:.3f}",
            "-show_entries", "packet=pts_time,flags:format=start_time", "-of", "json", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), KEYFRAME_TIMEOUT_S)
            answer = json.loads(out.decode() or "{}")
        except (asyncio.TimeoutError, ValueError):
            if proc.returncode is None:
                proc.kill()
            return offset
    try:
        start = float((answer.get("format") or {}).get("start_time") or 0.0)
        keys = [float(p["pts_time"]) - start for p in answer.get("packets", []) if "K" in str(p.get("flags", "")) and "pts_time" in p]
    except (TypeError, ValueError):
        return offset
    before = [k for k in keys if k <= offset + 0.05]
    return max(max(before), 0.0) if before else offset


# Им же встают сессии просмотра (`recorder_session.py`).
keyframe_before = _keyframe_before


async def has_audio(recorder: Recorder, path: Any) -> bool:
    """Есть ли в куске хоть один пакет звука. ⚠ У камер с динамиком и микрофоном дорожка объявлена всегда, а
    пакетов может не быть вовсе (EYEPLUS, 2026-10-27). Не узнали — звук есть: так играли и раньше."""
    binary = await recorder.env.run(shutil.which, "ffprobe")
    if not binary:
        return True
    async with _PROBES:
        proc = await asyncio.create_subprocess_exec(
            binary, "-v", "error", "-select_streams", "a:0", "-read_intervals", "%+#1",
            "-show_entries", "packet=pts_time", "-of", "csv=p=0", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), KEYFRAME_TIMEOUT_S)
        except asyncio.TimeoutError:
            if proc.returncode is None:
                proc.kill()
            return True
    return proc.returncode != 0 or bool(out.strip())


async def _playlist(request: web.Request) -> web.Response:
    """Список воспроизведения с метки `at` (мс): НЕПРЕРЫВНЫЙ участок записи, первый кусок — с
    `inpoint`.

    Ответ: `source` — строка источника go2rtc, `startMs` — где встанет первый кадр, `parts` —
    `[[начало мс, длительность мс], …]`, `nextMs` — где запись продолжается после пустоты (нет
    поля — дальше в окне записи нет).

    ⚠ Список кончается на первой пустоте, а не перешагивает её: ffconcat склеил бы участки
    встык, и время проигрывания перестало бы совпадать со временем записи (запись по движению —
    пустоты в минуты). Через пустоту бандл переходит новым списком с `nextMs`.

    ⚠ Метка в пустоте — ближайшая запись, как у регистратора: вперёд в пределах суток
    (`FORWARD_S`), иначе последний кусок ДО метки с его начала. Так бандл и зовёт без параметра
    направления: выбор дня в календаре — полночь, а запись по движению начинается к полудню (окно в
    3 ч отвечало 404); край прошлых суток (`recordedDayEdge`, 23:59:59 — следом пустые сутки,
    вперёд записи в сутках нет) — конец записи того дня, а не 404.
    """
    recorder: Recorder = request.app[_RECORDER]
    camera, index = await _index(request)
    at = _number(request, "at") / 1000
    first = index.first_after(at, at + FORWARD_S) or index.last_before(at)
    if first is None:
        return web.json_response({"error": "записи у этого момента нет"}, status=404)
    start = at if first[0] + first[1] > at else first[0]
    run = [first]
    for piece in index.between(first[0] + first[1], start + PLAYLIST_S):
        if piece[0] <= run[-1][0]:
            continue
        if piece[0] - (run[-1][0] + run[-1][1]) > JOIN_GAP_S:
            break
        run.append(piece)
    # Где запись продолжается — следующий кусок после последнего в списке: за пустотой (по движению —
    # ночь) или ВПЛОТНУЮ, если список кончился по `PLAYLIST_S` (иначе 10 с записи пропадали каждые 3 ч).
    following = index.first_after(run[-1][0], run[-1][0] + NEXT_LOOKUP_S, started=True)
    inpoint = max(start - run[0][0], 0.0)
    if inpoint > 0:
        inpoint = await _keyframe_before(recorder, run[0][2], inpoint)
    lines = ["ffconcat version 1.0"]
    for number, (_begin, _length, path) in enumerate(run):
        lines.append(f"file '{path}'")
        if number == 0 and inpoint > 0:
            lines.append(f"inpoint {inpoint:.3f}")
    folder = recorder.storage / ".cache" / "play"
    target = folder / f"{camera}-{uuid4().hex}.ffconcat"

    def write() -> None:
        # Без `parents`: `.cache` заводит старт камер на проверенном хранилище; пропал диск — каталога нет,
        # и путь не создаётся на флеше контроллера под точкой монтирования.
        folder.mkdir(exist_ok=True)
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    try:
        await recorder.env.run(write)
    except OSError:
        return web.json_response({"error": "хранилище записи недоступно"}, status=503)
    parts = [[int(b * 1000), int(n * 1000)] for b, n, _p in run]
    parts[0] = [int((run[0][0] + inpoint) * 1000), int((run[0][1] - inpoint) * 1000)]
    return web.json_response(
        {
            "source": f"ffmpeg:{target}#input={PLAYLIST_INPUT}#video=copy#audio=copy",
            "startMs": parts[0][0],
            "parts": parts,
            **({"nextMs": int(following[0] * 1000)} if following else {}),
            "createdAt": round(time.time(), 3),
        }
    )


def _refused(text: str) -> web.Response:
    return web.json_response({"error": text}, status=400)


async def _body(request: web.Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text='{"error": "тело — объект JSON"}', content_type="application/json")
    return body


async def _disks(request: web.Request) -> web.Response:
    """Разделы с файловой системой — USB-диск под запись выбирают по UUID (`storage.disks`)."""
    recorder: Recorder = request.app[_RECORDER]
    return web.json_response({"disks": await recorder.env.run(storage.disks)})


async def _folders(request: web.Request) -> web.Response:
    """Папки каталога хранения с диском каждой (системный ли, свободно, всего) — выбор папки."""
    recorder: Recorder = request.app[_RECORDER]
    try:
        return web.json_response(await recorder.env.run(storage.folders, request.query.get("path", ""), recorder.env.path()))
    except (ValueError, OSError) as err:
        return _refused(str(err))


async def _make_folder(request: web.Request) -> web.Response:
    recorder: Recorder = request.app[_RECORDER]
    body = await _body(request)
    try:
        created = await recorder.env.run(storage.make_folder, str(body.get("path") or ""), str(body.get("name") or ""))
    except (ValueError, OSError) as err:
        return _refused(str(err))
    return web.json_response({"path": created})


async def _preview_mount(request: web.Request) -> web.Response:
    """Подключить диск или сетевую папку ДЛЯ ВЫБОРА папки — в свою точку (`PREVIEW_POINT`), не
    трогая идущую запись; учётка — в теле, не в адресе. Ответ — папки подключённого."""
    recorder: Recorder = request.app[_RECORDER]
    problem = await recorder.env.run(storage.mount, await _body(request), storage.PREVIEW_POINT)
    if problem:
        return _refused(problem)
    return web.json_response(await recorder.env.run(storage.folders, str(storage.PREVIEW_POINT), recorder.env.path()))


async def _preview_unmount(request: web.Request) -> web.Response:
    recorder: Recorder = request.app[_RECORDER]
    await recorder.env.run(storage.unmount, storage.PREVIEW_POINT)
    return web.json_response({"ok": True})


async def _frame(request: web.Request) -> web.Response:
    """Кадр записи на метке `at` (мс), JPEG шириной `width`: превью ленты и шкалы.

    ⚠ ffmpeg декодирует кадр — не больше двух разом (`_FRAMES`): драг по шкале и прокрутка
    ленты иначе запускали бы их десятками на слабом контроллере. Метка в пустоте — ближайший
    кадр после неё в пределах минуты, дальше — 404.
    """
    recorder: Recorder = request.app[_RECORDER]
    _camera_id, index = await _index(request)
    at = _number(request, "at") / 1000
    width = int(min(max(_number(request, "width", 640.0), 64), 1920))
    found = [s for s in index.between(at, at + 60) if s[0] + s[1] > at]
    binary = await recorder.env.run(shutil.which, "ffmpeg")
    if not found or not binary:
        raise web.HTTPNotFound(text='{"error": "кадра в записи нет"}', content_type="application/json")
    begin, _length, path = found[0]
    async with _FRAMES:
        proc = await asyncio.create_subprocess_exec(
            binary, "-hide_banner", "-loglevel", "error", "-ss", f"{max(at - begin, 0):.3f}", "-i", str(path),
            "-frames:v", "1", "-vf", f"scale={width}:-2", "-f", "image2", "-c:v", "mjpeg", "pipe:1",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            image, _ = await asyncio.wait_for(proc.communicate(), 15)
        except asyncio.TimeoutError:
            proc.kill()
            image = b""
    if not image:
        raise web.HTTPNotFound(text='{"error": "кадр не извлёкся"}', content_type="application/json")
    # Запись под меткой не меняется — кадр можно держать в кэше браузера.
    return web.Response(body=image, content_type="image/jpeg", headers={"Cache-Control": "private, max-age=3600"})


async def _preview_index(request: web.Request) -> Any:
    recorder: Recorder = request.app[_RECORDER]
    camera = _sink(recorder, _camera(request))
    if not camera.previews.loaded:
        await recorder.env.run(camera.previews.load)
    return camera


async def _previews(request: web.Request) -> web.Response:
    """Куски превью-видео в окне `from`..`to` (секунды): `[[начало мс, конец мс], …]` и идущий
    (`live`: `[начало мс, конец мс по последней записи]` или null) — `recorder_preview.py`."""
    camera = await _preview_index(request)
    start, stop = _number(request, "from"), _number(request, "to")
    pieces = [[int(b * 1000), int((b + n) * 1000)] for b, n in camera.previews.between(start, stop)]
    current = await camera.rec.env.run(camera.preview.current)
    live = None
    if current is not None:
        stamp = await camera.rec.env.run(lambda: current[1].stat().st_mtime)
        if stamp > start and current[0] < stop:
            live = [int(current[0] * 1000), int(stamp * 1000)]
    # Обзор суток (дом 0.6.3+, `recorder_overview.py`): `[первый кадр мс, конец мс, шаг с, идёт ли]`.
    overview = camera.rec.overviews.listed(camera, start, stop)
    return web.json_response({"previews": pieces, "live": live, "overview": overview})


async def _overview(request: web.Request) -> web.Response:
    """Обзор суток превью с первым кадром `start` (мс) — mp4 целиком; идущие сутки растут — `no-store`."""
    camera = await _preview_index(request)
    start = _number(request, "start") / 1000
    path = camera.rec.overviews.path(camera, start)
    data = await camera.rec.env.run(_read, path) if path else b""
    if not data:
        raise web.HTTPNotFound(text='{"error": "обзора нет"}', content_type="application/json")
    cache = "no-store" if day_of(start) == day_of(time.time()) else "private, max-age=86400"
    return web.Response(body=data, content_type="video/mp4", headers={"Cache-Control": cache})


async def _state(request: web.Request) -> web.Response:
    """Состояние записи и камер (как в диагностике HA) с часами дома `now`: менеджер судит о простое камеры —
    отказ читателя, давний кадр — по ним, а не по своим часам (дом 0.6.4+)."""
    recorder: Recorder = request.app[_RECORDER]
    return web.json_response({"now": time.time(), **recorder.state()})


async def _activity(request: web.Request) -> web.Response:
    """Сила движения в окне `from`..`to` (секунды): `[[начало корзины мс, доля], …]` по `step` секунд —
    записанные ряды суток и ещё не записанные корзины детектора (`activity.py`)."""
    recorder: Recorder = request.app[_RECORDER]
    cid = _camera(request)
    camera = _sink(recorder, cid)
    start = _number(request, "from")
    # Лента спрашивает по суткам; окно шире недели — не её вопрос, и обход дней им не раздуть.
    stop = min(_number(request, "to"), start + 7 * 86400)
    days = sorted({day_of(start), day_of(stop)} | {day_of(at) for at in range(int(start), int(stop), 86400)})
    rows = dict(await recorder.env.run(lambda: [row for day in days for row in activity.read(camera.index.root / day, start, stop)]))
    rows.update(recorder.pending_activity(cid, start, stop))
    return web.json_response({"step": activity.STEP_S, "activity": [[b * 1000, round(v, 4)] for b, v in sorted(rows.items())]})


async def _preview(request: web.Request) -> web.Response:
    """Кусок превью-видео с началом `start` (мс) — mp4 целиком. Закрытый не меняется — сутки в кэше
    браузера; идущий — до последнего целого фрагмента (`whole_fragments`) и `no-store`."""
    camera = await _preview_index(request)
    start = _number(request, "start") / 1000
    path = camera.previews.path(start)
    current = None if path else await camera.rec.env.run(camera.preview.current)
    if current is not None and abs(current[0] - start) < 0.001:
        data = whole_fragments(await camera.rec.env.run(current[1].read_bytes))
        cache = "no-store"
    else:
        data = await camera.rec.env.run(_read, path) if path else b""
        cache = "private, max-age=86400"
    if not data:
        raise web.HTTPNotFound(text='{"error": "превью нет"}', content_type="application/json")
    return web.Response(body=data, content_type="video/mp4", headers={"Cache-Control": cache})


def _read(path: Any) -> bytes:
    try:
        return path.read_bytes()
    except OSError:
        return b""
