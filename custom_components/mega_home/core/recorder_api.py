"""Служба записи на петле (`recorder` в реестре `services.py`): дни, участки, список воспроизведения;
диски, папки и подключение хранилища для его выбора (`storage.py`).

Бандл и менеджер ходят сюда `connect` по имени службы — внутри дома и снаружи одинаково, без
нового маршрута двери (закрытый список `docs/home-gateway.md`: процесс и его хранилище). Служба
поднята всегда, а не только при записи: хранилище выбирают до неё.

⚠ Воспроизведение — не здесь: служба только пишет список кусков ffconcat с `inpoint` и отдаёт
готовую строку источника go2rtc (`ffmpeg:<список>#input=playlist`). Поток открывает go2rtc по
переговорам бандла, как живую камеру; перемотка — новый список (замер 2026-10-23: первый кадр
встаёт на ключевой кадр не дальше 0,2 с до метки, стык кусков без шва).
"""

from __future__ import annotations

import asyncio
import shutil
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from aiohttp import web

from . import services, storage
from .go2rtc_embed import PLAYLIST_INPUT
from .recorder import SERVICE, day_of
from .recorder_frames import sprite_for

if TYPE_CHECKING:
    from .recorder import Recorder
    from .recorder_index import SegmentIndex

# Куски с зазором меньше этого — один участок шкалы (стык кусков ffmpeg — доли секунды).
JOIN_GAP_S = 1.5
# Сколько записи вперёд кладётся в один список: дальше — новый список перемоткой.
PLAYLIST_S = 3 * 3600
# Насколько далеко за участком искать продолжение записи.
NEXT_LOOKUP_S = 2 * 24 * 3600
_RECORDER: web.AppKey[Any] = web.AppKey("recorder")
_FRAMES = asyncio.Semaphore(2)


async def start_api(recorder: Recorder) -> web.AppRunner:
    app = web.Application()
    app[_RECORDER] = recorder
    app.router.add_get("/days", _days)
    app.router.add_get("/segments", _segments)
    app.router.add_post("/playlist", _playlist)
    app.router.add_get("/frame", _frame)
    app.router.add_get("/sprite", _sprite)
    app.router.add_get("/disks", _disks)
    app.router.add_get("/folders", _folders)
    app.router.add_post("/folders", _make_folder)
    app.router.add_post("/mount", _preview_mount)
    app.router.add_delete("/mount", _preview_unmount)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    services.register(SERVICE, int(runner.addresses[0][1]), bundle_guard=_installer_only)
    return runner


def _installer_only(what: str, value: str) -> str | None:
    """Диски, папки и подключение хранилища — только менеджеру (инсталлятору): телефон с ключом
    устройства не монтирует сетевые папки ядром дома и не создаёт папки на его дисках."""
    if what == "path" and value.split("?", 1)[0] in ("/disks", "/folders", "/mount"):
        return "хранилище записи настраивает инсталлятор через менеджер"
    return None


def _camera(request: web.Request) -> str:
    recorder: Recorder = request.app[_RECORDER]
    camera = request.query.get("camera", "")
    if camera not in recorder.cameras:
        raise web.HTTPNotFound(text='{"error": "такой камеры в записи нет"}', content_type="application/json")
    return camera


async def _index(request: web.Request) -> tuple[str, SegmentIndex]:
    """Индекс записи камеры (`recorder_index.py`): ответы службы — из памяти, без обхода диска.
    До первого прохода записи (служба поднята раньше камер) индекс читается здесь, в исполнителе."""
    recorder: Recorder = request.app[_RECORDER]
    camera = _camera(request)
    index = recorder.cameras[camera].index
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


async def _playlist(request: web.Request) -> web.Response:
    """Список воспроизведения с метки `at` (мс): НЕПРЕРЫВНЫЙ участок записи, первый кусок — с
    `inpoint`.

    Ответ: `source` — строка источника go2rtc, `startMs` — где встанет первый кадр, `parts` —
    `[[начало мс, длительность мс], …]`, `nextMs` — где запись продолжается после пустоты (нет
    поля — дальше в окне записи нет).

    ⚠ Список кончается на первой пустоте, а не перешагивает её: ffconcat склеил бы участки
    встык, и время проигрывания перестало бы совпадать со временем записи (запись по движению —
    пустоты в минуты). Через пустоту бандл переходит новым списком с `nextMs`.
    """
    recorder: Recorder = request.app[_RECORDER]
    camera, index = await _index(request)
    at = _number(request, "at") / 1000
    found = [s for s in index.between(at, at + PLAYLIST_S) if s[0] + s[1] > at]
    if not found:
        return web.json_response({"error": "записи после этого момента нет"}, status=404)
    run = [found[0]]
    for piece in found[1:]:
        if piece[0] - (run[-1][0] + run[-1][1]) > JOIN_GAP_S:
            break
        run.append(piece)
    following = found[len(run)] if len(found) > len(run) else None
    if following is None:
        # Пустота длиннее окна списка (по движению — ночь): где запись продолжается, ищем дальше,
        # иначе просмотр стоял бы на последнем кадре, не зная, что дальше записи нет или она есть.
        end = run[-1][0] + run[-1][1]
        following = next((s for s in index.between(end + JOIN_GAP_S, end + NEXT_LOOKUP_S) if s[0] > end + JOIN_GAP_S), None)
    inpoint = max(at - run[0][0], 0.0)
    lines = ["ffconcat version 1.0"]
    for index, (_begin, _length, path) in enumerate(run):
        lines.append(f"file '{path}'")
        if index == 0 and inpoint > 0:
            lines.append(f"inpoint {inpoint:.3f}")
    folder = recorder.storage / ".cache" / "play"
    target = folder / f"{camera}-{uuid4().hex}.ffconcat"

    def write() -> None:
        folder.mkdir(parents=True, exist_ok=True)
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    await recorder.env.run(write)
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


async def _sprite(request: web.Request) -> web.Response:
    """Спрайт 10-минутного окна под меткой `at` (мс) — превью под пальцем на шкале
    (`recorder_frames.sprite_for`). Сборка — ffmpeg, под тем же `_FRAMES`, что и `/frame`.
    Закрытое окно не меняется — держится в кэше браузера; текущее дописывается — `no-store`."""
    recorder: Recorder = request.app[_RECORDER]
    camera, _ = await _index(request)
    at = _number(request, "at") / 1000
    async with _FRAMES:
        image, closed = await sprite_for(recorder, camera, at)
    if not image:
        raise web.HTTPNotFound(text='{"error": "кадров в окне нет"}', content_type="application/json")
    return web.Response(body=image, content_type="image/jpeg", headers={"Cache-Control": "private, max-age=3600" if closed else "no-store"})
