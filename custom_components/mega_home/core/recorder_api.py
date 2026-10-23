"""Служба записи на петле (`recorder` в реестре `services.py`): дни, участки, список воспроизведения.

Бандл и менеджер ходят сюда `connect` по имени службы — внутри дома и снаружи одинаково, без
нового маршрута двери (закрытый список `docs/home-gateway.md`: процесс и его хранилище).

⚠ Воспроизведение — не здесь: служба только пишет список кусков ffconcat с `inpoint` и отдаёт
готовую строку источника go2rtc (`ffmpeg:<список>#input=playlist`). Поток открывает go2rtc по
переговорам бандла, как живую камеру; перемотка — новый список (замер 2026-10-23: первый кадр
встаёт на ключевой кадр не дальше 0,2 с до метки, стык кусков без шва).
"""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from aiohttp import web

from . import services
from .go2rtc_embed import PLAYLIST_INPUT
from .recorder import SERVICE, segments_of

if TYPE_CHECKING:
    from .recorder import Recorder

# Куски с зазором меньше этого — один участок шкалы (стык кусков ffmpeg — доли секунды).
JOIN_GAP_S = 1.5
# Сколько записи вперёд кладётся в один список: дальше — новый список перемоткой.
PLAYLIST_S = 3 * 3600
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RECORDER: web.AppKey[Any] = web.AppKey("recorder")


async def start_api(recorder: Recorder) -> web.AppRunner:
    app = web.Application()
    app[_RECORDER] = recorder
    app.router.add_get("/days", _days)
    app.router.add_get("/segments", _segments)
    app.router.add_post("/playlist", _playlist)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    services.register(SERVICE, int(runner.addresses[0][1]))
    return runner


def _camera(request: web.Request) -> str:
    recorder: Recorder = request.app[_RECORDER]
    camera = request.query.get("camera", "")
    if camera not in recorder.cameras:
        raise web.HTTPNotFound(text='{"error": "такой камеры в записи нет"}', content_type="application/json")
    return camera


def _number(request: web.Request, name: str, default: float | None = None) -> float:
    try:
        return float(request.query[name])
    except (KeyError, ValueError) as err:
        if default is not None:
            return default
        raise web.HTTPBadRequest(text=f'{{"error": "нет числа {name}"}}', content_type="application/json") from err


async def _days(request: web.Request) -> web.Response:
    recorder: Recorder = request.app[_RECORDER]
    root = recorder.storage / _camera(request)

    def listing() -> list[str]:
        if not root.is_dir():
            return []
        return sorted(d.name for d in root.iterdir() if d.is_dir() and _DAY.match(d.name) and any(d.iterdir()))

    return web.json_response({"days": await recorder.env.run(listing)})


async def _segments(request: web.Request) -> web.Response:
    """Участки записи в окне `from`..`to` (секунды): `[[начало мс, конец мс], …]`."""
    recorder: Recorder = request.app[_RECORDER]
    camera = _camera(request)
    start, stop = _number(request, "from"), _number(request, "to")
    found = await recorder.env.run(segments_of, recorder.storage, camera, start, stop)
    merged: list[list[int]] = []
    for begin, length, _path in found:
        if merged and begin * 1000 - merged[-1][1] <= JOIN_GAP_S * 1000:
            merged[-1][1] = max(merged[-1][1], int((begin + length) * 1000))
        else:
            merged.append([int(begin * 1000), int((begin + length) * 1000)])
    return web.json_response({"segments": merged})


async def _playlist(request: web.Request) -> web.Response:
    """Список воспроизведения с метки `at` (мс): куски подряд, первый — с `inpoint`.

    Ответ: `source` — строка источника go2rtc, `startMs` — где встанет первый кадр, `parts` —
    `[[начало мс, длительность мс], …]` кусков списка: по ним бандл переводит время проигрывания
    в стенное (между кусками бывают пустоты — запись по движению).
    """
    recorder: Recorder = request.app[_RECORDER]
    camera = _camera(request)
    at = _number(request, "at") / 1000
    found = [s for s in await recorder.env.run(segments_of, recorder.storage, camera, at, at + PLAYLIST_S) if s[0] + s[1] > at]
    if not found:
        return web.json_response({"error": "записи после этого момента нет"}, status=404)
    inpoint = max(at - found[0][0], 0.0)
    lines = ["ffconcat version 1.0"]
    for index, (_begin, _length, path) in enumerate(found):
        lines.append(f"file '{path}'")
        if index == 0 and inpoint > 0:
            lines.append(f"inpoint {inpoint:.3f}")
    folder = recorder.storage / ".cache" / "play"
    target = folder / f"{camera}-{uuid4().hex}.ffconcat"

    def write() -> None:
        folder.mkdir(parents=True, exist_ok=True)
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    await recorder.env.run(write)
    parts = [[int(b * 1000), int(n * 1000)] for b, n, _p in found]
    parts[0] = [int((found[0][0] + inpoint) * 1000), int((found[0][1] - inpoint) * 1000)]
    return web.json_response(
        {
            "source": f"ffmpeg:{target}#input={PLAYLIST_INPUT}#video=copy#audio=copy",
            "startMs": parts[0][0],
            "parts": parts,
            "createdAt": round(time.time(), 3),
        }
    )
