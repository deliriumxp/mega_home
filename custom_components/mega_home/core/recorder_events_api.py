"""Маршруты службы `recorder` для событий камеры — меток на записи (`recorder_events.py`, дом 0.6.9+).

Контракт с бандлом и менеджером:
  * `GET /events?camera&from&to` (секунды UTC; окно не шире недели) → `{"events": [{"id", "start", "stop",
    "labels", "frame"}]}`, новые первыми; `start`/`stop` — мс UTC, `stop` — `null`, пока окно идёт;
  * `GET /event-frame?camera&id` → `image/jpeg` кадра метки | 404. Кадр метки не меняется никогда:
    `immutable` — телефон держит его в своём кэше, пока запись дня жива.
Событие есть, только пока есть запись под ним: метки уходят уборкой вместе с каталогом дня.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiohttp import web

from . import recorder_events

if TYPE_CHECKING:
    from .recorder import Recorder

# Окно ответа не шире недели: обход дней им не раздуть.
MAX_WINDOW_S = 7 * 86400


def add_routes(app: web.Application, recorder: Recorder) -> None:
    async def events(request: web.Request) -> web.Response:
        sink = recorder.sink(request.query.get("camera", ""))
        if sink is None:
            return web.json_response({"error": "такой камеры в записи нет"}, status=404)
        try:
            start = float(request.query["from"])
            stop = min(float(request.query["to"]), start + MAX_WINDOW_S)
        except (KeyError, ValueError):
            return web.json_response({"error": "нет чисел from и to"}, status=400)
        rows = await recorder.env.run(recorder_events.read, sink.index.root, start, stop)
        # Метки, ещё не легшие на диск (кусок окна в кэше), — не отдаём: события без записи нет.
        return web.json_response({"events": rows}, headers={"Cache-Control": "no-store"})

    async def frame(request: web.Request) -> web.Response:
        sink = recorder.sink(request.query.get("camera", ""))
        path = None if sink is None else await recorder.env.run(recorder_events.picture, sink.index.root, request.query.get("id", ""))
        if path is None:
            return web.json_response({"error": "кадра нет"}, status=404)
        body = await recorder.env.run(path.read_bytes)
        return web.Response(body=body, content_type="image/jpeg", headers={"Cache-Control": "private, max-age=31536000, immutable"})

    app.router.add_get("/events", events)
    app.router.add_get("/event-frame", frame)
