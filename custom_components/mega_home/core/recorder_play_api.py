"""Маршруты службы `recorder` для просмотра записи сессией и последнего кадра камеры (дом 0.6.0+).

Контракт с бандлом (`home-playback.ts` менеджера) и менеджером (кадр маски, кадр события):
  * `POST /sessions?camera&at=<мс UTC>` → `{"id", "stream", "startMs"}` | 404 — записи у метки нет | 429 — все
    места камеры (дома) заняты смотрящими (дом 0.6.5+; раньше вытеснялась смотрящая);
  * `POST /sessions/{id}/seek?at=<мс>` → `{"startMs"}` | 404 `{"error": "сессии нет"}` — бандл откроет новую |
    416 — записи у метки нет (сессия играет прежнее место);
  * `POST /sessions/{id}/pause` → `{"ok": true}`; `DELETE /sessions/{id}` → `{"ok": true}`;
  * `GET /sessions/{id}.ts` — поток сессии; читает только свой go2rtc на петле (`recorder_api._installer_only`
    закрывает его телефону: поток идёт к нему WebRTC, а не дверью);
  * `GET /latest?camera` → `image/jpeg` последнего кадра конвейера камеры | 404.
Сессии — `recorder_session.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import time

from aiohttp import web

from .recorder_session import SessionsBusy

# Кадр конвейера обновляется раз в 2 с: старше этого — читатель стоит.
LATEST_STALE_S = 30.0

if TYPE_CHECKING:
    from .recorder import Recorder

_NO_SESSION = {"error": "сессии нет"}


def add_routes(app: web.Application, recorder: Recorder) -> None:
    async def open_session(request: web.Request) -> web.Response:
        camera = request.query.get("camera", "")
        at = _ms(request)
        if recorder.sink(camera) is None:
            return web.json_response({"error": "такой камеры в записи нет"}, status=404)
        try:
            sid, stream, start = await recorder.sessions.open(camera, at)
        except LookupError as err:
            return web.json_response({"error": str(err)}, status=404)
        except SessionsBusy as err:
            # Все места заняты смотрящими: отказ новому, а не вытеснение чужого просмотра (ревью 2026-10-27).
            return web.json_response({"error": str(err)}, status=429)
        return web.json_response({"id": sid, "stream": stream, "startMs": int(start * 1000)})

    async def seek(request: web.Request) -> web.Response:
        try:
            start = await recorder.sessions.seek(request.match_info["sid"], _ms(request))
        except KeyError:
            return web.json_response(_NO_SESSION, status=404)
        except LookupError as err:
            # ⚠ Не 404: у перемотки 404 значит «сессии нет», и бандл открыл бы новую на той же пустой метке.
            return web.json_response({"error": str(err)}, status=416)
        return web.json_response({"startMs": int(start * 1000)})

    async def pause(request: web.Request) -> web.Response:
        try:
            await recorder.sessions.pause(request.match_info["sid"])
        except KeyError:
            return web.json_response(_NO_SESSION, status=404)
        return web.json_response({"ok": True})

    async def close(request: web.Request) -> web.Response:
        await recorder.sessions.close(request.match_info["sid"])
        return web.json_response({"ok": True})

    async def stream(request: web.Request) -> web.StreamResponse:
        return await recorder.sessions.serve(request.match_info["sid"], request)

    async def latest(request: web.Request) -> web.Response:
        pipeline = recorder.pipelines.get(request.query.get("camera", ""))
        shot = pipeline.latest() if pipeline is not None else None
        if shot is None:
            return web.json_response({"error": "кадра камеры нет"}, status=404)
        if time.time() - shot[1] > LATEST_STALE_S:
            # ⚠ Читатель стоит — кадр давний: отдать его как живой значило показать плитку «живой» и старый кадр к
            # звонку (ревью 2026-10-27). 404 — просящий берёт другой путь (кадр go2rtc).
            return web.json_response({"error": "кадр камеры устарел — поток не идёт"}, status=404)
        # Кадр обновляется раз в 2 с (`video_reader.LATEST_FPS`): держать дольше — показывать старое.
        return web.Response(body=shot[0], content_type="image/jpeg", headers={"Cache-Control": "private, max-age=2"})

    app.router.add_post("/sessions", open_session)
    app.router.add_post("/sessions/{sid}/seek", seek)
    app.router.add_post("/sessions/{sid}/pause", pause)
    app.router.add_delete("/sessions/{sid}", close)
    app.router.add_get("/sessions/{sid}.ts", stream)
    app.router.add_get("/latest", latest)


def _ms(request: web.Request) -> float:
    try:
        return float(request.query["at"]) / 1000
    except (KeyError, ValueError) as err:
        raise web.HTTPBadRequest(text='{"error": "нет числа at"}', content_type="application/json") from err
