"""Раздача маркерного сигнала акустического выравнивания колонок.

Зачем он и почему раскладка именно такая — `providers/marker.py`. Здесь только HTTP: приложение
жильца включает маркер на каждую колонку командой `play_stream` (тот же путь, что у радио), каждая
получает СВОЙ адрес — со своим слотом в круге (`?slot=<i>&slots=<n>`) и своей высотой щелчка.
"""

from __future__ import annotations

import asyncio
import shutil
from http import HTTPStatus
from typing import Any

from aiohttp import web

from .core.const import URL_PREFIX
from .http_door import _MegaHomeView, _coordinator
from .providers.marker import SLOTS_MAX, cycle_pcm, marker_description
from .providers.music_ffmpeg import ffmpeg_encoder

MARKER_HEADERS = {"Content-Type": "audio/mpeg", "Cache-Control": "no-store"}

# Сколько кругов наливаем в трубу за раз, прежде чем отдать ffmpeg: мельчить — лишние пробуждения,
# крупно — задержка первой секунды звука.
CHUNKS_PER_WRITE = 4


class MegaHomeMarkerView(_MegaHomeView):
    """`/mega-home/marker.mp3?slot=<i>&slots=<n>` — маркер для ОДНОЙ колонки круга.

    ⚠ Ни ключа устройства, ни куки: адрес забирает ПЛЕЕР (Sonos, Эхо, Станция), у него учётки нет —
    как у потока сессии (`http_music.py`). Секрета в сигнале нет: это щелчки.
    """

    url = f"{URL_PREFIX}/marker.mp3"
    name = "mega_home:marker"

    async def get(self, request: web.Request) -> web.StreamResponse:
        return await self._marker(request, body=True)

    async def head(self, request: web.Request) -> web.StreamResponse:
        return await self._marker(request, body=False)

    async def _marker(self, request: web.Request, *, body: bool) -> web.StreamResponse:
        slot, slots = _placement(request.query)
        if slot is None:
            # ⚠ На HEAD тела быть не может (aiohttp отвечает на него 500) — отказ без тела.
            return web.Response(
                status=HTTPStatus.BAD_REQUEST,
                text="" if not body else f"Слот маркера — от 0 до {SLOTS_MAX - 1}, и он меньше числа колонок",
            )
        if not body:
            return web.Response(status=HTTPStatus.OK, headers=MARKER_HEADERS)
        coordinator = _coordinator(request.app["hass"])
        env = getattr(coordinator, "env", None)
        binary = await env.run(shutil.which, "ffmpeg") if env is not None else None
        if not binary:
            return self.json_message(
                "В этом доме нет ffmpeg — маркер отдать нечем", HTTPStatus.SERVICE_UNAVAILABLE
            )
        cycle = cycle_pcm(slot, slots)
        response = web.StreamResponse(status=HTTPStatus.OK, headers=MARKER_HEADERS)
        await response.prepare(request)
        try:
            await _stream_cycle(binary, cycle, request, response)
        except (ConnectionError, OSError):
            # Колонка закрыла поток (замер закончился, плеер ушёл) — это норма, не ошибка.
            response.force_close()
            return response
        try:
            await response.write_eof()
        except (ConnectionError, OSError):
            response.force_close()
        return response


class MegaHomeMarkerSpecView(_MegaHomeView):
    """`/mega-home/marker.json?slots=<n>` — раскладка круга: приложение берёт числа отсюда."""

    url = f"{URL_PREFIX}/marker.json"
    name = "mega_home:marker_spec"

    async def get(self, request: web.Request) -> web.StreamResponse:
        try:
            slots = max(1, min(SLOTS_MAX, int(request.query.get("slots", "1"))))
        except ValueError:
            slots = 1
        return web.json_response(marker_description(slots), headers={"Cache-Control": "no-store"})


def _placement(query: Any) -> tuple[int | None, int]:
    """Слот и число колонок из адреса: слот обязан быть своим и в пределах круга."""
    try:
        slots = max(1, min(SLOTS_MAX, int(query.get("slots", "1"))))
        slot = int(query.get("slot", "0"))
    except (TypeError, ValueError):
        return None, 1
    if not 0 <= slot < slots:
        return None, slots
    return slot, slots


def _client_gone(request: web.Request) -> bool:
    """Колонка (или тест) закрыла соединение: транспорт на нашей стороне уже closing."""
    transport = request.transport
    return transport is None or transport.is_closing()


async def _stream_cycle(binary: str, cycle: bytes, request: web.Request, response: web.StreamResponse) -> None:
    """Крутим один круг в кодировщик, пока колонка слушает: файл кончается, а круг — нет.

    ⚠ Признак «колонка ушла» ставит НАША запись (кодировщик её ошибку глушит): без этого цикл крутил
    бы звук в никуда, а замер на объекте оставлял бы за собой живые процессы ffmpeg.
    """
    closed = False

    async def write(chunk: bytes) -> None:
        nonlocal closed
        if closed:
            return
        try:
            await response.write(chunk)
        except (ConnectionError, OSError):
            closed = True
            raise

    encoder = await ffmpeg_encoder(binary, write)
    try:
        # ⚠ Признак «колонка ушла» берём у ТРАНСПОРТА, а не у ошибки записи: aiohttp закрытию
        # соединения не всегда отвечает исключением, и без этой проверки цикл крутил бы звук в никуда
        # (а на объекте за замером оставались бы живые ffmpeg).
        while not closed and not _client_gone(request):
            for _ in range(CHUNKS_PER_WRITE):
                await encoder.write(cycle)
            await asyncio.sleep(0.05)
    finally:
        await encoder.stop()
