"""Раздача маркерного сигнала акустического выравнивания колонок.

Зачем он и почему числа шаблона именно такие — `providers/marker.py`. Здесь только HTTP: приложение
жильца включает маркер на колонку командой `play_stream` (тот же путь, что у радио), а плеер забирает
файл как обычную ссылку.
"""

from __future__ import annotations

import shutil
from http import HTTPStatus

from aiohttp import web

from .core.const import URL_PREFIX
from .http_door import _MegaHomeView, _coordinator
from .providers.marker import marker_description, marker_pcm
from .providers.music_flow import ffmpeg_encoder

MARKER_HEADERS = {"Content-Type": "audio/mpeg", "Cache-Control": "no-store"}

_MP3: bytes | None = None


class MegaHomeMarkerView(_MegaHomeView):
    """`/mega-home/marker.mp3` — маркер для плеера.

    ⚠ Ни ключа устройства, ни куки: адрес забирает ПЛЕЕР (Sonos, Эхо, Станция), у него учётки нет — как
    у потока сессии (`http_music.py`). Секрета в сигнале нет: это щелчки.
    """

    url = f"{URL_PREFIX}/marker.mp3"
    name = "mega_home:marker"

    async def get(self, request: web.Request) -> web.StreamResponse:
        return await self._marker(request, body=True)

    async def head(self, request: web.Request) -> web.StreamResponse:
        return await self._marker(request, body=False)

    async def _marker(self, request: web.Request, *, body: bool) -> web.StreamResponse:
        global _MP3
        if not body:
            return web.Response(status=HTTPStatus.OK, headers=MARKER_HEADERS)
        if _MP3 is None:
            coordinator = _coordinator(request.app["hass"])
            env = getattr(coordinator, "env", None)
            binary = await env.run(shutil.which, "ffmpeg") if env is not None else None
            if not binary:
                return self.json_message(
                    "В этом доме нет ffmpeg — маркер отдать нечем", HTTPStatus.SERVICE_UNAVAILABLE
                )
            _MP3 = await encode_mp3(binary, marker_pcm())
        return web.Response(status=HTTPStatus.OK, body=_MP3, headers=MARKER_HEADERS)


class MegaHomeMarkerSpecView(_MegaHomeView):
    """`/mega-home/marker.json` — описание шаблона: приложение берёт числа отсюда, а не держит копию."""

    url = f"{URL_PREFIX}/marker.json"
    name = "mega_home:marker_spec"

    async def get(self, request: web.Request) -> web.StreamResponse:  # noqa: ARG002 — запрос не нужен
        return web.json_response(marker_description(), headers={"Cache-Control": "no-store"})


async def encode_mp3(binary: str, pcm: bytes) -> bytes:
    """PCM → MP3 тем же кодировщиком, что и поток сессии: плееры читают MP3 все."""
    chunks: list[bytes] = []

    async def collect(chunk: bytes) -> None:
        chunks.append(chunk)

    encoder = await ffmpeg_encoder(binary, collect)
    await encoder.write(pcm)
    await encoder.stop()
    return b"".join(chunks)
