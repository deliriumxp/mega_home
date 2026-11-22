"""Маркер акустического выравнивания: числа шаблона и сам сигнал.

Числа маркера — контракт дома и приложения (`frontend/src/app/pages/app-preview/core/sync-probe.ts`):
если щелчки сдвинутся или сменят высоту, приложение перестанет опознавать шаблон и фазу не посчитает.
Поэтому здесь проверяется не «функция вернула байты», а то, что в этих байтах ровно шаблон.
"""

from __future__ import annotations

import asyncio
import math
import struct
from types import SimpleNamespace

import aiohttp
from aiohttp import web

from mega_home.core.const import URL_PREFIX
from mega_home.music_measure import MegaHomeMarkerSpecView, MegaHomeMarkerView

from mega_home.providers.marker import (
    CLICK_MS,
    PERIODS,
    PERIOD_MS,
    PITCHES,
    SILENCE_MS,
    SPACING_MS,
    marker_description,
    marker_pcm,
)

RATE = 48000


def samples(pcm: bytes) -> tuple[int, ...]:
    return struct.unpack(f"<{len(pcm) // 2}h", pcm)


def tone_level(values: tuple[int, ...] | list[int], freq: int) -> float:
    """Уровень тона в окне (один бин ДПФ) — им и опознаётся высота щелчка."""
    re = sum(value * math.cos(2 * math.pi * freq * index / RATE) for index, value in enumerate(values))
    im = sum(value * math.sin(2 * math.pi * freq * index / RATE) for index, value in enumerate(values))
    return math.hypot(re, im) / max(1, len(values))


def window(values: tuple[int, ...], at_ms: float, length_ms: float) -> tuple[int, ...]:
    start = int(at_ms / 1000 * RATE)
    return values[start : start + int(length_ms / 1000 * RATE)]


def level(values: tuple[int, ...]) -> float:
    return math.sqrt(sum(value * value for value in values) / max(1, len(values)))


def test_описание_отдаёт_те_же_числа_что_и_сигнал():
    described = marker_description()
    assert described["pitches"] == list(PITCHES)
    assert described["spacingMs"] == SPACING_MS
    assert described["silenceMs"] == SILENCE_MS
    assert described["clickMs"] == CLICK_MS
    assert described["periodMs"] == PERIOD_MS == (len(PITCHES) - 1) * SPACING_MS + SILENCE_MS
    assert described["periods"] == PERIODS
    # Длина записи — целое число сверхпериодов: приложению не приходится угадывать границу.
    assert len(marker_pcm()) == int(PERIODS * PERIOD_MS / 1000 * RATE) * 2


def test_щелчки_стоят_по_шаблону_и_звучат_своей_высотой():
    values = samples(marker_pcm())
    for period in range(PERIODS):
        base = period * PERIOD_MS
        for order, pitch in enumerate(PITCHES):
            at = base + order * SPACING_MS
            click = window(values, at, CLICK_MS)
            assert level(click) > 3000, f"щелчок {order} сверхпериода {period} не звучит"
            mine = tone_level(click, pitch)
            others = max(tone_level(click, other) for other in PITCHES if other != pitch)
            # Своя высота должна быть заметно сильнее чужой: иначе приложение не поймёт номер щелчка.
            assert mine > others * 2, f"высота щелчка {order} не опознаётся: {mine:.1f} против {others:.1f}"


def test_между_шаблонами_тишина():
    values = samples(marker_pcm())
    # Пауза начинается после последнего щелчка шаблона и идёт почти до конца сверхпериода.
    gap = window(values, PERIOD_MS - SILENCE_MS / 2, SILENCE_MS / 2)
    assert level(gap) < 100


# ── маршруты: их забирает ПЛЕЕР (Sonos, Эхо, Станция), у которого нет ни куки, ни ключа ──


class FakeEntries:
    def __init__(self, coordinator: object) -> None:
        self._coordinator = coordinator

    def async_loaded_entries(self, domain: str) -> list[object]:
        return [SimpleNamespace(runtime_data=self._coordinator)]


class FakeEnv:
    """`coordinator.env.run` дома: в тестах ffmpeg ищется в этой же машине."""

    async def run(self, fn, *args):  # noqa: ANN001, ANN002 — подпись дома
        return fn(*args)


def _app() -> web.Application:
    coordinator = SimpleNamespace(env=FakeEnv())
    app = web.Application()
    app["hass"] = SimpleNamespace(config_entries=FakeEntries(coordinator))
    for view in (MegaHomeMarkerView, MegaHomeMarkerSpecView):
        instance = view()
        app.router.add_route("GET", instance.url, instance.get)
        if hasattr(instance, "head"):
            app.router.add_route("HEAD", instance.url, instance.head)
    return app


def test_описание_маркера_отдаётся_приложению():
    async def run():
        runner = web.AppRunner(_app())
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}{URL_PREFIX}/marker.json") as response:
                assert response.status == 200
                described = await response.json()
        await runner.cleanup()
        return described

    described = asyncio.run(run())
    assert described["pitches"] == list(PITCHES)
    assert described["periodMs"] == PERIOD_MS


def test_маркер_отдаётся_плееру_готовым_mp3_и_кэшируется():
    async def run():
        app = _app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}{URL_PREFIX}/marker.mp3") as response:
                assert response.status == 200
                assert response.headers["Content-Type"] == "audio/mpeg"
                first = await response.read()
            async with session.get(f"http://127.0.0.1:{port}{URL_PREFIX}/marker.mp3") as response:
                second = await response.read()
        await runner.cleanup()
        return first, second

    first, second = asyncio.run(run())
    # MP3: кадр синхронизации (0xFF 0xEx/0xFx) либо тег ID3 — другого плееры не примут.
    assert first[:3] == b"ID3" or first[0] == 0xFF
    # 13 секунд при 192 кбит/с ≈ 312 КБ: проверяем порядок, а не байт в байт.
    assert 100_000 < len(first) < 900_000
    assert first == second  # второй запрос — из кэша, кодируем один раз
