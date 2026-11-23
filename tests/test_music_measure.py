"""Маркер акустического выравнивания: раскладка по кругу, сам сигнал и обе двери.

Числа раскладки — контракт дома и приложения (`frontend/src/app/pages/app-preview/core/sync-probe.ts`):
если щелчок съедет из своего слота или сменит высоту, приложение не поймёт, чья это колонка, и
выравнивание не посчитается. Поэтому здесь проверяется не «вернулись байты», а сама раскладка.
"""

from __future__ import annotations

import asyncio
import math
import struct
from types import SimpleNamespace

import aiohttp
import shutil
from aiohttp import web

from mega_home.core.const import URL_PREFIX
from mega_home.music_measure import MegaHomeMarkerSpecView, MegaHomeMarkerView
from mega_home.providers.music_flow import ffmpeg_encoder
from mega_home.providers.marker import (
    CLICK_MS,
    PITCHES,
    SLOTS_MAX,
    SPACING_MS,
    cycle_pcm,
    marker_description,
)

RATE = 48000


def samples(pcm: bytes) -> tuple[int, ...]:
    return struct.unpack(f"<{len(pcm) // 2}h", pcm)


def window(values: tuple[int, ...], at_ms: float, length_ms: float) -> tuple[int, ...]:
    start = int(at_ms / 1000 * RATE)
    return values[start : start + int(length_ms / 1000 * RATE)]


def level(values: tuple[int, ...]) -> float:
    return math.sqrt(sum(value * value for value in values) / max(1, len(values)))


def chirp_template(slot: int) -> tuple[float, ...]:
    """Плавающий эталон дома: sweep от своей высоты к удвоенной, окно Ханна."""
    length = int(CLICK_MS / 1000 * RATE)
    start = PITCHES[slot]
    sweep = (start * 2 - start) / (CLICK_MS / 1000)
    return tuple(
        math.sin(2 * math.pi * (start * index / RATE + 0.5 * sweep * (index / RATE) ** 2))
        * (0.5 - 0.5 * math.cos(2 * math.pi * index / (length - 1)))
        for index in range(length)
    )


def chirp_match(values: tuple[int, ...], slot: int) -> float:
    """Нормированная корреляция с шаблоном слота — именно её делает детектор приложения."""
    template = chirp_template(slot)
    own = math.sqrt(sum(value * value for value in values))
    ref = math.sqrt(sum(value * value for value in template))
    return abs(sum(value * ref for value, ref in zip(values, template))) / max(1e-9, own * ref)


def test_описание_отдаёт_раскладку_круга():
    described = marker_description(5)
    assert described["slots"] == 5
    assert described["spacingMs"] == SPACING_MS == 1000
    assert described["clickMs"] == CLICK_MS
    assert described["rotateMs"] == 5 * SPACING_MS
    assert described["pitches"] == list(PITCHES[:5])
    # Больше колонок, чем высот, в круге быть не может — число слотов ограничено.
    assert len(marker_description(SLOTS_MAX + 3)["pitches"]) == SLOTS_MAX


def test_каждая_колонка_щёлкает_в_своём_слоте_и_своей_высотой():
    slots = 5
    for slot in range(slots):
        values = samples(cycle_pcm(slot, slots))
        assert len(values) == int(slots * SPACING_MS / 1000 * RATE)
        for other in range(slots):
            click = window(values, other * SPACING_MS, CLICK_MS)
            if other == slot:
                assert level(click) > 3000, f"колонка {slot} не щёлкает в слоте {other}"
                mine = chirp_match(click, slot)
                others = max(chirp_match(click, other) for other in range(slots) if other != slot)
                # Свой sweep коррелирует эталоном почти в единицу, чужие — околонулевые.
                assert mine > 0.98, f"chirp колонки {slot} не опознаётся"
                assert others < 0.05, f"chirp колонки {slot} похож на чужой слот"
            else:
                assert level(click) < 100, f"колонка {slot} звучит в чужом слоте {other}"


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


async def _serve(app: web.Application) -> tuple[web.AppRunner, int]:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, runner.addresses[0][1]


def test_раскладка_отдаётся_приложению_по_числу_колонок():
    async def run():
        runner, port = await _serve(_app())
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}{URL_PREFIX}/marker.json?slots=3") as response:
                assert response.status == 200
                described = await response.json()
        await runner.cleanup()
        return described

    described = asyncio.run(run())
    assert described["slots"] == 3
    assert len(described["pitches"]) == 3


def test_чужая_колонка_и_слот_вне_круга_отвергаются():
    async def run():
        runner, port = await _serve(_app())
        answers = []
        async with aiohttp.ClientSession() as session:
            # HEAD: тело бесконечного потока тут не нужно, проверяем разбор адреса и отказ.
            for query in ("slot=5&slots=5", "slot=-1&slots=3", "slot=1&slots=99"):
                async with session.head(f"http://127.0.0.1:{port}{URL_PREFIX}/marker.mp3?{query}") as response:
                    answers.append(response.status)
        await runner.cleanup()
        return answers

    # Число колонок ограничено числом высот: «99» — это уже 8, а слот 1 из 8 законен.
    assert asyncio.run(run()) == [400, 400, 200]


def test_маршрут_колонки_отвечает_и_сигнал_кодируется_в_mp3():
    """Дверей ровно две: раскладка и поток. Живой поток проверяем HEAD + самим кодированием круга.

    ⚠ Тело потока здесь не читаем: он бесконечный, и проверка «клиент ушёл — обработчик закончился»
    тянет за собой жизненный цикл aiohttp вместо нашей логики. Адрес и кодировщик — это то, что
    действительно может сломаться; круг проверяется выше, а сам ход на объекте смотрит заказчик.
    """

    async def head() -> tuple[int, str]:
        runner, port = await _serve(_app())
        try:
            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{port}{URL_PREFIX}/marker.mp3?slot=2&slots=5"
                async with session.head(url) as response:
                    return response.status, response.headers.get("Content-Type", "")
        finally:
            await asyncio.wait_for(runner.cleanup(), 10)

    status, content_type = asyncio.run(head())
    assert status == 200
    assert content_type == "audio/mpeg"

    chunks: list[bytes] = []

    async def collect(chunk: bytes) -> None:
        chunks.append(chunk)

    async def encode() -> bytes:
        encoder = await ffmpeg_encoder(shutil.which("ffmpeg") or "ffmpeg", collect)
        await encoder.write(cycle_pcm(2, 5))
        await encoder.stop()
        return b"".join(chunks)

    mp3 = asyncio.run(encode())
    # MP3: кадр синхронизации либо тег ID3 — другого плееры не примут; длина круга 5 с в mp3 > 20 КБ.
    assert mp3[:3] == b"ID3" or mp3[0] == 0xFF
    assert len(mp3) > 20_000
