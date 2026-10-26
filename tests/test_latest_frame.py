"""Последний кадр камеры (`core/latest_frame.py`): JPEG из трубы `image2pipe` читателя конвейера."""

from __future__ import annotations

import asyncio

from mega_home.core.latest_frame import MAX_BYTES, LatestFrame, pump

JPEG_A = b"\xff\xd8" + b"A" * 50 + b"\xff\xd9"
JPEG_B = b"\xff\xd8" + b"B" * 70 + b"\xff\xd9"


def test_кадр_собирается_из_кусков_и_остаётся_последний() -> None:
    frame = LatestFrame()
    assert frame.feed(JPEG_A[:10]) is False and frame.jpeg() is None
    assert frame.feed(JPEG_A[10:] + JPEG_B[:5]) is True
    shot = frame.jpeg()
    assert shot is not None and shot[0] == JPEG_A
    frame.feed(JPEG_B[5:])
    shot = frame.jpeg()
    assert shot is not None and shot[0] == JPEG_B


def test_мусор_до_начала_кадра_пропускается() -> None:
    frame = LatestFrame()
    assert frame.feed(b"junk" + JPEG_A) is True
    shot = frame.jpeg()
    assert shot is not None and shot[0] == JPEG_A


def test_переполненный_буфер_сбрасывается() -> None:
    frame = LatestFrame()
    frame.feed(b"\xff\xd8" + b"x" * (MAX_BYTES + 10))
    assert frame.feed(JPEG_B) is True
    shot = frame.jpeg()
    assert shot is not None and shot[0] == JPEG_B


def test_труба_читается_до_конца() -> None:
    async def scenario() -> tuple[int, bytes | None]:
        reader = asyncio.StreamReader()
        reader.feed_data(JPEG_A + JPEG_B)
        reader.feed_eof()
        frame, seen = LatestFrame(), []
        await pump(reader, frame, lambda: seen.append(1))
        shot = frame.jpeg()
        return len(seen), shot[0] if shot else None

    count, last = asyncio.run(scenario())
    # `on_frame` — знак прогресса читателя («есть свежий кадр»), а не счётчик кадров.
    assert count >= 1 and last == JPEG_B
