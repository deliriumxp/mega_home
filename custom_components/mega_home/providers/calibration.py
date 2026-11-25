"""Подготовка зацикленного сигнала для выходов одной сессии."""

from __future__ import annotations

import asyncio
import secrets

from .marker import SLOTS_MAX, cycle_pcm, marker_description
from .music_ffmpeg import FLOW_RATE, MP3_FRAME_MS, mp3_args
from .music_flow import FlowSession

# Маркеры — свой формат: моно 192 кбит/с, кадр 576 байт (`marker.cycle_pcm` даёт моно PCM).
MARKER_CHANNELS = 1
MARKER_BITRATE_K = 192
MP3_FRAME_BYTES = 1152 * MARKER_BITRATE_K * 1000 // 8 // FLOW_RATE

# Стык круга приходится на границу MP3-кадра.
SPACING_MS = 42 * MP3_FRAME_MS


def layout(slots: int) -> dict:
    result = marker_description(slots)
    return {**result, "spacingMs": SPACING_MS, "rotateMs": slots * SPACING_MS}


async def _encode(binary: str, slot: int, slots: int) -> tuple[bytes, ...]:
    process = await asyncio.create_subprocess_exec(
        binary, *mp3_args(MARKER_CHANNELS, MARKER_BITRATE_K), stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        pcm = cycle_pcm(slot, slots, spacing_ms=SPACING_MS)
        # Берём средний круг, чтобы задержка кодека не резала щелчок на стыке.
        chunk, _ = await asyncio.wait_for(process.communicate(pcm * 3), timeout=8)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    count = slots * SPACING_MS // MP3_FRAME_MS
    size = count * MP3_FRAME_BYTES
    if process.returncode or len(chunk) < 2 * size or len(chunk) % MP3_FRAME_BYTES:
        raise RuntimeError("Не удалось закодировать круг маркеров")
    return tuple(chunk[i:i + MP3_FRAME_BYTES] for i in range(size, 2 * size, MP3_FRAME_BYTES))


async def prepare(binary: str, entities: list[str]) -> FlowSession:
    if not 2 <= len(entities) <= SLOTS_MAX:
        raise ValueError("Для замера нужно от двух до восьми систем")
    frames = []
    for slot in range(len(entities)):
        frames.append(await _encode(binary, slot, len(entities)))
    session = FlowSession(secrets.token_urlsafe(9))
    session.marker_frames = dict(zip(entities, frames, strict=True))
    return session
