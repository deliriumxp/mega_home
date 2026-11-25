"""Элемент калибровки: PCM со щелчками, который дом делает сам (без ffmpeg-декодера).

Зачем: микрофон телефона слышит щелчки на колонке, а дом помнит, когда отправил каждый из них в
кодировщик; разность — статическая задержка ЭТОЙ системы в нашем потоке. Щелчок — линейный свип
с окном Ханна: он узнаётся корреляцией даже в комнате с шумом и не зависит от тембра колонки.
"""

from __future__ import annotations

import asyncio
import math
from array import array

from .music_ffmpeg import FLOW_CHANNELS, FLOW_RATE, FLOW_WIDTH, FlowProcess

CHIRP_FROM_HZ = 2000
CHIRP_TO_HZ = 6000
CHIRP_MS = 50
CHIRP_AMPLITUDE = 0.5
LEAD_S = 3.0
STEP_S = 1.0
CLICKS = 5
TAIL_S = 1.0
# Кусок отдачи — кадр MP3 (24 мс): отметка щелчка попадает в начало куска с точностью до отсчёта.
PIECE_BYTES = 1152 * FLOW_CHANNELS * FLOW_WIDTH
_FRAME = FLOW_CHANNELS * FLOW_WIDTH


def chirp_description() -> dict:
    return {"fromHz": CHIRP_FROM_HZ, "toHz": CHIRP_TO_HZ, "ms": CHIRP_MS}


def click_offsets() -> list[int]:
    """Смещения начала щелчков от начала PCM, байты."""
    return [round((LEAD_S + index * STEP_S) * FLOW_RATE) * _FRAME for index in range(CLICKS)]


def _chirp() -> array:
    count = FLOW_RATE * CHIRP_MS // 1000
    duration = CHIRP_MS / 1000
    sweep = (CHIRP_TO_HZ - CHIRP_FROM_HZ) / duration
    out = array("h")
    for n in range(count):
        t = n / FLOW_RATE
        window = 0.5 - 0.5 * math.cos(2 * math.pi * n / (count - 1))
        value = int(32767 * CHIRP_AMPLITUDE * window * math.sin(2 * math.pi * (CHIRP_FROM_HZ * t + sweep * t * t / 2)))
        out.extend([value] * FLOW_CHANNELS)
    return out


def calibration_pcm() -> tuple[bytes, list[int]]:
    """Весь элемент и смещения щелчков: 3 с тишины, 5 щелчков с шагом 1 с, 1 с тишины."""
    total = round((LEAD_S + CLICKS * STEP_S + TAIL_S) * FLOW_RATE) * _FRAME
    data = bytearray(total)
    click = _chirp().tobytes()
    offsets = click_offsets()
    for offset in offsets:
        data[offset : offset + len(click)] = click
    return bytes(data), offsets


class ChirpProcess(FlowProcess):
    """Декодер-заменитель: отдаёт PCM щелчков кусками; кусок с началом щелчка помечает `mark_hit`.

    ⚠ Куски режутся ПО границе щелчка: отметка времени относится к его первому отсчёту, а не к
    произвольному месту 64-килобайтного куска (±340 мс сломали бы замер).
    """

    def __init__(self) -> None:
        self._data, self._marks = calibration_pcm()
        self._at = 0
        self.mark_hit = False
        self.stopped = False

    async def read(self, size: int) -> bytes:
        await asyncio.sleep(0)
        self.mark_hit = self._at in self._marks
        end = min(len(self._data), self._at + min(size, PIECE_BYTES))
        for mark in self._marks:
            if self._at < mark < end:
                end = mark
        chunk = self._data[self._at : end]
        self._at = end
        return chunk

    async def stop(self) -> None:
        self.stopped = True
