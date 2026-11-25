"""Тоны замера задержки: живой поток PCM, который дом делает сам (без ffmpeg-декодера).

Зачем: микрофон телефона слышит тоны на колонке, а дом помнит, когда отправил каждый в кодировщик;
разность — статическая задержка ЭТОЙ системы в нашем потоке. Тон — линейный свип с окном Ханна: он
узнаётся корреляцией даже в комнате с шумом и не зависит от тембра колонки.

Схема — как у Apple TV (Wireless Audio Sync): тоны звучат, пока телефон их не услышит, замер кончает
приложение (`calibration_stop`), а не длина звука.

⚠ Поток БЕЗ КОНЦА, а не короткий файл: короткий файл колонка с буфером в секунды (Яндекс Станция)
проглатывала целиком и молчала — её гасили раньше, чем она доходила до звука.

⚠ Интервалы между тонами НЕРАВНЫЕ: при ровном шаге пропущенный на старте тон (колонка глотает начало
потока) неотличим от сдвига на целый шаг. При неравных только верная задержка сводит ВСЕ услышанные
тоны к отправленным; телефону рисунок знать не нужно — дом присылает фактические времена.
"""

from __future__ import annotations

import asyncio
import math
import random
from array import array

from .music_ffmpeg import FLOW_CHANNELS, FLOW_RATE, FLOW_WIDTH, FlowProcess

CHIRP_FROM_HZ = 2000
CHIRP_TO_HZ = 6000
CHIRP_MS = 50
CHIRP_AMPLITUDE = 0.5
# Тишина до первого тона: колонка на первом старте потока глотает начало звука (буферизация, прогрев
# усилителя, плавный вход). Пропуск первых тонов замеру не вредит — интервалы неравные.
LEAD_S = 2.0
GAP_MIN_MS = 700
GAP_MAX_MS = 1300
# Кусок отдачи — кадр MP3 (24 мс): отметка тона попадает в начало куска с точностью до отсчёта.
PIECE_BYTES = 1152 * FLOW_CHANNELS * FLOW_WIDTH
_FRAME = FLOW_CHANNELS * FLOW_WIDTH


def chirp_description() -> dict:
    return {"fromHz": CHIRP_FROM_HZ, "toHz": CHIRP_TO_HZ, "ms": CHIRP_MS}


def _chirp() -> bytes:
    count = FLOW_RATE * CHIRP_MS // 1000
    duration = CHIRP_MS / 1000
    sweep = (CHIRP_TO_HZ - CHIRP_FROM_HZ) / duration
    out = array("h")
    for n in range(count):
        t = n / FLOW_RATE
        window = 0.5 - 0.5 * math.cos(2 * math.pi * n / (count - 1))
        value = int(32767 * CHIRP_AMPLITUDE * window * math.sin(2 * math.pi * (CHIRP_FROM_HZ * t + sweep * t * t / 2)))
        out.extend([value] * FLOW_CHANNELS)
    return out.tobytes()


class ChirpProcess(FlowProcess):
    """Декодер-заменитель: тишина и тоны без конца; кусок с началом тона помечает `mark_hit`.

    ⚠ Куски режутся ПО границе тона: отметка времени относится к его первому отсчёту, а не к
    произвольному месту куска.
    """

    def __init__(self, seed: int | None = None) -> None:
        self._click = _chirp()
        self._gaps = random.Random(seed)
        self._at = 0
        self._next = round(LEAD_S * FLOW_RATE) * _FRAME
        self._tone = -len(self._click)
        self.mark_hit = False
        self.stopped = False

    def _gap(self) -> int:
        return round(self._gaps.randint(GAP_MIN_MS, GAP_MAX_MS) * FLOW_RATE / 1000) * _FRAME

    async def read(self, size: int) -> bytes:
        await asyncio.sleep(0)
        if self.stopped:
            return b""
        self.mark_hit = self._at == self._next
        if self.mark_hit:
            self._tone = self._at
            self._next += self._gap()
        start = self._at
        end = min(start + min(size, PIECE_BYTES), self._next)
        chunk = bytearray(end - start)
        # Тон звучит дольше одного куска: дописываем его продолжение и в следующих.
        lo, hi = max(start, self._tone), min(end, self._tone + len(self._click))
        if lo < hi:
            chunk[lo - start : hi - start] = self._click[lo - self._tone : hi - self._tone]
        self._at = end
        return bytes(chunk)

    async def stop(self) -> None:
        self.stopped = True
