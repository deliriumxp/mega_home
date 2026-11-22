"""Маркерный сигнал акустического выравнивания колонок — сам сигнал, без слоя HTTP.

⚠ Числа шаблона — КОНТРАКТ дома и приложения (`frontend/src/app/pages/app-preview/core/sync-probe.ts`):
щелчки разной высоты нужны, чтобы приложение понимало, какой именно щелчок услышало (без номера щелчка
фазу сдвигало бы место начала записи), а тишина после них задаёт сверхпериод, в пределах которого фаза
однозначна. Описание отдаётся `/mega-home/marker.json`, чтобы у приложения не было своей копии чисел.
"""

from __future__ import annotations

import math
import struct
from typing import Any

from .music_flow import FLOW_RATE  # ⚠ тот же провод, что у потока сессии: кодировщик ждёт 48 кГц моно

# Высоты щелчков шаблона, Гц.
PITCHES = (900, 1350, 1800, 2250)
# Между щелчками шаблона, мс.
SPACING_MS = 500
# Тишина после щелчков, мс: вместе со щелчками даёт сверхпериод 6.5 с.
SILENCE_MS = 5000
# Длина щелчка, мс.
CLICK_MS = 20
# Сверхпериод шаблона, мс.
PERIOD_MS = (len(PITCHES) - 1) * SPACING_MS + SILENCE_MS
# Прохождений в файле: плееру с большим заливом буфера первый шаблон достаётся обрезанным.
PERIODS = 2
# Амплитуда: тише половины шкалы — перегрузка срезала бы щелчок и смазала приход.
AMPLITUDE = 0.6

_PCM: bytes | None = None


def marker_description() -> dict[str, Any]:
    """Описание шаблона для приложения: одно место правды о числах маркера."""
    return {
        "pitches": list(PITCHES),
        "spacingMs": SPACING_MS,
        "silenceMs": SILENCE_MS,
        "clickMs": CLICK_MS,
        "periodMs": PERIOD_MS,
        "periods": PERIODS,
    }


def marker_pcm(periods: int = PERIODS) -> bytes:
    """PCM 48 кГц моно (s16le) — ровно тот сигнал, который услышит телефон."""
    global _PCM
    if periods != PERIODS:
        return _build_pcm(periods)
    if _PCM is None:
        _PCM = _build_pcm(PERIODS)
    return _PCM


def _build_pcm(periods: int) -> bytes:
    rate = FLOW_RATE
    length = int(CLICK_MS / 1000 * rate)
    clicks = [
        [
            math.sin(2 * math.pi * pitch * index / rate)
            * (0.5 - 0.5 * math.cos(2 * math.pi * index / (length - 1)))
            for index in range(length)
        ]
        for pitch in PITCHES
    ]
    total = int(periods * PERIOD_MS / 1000 * rate)
    samples = [0.0] * total
    for period in range(periods):
        for order, click in enumerate(clicks):
            at = int(period * PERIOD_MS / 1000 * rate) + int(order * SPACING_MS / 1000 * rate)
            for index, value in enumerate(click):
                if at + index < total:
                    samples[at + index] += value
    scale = 32767 * AMPLITUDE
    return struct.pack(f"<{total}h", *(int(max(-1.0, min(1.0, value)) * scale) for value in samples))
