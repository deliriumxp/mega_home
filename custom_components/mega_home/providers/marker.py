"""Маркерный сигнал акустического выравнивания колонок — сигнал и раскладка по кругу.

Как это работает (протокол заказчика 2026-11-22): колонки щёлкают ПО КРУГУ — по одной с паузой в
секунду, круг повторяется. Жилец проходит по дому один раз, ничего не переключая: приложение пишет
микрофоном непрерывно, а каждый щелчок опознаёт по ВЫСОТЕ (своя у каждой колонки — это её место в
круге) и по ГРОМКОСТИ (громче всех слышно ту колонку, рядом с которой телефон). Разница фаз прихода
двух колонок и есть разница их задержек: часы дома и микрофонный тракт телефона в фазу не входят.

⚠ Числа раскладки — КОНТРАКТ дома и приложения (`frontend/src/app/pages/app-preview/core/sync-probe.ts`).
Описание отдаётся `/mega-home/marker.json`, чтобы у приложения не было своей копии чисел.
"""

from __future__ import annotations

import math
import struct
from typing import Any

from .music_flow import FLOW_RATE

# Пауза между щелчками круга, мс.
SPACING_MS = 1000
# Длина щелчка, мс.
CLICK_MS = 20
# Высоты по номеру колонки в круге, Гц: соседние различаются на 200 Гц — этого хватает, чтобы
# приложение не спутало колонки, а жильцу не резало слух.
PITCHES = (700, 900, 1100, 1300, 1500, 1700, 1900, 2100)
# Сколько колонок в круге максимум: больше в одном мультируме дома не бывает.
SLOTS_MAX = len(PITCHES)
# Амплитуда: тише половины шкалы — перегрузка срезала бы щелчок и смазала приход.
AMPLITUDE = 0.6

_CYCLES: dict[tuple[int, int], bytes] = {}


def marker_description(slots: int = 1) -> dict[str, Any]:
    """Описание раскладки для приложения: одно место правды о числах маркера."""
    return {
        "spacingMs": SPACING_MS,
        "clickMs": CLICK_MS,
        "rotateMs": max(1, slots) * SPACING_MS,
        "slots": max(1, slots),
        "pitches": list(PITCHES[: max(1, slots)]),
    }


def cycle_pcm(slot: int, slots: int, *, spacing_ms: int = SPACING_MS) -> bytes:
    """PCM 48 кГц моно одного КРУГА для этой колонки: тишина и её щелчок в её слоте.

    ⚠ Не бесконечная запись: поток крутит этот круг снова и снова (`music_measure.py`), поэтому
    кэшируется ровно один круг на колонку — сотни килобайт вместо мегабайтов файла.
    """
    if spacing_ms != SPACING_MS:
        return _build_cycle(slot, slots, spacing_ms)
    key = (slot, slots)
    cached = _CYCLES.get(key)
    if cached is None:
        cached = _build_cycle(slot, slots)
        _CYCLES[key] = cached
    return cached


def _build_cycle(slot: int, slots: int, spacing_ms: int = SPACING_MS) -> bytes:
    rate = FLOW_RATE
    length = int(CLICK_MS / 1000 * rate)
    pitch = PITCHES[slot]
    click = [
        math.sin(2 * math.pi * pitch * index / rate) * (0.5 - 0.5 * math.cos(2 * math.pi * index / (length - 1)))
        for index in range(length)
    ]
    total = int(slots * spacing_ms / 1000 * rate)
    samples = [0.0] * total
    at = int(slot * spacing_ms / 1000 * rate)
    for index, value in enumerate(click):
        if at + index < total:
            samples[at + index] = value
    scale = 32767 * AMPLITUDE
    return struct.pack(f"<{total}h", *(int(max(-1.0, min(1.0, value)) * scale) for value in samples))
