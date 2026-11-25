"""Мелодия замера задержки: живой поток PCM, который дом делает сам (без ffmpeg-декодера).

Зачем: микрофон телефона слышит ноты на колонке, а дом помнит, когда отправил каждую колонке; разность —
статическая задержка ЭТОЙ системы в нашем потоке. Та же мелодия звучит при проверке синхронно на
выбранных колонках — второго звука нет.

Схема — как у Apple TV (Wireless Audio Sync): мелодия звучит, пока телефон её не услышит; замер кончает
приложение (`calibration_stop`), а не длина звука. Звук — по примеру настройки Sonos: мелодия, а не щелчки.

⚠ Поток БЕЗ КОНЦА, а не короткий файл: короткий файл колонка с буфером в секунды (Яндекс Станция)
проглатывала целиком и молчала — её гасили раньше, чем она доходила до звука.

⚠ Ноты РАЗНЫЕ, и телефон сравнивает ноту только с той же нотой: пропуск первых нот на старте замер не
сдвигает. Внутри круга фраза не повторяется: иначе сдвиг на повтор набрал бы голоса наравне с верной
задержкой. Задержка БОЛЬШЕ круга (Станция, ~10 с при круге 9,8 с) даёт двойника «задержка − круг» с
теми же голосами — телефон берёт из них наибольший (`estimateLatency`): до начала мелодии дом ничего
не слал, и сдвига больше верного не бывает. Условие на партитуру — 8 нот (`DONE_TONES`) укладываются
в круг: замер кончается раньше, чем голоса набрал бы сдвиг на круг вперёд.

⚠ Звук не синтезируется по ходу: круг считается ОДИН раз, поток лишь отдаёт готовые отсчёты. Описание
нот (`melody_description`) едет телефону — он строит те же образцы нот у себя.
"""

from __future__ import annotations

import asyncio
import math
from array import array
from functools import cache

from .music_ffmpeg import FLOW_CHANNELS, FLOW_RATE, FLOW_WIDTH, FlowProcess

# До-мажор одной октавы (без октавных повторов: у ноты и её октавы общие гармоники).
NOTES_HZ = (523.25, 587.33, 659.26, 698.46, 783.99, 880.0, 987.77)
# Гармоники «щипка»: атака даёт точный момент, высота — какую ноту узнать.
HARMONICS = (1.0, 0.6, 0.4, 0.3, 0.2, 0.15)
NOTE_MS = 200
ATTACK_MS = 2
DECAY_MS = 60
AMPLITUDE = 0.5
BEAT_MS = 350
# (нота, доли): 18 нот, 28 долей — круг 9,8 с, фраза внутри круга не повторяется.
SCORE = (
    (2, 2), (4, 1), (5, 1), (4, 2), (2, 1), (1, 1), (0, 2), (1, 1), (2, 1),
    (4, 2), (5, 2), (6, 1), (5, 1), (4, 2), (3, 1), (2, 1), (1, 2), (0, 4),
)
# Тишина до первой ноты. ⚠ 0: пропущенные на старте ноты замер не сдвигают (телефон сравнивает ноту
# только с той же нотой), а 2 с тишины, которые здесь стояли, жилец ждал на каждом замере.
LEAD_S = 0.0
_FRAME = FLOW_CHANNELS * FLOW_WIDTH
# Кусок отдачи — кадр MP3 (24 мс).
PIECE_BYTES = 1152 * _FRAME


def melody_description() -> list[dict]:
    """Ноты, как их строит дом: телефон синтезирует по ним свои образцы."""
    return [
        {"hz": hz, "ms": NOTE_MS, "harmonics": list(HARMONICS), "attackMs": ATTACK_MS, "decayMs": DECAY_MS}
        for hz in NOTES_HZ
    ]


def note_wave(hz: float) -> list[float]:
    """Нота: сумма гармоник ниже 6 кГц, линейная атака и экспоненциальный спад. ⚠ Пара — `noteWave` приложения."""
    count = FLOW_RATE * NOTE_MS // 1000
    attack = FLOW_RATE * ATTACK_MS / 1000
    norm = sum(HARMONICS)
    out = []
    for n in range(count):
        t = n / FLOW_RATE
        envelope = min(1.0, n / attack) * math.exp(-t * 1000 / DECAY_MS)
        value = sum(a * math.sin(2 * math.pi * hz * (h + 1) * t) for h, a in enumerate(HARMONICS) if hz * (h + 1) < 6000)
        out.append(AMPLITUDE * envelope * value / norm)
    return out


def score_onsets() -> list[tuple[int, int]]:
    """(отсчёт начала ноты от начала круга, номер ноты)."""
    onsets, at = [], 0
    for note, beats in SCORE:
        onsets.append((at * FLOW_RATE // 1000, note))
        at += beats * BEAT_MS
    return onsets


def loop_samples() -> int:
    return sum(beats for _, beats in SCORE) * BEAT_MS * FLOW_RATE // 1000


@cache
def loop_pcm() -> bytes:
    """Круг мелодии, PCM s16le стерео — считается один раз на процесс."""
    total = loop_samples()
    mix = [0.0] * total
    waves = {note: note_wave(NOTES_HZ[note]) for note in {note for note, _ in SCORE}}
    for start, note in score_onsets():
        for i, value in enumerate(waves[note]):
            mix[(start + i) % total] += value
    out = array("h")
    for value in mix:
        sample = int(max(-1.0, min(1.0, value)) * 32767)
        out.extend([sample] * FLOW_CHANNELS)
    return out.tobytes()


class MelodyProcess(FlowProcess):
    """Декодер-заменитель: тишина `LEAD_S`, дальше круг мелодии без конца; кусок с началом ноты
    помечает `mark` — номером ноты (иначе `None`).

    ⚠ Куски режутся ПО началу ноты: отметка относится к её первому отсчёту.
    """

    def __init__(self) -> None:
        self._loop = loop_pcm()
        self._lead = round(LEAD_S * FLOW_RATE) * _FRAME
        self._onsets = [(start * _FRAME, note) for start, note in score_onsets()]
        self._at = 0
        self.mark: int | None = None
        self.stopped = False

    def _next_onset(self, position: int) -> tuple[int, int]:
        """Ближайшее начало ноты не раньше `position` (байты от начала потока) и её номер."""
        if position <= self._lead:
            return self._lead, self._onsets[0][1]
        cycle, offset = divmod(position - self._lead, len(self._loop))
        for start, note in self._onsets:
            if start >= offset:
                return self._lead + cycle * len(self._loop) + start, note
        return self._lead + (cycle + 1) * len(self._loop), self._onsets[0][1]

    async def read(self, size: int) -> bytes:
        await asyncio.sleep(0)
        if self.stopped:
            return b""
        onset, note = self._next_onset(self._at)
        self.mark = note if onset == self._at else None
        following = self._next_onset(self._at + 1)[0]
        end = min(self._at + min(size, PIECE_BYTES), following)
        chunk = bytearray(end - self._at)
        # Звучащая часть — из круга (с переходом через его конец).
        start = max(self._at, self._lead)
        while start < end:
            offset = (start - self._lead) % len(self._loop)
            take = min(end - start, len(self._loop) - offset)
            chunk[start - self._at : start - self._at + take] = self._loop[offset : offset + take]
            start += take
        self._at = end
        return bytes(chunk)

    async def stop(self) -> None:
        self.stopped = True
