"""Фейд на стыке элементов потока сессии: PCM s16le, кадры по `FLOW_CHANNELS` каналов."""

from __future__ import annotations

import sys
from array import array

from .music_ffmpeg import FLOW_CHANNELS, FLOW_WIDTH

FRAME_BYTES = FLOW_CHANNELS * FLOW_WIDTH


class _Fader:
    """Фейд на стыке элементов: хвост придерживается и микшуется с головкой следующего.

    ⚠ Хвост держим ВСЕГДА (в размере окна фейда): к моменту стыка его иначе уже не вернуть — звук
    ушёл плееру. Цена — задержка первого звука сессии на окно фейда (сотни миллисекунд).

    Фейд делается и когда элемент сменили на ходу (другая станция, «дальше»): это тот же стык.
    """

    def __init__(self, fade: int) -> None:
        self._fade = max(0, fade)
        self._held = b""

    def feed(self, chunk: bytes) -> bytes:
        """Отдать всё, кроме хвоста окна: он ждёт стыка."""
        if not self._fade:
            return chunk
        data = self._held + chunk
        if len(data) <= self._fade:
            self._held = data
            return b""
        self._held = data[-self._fade :]
        return data[: -self._fade]

    def join(self, head: bytes) -> bytes:
        """Смешать придержанный хвост с головкой нового элемента: микс и остатки обоих кусков."""
        held, self._held = self._held, b""
        if not held:
            return head
        # По границе кадра (все каналы отсчёта): иначе стерео поменяло бы каналы местами.
        border = min(len(held), len(head)) // FRAME_BYTES * FRAME_BYTES
        rest = held[border:] if len(held) > len(head) else head[border:]
        return mix_pcm(held[:border], head[:border]) + rest

    def flush(self) -> bytes:
        """Поток кончился: придержанное дописываем как есть."""
        held, self._held = self._held, b""
        return held


def mix_pcm(tail: bytes, head: bytes) -> bytes:
    """Кроссфейд кусков PCM s16le: хвост гаснет, головка нарастает (линейно).

    ⚠ `array('h')` читает РОДНОЙ порядок байт: PCM у нас little-endian, и на x86/ARM (где живут
    дома) это одно и то же. Дом на big-endian — фейда не делаем: отдаём хвост как есть (шум хуже
    стыка). Длина микса — по короткому куску, байты не теряются.
    """
    count = min(len(tail), len(head)) // 2
    if not count or sys.byteorder != "little":  # pragma: no cover — второе недостижимо на парке
        return tail[: count * 2]
    out, incoming = array("h"), array("h")
    out.frombytes(tail[: count * 2])
    incoming.frombytes(head[: count * 2])
    for index in range(count):
        out[index] = (out[index] * (count - index) + incoming[index] * index) // count
    return out.tobytes()
