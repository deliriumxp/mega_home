"""Последний кадр камеры — из памяти, а не ffmpeg на каждый запрос.

Зачем. Кадр плитки, кадр к событию и кадр редактора маски шли через `frame.jpeg` go2rtc: отдельный
ffmpeg на каждый запрос каждого зрителя (плитка — раз в 30 с), а у потока, который никто не держит, —
ещё и подключение к камере. Конвейер камеры её и так декодирует: читатель отдаёт JPEG раз в 2 с в трубу
(`video_reader.py`, `image2pipe`), здесь — последний целый кадр.

⚠ Граница кадра — маркеры JPEG `FF D8` … `FF D9`: кодировщик `mjpeg` ffmpeg встроенных миниатюр (второго
`FF D9` внутри кадра) не пишет.
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable

SOI, EOI = b"\xff\xd8", b"\xff\xd9"
# Кадр 640 px — десятки КБ; буфер больше — значит граница потерялась, ждём следующего начала.
MAX_BYTES = 4 * 1024**2


class LatestFrame:
    def __init__(self) -> None:
        self._buffer = bytearray()
        self._last: tuple[bytes, float] | None = None

    def feed(self, chunk: bytes) -> bool:
        """Дописать байты трубы; True — собран хотя бы один новый кадр."""
        self._buffer += chunk
        found = False
        while True:
            start = self._buffer.find(SOI)
            if start < 0:
                self._buffer.clear()
                return found
            del self._buffer[:start]
            end = self._buffer.find(EOI, 2)
            if end < 0:
                if len(self._buffer) > MAX_BYTES:
                    self._buffer.clear()
                return found
            self._last = (bytes(self._buffer[: end + 2]), time.time())
            del self._buffer[: end + 2]
            found = True

    def jpeg(self) -> tuple[bytes, float] | None:
        """Последний кадр и его время (`time.time()`); None — кадра ещё не было."""
        return self._last


async def pump(reader: asyncio.StreamReader, frame: LatestFrame, on_frame: Callable[[], None]) -> None:
    """Читать трубу до конца: непрочитанная труба остановила бы весь читатель (запись тоже)."""
    while chunk := await reader.read(65536):
        if frame.feed(chunk):
            on_frame()
