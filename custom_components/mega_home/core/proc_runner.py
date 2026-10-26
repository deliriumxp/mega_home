"""Один цикл жизни процесса дома: запуск, хвост stderr, «поток встал», перезапуск, учёт нагрузки.

Зачем. До 0.6.0 этот цикл был скопирован трижды — у записи (`recorder.py`), превью
(`recorder_preview.py`) и детекции (`motion.py`), и каждая копия чинилась отдельно (сторож вставшего
потока, вычитывание stderr, `host_load`). Читатели конвейера камеры (`video_pipeline.py`) идут этим одним.

⚠ stderr вычитывается ВСЕГДА: труба с непрочитанными строками остановила бы ffmpeg на записи в лог.
⚠ «Встал» — по прогрессу, который знает хозяин процесса (готовый кусок, кадр детекции, кадр JPEG), а не
по тому, что процесс жив: ffmpeg на RTSP ждёт данных без срока, и вставшая камера держала бы его вечно.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Awaitable, Callable

from . import host_load
from .const import LOGGER
from .host import Host


class ProcessRunner:
    def __init__(self, env: Host, key: str, *, stall_s: float, restart_s: float = 10.0, scan_s: float = 2.0) -> None:
        self.env, self.key = env, key
        self.stall_s, self.restart_s, self.scan_s = stall_s, restart_s, scan_s
        self.why = ""
        self._reported = ""
        self._tail: deque[str] = deque(maxlen=8)

    async def run(
        self,
        spawn: Callable[[], Awaitable[asyncio.subprocess.Process]],
        tick: Callable[[], Awaitable[float]],
    ) -> None:
        """Вечно: процесс → его жизнь → причина конца → пауза → снова. Отмена снимает процесс."""
        while True:
            try:
                await self._once(await spawn(), tick)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — один отказ не роняет камеру насовсем
                self.why = str(err)
            if self.why != self._reported:
                self._reported = self.why
                LOGGER.warning("%s: %s", self.key, self.why)
            await asyncio.sleep(self.restart_s)

    async def _once(self, proc: asyncio.subprocess.Process, tick: Callable[[], Awaitable[float]]) -> None:
        self._tail.clear()
        host_load.track(self.key, proc.pid)
        drain = asyncio.ensure_future(self._drain(proc))
        started = time.monotonic()
        try:
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(proc.wait()), self.scan_s)
                    break
                except asyncio.TimeoutError:
                    pass
                last = max(await tick(), started)
                if time.monotonic() - last > self.stall_s:
                    self.why = f"нет данных дольше {self.stall_s:g} с — поток встал"
                    return
                self.why = self._reported = ""
            await asyncio.wait_for(drain, 1)
            self.why = f"ffmpeg завершился ({proc.returncode}): {' | '.join(self._tail) or 'без слов'}"
        finally:
            await self._stop(proc)
            drain.cancel()
            host_load.untrack(self.key, proc.pid)

    async def _drain(self, proc: asyncio.subprocess.Process) -> None:
        if proc.stderr is None:
            return
        async for line in proc.stderr:
            if text := line.decode(errors="ignore").strip():
                self._tail.append(text)

    @staticmethod
    async def _stop(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), 5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
