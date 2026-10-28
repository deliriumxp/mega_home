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
import re
import time
from collections import deque
from typing import Awaitable, Callable

from . import host_load
from .const import LOGGER
from .host import Host


# Процесс прожил столько с прогрессом — отказ после этого не «подряд», пауза снова короткая.
HEALTHY_S = 60.0
# Адреса структур в тексте ffmpeg (`[segment @ 0x7cdf…]`) меняются от запуска к запуску — причина та же.
_ADDRESS = re.compile(r"0x[0-9a-f]+")


class ExitWatch:
    """Ожидание выхода процесса — ОДНОЙ задачей на его жизнь, с опросом по сроку.

    ⚠ Не `wait_for(shield(proc.wait()), …)` в цикле: по сроку отменяется только `shield`, а внутренняя задача
    `proc.wait()` висит, пока процесс жив. Читатель камеры живёт часами, цикл ходит раз в 2 с — объект .90.250
    (2026-10-28) копил 11 тысяч задач `Process.wait` (по 145 в минуту на 5 ffmpeg), HA упёрся в память контейнера
    и встал. Замок — `tests/test_proc_runner.py`.
    """

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self._task: asyncio.Future[int] = asyncio.ensure_future(proc.wait())

    async def within(self, seconds: float) -> bool:
        """Процесс вышел за `seconds`."""
        done, _pending = await asyncio.wait({self._task}, timeout=seconds)
        return bool(done)

    def close(self) -> None:
        """Хозяин ушёл раньше выхода процесса: задача ожидания снимается вместе с ним."""
        if not self._task.done():
            self._task.cancel()


class Refused(Exception):
    """Запуск бессмыслен до срока: источник сам сказал «не сейчас» (камера заблокировала вход). Пауза — его, а не
    удвоение: каждая попытка входа продлевала бы блокировку."""

    def __init__(self, why: str, retry_s: float) -> None:
        super().__init__(why)
        self.retry_s = retry_s


class ProcessRunner:
    """⚠ Отказ подряд растягивает паузу вдвое до `max_restart_s`: камера или панель с малым числом RTSP-сессий
    (вызывная панель, .90.250 2026-10-27) получала новое подключение каждые ~15 с, и журнал — строку на каждый перезапуск."""

    def __init__(
        self, env: Host, key: str, *, stall_s: float, restart_s: float = 10.0, scan_s: float = 2.0, max_restart_s: float = 120.0,
    ) -> None:
        self.env, self.key = env, key
        self.stall_s, self.restart_s, self.scan_s, self.max_restart_s = stall_s, restart_s, scan_s, max_restart_s
        self.why = ""
        self._reported = ""
        self._tail: deque[str] = deque(maxlen=8)
        # Сколько секунд шёл прогресс у текущего процесса (по `tick`).
        self._progressed = 0.0

    async def run(
        self,
        spawn: Callable[[], Awaitable[asyncio.subprocess.Process]],
        tick: Callable[[], Awaitable[float]],
    ) -> None:
        """Вечно: процесс → его жизнь → причина конца → пауза → снова. Отмена снимает процесс."""
        pause = self.restart_s
        while True:
            self._progressed = 0.0
            wait: float | None = None
            try:
                await self._once(await spawn(), tick)
            except asyncio.CancelledError:
                raise
            except Refused as err:
                self.why, wait = str(err), err.retry_s
            except Exception as err:  # noqa: BLE001 — один отказ не роняет камеру насовсем
                self.why = str(err)
            if _ADDRESS.sub("0x", self.why) != _ADDRESS.sub("0x", self._reported):
                self._reported = self.why
                LOGGER.warning("%s: %s", self.key, self.why)
            # ⚠ «Здоров» — по ПРОГРЕССУ, не по времени жизни: снятый по «встал» живёт дольше порога всегда, и пауза
            # сбрасывалась — камера без данных получала новое подключение каждую минуту (ревью 2026-10-27).
            pause = self.restart_s if self._progressed >= HEALTHY_S else min(pause * 2, self.max_restart_s)
            await asyncio.sleep(wait if wait is not None else pause)

    async def _once(self, proc: asyncio.subprocess.Process, tick: Callable[[], Awaitable[float]]) -> None:
        self._tail.clear()
        host_load.track(self.key, proc.pid)
        drain = asyncio.ensure_future(self._drain(proc))
        exited = ExitWatch(proc)
        started = time.monotonic()
        try:
            while True:
                if await exited.within(self.scan_s):
                    break
                last = max(await tick(), started)
                self._progressed = max(self._progressed, last - started)
                if time.monotonic() - last > self.stall_s:
                    self.why = f"нет данных дольше {self.stall_s:g} с — поток встал"
                    return
                self.why = self._reported = ""
            await asyncio.wait_for(drain, 1)
            tail = " | ".join(self._tail) or "без слов"
            # Копия без размера кадра — в потоке не пришло ни одного кадра видео (вызывная панель .90.250 2026-10-28: основной
            # поток без пакетов, дополнительный живой). Инсталлятору — что делать, а не текст ffmpeg.
            hint = "в потоке нет видео — камера не отдаёт кадры (перезагрузите её или запишите другой поток): " if "dimensions not set" in tail else ""
            self.why = f"{hint}ffmpeg завершился ({proc.returncode}): {tail}"
        finally:
            exited.close()
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
            try:
                # ⚠ Со сроком: ffmpeg в состоянии D (умирающий USB-диск, зависший NAS) не выходит и по SIGKILL — ждать
                # его без срока значило заморозить камеру и остановку HA (ревью 2026-10-27).
                await asyncio.wait_for(proc.wait(), 5)
            except asyncio.TimeoutError:
                LOGGER.warning("Процесс %s не снялся и по SIGKILL — брошен", proc.pid)
