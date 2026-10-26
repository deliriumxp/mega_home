"""Один цикл жизни процесса (`core/proc_runner.py`): запуск, хвост stderr, «поток встал», перезапуск.

Процессы настоящие — интерпретатор этого же Python: проверяется то, что ломалось бы на объекте
(процесс не снят, учёт нагрузки не снят, причина без слов), а не подделка.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

from fake_host import FakeHost

from mega_home.core import host_load
from mega_home.core.proc_runner import ProcessRunner


def _spawn(code: str, started: list[asyncio.subprocess.Process]):
    async def spawn() -> asyncio.subprocess.Process:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", code, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        started.append(proc)
        return proc

    return spawn


def test_упавший_процесс_перезапускается_и_причина_со_словами(tmp_path: Path) -> None:
    async def scenario() -> tuple[int, str]:
        runner = ProcessRunner(FakeHost(tmp_path), "cam:x:main", stall_s=30, restart_s=0.05, scan_s=0.05)
        started: list[asyncio.subprocess.Process] = []

        async def tick() -> float:
            return time.monotonic()

        task = asyncio.ensure_future(runner.run(_spawn("import sys; sys.stderr.write('boom\\n'); sys.exit(3)", started), tick))
        await asyncio.sleep(0.8)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return len(started), runner.why

    count, why = asyncio.run(scenario())
    assert count >= 2
    assert "3" in why and "boom" in why


def test_вставший_процесс_снимается_по_сроку(tmp_path: Path) -> None:
    async def scenario() -> tuple[int | None, str]:
        runner = ProcessRunner(FakeHost(tmp_path), "cam:x:main", stall_s=0.2, restart_s=10, scan_s=0.05)
        started: list[asyncio.subprocess.Process] = []
        since = time.monotonic()

        async def tick() -> float:
            return since

        task = asyncio.ensure_future(runner.run(_spawn("import time; time.sleep(60)", started), tick))
        await asyncio.sleep(0.8)
        code = started[0].returncode
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return code, runner.why

    code, why = asyncio.run(scenario())
    assert code is not None
    assert "встал" in why


def test_отмена_снимает_процесс_и_учёт_нагрузки(tmp_path: Path) -> None:
    async def scenario() -> tuple[int | None, int | None, bool]:
        runner = ProcessRunner(FakeHost(tmp_path), "cam:y:sub", stall_s=30, restart_s=10, scan_s=0.05)
        started: list[asyncio.subprocess.Process] = []

        async def tick() -> float:
            return time.monotonic()

        task = asyncio.ensure_future(runner.run(_spawn("import time; time.sleep(60)", started), tick))
        await asyncio.sleep(0.3)
        tracked = host_load._owners.get("cam:y:sub")  # noqa: SLF001
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return tracked, started[0].returncode, "cam:y:sub" in host_load._owners  # noqa: SLF001

    tracked, code, still = asyncio.run(scenario())
    assert tracked is not None
    assert code is not None
    assert still is False
