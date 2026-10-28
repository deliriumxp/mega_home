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


def test_отказ_подряд_растягивает_паузу_и_журнал_не_засыпает(tmp_path: Path, caplog) -> None:  # noqa: ANN001
    """.90.250 2026-10-27: основной поток панели Akuvox перестал отдавать видео, читатель записи падал каждые ~15 с —
    каждый раз новая RTSP-сессия к панели с малым числом сессий и новая строка журнала (адреса `0x…` в тексте
    ffmpeg разные): 158 строк за полчаса. Подряд идущие отказы растягивают паузу до предела, одна причина — одна строка."""

    async def scenario() -> tuple[list[float], int]:
        runner = ProcessRunner(FakeHost(tmp_path), "cam:x:main", stall_s=30, restart_s=0.05, scan_s=0.05, max_restart_s=0.4)
        started: list[asyncio.subprocess.Process] = []
        stamps: list[float] = []
        spawn = _spawn("import sys, random; sys.stderr.write(f'[segment @ 0x{random.getrandbits(40):x}] dimensions not set\\n'); sys.exit(234)", started)

        async def counted() -> asyncio.subprocess.Process:
            stamps.append(time.monotonic())
            return await spawn()

        async def tick() -> float:
            return time.monotonic()

        task = asyncio.ensure_future(runner.run(counted, tick))
        await asyncio.sleep(2.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return [b - a for a, b in zip(stamps, stamps[1:])], len(started)

    gaps, _count = asyncio.run(scenario())
    assert gaps[0] < 0.3 and max(gaps) >= 0.35  # пауза росла
    assert max(gaps) < 0.8  # и упёрлась в предел
    warned = [r for r in caplog.records if "dimensions not set" in r.getMessage()]
    assert len(warned) == 1


def test_пауза_сбрасывается_по_прогрессу_а_не_по_времени_жизни(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    """Ревью 2026-10-27: «здоров» считался по времени жизни, а процесс, снятый по «встал», всегда живёт дольше порога —
    пауза сбрасывалась, и камера без данных получала новое подключение каждую минуту. Здоров — только с прогрессом."""
    from mega_home.core import proc_runner

    monkeypatch.setattr(proc_runner, "HEALTHY_S", 0.1)

    async def scenario() -> list[float]:
        runner = ProcessRunner(FakeHost(tmp_path), "cam:x:main", stall_s=0.15, restart_s=0.05, scan_s=0.03, max_restart_s=0.4)
        started: list[asyncio.subprocess.Process] = []
        stamps: list[float] = []
        spawn = _spawn("import time; time.sleep(30)", started)

        async def counted() -> asyncio.subprocess.Process:
            stamps.append(time.monotonic())
            return await spawn()

        async def tick() -> float:
            return 0.0  # данных нет

        task = asyncio.ensure_future(runner.run(counted, tick))
        await asyncio.sleep(2.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return [b - a for a, b in zip(stamps, stamps[1:])]

    gaps = asyncio.run(scenario())
    # Жизнь ~0,15 с + пауза: пауза растёт 0,1 → 0,2 → 0,4, а не стоит на 0,05.
    assert gaps[-1] - gaps[0] >= 0.2


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


def test_источник_отказал_до_срока_пауза_его_а_не_удвоение(monkeypatch) -> None:
    """`Refused` — камера сама сказала «не сейчас»: повтор через её срок, а не через 10–120 с (каждая попытка
    входа продлевала бы блокировку)."""
    from mega_home.core.proc_runner import Refused

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) >= 2:
            raise asyncio.CancelledError

    async def spawn():
        raise Refused("камера заблокировала вход", 1800)

    async def tick() -> float:
        return 0.0

    runner = ProcessRunner(FakeHost(), "cam", stall_s=60)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    try:
        asyncio.run(runner.run(spawn, tick))
    except asyncio.CancelledError:
        pass
    assert sleeps == [1800, 1800] and runner.why == "камера заблокировала вход"


def test_нет_видео_в_потоке_причина_словами() -> None:
    started: list[asyncio.subprocess.Process] = []
    runner = ProcessRunner(FakeHost(), "rec", stall_s=60)
    code = "import sys; sys.stderr.write('[segment @ 0x1] dimensions not set\\n'); sys.exit(234)"

    async def once() -> None:
        await runner._once(await _spawn(code, started)(), _tick_now)  # noqa: SLF001

    asyncio.run(once())
    assert runner.why.startswith("в потоке нет видео")


async def _tick_now() -> float:
    return time.monotonic()


def _waiters() -> int:
    """Висящие задачи ожидания выхода процесса (`Process.wait`)."""
    return sum(1 for t in asyncio.all_tasks() if "Process.wait" in repr(t.get_coro()))


def test_долгий_процесс_не_копит_задачи_ожидания(tmp_path: Path) -> None:
    """Объект .90.250 (2026-10-28): цикл раз в 2 с заводил новую `proc.wait()` под `shield`, и за ночь висело
    11 тысяч задач — HA упёрся в память контейнера. Живой процесс — одна задача ожидания на всю его жизнь."""

    async def scenario() -> tuple[int, int]:
        runner = ProcessRunner(FakeHost(tmp_path), "cam:x:main", stall_s=30, restart_s=5, scan_s=0.01)
        started: list[asyncio.subprocess.Process] = []

        async def tick() -> float:
            return time.monotonic()

        task = asyncio.ensure_future(runner.run(_spawn("import time; time.sleep(30)", started), tick))
        await asyncio.sleep(0.6)  # десятки проходов цикла
        alive = _waiters()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0.05)
        return alive, _waiters()

    alive, after = asyncio.run(scenario())
    assert alive <= 1
    assert after == 0


def test_замок_нет_shield_вокруг_proc_wait() -> None:
    """Приём, который течёт, — запрещён во всём коде дома: ожидание выхода процесса только через `ExitWatch`."""
    root = Path(__file__).resolve().parents[1] / "custom_components" / "mega_home"
    bad = [str(p.relative_to(root)) for p in root.rglob("*.py") if "asyncio.shield(proc.wait())" in p.read_text(encoding="utf-8")]
    assert bad == []
