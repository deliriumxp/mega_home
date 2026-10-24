"""Нагрузка контроллера (`core/host_load.py`): дельты процессора по `/proc`, группы процессов, ушедшие
дети, кольцо и `since`, отказ чтения `/proc`, служба `host` только менеджеру.

`/proc` — каталог теста с файлами той же формы (`proc(5)`): `stat` хоста, `meminfo`, `loadavg`,
`<pid>/stat`, `status`, `cmdline`. Такты — настоящий `SC_CLK_TCK` машины.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
from mega_home.core import connect, host_load, services
from mega_home.core.ops_base import OpError

CLK = os.sysconf("SC_CLK_TCK")
HA = 100


class FakeProc:
    """Каталог `/proc`: процессы — (родитель, старт, своё, детей) в секундах процессора."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(exist_ok=True)

    def host(self, busy: int, idle: int, cpus: int = 2) -> None:
        # user nice system idle iowait irq softirq steal: занято — всё, кроме idle и iowait.
        lines = [f"cpu  {busy} 0 0 {idle} 0 0 0 0 0 0"] + [f"cpu{n} 0 0 0 0 0 0 0 0 0 0" for n in range(cpus)]
        (self.root / "stat").write_text("\n".join(lines + ["intr 0", "btime 1"]) + "\n")
        (self.root / "meminfo").write_text("MemTotal:  4000000 kB\nMemFree:  100 kB\nMemAvailable: 3000000 kB\n")
        (self.root / "loadavg").write_text("1.40 0.90 0.50 2/300 4000\n")

    def proc(self, pid: int, ppid: int, start: int, own_s: float, kids_s: float = 0.0, rss_kb: int | None = None, argv: str = "x") -> None:
        folder = self.root / str(pid)
        folder.mkdir(exist_ok=True)
        own, kids = round(own_s * CLK), round(kids_s * CLK)
        # Имя со скобкой и пробелом — поля считаются от последней «)».
        rest = f"S {ppid} 0 0 0 0 0 0 0 0 0 {own} 0 {kids} 0 20 0 1 0 {start} 0 0"
        (folder / "stat").write_text(f"{pid} (ff mpeg) x) {rest}\n")
        (folder / "status").write_text("Name:\tx\n" + (f"VmRSS:\t{rss_kb} kB\n" if rss_kb is not None else ""))
        (folder / "cmdline").write_bytes(argv.replace(" ", "\0").encode())

    def gone(self, pid: int) -> None:
        for name in ("stat", "status", "cmdline"):
            (self.root / str(pid) / name).unlink()
        (self.root / str(pid)).rmdir()


@pytest.fixture
def proc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeProc:
    monkeypatch.setattr(host_load, "PROC", tmp_path / "proc")
    return FakeProc(tmp_path / "proc")


@pytest.fixture(autouse=True)
def _clean():
    yield
    host_load._owners.clear()  # noqa: SLF001
    host_load._inside.clear()  # noqa: SLF001
    services.unregister(host_load.SERVICE)


def _rows(sample: dict) -> dict[str, dict]:
    return {row["key"]: row for row in sample["procs"]}


def test_дельты_процессора_хоста_и_процессов(proc: FakeProc) -> None:
    """Минута, два ядра (120 с процессора): go2rtc с ffmpeg воспроизведения — 9 с (7,5 %); запись —
    2,4 с; детекция — ffmpeg 1,2 с + счёт numpy в процессе дома 1,2 с; дом — 12 с своих минус
    счёт детекции; `frames` — дождавшийся короткий ffmpeg (3,6 с) и живой новый (1,2 с)."""
    owners = {"ha": HA, "go2rtc": 200, "record:cam-a": 400, "motion:cam-a": 500}
    proc.host(busy=1000, idle=9000)
    proc.proc(HA, 1, 10, 100, 5)
    proc.proc(200, HA, 20, 50, rss_kb=60_000)
    proc.proc(300, 200, 30, 10, rss_kb=20_000)  # ffmpeg воспроизведения — ребёнок go2rtc
    proc.proc(400, HA, 40, 30, rss_kb=24_000)
    proc.proc(500, HA, 50, 20, rss_kb=18_000)
    load = host_load.HostLoad(FakeHost(), "ha")
    assert load.measure(owners, {}, moment=1000.0) is None  # точка отсчёта

    proc.host(busy=1000 + 3000, idle=9000 + 9000)
    proc.proc(HA, 1, 10, 112, 5 + 3.6, rss_kb=420_000)
    proc.proc(200, HA, 20, 56, rss_kb=60_000)
    proc.proc(300, 200, 30, 13, rss_kb=20_000)
    proc.proc(400, HA, 40, 32.4, rss_kb=24_000)
    proc.proc(500, HA, 50, 21.2, rss_kb=18_000)
    proc.proc(600, HA, 70, 1.2, rss_kb=9_000)  # кадр превью: родился после прошлого замера
    sample = load.measure(owners, {"motion:cam-a": 1.2}, moment=1060.0)

    assert sample is not None and load.cpus == 2
    assert sample["host"] == {"cpu": 25.0, "memUsed": 1_000_000 * 1024, "memTotal": 4_000_000 * 1024, "load1": 1.4}
    rows = _rows(sample)
    assert rows["go2rtc"] == {"key": "go2rtc", "cpu": 7.5, "rss": 80_000 * 1024}
    assert rows["record:cam-a"] == {"key": "record:cam-a", "cpu": 2.0, "rss": 24_000 * 1024}
    assert rows["motion:cam-a"] == {"key": "motion:cam-a", "cpu": 2.0, "rss": 18_000 * 1024}
    assert rows["ha"] == {"key": "ha", "cpu": 9.0, "rss": 420_000 * 1024}
    # Памяти у коротких нет: их строка — время, а не процесс.
    assert rows["frames"] == {"key": "frames", "cpu": 4.0}
    assert set(rows) == {"go2rtc", "record:cam-a", "motion:cam-a", "ha", "frames"}


def test_перезапуск_процесса_не_уходит_в_минус(proc: FakeProc) -> None:
    """Запись перезапустилась: прежний ffmpeg (30 с) ушёл, новый — на ДРУГОМ pid, а соседняя камера —
    на том же pid, но это другой процесс (старт другой). Счёт обоих — с нуля, не отрицательный;
    дом получил время ушедшего ребёнка в `cutime` целиком — в `frames` остаётся только хвост после
    прошлого замера (0,6 с), а не 30 с, уже показанные строкой записи."""
    proc.host(1000, 9000)
    proc.proc(HA, 1, 10, 100, 0)
    proc.proc(400, HA, 40, 30)
    proc.proc(450, HA, 45, 50)
    load = host_load.HostLoad(FakeHost(), "ha")
    load.measure({"ha": HA, "record:a": 400, "record:b": 450}, {}, moment=0.0)

    proc.host(4000, 18000)
    proc.gone(400)
    # Дом дождался обоих прежних: 30,6 с первого (0,6 — после прошлого замера) и 50 с второго.
    proc.proc(HA, 1, 10, 100, 30.6 + 50)
    proc.proc(401, HA, 80, 1.2)
    proc.proc(450, HA, 90, 2.4)  # pid тот же, процесс новый
    rows = _rows(load.measure({"ha": HA, "record:a": 401, "record:b": 450}, {}, moment=60.0))

    assert rows["record:a"]["cpu"] == 1.0 and rows["record:b"]["cpu"] == 2.0
    assert rows["frames"]["cpu"] == 0.5 and rows["ha"]["cpu"] == 0.0


def test_ключи_по_камерам_и_неизвестный_pid_без_строки(proc: FakeProc) -> None:
    """Видно, какая камера ест: строка на камеру. Ключ, чей процесс уже кончился, нулём не подписывается."""
    proc.host(1000, 9000)
    proc.proc(HA, 1, 10, 0)
    proc.proc(400, HA, 40, 0)
    proc.proc(410, HA, 41, 0)
    owners = {"ha": HA, "record:a": 400, "record:b": 410, "sip": 999}
    load = host_load.HostLoad(FakeHost(), "ha")
    load.measure(owners, {}, moment=0.0)
    proc.host(2000, 18000)
    proc.proc(400, HA, 40, 6)
    proc.proc(410, HA, 41, 1.2)
    rows = _rows(load.measure(owners, {}, moment=60.0))

    assert rows["record:a"]["cpu"] == 5.0 and rows["record:b"]["cpu"] == 1.0
    assert "sip" not in rows


def test_владельцы_отмечают_и_снимают_свой_pid() -> None:
    """Перезапуск успел отметить новый pid — запоздалое снятие старого его не стирает."""
    host_load.track("record:a", 400)
    host_load.track("record:a", 401)
    host_load.untrack("record:a", 400)
    assert host_load._owners == {"record:a": 401}  # noqa: SLF001
    host_load.untrack("record:a")
    host_load.track("go2rtc", None)  # усыновлённый не найден — строки нет
    assert host_load._owners == {}  # noqa: SLF001


def test_замер_снимает_счёт_детекции_и_добавляет_процесс_дома(proc: FakeProc) -> None:
    proc.host(1000, 9000)
    seen: list[tuple] = []
    load = host_load.HostLoad(FakeHost(), "ha")
    load.measure = lambda owners, inside: seen.append((owners, inside))  # type: ignore[method-assign]
    host_load.track("record:a", 400)
    host_load.spent("motion:a", 0.2)
    asyncio.run(load.tick(None))
    assert seen == [({"record:a": 400, "ha": os.getpid()}, {"motion:a": 0.2})]
    assert host_load._inside == {}  # noqa: SLF001


def test_усыновлённый_находится_по_командной_строке(proc: FakeProc) -> None:
    proc.proc(700, 1, 500, 0, argv="/usr/bin/asterisk -n -C /c/etc/asterisk.conf -rx core")
    proc.proc(701, 1, 100, 0, argv="/usr/bin/asterisk -f -n -C /c/etc/asterisk.conf")
    proc.proc(702, 1, 50, 0, argv="/usr/bin/go2rtc -c /tmp/other.yaml")
    assert host_load.find("/c/etc/asterisk.conf") == 701  # старейший, а не короткая команда
    assert host_load.find("mega_home_go2rtc_") is None


def test_кольцо_сутки_и_since() -> None:
    load = host_load.HostLoad(FakeHost(), "ha")
    for minute in range(host_load.KEEP + 60):
        load.samples.append({"at": minute * 60, "host": {}, "procs": []})
    assert len(load.samples) == host_load.KEEP == 1440 and load.samples[0]["at"] == 3600

    async def scenario() -> tuple:
        app = await _app(load)
        async with TestClient(TestServer(app)) as client:
            tail = await (await client.get(f"/load?since={(host_load.KEEP + 57) * 60}")).json()
            everything = await (await client.get("/load")).json()
            bad = (await client.get("/load?since=вчера")).status
        return tail, everything, bad

    tail, everything, bad = asyncio.run(scenario())
    # Только СТАРШЕ курсора, по возрастанию: курсор менеджера — последний сохранённый `at`.
    assert tail == {"step": 60, "cpus": load.cpus, "samples": [{"at": (host_load.KEEP + n) * 60, "host": {}, "procs": []} for n in (58, 59)]}
    assert len(everything["samples"]) == 1440 and bad == 400


async def _app(load: host_load.HostLoad):  # noqa: ANN202
    from aiohttp import web

    app = web.Application()
    app.router.add_get("/load", load._load)  # noqa: SLF001
    return app


def test_proc_недоступен_без_падения(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Контейнер без доступа к `/proc`: замера нет, исключения нет. Видны только файлы хоста —
    только хост; процессы без `status` — без памяти, но со временем."""
    monkeypatch.setattr(host_load, "PROC", tmp_path / "nope")
    load = host_load.HostLoad(FakeHost(), "ha")
    assert load.measure({"ha": HA}, {}, moment=0.0) is None
    assert load.measure({"ha": HA}, {}, moment=60.0) is None
    assert host_load.find("x") is None

    fake = FakeProc(tmp_path / "nope")
    fake.host(1000, 9000)
    load.measure({"ha": HA}, {}, moment=120.0)
    fake.host(2000, 18000)
    sample = load.measure({"ha": HA}, {"motion:a": 1.0}, moment=180.0)
    assert sample is not None and sample["procs"] == [] and sample["host"]["cpu"] == 10.0

    (tmp_path / "nope" / "stat").write_text("мусор\n")
    fake.proc(HA, 1, 10, 0)
    (tmp_path / "nope" / str(HA) / "status").unlink()
    load.measure({"ha": HA}, {}, moment=240.0)
    fake.proc(HA, 1, 10, 6)
    (tmp_path / "nope" / str(HA) / "status").unlink()
    sample = load.measure({"ha": HA}, {}, moment=300.0)
    assert sample is not None and "cpu" not in sample["host"]
    assert _rows(sample)["ha"] == {"key": "ha", "cpu": round(100 * 6 / (60 * load.cpus), 2)}


def test_служба_host_только_менеджеру() -> None:
    """Нагрузка — для инсталлятора: бандлу жильца закрыта целиком, и вызов, и сессия; путь проходит
    через общий `connect.service_path`. Останов снимает службу из реестра."""
    env = FakeHost()
    load = host_load.HostLoad(env, "ha")
    load.measure = lambda owners, inside: None  # type: ignore[method-assign]

    async def scenario() -> None:
        await load.async_start()
        try:
            assert services.resolve("host")
            assert [interval.total_seconds() for _action, interval in env.timers] == [60.0]
            for path in ("/load?since=0", "/%6Coad", "/x/../load"):
                with pytest.raises(OpError, match="только менеджер") as err:
                    connect.with_service({"host": "host", "path": path}, bundle=True)
                assert err.value.status == 403
                assert connect.refused({"host": "host"}, "path", path)
            sent = connect.with_service({"host": "host", "path": "/./load?since=5"})
            assert sent["path"] == "/load?since=5"
        finally:
            await load.async_stop()
        assert services.resolve("host") is None

    asyncio.run(scenario())
