"""Нагрузка контроллера: процессор и память хоста и своих процессов дома — служба `host` реестра.

Решение заказчика 2026-10-24 (`docs/superpowers/specs/2026-10-24-home-load-design.md` менеджера):
инсталлятор видит, какая камера ест контроллер. Дом мерит раз в минуту и держит сутки В ПАМЯТИ
(флеш не трогаем, `docs/plan-device-presence.md`), менеджер забирает `GET /load?since=` по имени
службы и хранит ряд у себя. Только стандартная библиотека и `/proc`: `psutil` — пакет pip, а объекты
бывают офлайн (`docs/plan-core-without-ha.md`).

Какой процесс чей, говорит тот, кто его запускает (`track`): запись — `record:<камера>`, превью —
`preview:<камера>`, детекция — `motion:<камера>`, `go2rtc`, `sip`. Процесс самого дома — ключ хозяина (`own`, у адаптера HA — `ha`).

⚠ Строка — ГРУППА процесса: он сам и его неотмеченные потомки, живые и дождавшиеся (`cutime`): ffmpeg
воспроизведения архива — дети go2rtc, и без этого его нагрузки не было бы нигде. Дождавшиеся дети
процесса дома — `frames` (кадр под меткой, ffprobe, короткие команды): ядро прибавляет их время
родителю целиком в момент `wait`, поэтому из прироста `cutime` вычитается то, что ушедший ребёнок уже
показал живым в прошлых минутах (`_ticks`). В `frames` остаётся и хвост отмеченного ребёнка между
его последним замером и выходом (не больше минуты его работы на перезапуск), и дети, которых процесс
HA запускает не по нашей воле (интеграции HA, `shell_command`): их владельца дом не знает.
⚠ Процесс опознаётся парой (pid, старт): перезапуск или чужой процесс на том же pid — новый процесс
со счётом от нуля, а не отрицательная дельта. Нет pid у ключа — нет строки, нулём она не подписывается.
⚠ Доля — от ВСЕХ ядер, видимых в `/proc/stat` (как процессор монитора системы). В Docker `/proc/stat`
и `/proc/meminfo` — хоста целиком, в LXC с lxcfs — контейнера; процессы — всегда свои.
"""

from __future__ import annotations

import os
import time
from collections import deque
from datetime import timedelta
from pathlib import Path
from typing import Any

from aiohttp import web

from . import services
from .host import Host

SERVICE = "host"
STEP_S = 60
KEEP = 24 * 3600 // STEP_S
FRAMES = "frames"
PROC = Path("/proc")

# Ключ → pid: ставит запускающий процесс, снимает он же. Модульное, как реестр служб: владельцев
# много, а замер один.
_owners: dict[str, int] = {}
# Ключ → секунды процессора, потраченные ВНУТРИ процесса дома с прошлого замера (счёт кадра детекции).
_inside: dict[str, float] = {}


def track(key: str, pid: int | None) -> None:
    if pid:
        _owners[key] = pid


def untrack(key: str, pid: int | None = None) -> None:
    """Процесс кончился. С `pid` — только если ключ всё ещё его: перезапуск успел отметить новый."""
    if pid is None or _owners.get(key) == pid:
        _owners.pop(key, None)


def spent(key: str, seconds: float) -> None:
    _inside[key] = _inside.get(key, 0.0) + max(seconds, 0.0)


def is_process(pid: int) -> bool:
    """Верхний процесс, а не поток: у потока `Tgid` — pid процесса.

    ⚠ `/proc` перечисляет и потоки отдельными pid; их время и RSS повторяют процесс, и без
    этой проверки замер считал бы один ffmpeg десятком процессов. Нет `status` — считаем
    процессом: строку ВРЕМЕНИ терять нельзя, а память у неё всё равно не выдумается.
    """
    try:
        text = (PROC / str(pid) / "status").read_text()
    except OSError:
        return True
    for line in text.splitlines():
        if line.startswith("Tgid:"):
            return int(line.split()[1]) == pid
    return True


def _pids() -> list[int]:
    try:
        names = [name for name in os.listdir(PROC) if name.isdigit()]
    except OSError:
        return []
    return [pid for name in names if is_process(pid := int(name))]


def pids() -> list[int]:
    """Номера процессов в `/proc` — сторожам, которым мало собственного pid."""
    return _pids()


def _stat(pid: int) -> tuple[int, int, int, int]:
    """(родитель, старт, своё время, время дождавшихся детей) — такты `SC_CLK_TCK` (`proc(5)`).
    Имя процесса в скобках бывает с пробелами и скобками — поля считаются от ПОСЛЕДНЕЙ «)»."""
    text = (PROC / str(pid) / "stat").read_text()
    f = text[text.rindex(")") + 2 :].split()
    return int(f[1]), int(f[19]), int(f[11]) + int(f[12]), int(f[13]) + int(f[14])


def _table() -> dict[int, tuple[int, int, int, int]]:
    out: dict[int, tuple[int, int, int, int]] = {}
    for pid in _pids():
        try:
            out[pid] = _stat(pid)
        except (OSError, ValueError, IndexError):
            continue  # кончился между листингом и чтением, или не наш (hidepid)
    return out


def proc(pid: int) -> tuple[int, int] | None:
    """(родитель, старт в тактах `SC_CLK_TCK`); None — кончился, не наш или не читается."""
    try:
        ppid, start, _own, _kids = _stat(pid)
        return ppid, start
    except (OSError, ValueError, IndexError):
        return None


def argv(pid: int) -> list[str]:
    """Аргументы процесса; пусто — не прочли или зомби (у него командной строки уже нет)."""
    try:
        raw = (PROC / str(pid) / "cmdline").read_bytes().split(b"\0")
    except OSError:
        return []
    return [part.decode(errors="ignore") for part in raw if part]


def _is_ffmpeg(pid: int) -> bool:
    args = argv(pid)
    return bool(args) and Path(args[0]).name == "ffmpeg"


def find(marker: str) -> int | None:
    """Старейший процесс, у которого `marker` в командной строке: pid процесса прошлого запуска HA
    без handle — усыновлённого Asterisk (строка нагрузки) и снимаемого go2rtc (по имени своего
    конфига). Короткие команды тому же процессу (`asterisk -rx`) моложе его. Нет — None."""
    best: tuple[int, int] | None = None
    for pid in _pids():
        try:
            argv = (PROC / str(pid) / "cmdline").read_bytes().split(b"\0")
            start = _stat(pid)[1]
        except (OSError, ValueError, IndexError):
            continue
        if pid != os.getpid() and any(marker.encode() in arg for arg in argv) and (best is None or start < best[0]):
            best = (start, pid)
    return best[1] if best else None


def _status(pid: int) -> tuple[int | None, int | None]:
    """(`VmRSS` в байтах, потоков); None — не прочли или у процесса их нет (зомби)."""
    rss = threads = None
    try:
        for line in (PROC / str(pid) / "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1]) * 1024
            elif line.startswith("Threads:"):
                threads = int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return rss, threads


def _rss(pid: int) -> int | None:
    return _status(pid)[0]


def _cpu() -> tuple[tuple[int, int] | None, int]:
    """((занято, всего) тактов всех ядер, число ядер). Простой — `idle` и `iowait`."""
    try:
        lines = (PROC / "stat").read_text().splitlines()
        first = [int(v) for v in lines[0].split()[1:9]]
        cpus = sum(1 for line in lines if line.startswith("cpu") and line[3:4].isdigit())
        return (sum(first) - first[3] - first[4], sum(first)), cpus or os.cpu_count() or 1
    except (OSError, ValueError, IndexError):
        return None, os.cpu_count() or 1


def _memory() -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        info = {}
        for line in (PROC / "meminfo").read_text().splitlines():
            name, _, rest = line.partition(":")
            info[name] = int(rest.split()[0]) * 1024
        out = {"memUsed": info["MemTotal"] - info["MemAvailable"], "memTotal": info["MemTotal"]}
    except (OSError, ValueError, IndexError, KeyError):
        pass
    try:
        out["load1"] = float((PROC / "loadavg").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        pass
    return out


def _groups(table: dict[int, tuple[int, int, int, int]], tracked: dict[int, str]) -> dict[int, str]:
    """pid → ключ: свой у отмеченного, ключ ближайшего отмеченного предка у неотмеченных потомков."""
    out: dict[int, str | None] = {}
    for first in table:
        chain, pid = [], first
        while pid not in out:
            if pid in tracked or pid not in table:
                out[pid] = tracked.get(pid)
                break
            chain.append(pid)
            pid = table[pid][0]
        for member in chain:
            out[member] = out[pid]
    return {pid: key for pid, key in out.items() if key and pid in table}


class HostLoad:
    """Замер раз в минуту в исполнителе и служба `host` на петле: `GET /load?since=<unix-с>`."""

    def __init__(self, env: Host, own: str) -> None:
        self.env, self.own = env, own
        self.samples: deque[dict[str, Any]] = deque(maxlen=KEEP)
        self.cpus = os.cpu_count() or 1
        self._prev: tuple[float, tuple[int, int] | None, dict[int, tuple[int, int, int, int]]] | None = None
        self._runner: web.AppRunner | None = None
        self._timer: Any = None
        self._busy = False

    async def async_start(self) -> None:
        app = web.Application()
        app.router.add_get("/load", self._load)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        await web.TCPSite(self._runner, "127.0.0.1", 0).start()
        services.register(SERVICE, int(self._runner.addresses[0][1]), bundle_guard=_manager_only)
        self._timer = self.env.every(timedelta(seconds=STEP_S), self.tick)
        await self.tick(None)  # точка отсчёта: первая дельта — через минуту

    async def async_stop(self) -> None:
        services.unregister(SERVICE)
        if self._timer is not None:
            self._timer()
            self._timer = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def tick(self, _now: Any) -> None:
        if self._busy:
            return
        # Снимки — в цикле событий: владельцы и счёт детекции пишут туда же, исполнитель их не делит.
        owners = {**_owners, self.own: os.getpid()}
        inside = dict(_inside)
        _inside.clear()
        self._busy = True
        try:
            sample = await self.env.run(self.measure, owners, inside)
        finally:
            self._busy = False
        if sample is not None:
            self.samples.append(sample)

    def measure(self, owners: dict[str, int], inside: dict[str, float], moment: float | None = None) -> dict[str, Any] | None:
        """Замер к прошлому; None — точка отсчёта или читать нечего (контейнер без `/proc`)."""
        moment = time.monotonic() if moment is None else moment
        cpu, self.cpus = _cpu()
        table = _table()
        prev, self._prev = self._prev, (moment, cpu, table)
        if prev is None or moment <= prev[0]:
            return None
        host = _memory()
        if cpu and prev[1] and cpu[1] > prev[1][1]:
            host["cpu"] = round(100 * (cpu[0] - prev[1][0]) / (cpu[1] - prev[1][1]), 2)
        procs = self._procs(owners, inside, prev[2], table, (moment - prev[0]) * self.cpus) if table else []
        if not host and not procs:
            return None
        return {"at": int(time.time()), "host": host, "procs": procs}

    def _procs(self, owners: dict[str, int], inside: dict[str, float], before: dict, table: dict, span: float) -> list[dict[str, Any]]:
        clk = os.sysconf("SC_CLK_TCK")
        own_pid = owners[self.own]
        groups = _groups(table, {pid: key for key, pid in owners.items() if pid in table})
        ticks = self._ticks(groups, before, table, own_pid)
        # ⚠ Чужие дети процесса дома остаются в тиках `frames`, а не в строке `ha`: её счёт — сам HA.
        members: dict[str, list[int]] = {}
        for pid, key in groups.items():
            if key == self.own and pid != own_pid:
                continue
            members.setdefault(key, []).append(pid)
        stats: dict[str, dict[str, Any]] = {}
        for key, pids in members.items():
            sizes: list[int] = []
            threads = ffmpeg = 0
            for pid in pids:
                size, count_threads = _status(pid)
                if size is not None:
                    sizes.append(size)
                if count_threads is not None:
                    threads += count_threads
                if _is_ffmpeg(pid):
                    ffmpeg += 1
            stats[key] = {
                "count": len(pids),
                "ffmpeg": ffmpeg,
                **({"threads": threads} if threads else {}),
                **({"rss": sum(sizes)} if sizes else {}),
                **({"maxRss": max(sizes)} if sizes else {}),
            }
        seconds = {key: value / clk for key, value in ticks.items()}
        for key, value in inside.items():
            seconds[key] = seconds.get(key, 0.0) + value
            if self.own in seconds:
                seconds[self.own] -= value
        return [
            {
                "key": key,
                "cpu": round(100 * max(value, 0.0) / span, 2),
                **(stats.get(key) or {}),
            }
            for key, value in sorted(seconds.items())
        ]

    def _ticks(self, groups: dict[int, str], before: dict, table: dict, own_pid: int) -> dict[str, int]:
        """Такты за минуту по ключам. Дождавшиеся дети процесса дома — `frames`, а не `own`."""
        out: dict[str, int] = {}

        def add(key: str, value: int) -> None:
            out[key] = out.get(key, 0) + value

        for pid, key in groups.items():
            _ppid, start, own, kids = table[pid]
            old = before.get(pid)
            old_own, old_kids = (old[2], old[3]) if old and old[1] == start else (0, 0)
            if pid == own_pid:
                add(key, own - old_own)
                add(FRAMES, kids - old_kids)
            else:
                add(FRAMES if key == self.own else key, own + kids - old_own - old_kids)
        # Ушедший ребёнок: ядро отдало родителю его время ЦЕЛИКОМ, а прошлые минуты уже показали его живым.
        for pid, (ppid, start, own, kids) in before.items():
            if table.get(pid, (0, -1))[1] != start and ppid in groups:
                add(FRAMES if groups[ppid] == self.own else groups[ppid], -(own + kids))
        return {key: max(value, 0) for key, value in out.items()}

    async def _load(self, request: web.Request) -> web.Response:
        try:
            since = float(request.query.get("since") or 0)
        except ValueError:
            return web.json_response({"error": "since — unix-секунды"}, status=400)
        return web.json_response({"step": STEP_S, "cpus": self.cpus, "samples": [s for s in self.samples if s["at"] > since]})


def _manager_only(_what: str, _value: str) -> str | None:
    """Нагрузка — для инсталлятора: бандлу жильца служба закрыта целиком (путь, тело, кадр)."""
    return "нагрузку контроллера видит только менеджер"
